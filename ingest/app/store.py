"""
Schema management + high-throughput writer for line-protocol points.

Design
------
- One TimescaleDB hypertable per measurement (schema configured by `tsdb_schema`).
- Fixed columns: `time TIMESTAMPTZ`, `tag_hash BIGINT`.
- Dynamic columns: `tag_<name> TEXT`, `field_<name> <inferred-type>`.
- Unique constraint on `(time, tag_hash)` so re-sending the same line-protocol
  point performs an UPSERT — this is how line-protocol "updates" are expressed.

Write path
----------
1. Group points by measurement.
2. Ensure hypertable + all columns exist (cached in-process).
3. Compute the union of columns for the batch.
4. Stream rows into a session-scoped temp table via asyncpg binary COPY.
5. `INSERT ... SELECT ... ON CONFLICT (time, tag_hash) DO UPDATE` into the hypertable.

This combination — COPY + ON CONFLICT — is the fastest practical path for
high-cardinality time-series upserts on PostgreSQL/TimescaleDB.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass

import asyncpg

from .config import settings
from .lineproto import Point

log = logging.getLogger(__name__)

_IDENT_OK = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    """Sanitize a free-form identifier to a safe Postgres identifier.

    Anything that wouldn't survive ``CREATE TABLE`` cleanly is replaced with
    ``_``; the result is also lowercased and bounded to 63 chars.
    """
    s = re.sub(r"[^A-Za-z0-9_]", "_", name).lower()
    if not s or not s[0].isalpha() and s[0] != "_":
        s = "_" + s
    return s[:63]


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _measurement_table(name: str) -> str:
    return _safe_ident(name)


def _tag_col(name: str) -> str:
    return "t_" + _safe_ident(name)


def _field_col(name: str) -> str:
    return "f_" + _safe_ident(name)


def _infer_field_type(value: object) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    if isinstance(value, str):
        return "TEXT"
    return "TEXT"


def _hash_tags(tags: dict[str, str]) -> int:
    """Stable 63-bit signed hash of a tag set, used as the dedup/upsert key."""
    if not tags:
        return 0
    blob = "\x1f".join(f"{k}\x1e{tags[k]}" for k in sorted(tags))
    digest = hashlib.blake2b(blob.encode("utf-8"), digest_size=8).digest()
    h = int.from_bytes(digest, "big", signed=False)
    # Constrain to PostgreSQL BIGINT range.
    return h & 0x7FFFFFFFFFFFFFFF


@dataclass
class TableSchema:
    measurement: str
    table: str
    qualified: str
    tag_cols: dict[str, str]  # name -> column name
    field_cols: dict[str, tuple[str, str]]  # name -> (column name, type)


class Store:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self._schema_cache: dict[str, TableSchema] = {}
        self._schema_locks: dict[str, asyncio.Lock] = {}

    @classmethod
    async def connect(cls) -> Store:
        pool = await asyncpg.create_pool(
            dsn=settings.dsn,
            min_size=settings.ingest_pool_min,
            max_size=settings.ingest_pool_max,
            command_timeout=60,
            statement_cache_size=512,
        )
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(settings.tsdb_schema)}")
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    def _lock_for(self, measurement: str) -> asyncio.Lock:
        lock = self._schema_locks.get(measurement)
        if lock is None:
            lock = asyncio.Lock()
            self._schema_locks[measurement] = lock
        return lock

    async def _ensure_schema(
        self,
        measurement: str,
        tag_names: set[str],
        field_samples: dict[str, object],
    ) -> TableSchema:
        cached = self._schema_cache.get(measurement)
        needs_update = (
            cached is None
            or any(t not in cached.tag_cols for t in tag_names)
            or any(f not in cached.field_cols for f in field_samples)
        )
        if not needs_update and cached is not None:
            return cached

        async with self._lock_for(measurement):
            cached = self._schema_cache.get(measurement)
            if cached is None:
                cached = await self._create_or_load(measurement)

            missing_tags = [t for t in tag_names if t not in cached.tag_cols]
            missing_fields = [f for f in field_samples if f not in cached.field_cols]

            if missing_tags or missing_fields:
                async with self.pool.acquire() as conn, conn.transaction():
                    for t in missing_tags:
                        col = _tag_col(t)
                        await conn.execute(
                            f"ALTER TABLE {cached.qualified} "
                            f"ADD COLUMN IF NOT EXISTS {_quote_ident(col)} TEXT"
                        )
                        cached.tag_cols[t] = col
                    for f in missing_fields:
                        col = _field_col(f)
                        ftype = _infer_field_type(field_samples[f])
                        await conn.execute(
                            f"ALTER TABLE {cached.qualified} "
                            f"ADD COLUMN IF NOT EXISTS {_quote_ident(col)} {ftype}"
                        )
                        cached.field_cols[f] = (col, ftype)
            self._schema_cache[measurement] = cached
            return cached

    async def _create_or_load(self, measurement: str) -> TableSchema:
        table = _measurement_table(measurement)
        qualified = f"{_quote_ident(settings.tsdb_schema)}.{_quote_ident(table)}"

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {qualified} (
                        time TIMESTAMPTZ NOT NULL,
                        tag_hash BIGINT NOT NULL
                    )
                    """
                )
                await conn.execute(
                    f"""
                    SELECT create_hypertable(
                        '{settings.tsdb_schema}.{table}', 'time',
                        chunk_time_interval => INTERVAL '{settings.ingest_chunk_interval}',
                        if_not_exists => TRUE,
                        migrate_data => TRUE
                    )
                    """
                )
                await conn.execute(
                    f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                        {_quote_ident(table + '_time_taghash_uidx')}
                    ON {qualified} (time, tag_hash)
                    """
                )
                # Best-effort: enable compression and a retention-friendly policy.
                # These are optional; failures are logged but not fatal.
                try:
                    await conn.execute(
                        f"ALTER TABLE {qualified} SET ("
                        "  timescaledb.compress,"
                        "  timescaledb.compress_segmentby = 'tag_hash',"
                        "  timescaledb.compress_orderby = 'time DESC'"
                        ")"
                    )
                    await conn.execute(
                        f"""
                        SELECT add_compression_policy(
                            '{settings.tsdb_schema}.{table}',
                            INTERVAL '{settings.ingest_compression_after}',
                            if_not_exists => TRUE
                        )
                        """
                    )
                except Exception as exc:
                    log.warning("compression setup skipped for %s: %s", measurement, exc)

            tag_cols, field_cols = await self._introspect_columns(conn, table)

        return TableSchema(
            measurement=measurement,
            table=table,
            qualified=qualified,
            tag_cols=tag_cols,
            field_cols=field_cols,
        )

    async def _introspect_columns(
        self, conn: asyncpg.Connection, table: str
    ) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
        rows = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            settings.tsdb_schema,
            table,
        )
        tag_cols: dict[str, str] = {}
        field_cols: dict[str, tuple[str, str]] = {}
        for r in rows:
            col = r["column_name"]
            dtype = r["data_type"].upper()
            if col.startswith("t_"):
                tag_cols[col[2:]] = col
            elif col.startswith("f_"):
                field_cols[col[2:]] = (col, dtype)
        return tag_cols, field_cols

    async def write_batch(self, points: list[Point]) -> int:
        """Upsert a list of points. Returns the number of rows written."""
        if not points:
            return 0

        by_meas: dict[str, list[Point]] = {}
        for p in points:
            by_meas.setdefault(p.measurement, []).append(p)

        total = 0
        for meas, pts in by_meas.items():
            total += await self._write_measurement(meas, pts)
        return total

    async def _write_measurement(self, measurement: str, points: list[Point]) -> int:
        tag_names: set[str] = set()
        field_samples: dict[str, object] = {}
        for p in points:
            tag_names.update(p.tags)
            for k, v in p.fields.items():
                field_samples.setdefault(k, v)

        schema = await self._ensure_schema(measurement, tag_names, field_samples)

        cols: list[str] = ["time", "tag_hash"]
        for t in tag_names:
            cols.append(schema.tag_cols[t])
        for f in field_samples:
            cols.append(schema.field_cols[f][0])

        records: list[tuple] = []
        for p in points:
            row: list[object] = [p.time, _hash_tags(p.tags)]
            for t in tag_names:
                row.append(p.tags.get(t))
            for f in field_samples:
                v = p.fields.get(f)
                col_type = schema.field_cols[f][1]
                row.append(_coerce(v, col_type))
            records.append(tuple(row))

        async with self.pool.acquire() as conn, conn.transaction():
            tmp = f"_lp_stage_{schema.table}"
            col_defs = ", ".join(f"{_quote_ident(c)} {_col_type_for(c, schema)}" for c in cols)
            await conn.execute(f"CREATE TEMP TABLE {tmp} ({col_defs}) ON COMMIT DROP")

            await conn.copy_records_to_table(
                tmp,
                records=records,
                columns=cols,
            )

            col_list = ", ".join(_quote_ident(c) for c in cols)
            update_cols = [c for c in cols if c not in ("time", "tag_hash")]
            set_clause = (
                ", ".join(f"{_quote_ident(c)} = EXCLUDED.{_quote_ident(c)}" for c in update_cols)
                or '"time" = EXCLUDED."time"'
            )
            await conn.execute(
                f"""
                INSERT INTO {schema.qualified} ({col_list})
                SELECT {col_list} FROM {tmp}
                ON CONFLICT (time, tag_hash) DO UPDATE SET {set_clause}
                """
            )
        return len(points)

    async def query_json(self, sql: str, params: list[object] | None = None) -> list[dict]:
        params = params or []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(r) for r in rows]

    async def delete_lp(
        self,
        measurement: str,
        tag_filters: dict[str, str] | None,
        start: object | None,
        end: object | None,
    ) -> int:
        schema = self._schema_cache.get(measurement)
        if schema is None:
            schema = await self._create_or_load(measurement)
            self._schema_cache[measurement] = schema

        where: list[str] = []
        params: list[object] = []
        if start is not None:
            params.append(start)
            where.append(f"time >= ${len(params)}")
        if end is not None:
            params.append(end)
            where.append(f"time < ${len(params)}")
        if tag_filters:
            for k, v in tag_filters.items():
                col = schema.tag_cols.get(k)
                if col is None:
                    return 0
                params.append(v)
                where.append(f"{_quote_ident(col)} = ${len(params)}")

        sql = f"DELETE FROM {schema.qualified}"
        if where:
            sql += " WHERE " + " AND ".join(where)

        async with self.pool.acquire() as conn:
            tag = await conn.execute(sql, *params)
        try:
            return int(tag.split()[-1])
        except (ValueError, IndexError):
            return 0


def _col_type_for(col: str, schema: TableSchema) -> str:
    if col == "time":
        return "TIMESTAMPTZ"
    if col == "tag_hash":
        return "BIGINT"
    if col.startswith("t_"):
        return "TEXT"
    for _, (cname, ctype) in schema.field_cols.items():
        if cname == col:
            return ctype
    return "TEXT"


def _coerce(value: object, col_type: str) -> object:
    if value is None:
        return None
    if col_type == "BIGINT":
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return int(str(value))
    if col_type in ("DOUBLE PRECISION", "REAL"):
        return float(value)  # type: ignore[arg-type]
    if col_type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        if isinstance(value, int | float):
            return bool(value)
        return str(value).lower() in ("t", "true", "1")
    return str(value)
