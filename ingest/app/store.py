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
import itertools
import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import asyncpg

from .config import settings
from .lineproto import Point

if TYPE_CHECKING:
    from .coldstore import ColdStore

log = logging.getLogger(__name__)

_IDENT_OK = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Monotonic ingest sequence. Seeded from wall-clock nanoseconds so that a
# process restart never reissues a sequence below one already persisted (a
# lower seq on a newer write would let stale cold data shadow it on read).
# Reserved per-point; the surviving row for a (time, tag_hash) key carries the
# highest seq, which dedup-on-read uses to pick the latest version.
_SEQ_BASE = time.time_ns()
_seq_counter = itertools.count()


def _next_seq() -> int:
    return _SEQ_BASE + next(_seq_counter)


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


# Type-widening lattice: when a field column already exists but a newly
# observed value is "wider" than the column type, the column is promoted up
# this lattice. TEXT is sticky — once a field has been promoted to TEXT it
# never widens again, and all subsequent values are coerced to str.
#
#   BOOLEAN  <  BIGINT  <  DOUBLE PRECISION  <  TEXT
TYPE_RANK = {"BOOLEAN": 1, "BIGINT": 2, "DOUBLE PRECISION": 3, "TEXT": 4}


def _widened_type(current: str, observed: object) -> str:
    """Return the column type that can hold both the current type and `observed`."""
    new_t = _infer_field_type(observed)
    return new_t if TYPE_RANK.get(new_t, 4) > TYPE_RANK.get(current, 4) else current


def _alter_using(col: str, from_type: str, to_type: str) -> str:
    """Return a `USING` expression for `ALTER COLUMN ... TYPE ...`.

    BOOLEAN -> numeric needs to chain through int because Postgres has no
    direct bool->numeric assignment cast.
    """
    qcol = _quote_ident(col)
    target = to_type.lower()
    if from_type == "BOOLEAN" and to_type in ("BIGINT", "DOUBLE PRECISION"):
        return f"({qcol})::int::{target}"
    return f"({qcol})::{target}"


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
        # Optional write-through cold store (set by the app after construction).
        self.cold: ColdStore | None = None

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

        def _wider_needed(name: str, value: object) -> bool:
            if cached is None or name not in cached.field_cols:
                return False
            current = cached.field_cols[name][1]
            return _widened_type(current, value) != current

        needs_update = (
            cached is None
            or any(t not in cached.tag_cols for t in tag_names)
            or any(f not in cached.field_cols for f in field_samples)
            or any(_wider_needed(f, v) for f, v in field_samples.items())
        )
        if not needs_update and cached is not None:
            return cached

        async with self._lock_for(measurement):
            cached = self._schema_cache.get(measurement)
            if cached is None:
                cached = await self._create_or_load(measurement)

            missing_tags = [t for t in tag_names if t not in cached.tag_cols]
            missing_fields = [f for f in field_samples if f not in cached.field_cols]
            widen: list[tuple[str, str, str, str]] = []  # (field, col, from_type, to_type)
            for f, v in field_samples.items():
                if f not in cached.field_cols:
                    continue
                col, current = cached.field_cols[f]
                target = _widened_type(current, v)
                if target != current:
                    widen.append((f, col, current, target))

            if missing_tags or missing_fields or widen:
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
                    for fname, col, from_t, to_t in widen:
                        log.info(
                            "widening %s.%s: %s -> %s (line-protocol promotion)",
                            cached.measurement,
                            col,
                            from_t,
                            to_t,
                        )
                        using = _alter_using(col, from_t, to_t)
                        await conn.execute(
                            f"ALTER TABLE {cached.qualified} "
                            f"ALTER COLUMN {_quote_ident(col)} TYPE {to_t} USING {using}"
                        )
                        cached.field_cols[fname] = (col, to_t)
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
                        tag_hash BIGINT NOT NULL,
                        seq BIGINT NOT NULL DEFAULT 0
                    )
                    """
                )
                # Defensive for tables created before `seq` existed.
                await conn.execute(
                    f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS seq BIGINT NOT NULL DEFAULT 0"
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
        # Per field, retain the value whose inferred type sits highest in the
        # widening lattice — this is the value `_ensure_schema` uses to decide
        # whether to add or widen the column.
        field_samples: dict[str, object] = {}
        for p in points:
            tag_names.update(p.tags)
            for k, v in p.fields.items():
                existing = field_samples.get(k)
                if existing is None or TYPE_RANK.get(_infer_field_type(v), 4) > TYPE_RANK.get(
                    _infer_field_type(existing), 4
                ):
                    field_samples[k] = v

        schema = await self._ensure_schema(measurement, tag_names, field_samples)

        cols: list[str] = ["time", "tag_hash", "seq"]
        for t in tag_names:
            cols.append(schema.tag_cols[t])
        for f in field_samples:
            cols.append(schema.field_cols[f][0])

        # Build rows, assigning a monotonic seq per point, and dedup within the
        # batch by (time, tag_hash) keeping the last (highest-seq) occurrence.
        # The dedup also avoids "ON CONFLICT cannot affect row a second time".
        deduped: dict[tuple, tuple] = {}
        for p in points:
            key = (p.time, _hash_tags(p.tags))
            row: list[object] = [p.time, key[1], _next_seq()]
            for t in tag_names:
                row.append(p.tags.get(t))
            for f in field_samples:
                col_type = schema.field_cols[f][1]
                row.append(_coerce(p.fields.get(f), col_type))
            deduped[key] = tuple(row)
        records: list[tuple] = list(deduped.values())

        # Write-through: persist this batch to cold storage BEFORE Postgres, so
        # cold (the source of truth) always has every acknowledged write. An
        # interrupted PG write just leaves a harmless lower-seq cold object.
        if self.cold is not None and settings.write_through:
            await self._write_through(measurement, schema, cols, records)

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
        return len(records)

    async def _write_through(
        self,
        measurement: str,
        schema: TableSchema,
        cols: list[str],
        records: list[tuple],
    ) -> None:
        """Write this batch to a cold-staging Parquet object and record it."""
        from .coldstore import ObjectMeta, staging_key

        assert self.cold is not None
        table = build_arrow_table(cols, records, schema)
        # time is column 0, seq is column 2 (see cols order in _write_measurement).
        times = [r[0] for r in records]
        seqs = [r[2] for r in records]
        min_seq, max_seq = min(seqs), max(seqs)
        key = staging_key(schema.table, min_seq, max_seq)
        await asyncio.to_thread(self.cold.write_table, key, table)
        await self.cold.record(
            ObjectMeta(
                measurement=schema.table,
                kind="staging",
                object_key=key,
                range_start=min(times),
                range_end=max(times),
                rows=len(records),
                min_seq=min_seq,
                max_seq=max_seq,
            )
        )

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


def _pa_type(col: str, schema: TableSchema):
    import pyarrow as pa

    if col == "time":
        return pa.timestamp("us", tz="UTC")
    if col in ("tag_hash", "seq"):
        return pa.int64()
    if col.startswith("t_"):
        return pa.string()
    pg = _col_type_for(col, schema)
    return {
        "BIGINT": pa.int64(),
        "DOUBLE PRECISION": pa.float64(),
        "BOOLEAN": pa.bool_(),
    }.get(pg, pa.string())


def build_arrow_table(cols: list[str], records: list[tuple], schema: TableSchema):
    """Build a pyarrow Table (typed per the hot-table column types) from the
    same row tuples used for the Postgres COPY. Used for write-through staging."""
    import pyarrow as pa

    arrays = []
    for i, col in enumerate(cols):
        values = [r[i] for r in records]
        arrays.append(pa.array(values, type=_pa_type(col, schema)))
    return pa.Table.from_arrays(arrays, names=cols)


def _col_type_for(col: str, schema: TableSchema) -> str:
    if col == "time":
        return "TIMESTAMPTZ"
    if col in ("tag_hash", "seq"):
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
