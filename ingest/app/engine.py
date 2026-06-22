"""
DuckDB-backed query/compaction engine.

DuckDB is the read/compute layer over the two tiers:

1. **Federated query** — each measurement is exposed as a view that unions the
   hot Postgres table with its cold Parquet objects and **dedups by
   `(time, tag_hash)` keeping the highest `seq`**, so updates are reflected
   across tiers (write-through appends a new, higher-seq version to cold).

2. **Compaction** — many small write-through staging objects are merged (and
   deduped) into large day-partitioned Parquet files.

DuckDB is synchronous, so execution is dispatched to threads; each call uses
its own cursor. The SQL builders are pure functions for unit-testing.
"""

from __future__ import annotations

import asyncio
import logging

from .config import settings

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Pure SQL builders
# --------------------------------------------------------------------------


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def s3_secret_sql() -> str:
    use_ssl = "true" if settings.s3_use_ssl else "false"
    return (
        "CREATE OR REPLACE SECRET cold_s3 ("
        "  TYPE S3,"
        f"  KEY_ID '{settings.s3_access_key}',"
        f"  SECRET '{settings.s3_secret_key}',"
        f"  ENDPOINT '{settings.s3_endpoint_host}',"
        f"  URL_STYLE '{settings.s3_url_style}',"
        f"  USE_SSL {use_ssl},"
        f"  REGION '{settings.s3_region}'"
        ")"
    )


def attach_pg_sql(alias: str = "pg") -> str:
    dsn = settings.duckdb_pg_dsn.replace("'", "''")
    return f"ATTACH IF NOT EXISTS '{dsn}' AS {alias} (TYPE postgres, READ_ONLY)"


def read_parquet_ref(uris: list[str]) -> str:
    """A `read_parquet([...])` relation over the given object URIs.

    `union_by_name` aligns objects that differ in columns (the live schema can
    gain/widen columns after older objects were written); missing columns read
    as NULL.
    """
    arr = ", ".join(f"'{u}'" for u in uris)
    return f"read_parquet([{arr}], union_by_name=true)"


def build_federated_view_sql(view_schema: str, table: str, refs: list[str]) -> str:
    """`CREATE OR REPLACE VIEW` that unions `refs` and dedups by
    `(time, tag_hash)` keeping the newest `seq`. `seq` is excluded from output.

    Every ref must expose `time`, `tag_hash`, and `seq`.
    """
    view = f"{_quote_ident(view_schema)}.{_quote_ident(table)}"
    if not refs:
        return f"CREATE OR REPLACE VIEW {view} AS SELECT WHERE false"
    union = "\nUNION ALL BY NAME\n".join(f"SELECT * FROM {r}" for r in refs)
    return (
        f"CREATE OR REPLACE VIEW {view} AS\n"
        f"SELECT * EXCLUDE (seq, _rn) FROM (\n"
        f"  SELECT *, row_number() OVER "
        f"(PARTITION BY time, tag_hash ORDER BY seq DESC) AS _rn\n"
        f"  FROM (\n{union}\n  )\n"
        f") WHERE _rn = 1"
    )


def compact_sql(input_uris: list[str], output_uri: str) -> str:
    """`COPY` that merges + dedups staging objects into one compacted object.

    `seq` is retained so cross-object dedup with the hot tier still works on
    read.
    """
    src = read_parquet_ref(input_uris)
    dedup = (
        f"SELECT * EXCLUDE (_rn) FROM ("
        f"  SELECT *, row_number() OVER "
        f"(PARTITION BY time, tag_hash ORDER BY seq DESC) AS _rn FROM {src}"
        f") WHERE _rn = 1"
    )
    return (
        f"COPY ({dedup}) TO '{output_uri}' "
        f"(FORMAT parquet, COMPRESSION zstd, "
        f"ROW_GROUP_SIZE {settings.cold_row_group_size}, OVERWRITE_OR_IGNORE true)"
    )


# --------------------------------------------------------------------------
# Engine
# --------------------------------------------------------------------------


class DuckEngine:
    def __init__(self, con) -> None:
        self._con = con
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(cls) -> DuckEngine:
        con = await asyncio.to_thread(cls._build_connection)
        return cls(con)

    @staticmethod
    def _build_connection():
        import duckdb

        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(s3_secret_sql())
        con.execute(attach_pg_sql())
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(settings.tsdb_schema)}")
        return con

    async def close(self) -> None:
        await asyncio.to_thread(self._con.close)

    async def execute(self, sql: str, params: list | None = None) -> None:
        def _run():
            cur = self._con.cursor()
            try:
                cur.execute(sql, params or [])
            finally:
                cur.close()

        await asyncio.to_thread(_run)

    async def query_rows(self, sql: str, params: list | None = None) -> list[dict]:
        def _run() -> list[dict]:
            cur = self._con.cursor()
            try:
                cur.execute(sql, params or [])
                cols = [d[0] for d in cur.description] if cur.description else []
                return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
            finally:
                cur.close()

        return await asyncio.to_thread(_run)

    async def register_views(self, specs: dict[str, list[str]]) -> None:
        """Create/refresh federated views. `specs` maps table -> [relation refs]."""
        async with self._lock:
            for table, refs in specs.items():
                await self.execute(build_federated_view_sql(settings.tsdb_schema, table, refs))

    async def compact(self, input_uris: list[str], output_uri: str) -> int:
        """Merge+dedup `input_uris` into `output_uri`. Returns rows written."""
        async with self._lock:
            await self.execute(compact_sql(input_uris, output_uri))
            rows = await self.query_rows(f"SELECT count(*) AS n FROM read_parquet('{output_uri}')")
        return int(rows[0]["n"]) if rows else 0

    async def export_hot_range(self, hot_ref: str, output_uri: str, start, end) -> int:
        """Export `[start, end)` from a hot relation to a Parquet object.

        Used to evict chunks that aren't already in cold (e.g. write-through
        was disabled). Returns rows written.
        """
        select = (
            f"SELECT * FROM {hot_ref} "
            f"WHERE time >= TIMESTAMPTZ '{start.isoformat()}' "
            f"AND time < TIMESTAMPTZ '{end.isoformat()}'"
        )
        async with self._lock:
            await self.execute(
                f"COPY ({select}) TO '{output_uri}' "
                f"(FORMAT parquet, COMPRESSION zstd, OVERWRITE_OR_IGNORE true)"
            )
            rows = await self.query_rows(f"SELECT count(*) AS n FROM read_parquet('{output_uri}')")
        return int(rows[0]["n"]) if rows else 0
