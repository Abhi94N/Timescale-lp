"""
DuckDB-backed query/export engine.

DuckDB is used for two things, both reading from the same attached sources:

1. **Tiering export** — `COPY (SELECT ... FROM pg.lp.<table> WHERE <range>)
   TO 's3://.../<key>.parquet' (FORMAT parquet)` streams a TimescaleDB chunk
   straight into a Parquet object without round-tripping rows through Python.

2. **Federated query** — each measurement is exposed as a DuckDB view that
   `UNION ALL BY NAME`s the hot Postgres table with the cold Parquet objects,
   so a single SQL statement transparently spans both tiers.

DuckDB is synchronous, so all execution is dispatched to a thread via
`asyncio.to_thread`, each call using its own cursor (`con.cursor()`), which
DuckDB supports for concurrent reads.

The SQL builders below are pure functions (no DuckDB handle) so they can be
unit-tested without a live engine.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from .config import settings

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Pure SQL builders
# --------------------------------------------------------------------------


def cold_key(measurement_table: str, range_start: datetime) -> str:
    """Object key (within the bucket) for a chunk starting at `range_start`.

    The timestamp is normalized to UTC so the key is deterministic regardless
    of the server's local timezone.
    """
    stamp = range_start.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{measurement_table}/{stamp}.parquet"


def s3_uri(key: str) -> str:
    return f"s3://{settings.s3_bucket}/{key}"


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


def copy_to_parquet_sql(select_sql: str, key: str) -> str:
    uri = s3_uri(key)
    return f"COPY ({select_sql}) TO '{uri}' (FORMAT parquet, COMPRESSION zstd, OVERWRITE_OR_IGNORE true)"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_view_sql(
    view_schema: str,
    measurement_table: str,
    hot_ref: str | None,
    cold_keys: list[str],
) -> str:
    """Return `CREATE OR REPLACE VIEW` SQL unioning hot + cold for a measurement.

    - `hot_ref` is the fully-qualified attached Postgres relation
      (e.g. ``pg.lp.cpu``), or None to build a cold-only view.
    - `cold_keys` are object keys (within the bucket); empty for hot-only.

    Hot and cold snapshots may differ in columns (the live table can gain or
    widen columns after a chunk was frozen), so `UNION ALL BY NAME` aligns by
    column name and fills missing columns with NULL.
    """
    view = f"{_quote_ident(view_schema)}.{_quote_ident(measurement_table)}"
    parts: list[str] = []
    if hot_ref:
        parts.append(f"SELECT * FROM {hot_ref}")
    if cold_keys:
        uris = ", ".join(f"'{s3_uri(k)}'" for k in cold_keys)
        parts.append(f"SELECT * FROM read_parquet([{uris}], union_by_name=true)")
    # Degenerate case (no hot, no cold): produce an always-empty body.
    body = "\nUNION ALL BY NAME\n".join(parts) if parts else "SELECT WHERE false"
    return f"CREATE OR REPLACE VIEW {view} AS {body}"


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

    async def export_range(
        self,
        qualified_hot: str,
        key: str,
        range_start: datetime,
        range_end: datetime,
    ) -> int:
        """Export `[range_start, range_end)` from a hot relation to Parquet.

        Returns the number of rows written.
        """
        select_sql = (
            f"SELECT * FROM {qualified_hot} "
            f"WHERE time >= TIMESTAMPTZ '{range_start.isoformat()}' "
            f"AND time < TIMESTAMPTZ '{range_end.isoformat()}'"
        )
        async with self._lock:
            await self.execute(copy_to_parquet_sql(select_sql, key))
            rows = await self.query_rows(f"SELECT count(*) AS n FROM read_parquet('{s3_uri(key)}')")
        return int(rows[0]["n"]) if rows else 0

    async def register_views(self, views: dict[str, tuple[str | None, list[str]]]) -> None:
        """Create/refresh federated views.

        `views` maps measurement_table -> (hot_ref_or_None, [cold_keys]).
        """
        async with self._lock:
            for table, (hot_ref, cold_keys) in views.items():
                await self.execute(build_view_sql(settings.tsdb_schema, table, hot_ref, cold_keys))
