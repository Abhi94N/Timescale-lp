"""
Hot→cold tiering.

TimescaleDB is the hot cache; Parquet objects in S3/MinIO are cold storage.
A tiering pass, per hypertable:

1. Finds chunks whose time range is entirely older than the hot window.
2. Exports each chunk (oldest first) to a Parquet object via DuckDB.
3. Records the object in the `<schema>._cold_chunks` manifest.
4. Drops the now-archived chunk from TimescaleDB.

Re-running is safe: exports overwrite the same deterministic key and the
manifest upserts, so a crash mid-pass simply re-does the unfinished chunk.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime

from .config import settings
from .engine import DuckEngine, cold_key
from .store import Store, _quote_ident

log = logging.getLogger(__name__)

MANIFEST = "_cold_chunks"


def _manifest_qualified() -> str:
    return f"{_quote_ident(settings.tsdb_schema)}.{_quote_ident(MANIFEST)}"


def _hot_ref(table: str) -> str:
    """DuckDB reference to the attached Postgres hypertable."""
    return f"pg.{_quote_ident(settings.tsdb_schema)}.{_quote_ident(table)}"


@dataclass
class TierResult:
    measurement: str
    object_key: str
    range_start: datetime
    range_end: datetime
    rows: int


class Tierer:
    def __init__(self, store: Store, engine: DuckEngine) -> None:
        self.store = store
        self.engine = engine
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    async def ensure_manifest(self) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_manifest_qualified()} (
                    hypertable  TEXT        NOT NULL,
                    object_key  TEXT        NOT NULL,
                    range_start TIMESTAMPTZ NOT NULL,
                    range_end   TIMESTAMPTZ NOT NULL,
                    rows        BIGINT      NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (hypertable, range_start)
                )
                """
            )

    # ---- discovery -------------------------------------------------------

    async def _hypertables(self) -> list[str]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT hypertable_name
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = $1
                ORDER BY hypertable_name
                """,
                settings.tsdb_schema,
            )
        return [r["hypertable_name"] for r in rows]

    async def _eligible_chunks(
        self, table: str, older_than: str
    ) -> list[tuple[datetime, datetime]]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT range_start, range_end
                FROM timescaledb_information.chunks
                WHERE hypertable_schema = $1 AND hypertable_name = $2
                  AND range_end <= now() - INTERVAL '{older_than}'
                ORDER BY range_start
                """,
                settings.tsdb_schema,
                table,
            )
        return [(r["range_start"], r["range_end"]) for r in rows]

    # ---- one pass --------------------------------------------------------

    async def run_once(
        self, older_than: str | None = None, measurement: str | None = None
    ) -> list[TierResult]:
        await self.ensure_manifest()
        window = older_than or settings.tier_hot_window
        tables = [measurement] if measurement else await self._hypertables()

        results: list[TierResult] = []
        for table in tables:
            chunks = await self._eligible_chunks(table, window)
            for range_start, range_end in chunks:
                key = cold_key(table, range_start)
                rows = await self.engine.export_range(_hot_ref(table), key, range_start, range_end)
                await self._record(table, key, range_start, range_end, rows)
                await self._drop_chunk(table, range_start, range_end)
                log.info(
                    "tiered %s [%s, %s) -> %s (%d rows)",
                    table,
                    range_start.isoformat(),
                    range_end.isoformat(),
                    key,
                    rows,
                )
                results.append(TierResult(table, key, range_start, range_end, rows))
        return results

    async def _record(
        self, table: str, key: str, start: datetime, end: datetime, rows: int
    ) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {_manifest_qualified()}
                    (hypertable, object_key, range_start, range_end, rows)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (hypertable, range_start) DO UPDATE SET
                    object_key = EXCLUDED.object_key,
                    range_end  = EXCLUDED.range_end,
                    rows       = EXCLUDED.rows,
                    created_at = now()
                """,
                table,
                key,
                start,
                end,
                rows,
            )

    async def _drop_chunk(self, table: str, start: datetime, end: datetime) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"SELECT drop_chunks('{settings.tsdb_schema}.{table}', "
                f"older_than => $1::timestamptz, newer_than => $2::timestamptz)",
                end,
                start,
            )

    # ---- manifest reads --------------------------------------------------

    async def cold_keys(self, table: str) -> list[str]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT object_key FROM {_manifest_qualified()} "
                f"WHERE hypertable = $1 ORDER BY range_start",
                table,
            )
        return [r["object_key"] for r in rows]

    async def status(self) -> list[dict]:
        await self.ensure_manifest()
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT hypertable,
                       count(*)        AS objects,
                       sum(rows)       AS rows,
                       min(range_start) AS oldest,
                       max(range_end)   AS newest
                FROM {_manifest_qualified()}
                GROUP BY hypertable
                ORDER BY hypertable
                """
            )
        return [dict(r) for r in rows]

    async def view_specs(
        self, include_hot: bool, include_cold: bool
    ) -> dict[str, tuple[str | None, list[str]]]:
        """Build the {table: (hot_ref, cold_keys)} map for federated views."""
        tables: set[str] = set()
        if include_hot:
            tables.update(await self._hypertables())
        if include_cold:
            async with self.store.pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT DISTINCT hypertable FROM {_manifest_qualified()}")
            tables.update(r["hypertable"] for r in rows)

        specs: dict[str, tuple[str | None, list[str]]] = {}
        for table in sorted(tables):
            hot_ref = _hot_ref(table) if include_hot else None
            cold = await self.cold_keys(table) if include_cold else []
            specs[table] = (hot_ref, cold)
        return specs

    # ---- background loop -------------------------------------------------

    async def start(self) -> None:
        if not settings.tier_enabled:
            return
        await self.ensure_manifest()
        self._task = asyncio.create_task(self._loop(), name="tierer")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.run_once()
            except Exception:
                log.exception("tiering pass failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=settings.tier_interval_seconds
                )
