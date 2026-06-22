"""
Cold-tier maintenance: compaction, eviction, and federation-ref assembly.

With write-through on, every batch is already durable in cold staging, so the
two background jobs are:

- **Compaction** merges many small staging objects into large day-partitioned
  cold objects (deduping by seq), keeping the queryable cold tier efficient at
  TB scale.
- **Eviction** drops TimescaleDB chunks older than the hot window once cold
  fully covers them — pure cache eviction, no data movement.

It also assembles the DuckDB relation refs (hot table + range-pruned cold/
staging Parquet) that back the federated query views.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime

from .coldstore import KIND_COLD, KIND_STAGING, ColdStore, cold_key, day_of, select_overlapping
from .config import settings
from .engine import DuckEngine, read_parquet_ref
from .store import Store, _quote_ident

log = logging.getLogger(__name__)


def _hot_ref(table: str) -> str:
    return f"pg.{_quote_ident(settings.tsdb_schema)}.{_quote_ident(table)}"


# --------------------------------------------------------------------------
# Pure planning helpers
# --------------------------------------------------------------------------


def plan_compaction(staging_objects: list[dict]) -> dict[str, list[dict]]:
    """Group staging objects by UTC day (of range_start) for merging."""
    groups: dict[str, list[dict]] = {}
    for o in staging_objects:
        groups.setdefault(day_of(o["range_start"]), []).append(o)
    return groups


def merge_intervals(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


def is_covered(
    chunk_start: datetime, chunk_end: datetime, intervals: list[tuple[datetime, datetime]]
) -> bool:
    """Is [chunk_start, chunk_end) fully covered by the union of `intervals`?"""
    cursor = chunk_start
    for start, end in merge_intervals(intervals):
        if start > cursor:
            return False
        if end > cursor:
            cursor = end
        if cursor >= chunk_end:
            return True
    return cursor >= chunk_end


# --------------------------------------------------------------------------
# Tierer
# --------------------------------------------------------------------------


@dataclass
class CompactResult:
    measurement: str
    day: str
    object_key: str
    merged: int
    rows: int


@dataclass
class EvictResult:
    measurement: str
    range_start: datetime
    range_end: datetime


class Tierer:
    def __init__(self, store: Store, engine: DuckEngine, cold: ColdStore) -> None:
        self.store = store
        self.engine = engine
        self.cold = cold
        self._task: asyncio.Task | None = None
        self._stopping = asyncio.Event()

    async def ensure_manifest(self) -> None:
        await self.cold.ensure_manifest()

    # ---- compaction ------------------------------------------------------

    async def compact_once(self, measurement: str | None = None) -> list[CompactResult]:
        await self.ensure_manifest()
        measurements = [measurement] if measurement else await self.cold.measurements()
        results: list[CompactResult] = []
        for m in measurements:
            staging = await self.cold.objects(m, kind=KIND_STAGING)
            if not staging:
                continue
            for day, objs in plan_compaction(staging).items():
                min_seq = min(o["min_seq"] for o in objs)
                out_key = cold_key(m, day, min_seq)
                in_uris = [self.cold.uri(o["object_key"]) for o in objs]
                rows = await self.engine.compact(in_uris, self.cold.uri(out_key))
                await self.cold.record(_meta(m, KIND_COLD, out_key, objs, rows))
                # Drop the now-merged staging objects.
                for o in objs:
                    await asyncio.to_thread(self.cold.delete, o["object_key"])
                    await self.cold.forget(o["object_key"])
                log.info(
                    "compacted %s dt=%s: %d objs -> %s (%d rows)", m, day, len(objs), out_key, rows
                )
                results.append(CompactResult(m, day, out_key, len(objs), rows))
        return results

    # ---- eviction --------------------------------------------------------

    async def evict_once(
        self, older_than: str | None = None, measurement: str | None = None
    ) -> list[EvictResult]:
        await self.ensure_manifest()
        window = older_than or settings.tier_hot_window
        tables = [measurement] if measurement else await self._hypertables()
        results: list[EvictResult] = []
        for table in tables:
            chunks = await self._eligible_chunks(table, window)
            if not chunks:
                continue
            objs = await self.cold.objects(table)
            intervals = [(o["range_start"], o["range_end"]) for o in objs]
            for cs, ce in chunks:
                if is_covered(cs, ce, intervals):
                    await self._drop_chunk(table, cs, ce)
                    log.info("evicted %s chunk [%s, %s) (covered by cold)", table, cs, ce)
                    results.append(EvictResult(table, cs, ce))
                else:
                    # Not in cold (e.g. write-through was off). Export then drop.
                    out_key = cold_key(table, day_of(cs), 0)
                    rows = await self.engine.export_hot_range(
                        _hot_ref(table), self.cold.uri(out_key), cs, ce
                    )
                    from .coldstore import ObjectMeta

                    await self.cold.record(
                        ObjectMeta(table, KIND_COLD, out_key, cs, ce, rows, 0, 0)
                    )
                    await self._drop_chunk(table, cs, ce)
                    log.info("exported+evicted %s chunk [%s, %s) (%d rows)", table, cs, ce, rows)
                    results.append(EvictResult(table, cs, ce))
        return results

    async def run_once(self, older_than: str | None = None, measurement: str | None = None) -> dict:
        compacted = await self.compact_once(measurement)
        evicted = await self.evict_once(older_than, measurement)
        return {"compacted": compacted, "evicted": evicted}

    # ---- discovery -------------------------------------------------------

    async def _hypertables(self) -> list[str]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT hypertable_name FROM timescaledb_information.hypertables
                WHERE hypertable_schema = $1 ORDER BY hypertable_name
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
                SELECT range_start, range_end FROM timescaledb_information.chunks
                WHERE hypertable_schema = $1 AND hypertable_name = $2
                  AND range_end <= now() - INTERVAL '{older_than}'
                ORDER BY range_start
                """,
                settings.tsdb_schema,
                table,
            )
        return [(r["range_start"], r["range_end"]) for r in rows]

    async def _drop_chunk(self, table: str, start: datetime, end: datetime) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"SELECT drop_chunks('{settings.tsdb_schema}.{table}', "
                f"older_than => $1::timestamptz, newer_than => $2::timestamptz)",
                end,
                start,
            )

    # ---- federation ------------------------------------------------------

    async def view_specs(
        self,
        include_hot: bool,
        include_cold: bool,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, list[str]]:
        """Build {table: [relation refs]} for federated views, range-pruning
        cold objects by [start, end] when provided."""
        tables: set[str] = set()
        if include_hot:
            tables.update(await self._hypertables())
        if include_cold:
            tables.update(await self.cold.measurements())

        specs: dict[str, list[str]] = {}
        for table in sorted(tables):
            refs: list[str] = []
            if include_hot:
                refs.append(_hot_ref(table))
            if include_cold:
                objs = select_overlapping(await self.cold.objects(table), start, end)
                if objs:
                    uris = [self.cold.uri(o["object_key"]) for o in objs]
                    refs.append(read_parquet_ref(uris))
            specs[table] = refs
        return specs

    async def status(self) -> list[dict]:
        await self.ensure_manifest()
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT measurement, kind,
                       count(*) AS objects, sum(rows) AS rows,
                       min(range_start) AS oldest, max(range_end) AS newest
                FROM {_quote_ident(settings.tsdb_schema)}._cold_objects
                GROUP BY measurement, kind ORDER BY measurement, kind
                """
            )
        return [dict(r) for r in rows]

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


def _meta(measurement: str, kind: str, key: str, objs: list[dict], rows: int):
    from .coldstore import ObjectMeta

    return ObjectMeta(
        measurement=measurement,
        kind=kind,
        object_key=key,
        range_start=min(o["range_start"] for o in objs),
        range_end=max(o["range_end"] for o in objs),
        rows=rows,
        min_seq=min(o["min_seq"] for o in objs),
        max_seq=max(o["max_seq"] for o in objs),
    )
