"""
Background batcher for line-protocol writes.

Each enqueued point is buffered until either:
  - the buffer reaches `ingest_batch_size`, or
  - `ingest_batch_flush_ms` milliseconds have elapsed since the oldest
    buffered point.

`ingest_workers` flush coroutines drain a shared queue in parallel.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .config import settings
from .lineproto import Point
from .store import Store

log = logging.getLogger(__name__)


class Batcher:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.queue: asyncio.Queue[Point] = asyncio.Queue(maxsize=settings.ingest_batch_size * 8)
        self._workers: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self.points_written = 0
        self.points_failed = 0

    async def start(self) -> None:
        for i in range(settings.ingest_workers):
            t = asyncio.create_task(self._worker(i), name=f"batcher-{i}")
            self._workers.append(t)

    async def stop(self) -> None:
        self._stopping.set()
        for t in self._workers:
            t.cancel()
        for t in self._workers:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._workers.clear()

    async def submit(self, points: list[Point]) -> None:
        for p in points:
            await self.queue.put(p)

    async def submit_sync(self, points: list[Point]) -> int:
        """Bypass the queue and write immediately. Used for low-latency writers."""
        return await self.store.write_batch(points)

    async def _worker(self, idx: int) -> None:
        buf: list[Point] = []
        flush_at: float | None = None
        flush_interval = settings.ingest_batch_flush_ms / 1000.0
        max_size = settings.ingest_batch_size

        while not self._stopping.is_set():
            timeout: float | None = (
                None if flush_at is None else max(0.0, flush_at - time.monotonic())
            )

            try:
                if timeout is None:
                    point = await self.queue.get()
                else:
                    point = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                buf.append(point)
                if flush_at is None:
                    flush_at = time.monotonic() + flush_interval
            except TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if buf and (
                len(buf) >= max_size or (flush_at is not None and time.monotonic() >= flush_at)
            ):
                await self._flush(buf)
                buf = []
                flush_at = None

        if buf:
            await self._flush(buf)

    async def _flush(self, buf: list[Point]) -> None:
        try:
            written = await self.store.write_batch(buf)
            self.points_written += written
        except Exception:
            log.exception("batch flush failed (size=%d)", len(buf))
            self.points_failed += len(buf)
