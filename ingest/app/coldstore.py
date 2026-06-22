"""
Cold object store (Parquet on S3/MinIO) + manifest.

The store is abstracted over a `pyarrow.fs.FileSystem`, so the identical code
path runs against:
  - S3/MinIO in production (`ColdStore.from_settings()`), and
  - a local temp directory in tests (`ColdStore.local(tmp)`).

DuckDB reads both `s3://…` and local paths, so the federated query/compaction
engine works the same way in either mode.

Object layout
-------------
    <measurement>/staging/<min_seq>-<max_seq>.parquet     # write-through unit
    <measurement>/cold/dt=<YYYY-MM-DD>/<min_seq>.parquet  # compacted, queryable

Each object is recorded in the `<schema>._cold_objects` manifest with its time
range and seq range, which drives partition pruning and dedup on read.
"""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from .config import settings
from .store import Store, _quote_ident

log = logging.getLogger(__name__)

MANIFEST = "_cold_objects"
KIND_STAGING = "staging"
KIND_COLD = "cold"


def manifest_qualified() -> str:
    return f"{_quote_ident(settings.tsdb_schema)}.{_quote_ident(MANIFEST)}"


# --------------------------------------------------------------------------
# Key layout (pure)
# --------------------------------------------------------------------------


def staging_key(measurement_table: str, min_seq: int, max_seq: int) -> str:
    return f"{measurement_table}/staging/{min_seq:020d}-{max_seq:020d}.parquet"


def cold_key(measurement_table: str, day: str, min_seq: int) -> str:
    return f"{measurement_table}/cold/dt={day}/{min_seq:020d}.parquet"


def day_of(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%d")


def select_overlapping(
    objects: list[dict], start: datetime | None, end: datetime | None
) -> list[dict]:
    """Partition pruning: keep manifest rows whose [range_start, range_end)
    overlaps the (optional) query window [start, end]."""
    out = []
    for o in objects:
        if start is not None and o["range_end"] < start:
            continue
        if end is not None and o["range_start"] > end:
            continue
        out.append(o)
    return out


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------


@dataclass
class ObjectMeta:
    measurement: str
    kind: str
    object_key: str
    range_start: datetime
    range_end: datetime
    rows: int
    min_seq: int
    max_seq: int


class ColdStore:
    def __init__(self, store: Store, fs, root: str, uri_scheme: str) -> None:
        self.store = store
        self.fs = fs
        self.root = root.rstrip("/")
        self._uri_scheme = uri_scheme  # "s3" or "" (local)

    @classmethod
    def from_settings(cls, store: Store) -> ColdStore:
        import pyarrow.fs as pafs

        fs = pafs.S3FileSystem(
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            endpoint_override=settings.s3_endpoint,
            scheme="https" if settings.s3_use_ssl else "http",
            region=settings.s3_region,
            allow_bucket_creation=True,
            allow_bucket_deletion=False,
        )
        return cls(store, fs, settings.s3_bucket, "s3")

    @classmethod
    def local(cls, store: Store, root: str) -> ColdStore:
        import os

        import pyarrow.fs as pafs

        os.makedirs(root, exist_ok=True)
        return cls(store, pafs.LocalFileSystem(), root, "")

    # ---- URIs ----

    def fs_path(self, key: str) -> str:
        """Path as the pyarrow FileSystem expects it (root-relative joined)."""
        return f"{self.root}/{key}"

    def uri(self, key: str) -> str:
        """Full URI for DuckDB's `read_parquet`."""
        if self._uri_scheme == "s3":
            return f"s3://{self.root}/{key}"
        return f"{self.root}/{key}"

    # ---- writes ----

    def write_table(self, key: str, table) -> int:
        """Write an Arrow table to `key`. Returns bytes written. Synchronous;
        callers dispatch via asyncio.to_thread."""
        import pyarrow.parquet as pq

        path = self.fs_path(key)
        parent = path.rsplit("/", 1)[0]
        with contextlib.suppress(Exception):  # S3 has no real dirs
            self.fs.create_dir(parent, recursive=True)
        with self.fs.open_output_stream(path) as sink:
            pq.write_table(
                table,
                sink,
                compression="zstd",
                row_group_size=settings.cold_row_group_size,
            )
        info = self.fs.get_file_info(path)
        return info.size or 0

    def delete(self, key: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.fs.delete_file(self.fs_path(key))

    # ---- manifest ----

    async def ensure_manifest(self) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {manifest_qualified()} (
                    measurement TEXT        NOT NULL,
                    kind        TEXT        NOT NULL,
                    object_key  TEXT        NOT NULL,
                    range_start TIMESTAMPTZ NOT NULL,
                    range_end   TIMESTAMPTZ NOT NULL,
                    rows        BIGINT      NOT NULL,
                    min_seq     BIGINT      NOT NULL,
                    max_seq     BIGINT      NOT NULL,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (object_key)
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS {_quote_ident(MANIFEST + '_range_idx')} "
                f"ON {manifest_qualified()} (measurement, range_start, range_end)"
            )

    async def record(self, meta: ObjectMeta) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {manifest_qualified()}
                    (measurement, kind, object_key, range_start, range_end,
                     rows, min_seq, max_seq)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (object_key) DO UPDATE SET
                    range_start = EXCLUDED.range_start,
                    range_end   = EXCLUDED.range_end,
                    rows        = EXCLUDED.rows,
                    min_seq     = EXCLUDED.min_seq,
                    max_seq     = EXCLUDED.max_seq,
                    created_at  = now()
                """,
                meta.measurement,
                meta.kind,
                meta.object_key,
                meta.range_start,
                meta.range_end,
                meta.rows,
                meta.min_seq,
                meta.max_seq,
            )

    async def forget(self, object_key: str) -> None:
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {manifest_qualified()} WHERE object_key = $1", object_key
            )

    async def objects(self, measurement: str, kind: str | None = None) -> list[dict]:
        q = f"SELECT * FROM {manifest_qualified()} WHERE measurement = $1"
        args: list = [measurement]
        if kind is not None:
            q += " AND kind = $2"
            args.append(kind)
        q += " ORDER BY range_start, min_seq"
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]

    async def measurements(self) -> list[str]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT DISTINCT measurement FROM {manifest_qualified()} ORDER BY 1"
            )
        return [r["measurement"] for r in rows]
