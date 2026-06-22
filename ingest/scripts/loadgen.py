#!/usr/bin/env python
"""
Synthetic line-protocol load generator.

Drives the ingest service at a configurable scale to validate throughput and
the hot→cold path. The same script scales from a CI smoke (a few million rows)
to terabytes — it's just a bigger `--rows`.

Scale math (rough): a row is ~30-60 B compressed in cold Parquet. So
  1 TB cold  ~  20-30 billion rows.
Run distributed (many `--concurrency`, multiple processes/hosts) to reach that;
a single process here is for correctness + per-core throughput, not to actually
move a TB on a laptop.

Examples:
  # CI smoke: 2M rows across 1k series, then verify the count
  uv run python scripts/loadgen.py --rows 2_000_000 --series 1000 --verify

  # soak: 10 minutes, 64 in-flight batches
  uv run python scripts/loadgen.py --duration 600 --concurrency 64
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def build_batch(
    measurement: str, start_row: int, n: int, series: int, base_ts_ns: int, step_ns: int
) -> tuple[str, int]:
    """Build `n` line-protocol rows. Returns (payload, max_ts_ns).

    Pure + deterministic given inputs, so it's unit-testable without a server.
    """
    lines = []
    ts = base_ts_ns
    for i in range(start_row, start_row + n):
        h = i % series
        ts = base_ts_ns + i * step_ns
        val = (i % 1000) / 10.0
        lines.append(
            f"{measurement},host=h{h},region=r{h % 8} " f"value={val},count={i % 100}i {ts}"
        )
    return "\n".join(lines), ts


def _post(url: str, data: bytes, content_type: str) -> bytes:
    req = urllib.request.Request(url, data=data, headers={"content-type": content_type})
    with urllib.request.urlopen(req, timeout=60.0) as resp:
        return resp.read()


def _post_batch(url: str, payload: str) -> None:
    _post(url, payload.encode(), "text/plain")


def run(args) -> None:
    write_url = f"{args.url}/api/v1/write"
    base_ts_ns = args.base_ts_ns or (time.time_ns() - args.rows * args.step_ns)

    def batches():
        row = 0
        deadline = time.monotonic() + args.duration if args.duration else None
        while True:
            if args.duration is None and row >= args.rows:
                return
            if deadline is not None and time.monotonic() >= deadline:
                return
            payload, _ = build_batch(
                args.measurement, row, args.batch, args.series, base_ts_ns, args.step_ns
            )
            row += args.batch
            yield payload

    sent = 0
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures: set = set()
        for payload in batches():
            futures.add(pool.submit(_post_batch, write_url, payload))
            sent += args.batch
            if len(futures) >= args.concurrency:
                done = next(as_completed(futures))
                done.result()
                futures.discard(done)
        for f in as_completed(futures):
            f.result()

    dt = time.monotonic() - t0
    rate = sent / dt if dt else 0
    print(f"sent {sent:,} rows in {dt:.1f}s  =>  {rate:,.0f} rows/s")

    if args.verify:
        body = json.dumps({"sql": f"SELECT count(*) AS n FROM lp.{args.measurement}"}).encode()
        out = _post(f"{args.url}/api/v1/query", body, "application/json")
        print(f"hot count: {out.decode()}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://localhost:8080")
    p.add_argument("--measurement", default="load")
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--series", type=int, default=1000, help="distinct host tag values")
    p.add_argument("--batch", type=int, default=5000, help="rows per HTTP request")
    p.add_argument("--concurrency", type=int, default=16, help="in-flight requests")
    p.add_argument("--step-ns", type=int, default=1_000_000, help="ns between consecutive rows")
    p.add_argument("--base-ts-ns", type=int, default=0, help="0 = derive from now")
    p.add_argument("--duration", type=int, default=0, help="seconds; overrides --rows when >0")
    p.add_argument("--verify", action="store_true")
    args = p.parse_args(argv)
    if args.duration == 0:
        args.duration = None
    if args.base_ts_ns == 0:
        args.base_ts_ns = None
    return args


if __name__ == "__main__":
    run(parse_args())
