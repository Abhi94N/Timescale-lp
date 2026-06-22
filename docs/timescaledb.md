# Stack walkthrough — TimescaleDB → Grafana → line-protocol ingest

This doc builds the stack up in three layers so you can use any layer on its
own. The image versions used:

| Component   | Image                                | Notes                                |
| ----------- | ------------------------------------ | ------------------------------------ |
| Database    | `timescale/timescaledb:latest-pg17`  | PostgreSQL 17 + latest TimescaleDB    |
| Dashboards  | `grafana/grafana:latest`             | Auto-provisioned datasource + dashboard |
| Ingest      | local `./ingest` build (Python 3.12) | InfluxDB-compatible line protocol     |

The layers:

1. **DB only** — TimescaleDB by itself, ingest and query directly with `psql`
   or any Postgres client.
2. **DB + Grafana** — same DB, plus Grafana auto-wired as a viewer.
3. **DB + Grafana + ingest** — full stack, write line protocol over HTTP.

Layer 3 is what `docker compose up` brings up by default in this repo. To use
just layer 1 or layer 2, start the specific services.

---

## Layer 1 — TimescaleDB on its own

### Start

```bash
docker compose up -d timescaledb
docker compose ps timescaledb
docker compose logs -f timescaledb         # watch for "database system is ready"
```

The container runs `init-db/00-init.sql` on first boot, which creates the
`timescaledb` extension, `pg_stat_statements`, and an `lp` schema (used by the
ingest service). Connection details come from `docker-compose.yml` /
`.env`:

```
host:     localhost
port:     5432           (override with POSTGRES_PORT)
user:     tsdbadmin      (override with POSTGRES_USER)
password: tsdbpass       (override with POSTGRES_PASSWORD)
database: metrics        (override with POSTGRES_DB)
```

### Connect

```bash
docker exec -it timescale-lo-db psql -U tsdbadmin -d metrics
```

```sql
\dx                          -- confirm timescaledb is loaded
SELECT default_version, installed_version FROM pg_available_extensions WHERE name='timescaledb';
```

### Create your own hypertable + ingest

This is the "do it without the line-protocol service" path:

```sql
CREATE TABLE sensors (
    time        TIMESTAMPTZ      NOT NULL,
    sensor_id   INT              NOT NULL,
    location    TEXT             NOT NULL,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION
);

SELECT create_hypertable('sensors', 'time', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX ON sensors (sensor_id, time DESC);

-- Optional: enable native compression on chunks older than 7 days
ALTER TABLE sensors SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('sensors', INTERVAL '7 days');

-- Optional: drop chunks older than 90 days
SELECT add_retention_policy('sensors', INTERVAL '90 days');
```

Insert a few rows:

```sql
INSERT INTO sensors (time, sensor_id, location, temperature, humidity)
VALUES (now(),                  1, 'kitchen', 21.4, 47.2),
       (now() - INTERVAL '1m',  1, 'kitchen', 21.5, 47.0),
       (now() - INTERVAL '2m',  2, 'lab',     19.9, 31.5);
```

Bulk-load from a CSV (the fast path):

```bash
docker exec -i timescale-lo-db \
  psql -U tsdbadmin -d metrics \
  -c "\COPY sensors (time,sensor_id,location,temperature,humidity) FROM STDIN WITH (FORMAT csv, HEADER true)" \
  < ./my-data.csv
```

### Query

```sql
-- raw recent data
SELECT * FROM sensors ORDER BY time DESC LIMIT 10;

-- 1-minute downsampled view, last hour
SELECT
  time_bucket('1 minute', time) AS bucket,
  sensor_id,
  avg(temperature) AS temp,
  avg(humidity)    AS humidity
FROM sensors
WHERE time > now() - INTERVAL '1 hour'
GROUP BY bucket, sensor_id
ORDER BY bucket DESC;

-- inspect chunks
SELECT show_chunks('sensors');
SELECT * FROM chunks_detailed_size('sensors');
```

### Continuous aggregates (built-in materialized rollups)

```sql
CREATE MATERIALIZED VIEW sensors_1m
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time) AS bucket,
  sensor_id,
  avg(temperature) AS temp,
  avg(humidity)    AS humidity
FROM sensors
GROUP BY bucket, sensor_id;

SELECT add_continuous_aggregate_policy('sensors_1m',
  start_offset => INTERVAL '1 hour',
  end_offset   => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute');
```

Now `SELECT * FROM sensors_1m` is served from a precomputed table.

### Tear down (just this layer)

```bash
docker compose stop timescaledb
docker compose rm -f timescaledb
# nuke the volume too:
docker compose down -v
```

---

## Layer 2 — TimescaleDB + Grafana

```bash
docker compose up -d timescaledb grafana
```

Grafana comes up at http://localhost:3000 (`admin` / `admin` by default).

The datasource is **already provisioned** by
`grafana/provisioning/datasources/timescaledb.yml` — open Grafana →
*Connections* → *Data sources* → **TimescaleDB**. The `timescaledb: true`
flag makes Grafana surface TimescaleDB-specific helpers like
`time_bucket()`.

A starter dashboard ships in `grafana/dashboards/cpu-overview.json` (it
expects rows in `lp.cpu` from the ingest service). Build your own against
the `sensors` table from layer 1:

```sql
SELECT
  time_bucket('1 minute', time) AS time,
  location AS metric,
  avg(temperature) AS value
FROM sensors
WHERE $__timeFilter(time)
GROUP BY 1, 2
ORDER BY 1
```

The `$__timeFilter(time)` macro is Grafana's time-range filter — it expands
to a `BETWEEN` over the dashboard's time picker.

---

## Type handling — line protocol → TimescaleDB columns

The ingest service is **schemaless on input** but materializes **typed
columns** on the underlying hypertables. The mapping is deterministic.

### 1. Line protocol → Python (parsing)

| Line protocol value     | Example         | Parsed Python type |
| ----------------------- | --------------- | ------------------ |
| `<n>i`                  | `count=3i`      | `int`              |
| `<n>u`                  | `count=3u`      | `int`              |
| Numeric, suffix-free    | `value=12`      | `float`            |
| Decimal                 | `value=1.5`     | `float`            |
| `t` / `true` …          | `ok=true`       | `bool`             |
| `f` / `false` …         | `ok=false`      | `bool`             |
| Quoted string           | `msg="hi"`      | `str`              |

### 2. Python → Postgres column type (first sight)

| Python type | Postgres column type |
| ----------- | -------------------- |
| `bool`      | `BOOLEAN`            |
| `int`       | `BIGINT`             |
| `float`     | `DOUBLE PRECISION`   |
| `str`       | `TEXT`               |

**Tags are always `TEXT`.**

### 3. Type widening (auto-promotion)

If a field column already exists but a newer point would not fit, the
column is **promoted up a strict lattice**:

```
BOOLEAN  <  BIGINT  <  DOUBLE PRECISION  <  TEXT
```

The promotion runs as `ALTER TABLE ... ALTER COLUMN ... TYPE ... USING ...`
in the same transaction that adds other columns for the batch.

| Existing column type | New value          | Action                                                        |
| -------------------- | ------------------ | ------------------------------------------------------------- |
| `BIGINT`             | `1.5` (float)      | Promote to `DOUBLE PRECISION` (`USING col::double precision`) |
| `BIGINT`             | `"hello"` (string) | Promote to `TEXT` (`USING col::text`)                         |
| `BOOLEAN`            | `42` (int)         | Promote to `BIGINT` (`USING col::int::bigint`)                |
| `DOUBLE PRECISION`   | `42` (int)         | No change — int fits, coerced to `42.0`                       |
| `TEXT`               | anything           | No change — value coerced to `str(...)`                       |

Properties: promotions are **monotonic** (never demote), `TEXT` is
**sticky**, the **widest sample in a batch** drives a single promotion
before COPY, and the change runs under the per-measurement lock. Cost:
`ALTER COLUMN ... TYPE` rewrites every chunk for the table, so treat
promotions on large measurements as a real operation. To opt out (enforce
a strict schema), pre-create the hypertable + columns and the service will
use them as-is. Promotions are visible in the ingest logs:

```
... INFO app.store widening cpu.f_value: BIGINT -> DOUBLE PRECISION (line-protocol promotion)
```

---

## Building the ingest service image standalone

The microservice has its own multi-stage Dockerfile at
[`ingest/Dockerfile`](../ingest/Dockerfile). It uses [`uv`](https://github.com/astral-sh/uv)
to install from `pyproject.toml` + `uv.lock` (reproducible), then copies the
resulting venv and application into a slim runtime layer that runs as a
non-root user.

Build it on its own (no compose needed):

```bash
docker build -t timescale-lo-ingest:latest ingest/
```

Run it against an externally-managed TimescaleDB:

```bash
docker run --rm -p 8080:8080 \
  -e TSDB_HOST=<your-db-host> \
  -e TSDB_PORT=5432 \
  -e TSDB_USER=tsdbadmin \
  -e TSDB_PASSWORD=tsdbpass \
  -e TSDB_DATABASE=metrics \
  timescale-lo-ingest:latest
```

If your TimescaleDB is the one launched by this repo's `docker compose`, join
its network:

```bash
docker network ls | grep tsnet                            # find the network
docker run --rm -p 8080:8080 --network timescale-lo_tsnet \
  -e TSDB_HOST=timescaledb -e TSDB_USER=tsdbadmin \
  -e TSDB_PASSWORD=tsdbpass -e TSDB_DATABASE=metrics \
  timescale-lo-ingest:latest
```

Then write line protocol exactly as in layer 3 below.

---

## Layer 3 — Add the line-protocol ingest service

```bash
docker compose up -d                      # brings up all three
curl -fsS http://localhost:8080/health
```

Send InfluxDB line protocol over HTTP — the service auto-creates
`lp.<measurement>` hypertables and upserts on `(time, tag_hash)`:

```bash
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary $'cpu,host=a,region=us value=0.5,count=1i 1700000000000000000
cpu,host=b,region=us value=0.7,count=2i 1700000000000000000'
```

Then query — either through the service or straight against Postgres:

```bash
# through the service
curl -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT count(*) FROM lp.cpu"}'

# directly
docker exec -it timescale-lo-db psql -U tsdbadmin -d metrics \
  -c 'SELECT * FROM lp.cpu ORDER BY time DESC LIMIT 5;'
```

Update via line protocol — re-send the same `(measurement, tags, time)`
and the row is overwritten:

```bash
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a,region=us value=0.95,count=99i 1700000000000000000'
```

Delete via the API:

```bash
curl -X POST http://localhost:8080/api/v1/delete \
  -H 'content-type: application/json' \
  -d '{"measurement":"cpu","tags":{"host":"a"}}'
```

The starter dashboard "CPU overview" (auto-loaded under
`grafana/dashboards/cpu-overview.json`) renders these rows.

---

## How the layers compose

```
┌───────────────────────────────────────────────────────────────┐
│ Layer 3: full stack                                           │
│   docker compose up -d                                        │
│   ├─ timescaledb  (PG17 + Timescale)                          │
│   ├─ ingest       (FastAPI + asyncpg, line protocol)          │
│   └─ grafana      (auto-provisioned)                          │
├───────────────────────────────────────────────────────────────┤
│ Layer 2: DB + Grafana                                         │
│   docker compose up -d timescaledb grafana                    │
├───────────────────────────────────────────────────────────────┤
│ Layer 1: DB only                                              │
│   docker compose up -d timescaledb                            │
│   psql / your driver of choice                                │
└───────────────────────────────────────────────────────────────┘
```

Every layer reuses the same volume (`tsdb_data`), so data persists across
layer changes — start with layer 1, add Grafana later, add the ingest
service later still without re-creating tables.

---

## Tiered storage — TimescaleDB as a write-through cache over a Parquet lakehouse

TimescaleDB is a **write-through cache**: every write lands in TimescaleDB
*and* is durably persisted to **Parquet on S3-compatible storage** (MinIO in
the compose stack) before it is acknowledged. Cold storage is the **source of
truth**, so TimescaleDB holds only recent data and can be evicted at any time
with zero data-loss risk. Queries transparently span both tiers.

```
   write (line protocol)
          │
          ▼  (1) staging Parquet         (2) COPY upsert
   ┌──────────────┐  ───────────────▶ S3 ◀───────────────  ┌──────────────┐
   │   batcher    │   durable cold                          │ TimescaleDB  │
   └──────────────┘   (source of truth)                     │ (hot cache)  │
          │  ack only after BOTH (1) and (2) succeed        └──────┬───────┘
          ▼                                                        │
   compaction: staging ─▶ big day-partitioned cold objects        │
   eviction:   drop hot chunks already covered by cold            │
                                                                   │
              DuckDB federation (dedup by seq, range-pruned)       │
   /api/v1/query?tier=all  ◀────────── S3 cold ─────────  hot ◀────┘
```

### Write-through path

For each flushed batch the service:

1. Builds a typed Arrow table and writes one **staging Parquet object**
   (`<m>/staging/<minSeq>-<maxSeq>.parquet`) to S3 — durable in cold.
2. `COPY`-upserts the same rows into the TimescaleDB hypertable.
3. **Acks only after both succeed.** If step 2 fails, the staging object is a
   harmless lower-`seq` copy that a retry supersedes — no data is lost.

Every row carries a monotonic **`seq`**. The hot table keeps the latest `seq`
per `(time, tag_hash)` (upsert); cold is append-only and keeps every version.

### Compaction + eviction (`POST /api/v1/tier/run`, or the background loop)

- **Compaction** merges many small staging objects into large, day-partitioned,
  deduped cold objects (`<m>/cold/dt=YYYY-MM-DD/…parquet`) — this is what keeps
  the cold tier efficient at TB scale.
- **Eviction** drops TimescaleDB chunks older than `TIER_HOT_WINDOW` once cold
  fully covers them (it always does under write-through), reclaiming cache
  space with **no data movement**.

### Reads — dedup + partition pruning

Federated reads (`tier=all`/`cold`) go through DuckDB, which exposes each
measurement as a view that unions hot + cold and **dedups by `(time, tag_hash)`
keeping the highest `seq`** — so an update applied while hot is reflected even
after the older version was archived to cold.

| `tier`          | Source                | Engine   | Notes                                                  |
| --------------- | --------------------- | -------- | ------------------------------------------------------ |
| `hot` (default) | TimescaleDB only      | Postgres | Full Timescale SQL (`time_bucket`, hyperfunctions); supports `params` |
| `cold`          | Parquet only          | DuckDB   | Historical, deduped                                    |
| `all`           | TimescaleDB ∪ Parquet | DuckDB   | Transparent, deduped across tiers                      |

Pass `start`/`end` to prune which cold objects are scanned — the key
TB-scale read lever, since it skips Parquet objects whose time range doesn't
overlap the query:

```bash
curl -G 'http://localhost:8080/api/v1/query' \
  --data-urlencode 'tier=all' \
  --data-urlencode 'start=2026-06-01T00:00:00Z' \
  --data-urlencode 'end=2026-06-02T00:00:00Z' \
  --data-urlencode 'sql=SELECT count(*) FROM lp.cpu'
```

### Try it

```bash
# write a point dated in the past (write-through persists it to cold immediately)
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a value=0.5 1577836800000000000'   # 2020-01-01

# update it (same series+timestamp, new value)
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a value=0.9 1577836800000000000'

# compact staging + evict chunks older than 1 hour (data already in cold)
curl -X POST http://localhost:8080/api/v1/tier/run \
  -H 'content-type: application/json' -d '{"older_than":"1 hour"}'

# gone from the hot cache ...
curl -s -X POST 'http://localhost:8080/api/v1/query?tier=hot' \
  -H 'content-type: application/json' -d '{"sql":"SELECT count(*) n FROM lp.cpu"}'   # [{"n":0}]

# ... still served from cold, and the update (0.9) wins via seq dedup
curl -s -X POST 'http://localhost:8080/api/v1/query?tier=all' \
  -H 'content-type: application/json' -d '{"sql":"SELECT f_value FROM lp.cpu WHERE t_host='"'"'a'"'"'"}'
# [{"f_value": 0.9}]

curl -s http://localhost:8080/api/v1/tier/status   # manifest summary (staging + cold)
```

Browse cold objects in the MinIO console at <http://localhost:9001>
(`minioadmin` / `minioadmin`).

### Scaling to terabytes

The path is built to stay memory-bounded and query-efficient as cold grows:

- **Streaming everywhere** — staging is written per batch; compaction and
  eviction are `COPY`/`drop_chunks` operations that never materialize a chunk
  in Python.
- **Right-sized cold files** — compaction targets large day-partitioned
  Parquet (`COLD_COMPACT_TARGET_BYTES`, default 256 MB) so a TB is thousands of
  files, not millions.
- **Pruned reads** — `start`/`end` skip non-overlapping objects, and DuckDB
  applies Parquet row-group statistics pushdown within the rest.
- **Cheap eviction** — write-through means eviction is a metadata drop, not a
  data copy.

Validate with the bundled load generator (stdlib-only, ships in the image):

```bash
# a few million rows (CI scale), then verify the hot count
docker compose exec ingest python scripts/loadgen.py \
  --rows 2_000_000 --series 2000 --concurrency 32 --verify

# crank up for a real soak (distribute across hosts/processes for a true TB)
docker compose exec ingest python scripts/loadgen.py --duration 600 --concurrency 64
```

Rough sizing: a row is ~30–60 B in compacted Parquet, so **1 TB cold ≈ 20–30
billion rows**. Reach that by running the generator distributed; a single
process measures per-core throughput and correctness, not raw TB movement.

### Configuration

| Variable                   | Default              | Effect                                              |
| -------------------------- | -------------------- | --------------------------------------------------- |
| `WRITE_THROUGH`            | `true`               | Persist every batch to cold before acking           |
| `QUERY_ENGINE_ENABLED`     | `true`               | Enables the DuckDB federated query + tiering engine |
| `TIER_ENABLED`             | `false`              | Runs the background compaction+eviction loop         |
| `TIER_HOT_WINDOW`          | `7 days`             | Data younger than this stays in TimescaleDB         |
| `TIER_INTERVAL_SECONDS`    | `3600`               | Background maintenance cadence                       |
| `COLD_COMPACT_TARGET_BYTES`| `268435456` (256 MB) | Target compacted-object size                         |
| `S3_ENDPOINT`              | `http://minio:9000`  | S3 endpoint (swap for R2/S3/B2 in prod)             |
| `S3_BUCKET`                | `timescale-cold`     | Cold object bucket                                   |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin`  | S3 credentials                                       |

### Operational notes

- **Crash safety**: staging keys are deterministic and the manifest upserts;
  an interrupted pass simply redoes the unfinished object.
- **Source of truth**: cold (S3) is authoritative. To rebuild the cache, read
  cold Parquet with DuckDB (or any reader) and `COPY` it back into a hypertable.
- **Compression**: cold Parquet is zstd; TimescaleDB native compression still
  applies to hot chunks.
- **Multi-replica**: `seq` is process-monotonic. Running multiple ingest
  replicas needs a shared sequence source (e.g. a Postgres sequence) so seq
  ordering is global — documented as a follow-up.
- **Extensions**: the image pre-installs DuckDB's `httpfs` + `postgres`
  extensions, so the runtime container needs no network to load them.

---

## Useful TimescaleDB references

- Hypertables: <https://docs.timescale.com/use-timescale/latest/hypertables/>
- `time_bucket`: <https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/>
- Compression: <https://docs.timescale.com/use-timescale/latest/compression/>
- Continuous aggregates: <https://docs.timescale.com/use-timescale/latest/continuous-aggregates/>
- Retention policies: <https://docs.timescale.com/use-timescale/latest/data-retention/>
