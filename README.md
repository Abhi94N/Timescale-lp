# timescale-lp

A small, performance-focused stack for ingesting **InfluxDB line protocol**
into **TimescaleDB hypertables**, with **Grafana** pre-wired for querying.

```
+-----------------+        line protocol        +-----------------+
|  Producers /    | ---------------------------> |  ingest (Py)    |
|  Telegraf / curl|        HTTP POST /write      |  FastAPI +      |
+-----------------+                              |  asyncpg COPY   |
                                                 +--------+--------+
                                                          | binary COPY
                                                          v
                                                 +-----------------+
                                                 |   TimescaleDB   |
                                                 |   (hypertables) |
                                                 +--------+--------+
                                                          ^
                                                          | SQL
                                                 +--------+--------+
                                                 |     Grafana     |
                                                 +-----------------+
```

The microservice speaks a small **InfluxDB-compatible HTTP API**:

| Verb | Path                 | Purpose                                          |
| ---- | -------------------- | ------------------------------------------------ |
| POST | `/api/v1/write`      | Ingest line protocol (and **update** rows)       |
| POST | `/api/v1/query`      | Run SQL, get JSON (`?tier=hot\|cold\|all`)        |
| GET  | `/api/v1/query`      | Run SQL via querystring (`?tier=…`)              |
| POST | `/api/v1/delete`     | Delete by measurement/tags/time                  |
| POST | `/api/v1/tier/run`   | Move chunks older than a window to cold storage  |
| GET  | `/api/v1/tier/status`| Cold-storage manifest summary                    |
| GET  | `/health`            | Liveness + queue/throughput stats                |

Re-sending the same `(measurement, tag-set, timestamp)` performs an **UPSERT**
(via `ON CONFLICT (time, tag_hash) DO UPDATE`), which is how line protocol
expresses updates in this service — matching InfluxDB semantics.

**Tiered storage:** TimescaleDB is the hot cache; data older than
`TIER_HOT_WINDOW` is aged out to **Parquet on S3/MinIO** and queried
transparently via DuckDB with `?tier=all`. See
[the tiered-storage section](docs/timescaledb.md#tiered-storage--timescaledb-hot--parquet-on-s3minio-cold).

---

> Want to use just the database (no microservice) or just DB + Grafana? See
> [`docs/timescaledb.md`](docs/timescaledb.md) for a layered walkthrough.

## Repository layout

```
.
├── docker-compose.yml          # TimescaleDB + ingest + Grafana + MinIO
├── docs/timescaledb.md         # Layered walkthrough + tiered-storage guide
├── init-db/                    # SQL run on first DB boot (creates extension + schema)
├── grafana/provisioning/       # Auto-wires the TimescaleDB datasource + dashboards
├── grafana/dashboards/         # Starter dashboards loaded by provisioning
├── ingest/                     # Python microservice
│   ├── app/
│   │   ├── api.py              # FastAPI routes
│   │   ├── batcher.py          # Per-worker batching writer
│   │   ├── config.py           # pydantic-settings
│   │   ├── lineproto.py        # Line protocol parser
│   │   ├── store.py            # Schema mgmt + binary COPY upserts
│   │   ├── engine.py           # DuckDB: PG↔Parquet export + federated query
│   │   ├── tiering.py          # Hot→cold tierer + manifest
│   │   └── main.py             # uvicorn entrypoint
│   ├── tests/
│   ├── pyproject.toml          # uv + ruff config
│   └── Dockerfile              # uv-based slim image
├── .github/workflows/ci.yml    # ruff + pytest + docker build + integration smoke
├── Makefile
└── .env.example
```

---

## Quick start (Docker Compose)

Bring everything up:

```bash
cp .env.example .env             # optional — defaults are sensible
make up                          # docker compose up -d --build
make ps
```

Services:

| Service       | URL                          | Notes                                      |
| ------------- | ---------------------------- | ------------------------------------------ |
| Ingest API    | http://localhost:8080        | `/health`, `/api/v1/*`                     |
| Grafana       | http://localhost:3000        | login `admin` / `admin` (overridable)      |
| TimescaleDB   | postgres://localhost:5432    | `tsdbadmin` / `tsdbpass` / db `metrics`    |
| MinIO console | http://localhost:9001        | `minioadmin` / `minioadmin`; cold Parquet  |

Tear down:

```bash
make down                        # stop, keep volumes
make reset                       # stop and delete volumes
```

---

## Sending data — InfluxDB line protocol

### Write (asynchronous, batched)

```bash
curl -X POST 'http://localhost:8080/api/v1/write' \
  --data-binary $'cpu,host=a,region=us value=0.5,count=1i 1700000000000000000
cpu,host=b,region=us value=0.7,count=2i 1700000000000000000'
```

Default precision is **nanoseconds**. Override with `?precision=ns|us|ms|s`.
The default mode queues into the in-process batcher and returns `204 No Content`.

### Write (synchronous — wait for the COPY to land)

```bash
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a value=0.9'
# {"accepted":1,"parse_errors":[],"queued":false}
```

### Update via line protocol

Re-sending the same point upserts the row:

```bash
# initial
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a value=0.5 1700000000000000000'

# update (same measurement+tags+timestamp -> overwrites fields)
curl -X POST 'http://localhost:8080/api/v1/write?sync=true' \
  --data-binary 'cpu,host=a value=0.9,count=42i 1700000000000000000'
```

The conflict key is `(time, tag_hash)` where `tag_hash` is a stable
BLAKE2b digest of the tag set.

### Delete

```bash
curl -X POST http://localhost:8080/api/v1/delete \
  -H 'content-type: application/json' \
  -d '{
        "measurement": "cpu",
        "tags": {"host": "a"},
        "start": "2024-01-01T00:00:00Z",
        "end":   "2024-12-31T23:59:59Z"
      }'
```

### Query — SQL over the hypertables

Each measurement becomes a hypertable in the `lp` schema, with columns
`time`, `tag_hash`, `t_<tag>`, `f_<field>`.

```bash
# POST
curl -X POST http://localhost:8080/api/v1/query \
  -H 'content-type: application/json' \
  -d '{"sql": "SELECT time, t_host AS host, f_value AS value FROM lp.cpu ORDER BY time DESC LIMIT 10"}'

# GET
curl -G --data-urlencode \
  'sql=SELECT time_bucket('"'"'1 minute'"'"', time) AS bucket, avg(f_value) FROM lp.cpu GROUP BY bucket ORDER BY bucket DESC LIMIT 5' \
  http://localhost:8080/api/v1/query
```

Or query Postgres directly:

```bash
docker exec -it timescale-lo-db psql -U tsdbadmin -d metrics
metrics=# \dt lp.*
metrics=# SELECT * FROM lp.cpu ORDER BY time DESC LIMIT 5;
```

### Grafana

Visit http://localhost:3000 and pick the pre-provisioned **TimescaleDB**
datasource — the URL, credentials, and TimescaleDB awareness flag are wired
in `grafana/provisioning/datasources/timescaledb.yml`. Try a query like:

```sql
SELECT
  time_bucket('1 minute', time) AS time,
  t_host AS host,
  avg(f_value) AS value
FROM lp.cpu
WHERE $__timeFilter(time)
GROUP BY 1, 2
ORDER BY 1
```

---

## Local development (no Docker for the ingest service)

You still need Postgres+Timescale. The simplest mix is to run TimescaleDB in
Docker and the ingest service from your shell against it.

### 1. Install [`uv`](https://github.com/astral-sh/uv)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Start only TimescaleDB

```bash
docker compose up -d timescaledb
```

### 3. Run the ingest service from source

```bash
cd ingest
uv sync --extra dev
TSDB_HOST=localhost \
TSDB_PORT=5432 \
TSDB_USER=tsdbadmin \
TSDB_PASSWORD=tsdbpass \
TSDB_DATABASE=metrics \
uv run python -m app.main
```

…or just `make dev` from the repo root.

### Tests + linters

```bash
make test        # pytest
make lint        # ruff format --check + ruff check
make fmt         # ruff format + ruff check --fix
```

---

## Configuration (env vars)

All have sensible defaults. The most useful tuning knobs:

| Variable                  | Default     | Effect                                           |
| ------------------------- | ----------- | ------------------------------------------------ |
| `TSDB_HOST` / `TSDB_PORT` | depends     | Where the ingest service finds Postgres          |
| `TSDB_USER` / `TSDB_PASSWORD` / `TSDB_DATABASE` | `tsdbadmin` / `tsdbpass` / `metrics` | Credentials |
| `TSDB_SCHEMA`             | `lp`        | Schema holding line-protocol hypertables         |
| `INGEST_HTTP_ADDR`        | `:8080`     | HTTP bind address                                |
| `INGEST_BATCH_SIZE`       | `5000`      | Max points per COPY batch                        |
| `INGEST_BATCH_FLUSH_MS`   | `500`       | Flush even if the batch isn't full               |
| `INGEST_WORKERS`          | `8`         | Concurrent COPY workers                          |
| `INGEST_POOL_MIN/MAX`     | `4` / `32`  | asyncpg connection pool bounds                   |
| `INGEST_CHUNK_INTERVAL`   | `1 day`     | Hypertable chunk size                            |
| `INGEST_COMPRESSION_AFTER`| `7 days`    | Auto-compression policy threshold                |

---

## Why this is fast

The hot path is built around the patterns TimescaleDB recommends for
high-throughput ingest:

1. **Binary `COPY`** via `asyncpg.copy_records_to_table` instead of row-by-row
   `INSERT`s — this is the largest single throughput win.
2. **Per-worker batching** with size + time triggers — small clients get low
   latency, bulk producers get full batches.
3. **Connection pool** with prepared-statement cache and `synchronous_commit=off`
   for ingest-friendly durability.
4. **Hypertable chunking** sized to your interval (`INGEST_CHUNK_INTERVAL`)
   for chunk-exclusion query performance.
5. **Native compression** with `tag_hash` segment-by — older chunks are
   transparently compressed (~10–20× space savings, faster scans).
6. **`uvloop` + `httptools` + `orjson`** under FastAPI for low-overhead HTTP.
7. **Schema cache** — column lookups for the steady-state path never hit the DB.
8. **Dedicated upsert key** (`(time, tag_hash)`) so `ON CONFLICT` resolution
   stays an index seek, no matter how many tags appear.

---

## CI/CD

`.github/workflows/ci.yml` runs on every push and pull request:

| Job              | What it does                                              |
| ---------------- | --------------------------------------------------------- |
| `lint-and-test`  | `uv sync` → `ruff format --check` → `ruff check` → `pytest` |
| `integration`    | `docker compose up`, write a sample, query it, tear down  |
| `docker-build`   | Build the ingest image with Buildx (no push)              |

Add a `docker/login-action` + `push: true` step to the `docker-build` job
to publish to a registry.

---

## Troubleshooting

- **`/health` reports growing `points_failed`:** check `docker compose logs ingest`
  for COPY errors. Usually a type mismatch — e.g. a field that started as `BIGINT`
  receiving a string.
- **Hypertable not auto-created:** make sure the `timescaledb` extension is
  installed (the init script does this; logs show `loaded TimescaleDB`).
- **Slow inserts:** raise `INGEST_BATCH_SIZE`, `INGEST_WORKERS`, and
  `INGEST_POOL_MAX`. Inspect `pg_stat_activity` for lock contention.
