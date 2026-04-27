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

## Useful TimescaleDB references

- Hypertables: <https://docs.timescale.com/use-timescale/latest/hypertables/>
- `time_bucket`: <https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/>
- Compression: <https://docs.timescale.com/use-timescale/latest/compression/>
- Continuous aggregates: <https://docs.timescale.com/use-timescale/latest/continuous-aggregates/>
- Retention policies: <https://docs.timescale.com/use-timescale/latest/data-retention/>
