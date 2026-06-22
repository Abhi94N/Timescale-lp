# syntax=docker/dockerfile:1.7
#
# Multi-stage Dockerfile for the line-protocol ingest microservice.
#
# Stage 1 (builder): install dependencies into an isolated /app/.venv using uv,
# from the project's locked pyproject.toml + uv.lock for reproducible builds.
#
# Stage 2 (runtime): copy that venv plus the application source into a slim
# image and run via uvicorn under uvloop/httptools.
#
# Build standalone:
#   docker build -t timescale-lo-ingest:latest ingest/
#
# Run against an existing TimescaleDB (e.g. one started by docker compose):
#   docker run --rm -p 8080:8080 \
#     -e TSDB_HOST=timescaledb -e TSDB_USER=tsdbadmin -e TSDB_PASSWORD=tsdbpass \
#     -e TSDB_DATABASE=metrics --network timescale-lo_tsnet \
#     timescale-lo-ingest:latest

# ---------- Stage 1: build deps with uv ----------
FROM python:3.12-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Install uv from its official distroless image (small, signed).
COPY --from=ghcr.io/astral-sh/uv:0.5.4 /uv /uvx /usr/local/bin/

# Copy only the dependency manifest first so the layer caches across code changes.
COPY pyproject.toml uv.lock* ./

# Frozen install when the lockfile is present; fall back to a fresh resolution if not.
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f uv.lock ]; then \
        uv sync --no-dev --frozen --no-install-project; \
    else \
        uv sync --no-dev --no-install-project; \
    fi

# Now bring in the application source and install the project itself.
COPY app ./app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-editable

# Pre-download DuckDB's httpfs + postgres extensions into the image so the
# runtime container needs no network to load them (used by the tiering/query
# engine). Stored under the home DuckDB extension dir, copied to runtime.
RUN /app/.venv/bin/python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL postgres')"

# ---------- Stage 2: minimal runtime ----------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/app \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# curl is needed for the HEALTHCHECK; ca-certificates for outbound TLS if ever needed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app && useradd --system --gid app --home /app --shell /usr/sbin/nologin app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --from=builder --chown=app:app /app/app /app/app
# Carry the pre-installed DuckDB extensions (downloaded as root in the builder).
COPY --from=builder --chown=app:app /root/.duckdb /app/.duckdb

USER app

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=10s \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

CMD ["python", "-m", "app.main"]
