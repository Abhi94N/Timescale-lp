.PHONY: help up down reset logs ps restart rebuild test lint fmt fmt-check dev write query tier tier-status query-all loadtest

help:
	@echo "Targets:"
	@echo "  up          - start the full docker-compose stack"
	@echo "  down        - stop the stack (keep volumes)"
	@echo "  reset       - stop the stack and delete volumes"
	@echo "  logs        - tail logs from all services"
	@echo "  ps          - list service status"
	@echo "  rebuild     - rebuild ingest image and restart"
	@echo "  dev         - run the ingest service locally with uv"
	@echo "  test        - run pytest in ingest/"
	@echo "  lint        - ruff lint + format check"
	@echo "  fmt         - ruff format + autofix"
	@echo "  write       - send a sample line-protocol payload"
	@echo "  query       - run a sample SQL query (hot tier)"
	@echo "  query-all   - sample query across hot + cold (federated)"
	@echo "  tier        - force a compaction + eviction pass (older_than 1 hour)"
	@echo "  tier-status - show the cold-storage manifest summary"
	@echo "  loadtest    - run the synthetic load generator (a few million rows)"

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

ps:
	docker compose ps

rebuild:
	docker compose up -d --build ingest

dev:
	cd ingest && uv sync --extra dev && uv run python -m app.main

test:
	cd ingest && uv run pytest

lint:
	cd ingest && uv run ruff format --check . && uv run ruff check .

fmt:
	cd ingest && uv run ruff format . && uv run ruff check . --fix

write:
	curl -fsS -X POST 'http://localhost:8080/api/v1/write?sync=true' \
	  --data-binary $$'cpu,host=a,region=us value=0.5,count=1i 1700000000000000000\ncpu,host=b value=0.7'

query:
	curl -fsS -X POST http://localhost:8080/api/v1/query \
	  -H 'content-type: application/json' \
	  -d '{"sql":"SELECT count(*) AS n FROM lp.cpu"}'

# Force a tiering pass for anything older than 1 hour (demo).
tier:
	curl -fsS -X POST http://localhost:8080/api/v1/tier/run \
	  -H 'content-type: application/json' \
	  -d '{"older_than":"1 hour"}'

tier-status:
	curl -fsS http://localhost:8080/api/v1/tier/status

# Count rows across hot + cold via the federated DuckDB path.
query-all:
	curl -fsS -X POST 'http://localhost:8080/api/v1/query?tier=all' \
	  -H 'content-type: application/json' \
	  -d '{"sql":"SELECT count(*) AS n FROM lp.cpu"}'

# Synthetic load test (a few million rows) against the running stack.
loadtest:
	docker compose exec ingest python scripts/loadgen.py \
	  --rows 2000000 --series 2000 --batch 10000 --concurrency 32 --verify
