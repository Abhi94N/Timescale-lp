.PHONY: help up down logs ps restart rebuild test lint fmt fmt-check dev write query

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
	@echo "  query       - run a sample SQL query"

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
