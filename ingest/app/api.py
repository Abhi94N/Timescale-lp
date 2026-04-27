"""HTTP API for line-protocol write/query/delete."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import orjson
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse, Response
from pydantic import BaseModel, Field

from .batcher import Batcher
from .config import settings
from .lineproto import parse_batch
from .store import Store

log = logging.getLogger(__name__)

PRECISIONS = {"ns", "us", "ms", "s"}


class WriteResult(BaseModel):
    accepted: int
    parse_errors: list[dict] = Field(default_factory=list)
    queued: bool


class QueryRequest(BaseModel):
    sql: str
    params: list[object] = Field(default_factory=list)


class DeleteRequest(BaseModel):
    measurement: str
    tags: dict[str, str] = Field(default_factory=dict)
    start: datetime | None = None
    end: datetime | None = None


def _normalize_dt(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def create_app() -> FastAPI:
    state: dict[str, object] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = await Store.connect()
        batcher = Batcher(store)
        await batcher.start()
        state["store"] = store
        state["batcher"] = batcher
        log.info(
            "ingest started: pool=%s..%s workers=%s batch=%s/%sms",
            settings.ingest_pool_min,
            settings.ingest_pool_max,
            settings.ingest_workers,
            settings.ingest_batch_size,
            settings.ingest_batch_flush_ms,
        )
        try:
            yield
        finally:
            await batcher.stop()
            await store.close()

    app = FastAPI(
        title="timescale-lo ingest",
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict:
        batcher: Batcher = state["batcher"]  # type: ignore[assignment]
        return {
            "ok": True,
            "queue_depth": batcher.queue.qsize(),
            "points_written": batcher.points_written,
            "points_failed": batcher.points_failed,
        }

    @app.post("/api/v1/write")
    async def write(
        request: Request,
        precision: str = Query("ns"),
        sync: bool = Query(False, description="If true, write synchronously instead of queueing."),
    ) -> Response:
        if precision not in PRECISIONS:
            raise HTTPException(400, f"invalid precision; expected one of {sorted(PRECISIONS)}")
        body = (await request.body()).decode("utf-8", errors="replace")
        if not body.strip():
            raise HTTPException(400, "empty body")

        points, errors = parse_batch(body, precision=precision)

        batcher: Batcher = state["batcher"]  # type: ignore[assignment]
        if sync:
            written = await batcher.submit_sync(points)
            payload = WriteResult(
                accepted=written,
                parse_errors=[{"line": ln, "error": err} for ln, err in errors],
                queued=False,
            )
        else:
            await batcher.submit(points)
            payload = WriteResult(
                accepted=len(points),
                parse_errors=[{"line": ln, "error": err} for ln, err in errors],
                queued=True,
            )

        status = 204 if (not errors and not sync) else 200
        if status == 204:
            return Response(status_code=204)
        return ORJSONResponse(payload.model_dump(), status_code=status)

    @app.post("/api/v1/query")
    async def query(req: QueryRequest) -> Response:
        store: Store = state["store"]  # type: ignore[assignment]
        try:
            rows = await store.query_json(req.sql, req.params)
        except Exception as exc:
            raise HTTPException(400, f"query error: {exc}") from exc
        return Response(
            content=orjson.dumps(rows, default=_default_json), media_type="application/json"
        )

    @app.get("/api/v1/query")
    async def query_get(sql: str = Query(...)) -> Response:
        store: Store = state["store"]  # type: ignore[assignment]
        try:
            rows = await store.query_json(sql, [])
        except Exception as exc:
            raise HTTPException(400, f"query error: {exc}") from exc
        return Response(
            content=orjson.dumps(rows, default=_default_json), media_type="application/json"
        )

    @app.post("/api/v1/delete")
    async def delete(req: DeleteRequest) -> dict:
        store: Store = state["store"]  # type: ignore[assignment]
        deleted = await store.delete_lp(
            req.measurement,
            req.tags or None,
            _normalize_dt(req.start),
            _normalize_dt(req.end),
        )
        return {"measurement": req.measurement, "deleted": deleted}

    return app


def _default_json(o: object) -> object:
    if isinstance(o, datetime):
        return o.isoformat()
    if hasattr(o, "isoformat"):
        return o.isoformat()  # type: ignore[no-any-return]
    return str(o)
