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
from .coldstore import ColdStore
from .config import settings
from .engine import DuckEngine
from .lineproto import parse_batch
from .store import Store
from .tiering import Tierer

log = logging.getLogger(__name__)

PRECISIONS = {"ns", "us", "ms", "s"}
TIERS = {"hot", "cold", "all"}


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


class TierRunRequest(BaseModel):
    older_than: str | None = None
    measurement: str | None = None


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

        engine: DuckEngine | None = None
        tierer: Tierer | None = None
        if settings.query_engine_enabled or settings.tier_enabled or settings.write_through:
            try:
                cold = ColdStore.from_settings(store)
                await cold.ensure_manifest()
                store.cold = cold  # enables write-through on the hot write path
                engine = await DuckEngine.connect()
                tierer = Tierer(store, engine, cold)
                await tierer.start()
            except Exception:
                log.exception("cold/tiering engine unavailable; running hot-only")
                store.cold = None
                engine = None
                tierer = None
        state["engine"] = engine
        state["tierer"] = tierer

        log.info(
            "ingest started: pool=%s..%s workers=%s batch=%s/%sms engine=%s tierer=%s",
            settings.ingest_pool_min,
            settings.ingest_pool_max,
            settings.ingest_workers,
            settings.ingest_batch_size,
            settings.ingest_batch_flush_ms,
            engine is not None,
            settings.tier_enabled,
        )
        try:
            yield
        finally:
            if tierer is not None:
                await tierer.stop()
            if engine is not None:
                await engine.close()
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

    async def _run_query(
        sql: str,
        params: list[object],
        tier: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict]:
        if tier not in TIERS:
            raise HTTPException(400, f"invalid tier; expected one of {sorted(TIERS)}")
        if tier == "hot":
            store: Store = state["store"]  # type: ignore[assignment]
            return await store.query_json(sql, params)

        # Federated path (cold-only or hot+cold) via DuckDB.
        engine: DuckEngine | None = state.get("engine")  # type: ignore[assignment]
        tierer: Tierer | None = state.get("tierer")  # type: ignore[assignment]
        if engine is None or tierer is None:
            raise HTTPException(
                503, "federated query engine is disabled (set QUERY_ENGINE_ENABLED=true)"
            )
        if params:
            raise HTTPException(400, "parameterized queries are only supported for tier=hot")
        include_hot = tier == "all"
        include_cold = tier in ("all", "cold")
        # start/end prune which cold objects are scanned — the key TB-scale lever.
        specs = await tierer.view_specs(
            include_hot=include_hot,
            include_cold=include_cold,
            start=_normalize_dt(start),
            end=_normalize_dt(end),
        )
        await engine.register_views(specs)
        return await engine.query_rows(sql)

    @app.post("/api/v1/query")
    async def query(
        req: QueryRequest,
        tier: str = Query("hot"),
        start: datetime | None = Query(None, description="Prune cold objects to time >= start"),
        end: datetime | None = Query(None, description="Prune cold objects to time <= end"),
    ) -> Response:
        try:
            rows = await _run_query(req.sql, req.params, tier, start, end)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"query error: {exc}") from exc
        return Response(
            content=orjson.dumps(rows, default=_default_json), media_type="application/json"
        )

    @app.get("/api/v1/query")
    async def query_get(
        sql: str = Query(...),
        tier: str = Query("hot"),
        start: datetime | None = Query(None),
        end: datetime | None = Query(None),
    ) -> Response:
        try:
            rows = await _run_query(sql, [], tier, start, end)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"query error: {exc}") from exc
        return Response(
            content=orjson.dumps(rows, default=_default_json), media_type="application/json"
        )

    @app.post("/api/v1/tier/run")
    async def tier_run(req: TierRunRequest) -> dict:
        tierer: Tierer | None = state.get("tierer")  # type: ignore[assignment]
        if tierer is None:
            raise HTTPException(503, "tiering is disabled (set QUERY_ENGINE_ENABLED=true)")
        try:
            res = await tierer.run_once(req.older_than, req.measurement)
        except Exception as exc:
            raise HTTPException(400, f"tiering error: {exc}") from exc
        return {
            "compacted": [
                {
                    "measurement": c.measurement,
                    "day": c.day,
                    "object_key": c.object_key,
                    "merged": c.merged,
                    "rows": c.rows,
                }
                for c in res["compacted"]
            ],
            "evicted": [
                {
                    "measurement": e.measurement,
                    "range_start": e.range_start.isoformat(),
                    "range_end": e.range_end.isoformat(),
                }
                for e in res["evicted"]
            ],
        }

    @app.get("/api/v1/tier/status")
    async def tier_status() -> dict:
        tierer: Tierer | None = state.get("tierer")  # type: ignore[assignment]
        if tierer is None:
            raise HTTPException(503, "tiering is disabled (set QUERY_ENGINE_ENABLED=true)")
        return {
            "enabled": settings.tier_enabled,
            "hot_window": settings.tier_hot_window,
            "cold": await tierer.status(),
        }

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
