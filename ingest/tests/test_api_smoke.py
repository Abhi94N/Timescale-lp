"""
API smoke test using FastAPI's TestClient with the Store/Batcher monkeypatched
to in-memory fakes. Verifies request parsing/dispatch without TimescaleDB.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import api as api_module
from app.lineproto import Point


class FakeStore:
    def __init__(self):
        self.queries: list[tuple[str, list]] = []
        self.deletes: list[tuple] = []

    async def query_json(self, sql, params):
        self.queries.append((sql, params))
        return [{"sql": sql, "params": params}]

    async def delete_lp(self, measurement, tags, start, end):
        self.deletes.append((measurement, tags, start, end))
        return 7

    async def close(self):
        pass


class FakeBatcher:
    def __init__(self, store):
        self.store = store
        self.submitted: list[Point] = []
        self.sync_calls: list[list[Point]] = []
        self.queue = type("Q", (), {"qsize": staticmethod(lambda: 0)})()
        self.points_written = 0
        self.points_failed = 0

    async def start(self):
        pass

    async def stop(self):
        pass

    async def submit(self, points):
        self.submitted.extend(points)

    async def submit_sync(self, points):
        self.sync_calls.append(list(points))
        return len(points)


@pytest.fixture
def client(monkeypatch):
    fake_store = FakeStore()
    fake_batcher_holder: dict = {}

    async def fake_connect():
        return fake_store

    def make_batcher(store):
        b = FakeBatcher(store)
        fake_batcher_holder["b"] = b
        return b

    monkeypatch.setattr(api_module.Store, "connect", staticmethod(fake_connect))
    monkeypatch.setattr(api_module, "Batcher", make_batcher)

    app = api_module.create_app()
    with TestClient(app) as c:
        c.state_store = fake_store
        c.state_batcher = fake_batcher_holder["b"]
        yield c


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True


def test_write_async_returns_204(client):
    body = "cpu,host=a value=1\ncpu,host=b value=2 1700000000000000000\n"
    r = client.post("/api/v1/write", content=body)
    assert r.status_code == 204
    assert len(client.state_batcher.submitted) == 2


def test_write_sync_returns_count(client):
    body = "m v=1\n"
    r = client.post("/api/v1/write?sync=true", content=body)
    assert r.status_code == 200
    payload = r.json()
    assert payload["accepted"] == 1
    assert payload["queued"] is False
    assert len(client.state_batcher.sync_calls) == 1


def test_write_with_parse_errors_reports_them(client):
    body = "good v=1\ngarbage line\n"
    r = client.post("/api/v1/write", content=body)
    assert r.status_code == 200
    payload = r.json()
    assert payload["accepted"] == 1
    assert len(payload["parse_errors"]) == 1


def test_write_invalid_precision(client):
    r = client.post("/api/v1/write?precision=bogus", content="m v=1")
    assert r.status_code == 400


def test_query_post(client):
    r = client.post("/api/v1/query", json={"sql": "SELECT 1", "params": []})
    assert r.status_code == 200
    body = r.json()
    assert body[0]["sql"] == "SELECT 1"
    assert client.state_store.queries == [("SELECT 1", [])]


def test_delete_invokes_store(client):
    r = client.post(
        "/api/v1/delete",
        json={"measurement": "cpu", "tags": {"host": "a"}, "start": "2024-01-01T00:00:00Z"},
    )
    assert r.status_code == 200
    assert r.json() == {"measurement": "cpu", "deleted": 7}
    assert client.state_store.deletes[0][0] == "cpu"
    assert client.state_store.deletes[0][1] == {"host": "a"}
