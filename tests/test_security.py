import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from security import SecurityMiddleware, SlidingWindowLimiter, load_tokens


def make_app(tokens=None, rate=120):
    app = FastAPI()

    @app.post("/mcp")
    async def mcp(): return {"ok": True}

    @app.get("/")
    async def root(): return {"ok": True}

    @app.get("/health/transcripts")
    async def h(): return {"ok": True}
    app.add_middleware(SecurityMiddleware, tokens=tokens or [], rate_limit_per_minute=rate)
    return TestClient(app)


def test_open_by_default_keeps_existing_clients_working():
    c = make_app()
    assert c.post("/mcp").status_code == 200 and c.get("/health/transcripts").status_code == 200


def test_auth_enforced_when_token_configured():
    c = make_app(["s3cret-token"])
    assert c.post("/mcp").status_code == 401
    assert c.post("/mcp", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert c.post("/mcp", headers={"Authorization": "Bearer s3cret-token"}).status_code == 200
    assert c.post("/mcp", headers={"X-API-Key": "s3cret-token"}).status_code == 200
    assert c.post("/mcp?token=s3cret-token").status_code == 200
    assert c.get("/health/transcripts").status_code == 401


def test_root_health_stays_public_and_preflight_allowed():
    c = make_app(["t"])
    assert c.get("/").status_code == 200
    assert c.options("/mcp").status_code in (200, 405)       # not 401


def test_token_rotation_two_tokens():
    c = make_app(["old", "new"])
    assert c.post("/mcp", headers={"Authorization": "Bearer old"}).status_code == 200
    assert c.post("/mcp", headers={"Authorization": "Bearer new"}).status_code == 200


def test_rate_limit_returns_429_with_retry_after():
    c = make_app(rate=3)
    codes = [c.post("/mcp").status_code for _ in range(5)]
    assert codes == [200, 200, 200, 429, 429]
    r = c.post("/mcp"); assert int(r.headers["retry-after"]) >= 1 and r.json()["error"]["message"]


def test_rate_limit_is_per_client_and_get_not_counted():
    c = make_app(["a", "b"], rate=1)
    assert c.post("/mcp", headers={"Authorization": "Bearer a"}).status_code == 200
    assert c.post("/mcp", headers={"Authorization": "Bearer b"}).status_code == 200
    assert c.post("/mcp", headers={"Authorization": "Bearer a"}).status_code == 429
    for _ in range(5):
        assert c.get("/health/transcripts", headers={"Authorization": "Bearer a"}).status_code == 200


def test_rate_limit_zero_disables():
    c = make_app(rate=0)
    assert all(c.post("/mcp").status_code == 200 for _ in range(10))


def test_limiter_window_slides():
    t = [0.0]
    lim = SlidingWindowLimiter(2, 60, clock=lambda: t[0])
    assert lim.check("k") is None and lim.check("k") is None and lim.check("k") is not None
    t[0] = 61
    assert lim.check("k") is None


def test_load_tokens():
    assert load_tokens({"MCP_AUTH_TOKEN": "a", "MCP_AUTH_TOKENS": "b, c"}) == ["b", "c", "a"]
    assert load_tokens({}) == []
