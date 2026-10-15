"""Queue security regression tests (OFFLINE; Upstash/Render/YouTube are mocks)."""
import asyncio
import json
import logging

import pytest
from starlette.testclient import TestClient

from tests.queue_harness import (E, Extractor, MCP_TOKEN, Stack, VID, WORKER_TOKEN, main, make_result, run_until_idle)

MH = {"Authorization": f"Bearer {MCP_TOKEN}"}
WH = {"Authorization": f"Bearer {WORKER_TOKEN}"}
NINE = [t["name"] for t in main.TOOLS]


def rpc(client, method, params=None, headers=None):
    return client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}, headers=headers or {})


def call(client, name, args, headers):
    r = rpc(client, "tools/call", {"name": name, "arguments": args}, headers)
    return r.status_code, (json.loads(r.json()["result"]["content"][0]["text"]) if r.status_code == 200 else None)


def queue_keys(stack):
    return [k for k in stack.fake.keys() if k.startswith("yt:q:")]


@pytest.fixture
def secured(monkeypatch):
    s = Stack(monkeypatch); s.client = TestClient(main.app, raise_server_exceptions=False); return s


# ── fail-closed: queue enabled but MCP is not authenticated ──
def test_queue_is_refused_without_mcp_auth_and_nothing_is_written(monkeypatch):
    s = Stack(monkeypatch, env={"_no_mcp_auth": "1"})
    monkeypatch.delenv("MCP_AUTH_TOKEN", raising=False); monkeypatch.delenv("MCP_AUTH_TOKENS", raising=False)
    c = TestClient(main.app, raise_server_exceptions=False)
    names = [t["name"] for t in rpc(c, "tools/list").json()["result"]["tools"]]
    assert names == NINE                                                      # queue tools are not even listed
    status, out = call(c, "yt_queue_transcripts", {"video_ids": [VID(1)]}, {})
    assert status == 200 and out["status"] == "queue_unavailable" and out["reason"] == "needs_mcp_auth_token"
    status, out = call(c, "yt_transcript_queue_status", {}, {})
    assert out["status"] == "queue_unavailable"
    assert c.post("/worker/claim", json={"worker_id": "laptop-1"}, headers=WH).status_code == 503
    r = asyncio.run(main.tool_get_video_transcript(VID(2), "en"))             # Render's own extraction fails (blocked)...
    assert r["status"] == "blocked" and "queued_job" not in r                 # ...and an anonymous caller does NOT create a job
    assert queue_keys(s) == []                                                # nothing at all was written
    assert "queue_blocked_needs_mcp_auth_token" in asyncio.run(main.get_transcript_service().diagnostics())["alerts"]


def test_queue_is_refused_without_upstash(monkeypatch):
    s = Stack(monkeypatch)
    s.svc.remote = None
    s.svc._queue = None
    assert s.svc.queue_block_reason() in ("needs_upstash", "disabled") and s.svc.queue is None


# ── authenticated submissions only ──
def test_unauthenticated_http_cannot_submit_or_read_queue_state(secured):
    c = secured.client
    for name, args in (("yt_queue_transcripts", {"video_ids": [VID(1)]}), ("yt_transcript_queue_status", {}),
                       ("yt_get_video_transcript", {"video_id": VID(1)})):
        assert rpc(c, "tools/call", {"name": name, "arguments": args}).status_code == 401
        assert rpc(c, "tools/call", {"name": name, "arguments": args}, {"Authorization": "Bearer wrong"}).status_code == 401
    assert queue_keys(secured) == []


def test_flood_of_unauthorized_requests_leaves_the_queue_empty(secured):
    c = secured.client
    codes = [rpc(c, "tools/call", {"name": "yt_queue_transcripts", "arguments": {"video_ids": [VID(i) for i in range(1, 50)]}}).status_code
             for _ in range(40)]
    assert set(codes) == {401} and queue_keys(secured) == []


def test_authenticated_submission_works(secured):
    status, out = call(secured.client, "yt_queue_transcripts", {"video_ids": [VID(1), VID(2)]}, MH)
    assert status == 200 and out["summary"] == {"queued": 2}


# ── separate credentials ──
def test_worker_token_cannot_use_mcp_and_mcp_token_cannot_use_worker_api(secured):
    c = secured.client
    for method in ("tools/list", "initialize"):
        assert rpc(c, method, headers=WH).status_code == 401
    assert call(c, "yt_queue_transcripts", {"video_ids": [VID(1)]}, WH)[0] == 401
    for path, body in (("/worker/claim", {"worker_id": "laptop-1"}), ("/worker/renew", {})):
        assert c.post(path, json=body, headers=MH).status_code == 401
    assert c.get("/worker/ping", headers=MH).status_code == 401
    assert queue_keys(secured) == []


def test_job_status_and_result_retrieval_are_protected(secured):
    call(secured.client, "yt_queue_transcripts", {"video_ids": [VID(1)]}, MH)
    c = secured.client
    for path in (f"/worker/jobs/{VID(1)}:en", f"/worker/result/{VID(1)}"):
        assert c.get(path).status_code == 401                                  # anonymous
        assert c.get(path, headers=MH).status_code == 401                      # MCP token is not a worker credential
    assert c.get(f"/worker/jobs/{VID(1)}:en", headers=WH).status_code == 200
    assert c.get("/health/transcripts").status_code == 401                     # diagnostics need the MCP token too
    assert c.get("/health/transcripts", headers=MH).status_code == 200


def test_worker_token_in_url_or_query_is_never_accepted(secured):
    assert secured.client.get(f"/worker/ping?token={WORKER_TOKEN}").status_code == 401
    assert secured.client.get(f"/worker/ping?access_token={WORKER_TOKEN}", headers={"X-API-Key": WORKER_TOKEN}).status_code == 401


# ── anti-flood for authenticated callers ──
def test_pending_cap_and_daily_enqueue_cap_bound_the_queue(monkeypatch):
    s = Stack(monkeypatch, env={"QUEUE_MAX_PENDING": "8", "QUEUE_MAX_ENQUEUE_PER_DAY": "5"})
    r = asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(1, 11)], "en"))
    assert r["summary"] == {"queued": 5, "daily_limit": 5}                      # daily cap hits first
    s.clock.advance(86400)                                                      # next UTC day
    r2 = asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(20, 30)], "en"))
    assert r2["summary"]["queued"] == 3 and r2["summary"]["full"] == 7          # pending cap (8) bounds total depth
    assert asyncio.run(main.get_transcript_service().queue.summary())["pending_total"] == 8


def test_oversized_batch_and_bad_language_rejected_without_side_effects(secured):
    assert asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(101)], "en"))["status"] == "invalid_request"
    assert asyncio.run(main.tool_queue_transcripts([VID(1)], "EN; DROP"))["status"] == "invalid_request"
    assert queue_keys(secured) == []


# ── backward compatibility ──
def test_queue_disabled_keeps_original_behaviour(monkeypatch):
    s = Stack(monkeypatch, env={"TRANSCRIPT_QUEUE_ENABLED": "false", "_no_mcp_auth": "1"})
    monkeypatch.delenv("MCP_AUTH_TOKEN", raising=False)
    c = TestClient(main.app, raise_server_exceptions=False)
    assert [t["name"] for t in rpc(c, "tools/list").json()["result"]["tools"]] == NINE      # still open, as before
    status, out = call(c, "yt_queue_transcripts", {"video_ids": [VID(1)]}, {})
    assert "Unknown tool" in json.dumps(out)
    assert asyncio.run(main.tool_get_video_transcript(VID(1), "en"))["status"] == "blocked"
    assert c.post("/worker/claim", json={"worker_id": "laptop-1"}, headers=WH).status_code == 503
    assert queue_keys(s) == []


def test_original_nine_tools_unchanged_in_list_and_schema_when_queue_on(secured):
    tools = rpc(secured.client, "tools/list", headers=MH).json()["result"]["tools"]
    assert [t["name"] for t in tools][:9] == NINE and [t for t in tools[:9]] == main.TOOLS


# ── credentials never leak ──
def test_no_credential_appears_in_logs_responses_or_diagnostics(secured, caplog):
    caplog.set_level(logging.DEBUG)
    c = secured.client
    seen = []
    seen.append(call(c, "yt_queue_transcripts", {"video_ids": [VID(1)]}, MH)[1])
    j = c.post("/worker/claim", json={"worker_id": "laptop-1", "max_jobs": 1}, headers=WH).json()["jobs"][0]
    seen.append(c.post("/worker/complete", json={"worker_id": "laptop-1", "job_id": j["job_id"], "fence": j["fence"],
                                                 "result": make_result(10)}, headers=WH).json())
    seen.append(c.get("/health/transcripts", headers=MH).json())
    seen.append(c.get("/worker/ping", headers=WH).json())
    secrets = [MCP_TOKEN, WORKER_TOKEN, secured.fake.token, "fake-upstash.example"]
    blob = json.dumps(seen) + caplog.text
    for sct in secrets:
        assert sct not in blob


def test_upstash_credential_failure_does_not_echo_secrets(monkeypatch):
    s = Stack(monkeypatch)
    s.svc.remote.token = "wrong-token-value-123456"
    r = asyncio.run(main.tool_queue_transcripts([VID(1)], "en"))
    assert r["status"] == "queue_unavailable"
    blob = json.dumps(r) + json.dumps(asyncio.run(main.get_transcript_service().diagnostics()), default=str)
    assert "wrong-token-value-123456" not in blob and s.fake.token not in blob and "fake-upstash" not in blob


# ── paid APIs stay off ──
def test_paid_apis_disabled_by_default_and_unused_through_the_whole_queue_flow(monkeypatch, tmp_path):
    for k, v in {"SUPADATA_API_KEY": "sd_key_" + "x" * 20, "TRANSCRIPT_API_KEY": "tk_key_" + "x" * 20}.items():
        monkeypatch.setenv(k, v)
    s = Stack(monkeypatch, env={"SUPADATA_API_KEY": "sd_key_" + "x" * 20, "TRANSCRIPT_API_KEY": "tk_key_" + "x" * 20})
    assert s.svc.cfg.enable_paid is False
    asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(1, 4)], "en"))
    for i in range(1, 4):
        asyncio.run(main.tool_get_video_transcript(VID(i), "en"))             # Render-side attempt (blocked) must not reach a paid API
    s.dir = str(tmp_path)
    w = s.worker(s.settings(state_dir=s.dir), Extractor())
    run_until_idle(w)
    assert s.paid_calls == 0 and s.fake.keys("yt:u:*") == []
    assert asyncio.run(main.get_transcript_service().queue.summary())["completed_retained"] == 3
