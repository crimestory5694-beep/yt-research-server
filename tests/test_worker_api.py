"""/worker/* endpoints through the real ASGI app, security middleware and Lua queue. MOCK: Upstash (fakeredis), YouTube."""
import pytest
from starlette.testclient import TestClient

from tests.queue_harness import Stack, WORKER_TOKEN, MCP_TOKEN, make_result, main, VID

H = {"Authorization": f"Bearer {WORKER_TOKEN}"}
MH = {"Authorization": f"Bearer {MCP_TOKEN}"}


@pytest.fixture
def stack(monkeypatch):
    s = Stack(monkeypatch)
    s.client = TestClient(main.app, raise_server_exceptions=False)
    return s


def enqueue(stack, ids, lang="en"):
    import asyncio
    return asyncio.run(main.tool_queue_transcripts(ids, lang))


def claim(stack, worker="laptop-1", n=1):
    return stack.client.post("/worker/claim", json={"worker_id": worker, "max_jobs": n, "lease_seconds": 300}, headers=H)


# ── authentication / least privilege ──
def test_worker_endpoints_require_the_worker_token(stack):
    c = stack.client
    assert c.get("/worker/ping").status_code == 401
    assert c.get("/worker/ping", headers={"Authorization": "Bearer nope"}).status_code == 401
    assert c.post("/worker/claim", json={"worker_id": "abc", "max_jobs": 1}).status_code == 401
    assert c.get("/worker/ping?token=" + WORKER_TOKEN).status_code == 401           # tokens never accepted in URLs
    assert c.get("/worker/ping", headers=H).json()["queue_enabled"] is True


def test_disabled_when_no_worker_token_is_configured(stack, monkeypatch):
    monkeypatch.delenv("WORKER_AUTH_TOKEN")
    r = stack.client.get("/worker/ping", headers=H)
    assert r.status_code == 503 and "disabled" in r.text


def test_mcp_token_cannot_use_worker_api_and_worker_token_is_not_an_mcp_token():
    """Fresh middleware instance with both token sets (the live app reads MCP tokens once, at start-up)."""
    from fastapi import FastAPI
    from security import SecurityMiddleware
    app = FastAPI()

    @app.post("/mcp")
    async def mcp(): return {"ok": 1}

    @app.get("/worker/ping")
    async def ping(): return {"ok": 1}
    app.add_middleware(SecurityMiddleware, tokens=["mcp-token-" + "m" * 30], rate_limit_per_minute=0, worker_tokens=[WORKER_TOKEN])
    c = TestClient(app)
    mcp_h = {"Authorization": "Bearer mcp-token-" + "m" * 30}
    assert c.get("/worker/ping", headers=mcp_h).status_code == 401
    assert c.post("/mcp", headers=H).status_code == 401
    assert c.post("/mcp", headers=mcp_h).status_code == 200 and c.get("/worker/ping", headers=H).status_code == 200


def test_per_token_rate_limit(stack, monkeypatch):
    tok = "unique-rate-test-token-" + "r" * 20
    monkeypatch.setenv("WORKER_AUTH_TOKEN", tok)
    monkeypatch.setenv("WORKER_RATE_LIMIT_PER_MINUTE", "3")
    h = {"Authorization": f"Bearer {tok}"}
    codes = [stack.client.get("/worker/ping", headers=h).status_code for _ in range(5)]
    assert codes == [200, 200, 200, 429, 429]
    assert stack.client.get("/worker/ping", headers=H).status_code in (200, 401)       # other tokens have their own budget


# ── input validation ──
@pytest.mark.parametrize("body", [
    {"worker_id": "a", "max_jobs": 1}, {"worker_id": "ok-worker", "max_jobs": 99}, {"worker_id": "bad worker!", "max_jobs": 1},
    {"worker_id": "ok-worker", "max_jobs": 1, "lease_seconds": 5}, {"worker_id": "ok-worker", "max_jobs": 1, "lease_seconds": 99999}, {}])
def test_claim_validation(stack, body):
    assert stack.client.post("/worker/claim", json=body, headers=H).status_code == 422


def test_job_and_result_paths_are_validated(stack):
    c = stack.client
    assert c.get("/worker/jobs/not-a-job", headers=H).status_code == 422
    assert c.get("/worker/jobs/../../etc", headers=H).status_code in (404, 422)
    assert c.get("/worker/result/short", headers=H).status_code == 422
    assert c.get("/worker/jobs/aaaaaaaaaaa:en", headers=H).status_code == 404


def _complete_body(job, **over):
    r = make_result(25)
    r.update(over)
    return {"worker_id": "laptop-1", "job_id": job["job_id"], "fence": job["fence"], "result": r}


def test_complete_validation(stack):
    enqueue(stack, [VID(1)]); (j,) = claim(stack).json()["jobs"]
    c = stack.client
    for bad in ({"full_transcript": "short"}, {"source": "supadata"}, {"language_code": "english!!"},
                {"caption_type": "weird"}, {"segments": [{"text": "x", "start": -1, "duration": 1}]},
                {"full_transcript": "x" * 3_000_001}):
        r = c.post("/worker/complete", json=_complete_body(j, **bad), headers=H)
        assert r.status_code in (413, 422), bad.keys()
        assert c.post("/worker/complete", json=_complete_body({"job_id": "zzzzzzzzzzz:en", "fence": 1}), headers=H).status_code == 404
    assert c.post("/worker/fail", json={"worker_id": "laptop-1", "job_id": j["job_id"], "fence": j["fence"], "error_class": "Bad Class!"}, headers=H).status_code == 422


def test_oversized_body_is_rejected_before_parsing(stack):
    r = stack.client.post("/worker/complete", content=b"x" * 5_000_000, headers={**H, "Content-Type": "application/json"})
    assert r.status_code == 413


# ── the lifecycle through the API ──
def test_full_lifecycle_claim_renew_complete_then_read_back(stack):
    res = enqueue(stack, [VID(1), VID(2)])
    assert res["summary"] == {"queued": 2}
    jobs = claim(stack, n=2).json()["jobs"]
    assert {j["video_id"] for j in jobs} == {VID(1), VID(2)}
    j = jobs[0]
    assert stack.client.post("/worker/renew", json={"worker_id": "laptop-1", "job_id": j["job_id"], "fence": j["fence"], "lease_seconds": 300}, headers=H).json()["renewed"] is True
    assert stack.client.get("/worker/result/" + j["video_id"], headers=H).status_code == 404            # not done yet
    r = stack.client.post("/worker/complete", json=_complete_body(j), headers=H)
    assert r.status_code == 200 and r.json()["state"] == "completed"
    assert stack.client.post("/worker/complete", json=_complete_body(j), headers=H).json()["state"] == "already_completed"
    got = stack.client.get("/worker/result/" + j["video_id"], headers=H).json()
    assert got["status"] == "ok" and got["cached"] is True and got["total_segments"] > 200 and len(got["segments"]) == 200
    st = stack.client.get("/worker/jobs/" + j["job_id"], headers=H).json()
    assert st["status"] == "completed"


def test_synchronous_tool_serves_worker_result_from_cache(stack):
    import asyncio
    enqueue(stack, [VID(3)]); (j,) = claim(stack).json()["jobs"]
    stack.client.post("/worker/complete", json=_complete_body(j), headers=H)
    res = asyncio.run(main.tool_get_video_transcript(VID(3), "en"))
    assert res["status"] == "ok" and res["cached"] is True and res["source"] == "youtube-transcript-api"
    # ...and still after a Render restart (state lives in the shared store)
    stack.restart_render()
    again = asyncio.run(main.tool_get_video_transcript(VID(3), "en"))
    assert again["status"] == "ok" and again["cached"] is True


def test_definitive_failure_writes_negative_cache_and_blocks_retries(stack):
    import asyncio
    enqueue(stack, [VID(4)]); (j,) = claim(stack).json()["jobs"]
    r = stack.client.post("/worker/fail", json={"worker_id": "laptop-1", "job_id": j["job_id"], "fence": j["fence"], "error_class": "no_captions", "message": "disabled"}, headers=H)
    assert r.json()["state"] == "failed"
    assert asyncio.run(main.tool_get_video_transcript(VID(4), "en"))["status"] == "no_captions"
    assert enqueue(stack, [VID(4)])["results"][0]["state"] in ("failed", "cached")


def test_blocked_report_defers_job(stack):
    enqueue(stack, [VID(5)]); (j,) = claim(stack).json()["jobs"]
    r = stack.client.post("/worker/fail", json={"worker_id": "laptop-1", "job_id": j["job_id"], "fence": j["fence"], "error_class": "blocked", "message": "bot"}, headers=H).json()
    assert r["state"] == "deferred" and r["retry_at"] > stack.clock()
    assert claim(stack).json()["jobs"] == []


def test_complete_is_not_acknowledged_if_shared_store_cannot_persist(stack):
    enqueue(stack, [VID(6)]); (j,) = claim(stack).json()["jobs"]
    stack.fake.down = True
    stack.svc.remote._open_until = 0
    r = stack.client.post("/worker/complete", json=_complete_body(j), headers=H)
    assert r.status_code == 503
    stack.fake.down = False; stack.svc.remote._open_until = 0
    assert stack.client.get("/worker/jobs/" + j["job_id"], headers=H).json()["status"] == "processing"   # not marked done
    assert stack.client.post("/worker/complete", json=_complete_body(j), headers=H).json()["state"] == "completed"


def test_queue_disabled_returns_503(monkeypatch):
    s = Stack(monkeypatch, env={"TRANSCRIPT_QUEUE_ENABLED": "false"})
    c = TestClient(main.app, raise_server_exceptions=False)
    assert c.post("/worker/claim", json={"worker_id": "laptop-1"}, headers=H).status_code == 503
    assert [t["name"] for t in c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=MH).json()["result"]["tools"]].__len__() == 9


# ── MCP tools ──
def test_queue_tools_appear_only_when_enabled_and_original_nine_are_untouched(stack, monkeypatch):
    names = [t["name"] for t in stack.client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=MH).json()["result"]["tools"]]
    assert names[:9] == [t["name"] for t in main.TOOLS] and names[9:] == ["yt_queue_transcripts", "yt_transcript_queue_status"]


def test_enqueue_tool_dedupes_reports_cached_and_validates(stack):
    import asyncio
    r = enqueue(stack, [VID(1), VID(1), "not a video", f"https://youtu.be/{VID(2)}"])
    states = {x["video_id"]: x["state"] for x in r["results"]}
    assert states[VID(1)] == "queued" and states["not a video"] == "invalid_video_id" and states[VID(2)] == "queued"
    assert [x["state"] for x in r["results"]].count("duplicate_in_request") == 1
    assert enqueue(stack, [VID(1)])["results"][0]["state"] == "already_queued"
    assert asyncio.run(main.tool_queue_transcripts([], "en"))["status"] == "invalid_request"
    assert asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(101)], "en"))["status"] == "invalid_request"
    assert asyncio.run(main.tool_queue_transcripts([VID(1)], "EN!!"))["status"] == "invalid_request"


def test_enqueue_tool_never_acknowledges_when_store_is_down(stack):
    stack.fake.down = True; stack.svc.remote._open_until = 0
    r = enqueue(stack, [VID(1), VID(2)])
    assert r["status"] == "queue_unavailable" and r["queued"] == 0 and all(x["state"] == "not_queued" for x in r["results"])
    stack.fake.down = False; stack.svc.remote._open_until = 0
    import asyncio
    assert asyncio.run(main.tool_transcript_queue_status([VID(1)]))["videos"][0]["state"] == "not_queued"


def test_status_tool_reports_progress_and_readiness(stack):
    import asyncio
    enqueue(stack, [VID(1), VID(2), VID(3)])
    (j,) = claim(stack).json()["jobs"]
    stack.client.post("/worker/complete", json=_complete_body(j), headers=H)
    st = asyncio.run(main.tool_transcript_queue_status([VID(1), VID(2), VID(3), VID(9)]))
    by = {v["video_id"]: v for v in st["videos"]}
    assert st["queue"]["pending_total"] == 2 and st["queue"]["completed_retained"] == 1
    assert by[j["video_id"]]["ready"] is True and by[j["video_id"]]["state"] == "completed"
    assert by[VID(9)]["state"] == "not_queued"


def test_sync_tool_auto_queues_on_retryable_failure_and_keeps_original_status(stack):
    import asyncio
    r = asyncio.run(main.tool_get_video_transcript(VID(7), "en"))          # Render's own extraction is "blocked" in this stack
    assert r["status"] == "blocked" and r["queued_job"]["state"] == "created"
    assert asyncio.run(main.tool_get_video_transcript(VID(7), "en"))["queued_job"]["state"] in ("exists",)
    r2 = asyncio.run(main.tool_get_video_transcript("bad", "en"))
    assert r2["status"] == "invalid_video_id" and "queued_job" not in r2


def test_sync_tool_unchanged_when_queue_disabled(monkeypatch):
    import asyncio
    Stack(monkeypatch, env={"TRANSCRIPT_QUEUE_ENABLED": "false"})
    r = asyncio.run(main.tool_get_video_transcript(VID(7), "en"))
    assert r["status"] == "blocked" and "queued_job" not in r
