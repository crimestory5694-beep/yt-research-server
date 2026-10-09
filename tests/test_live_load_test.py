"""Offline tests of the bounded live-test runner against a fake MCP endpoint (httpx.MockTransport)."""
import importlib.util, json, os
import httpx

spec = importlib.util.spec_from_file_location("llt", os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "live_load_test.py"))
llt = importlib.util.module_from_spec(spec); spec.loader.exec_module(llt)


def fake(statuses, seen):
    def handler(req):
        if req.url.path == "/health/transcripts":
            return httpx.Response(200, json={"alerts": [], "outcomes_since_start": {}, "counters": {}, "block_streaks": {}})
        vid = json.loads(req.content)["params"]["arguments"]["video_id"]
        n = seen[vid] = seen.get(vid, 0) + 1
        st = statuses(vid, n)
        body = {"status": st, "video_id": vid, "cached": n > 1}
        if st == "ok":
            body["full_transcript"] = "x" * 100
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": {"content": [{"type": "text", "text": json.dumps(body)}], "isError": False}})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def nosleep(_): pass


async def test_phase1_happy_path_and_cache_check():
    seen = {}
    async with fake(lambda v, n: "ok", seen) as c:
        out = await llt.run(c, "http://t", [f"id{i:09d}" for i in range(30)], 1, sleep=nosleep)
    assert out["requested"] == 5 and out["ok_pct"] == 100 and out["second_pass_cached"] == "5/5" and out["stopped_early"] is None
    assert len(seen) == 5 and "full_transcript" not in json.dumps(out)


async def test_stops_after_three_consecutive_blocks():
    seen = {}
    async with fake(lambda v, n: "blocked", seen) as c:
        out = await llt.run(c, "http://t", [f"id{i:09d}" for i in range(30)], 2, sleep=nosleep)
    assert out["stopped_early"] == "3 consecutive blocked" and sum(seen.values()) == 3


async def test_stops_on_any_paid_provider_status():
    seen = {}
    async with fake(lambda v, n: "provider_quota_exhausted" if n else "ok", seen) as c:
        out = await llt.run(c, "http://t", [f"id{i:09d}" for i in range(30)], 1, sleep=nosleep)
    assert out["stopped_early"].startswith("unexpected provider status") and sum(seen.values()) == 1


async def test_phase_limits_requests():
    seen = {}
    async with fake(lambda v, n: "ok", seen) as c:
        await llt.run(c, "http://t", [f"id{i:09d}" for i in range(200)], 3, sleep=nosleep)
    assert len(seen) == 100
