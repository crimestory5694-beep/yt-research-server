"""Offline review of our Upstash usage against its DOCUMENTED REST/Lua behaviour. MOCK: no real Upstash is contacted.
Anything marked LIVE in docs/UPSTASH_PROTOCOL_REVIEW.md still needs a real database."""
import asyncio
import json
import re

import httpx
import pytest

import job_queue
from job_queue import JobQueue, QueueConfig, QueueUnavailable, job_id
from remote_store import RemoteCommandError, RemoteStore, RemoteUnavailable
from tests.helpers import Clock
from tests.redis_fake import RedisBackedUpstash, make_remote

SCRIPTS = {n: getattr(job_queue, n) for n in ("ENQUEUE", "CLAIM", "RENEW", "COMPLETE", "FAIL", "PRUNE")}
ALLOWED_REDIS_COMMANDS = {"HGET", "HSET", "HINCRBY", "HDEL", "ZADD", "ZREM", "ZCARD", "ZRANGEBYSCORE", "SET", "EXPIRE"}
FORBIDDEN = ["cjson", "cmsgpack", "struct.", "bit.", "os.", "io.", "loadstring", "dofile", "require", "math.random",
             "redis.pcall", "redis.log", "redis.sha1hex", "redis.replicate", "redis.set_repl", "'TIME'", "'EVALSHA'", "'SCRIPT'",
             "'SCAN'", "'KEYS'", "'RANDOMKEY'", "'SRANDMEMBER'", "'SPOP'", "'FLUSH"]


@pytest.mark.parametrize("name", list(SCRIPTS))
def test_script_compiles_under_real_lua_51(name):
    import lupa.lua51 as lupa51
    lua = lupa51.LuaRuntime()
    out = lua.eval("loadstring")(SCRIPTS[name])
    assert out is not None and not isinstance(out, tuple), out            # a failed compile returns (nil, message)


@pytest.mark.parametrize("name", list(SCRIPTS))
def test_script_uses_only_plain_deterministic_lua_and_whitelisted_commands(name):
    src = SCRIPTS[name]
    for bad in FORBIDDEN:
        assert bad not in src, f"{name} uses {bad}"
    used = set(re.findall(r"redis\.call\('([A-Z]+)'", src))
    assert used <= ALLOWED_REDIS_COMMANDS, used - ALLOWED_REDIS_COMMANDS
    globals_used = set(re.findall(r"(?<![\w.'\"])([a-z]+)\.[a-z]+\(", src)) - {"redis", "string", "table", "math"}
    assert globals_used == set(), globals_used                           # no unexpected libraries
    assert not re.search(r"redis\.call\('[A-Z]+', (?!KEYS\[)", src)       # every key argument is a declared KEYS[n]


def _lifecycle(f, clock):
    async def run():
        q = JobQueue(make_remote(f, clock), QueueConfig(enabled=True, max_attempts=3, retention_days=1, max_age_days=2), clock)
        ids = [f"v{i:010d}"[-11:] for i in range(8)]
        await q.enqueue_many([(v, "en") for v in ids] + [(ids[0], "en")])
        js = await q.claim("w1", 5, 120)
        await q.renew(js[0]["job_id"], "w1", js[0]["fence"], 120)
        await q.complete(js[0]["job_id"], "w1", js[0]["fence"], ["yt:c:a", "yt:c:b"], "VAL", 100)
        await q.fail(js[1]["job_id"], "w1", js[1]["fence"], "no_captions", "x", negative=("yt:c:n", "NEG", 100))
        await q.fail(js[2]["job_id"], "w1", js[2]["fence"], "blocked", "x")
        await q.fail(js[3]["job_id"], "w1", js[3]["fence"], "timeout", "x")
        await q.fail(js[4]["job_id"], "w1", js[4]["fence"], "interrupted", "x")
        await q.fail(js[4]["job_id"], "w9", js[4]["fence"], "interrupted", "stale")
        clock.advance(3000); await q.claim("w2", 5, 60)
        clock.advance(200); await q.claim("w2", 5, 60)                    # lease recovery path
        clock.advance(3 * 86400); await q.claim("w2", 5, 60)              # expiry path
        await q.prune(); await q.summary(); await q.recent_finished(10)
        await q.enqueue(ids[1], "en", force=True)
    asyncio.run(run())


def test_scripts_touch_only_declared_keys_when_actually_executed():
    f = RedisBackedUpstash(); f._install_counter()
    try:
        _lifecycle(f, Clock())
    finally:
        f._restore()
    assert f.inner > 50                                                    # the scripts really ran
    assert f.undeclared == [], f.undeclared[:5]


def test_rest_request_shape_matches_documented_pipeline_format():
    f = RedisBackedUpstash()
    clock = Clock()
    q = JobQueue(make_remote(f, clock), QueueConfig(enabled=True), clock)
    asyncio.run(q.enqueue_many([("aaaaaaaaaaa", "en"), ("bbbbbbbbbbb", "en")]))
    asyncio.run(q.summary())
    for req in f.requests:
        assert req["url"].endswith("/pipeline")                            # POST {url}/pipeline, body = array of command arrays
        assert isinstance(req["body"], list) and all(isinstance(c, list) and c for c in req["body"])
        for cmd in req["body"]:
            assert isinstance(cmd[0], str)
            assert all(isinstance(a, (str, int)) and not isinstance(a, bool) for a in cmd[1:]), cmd     # JSON strings/ints only
            if cmd[0] == "EVAL":
                assert isinstance(cmd[1], str) and cmd[2] == 5            # script text, numkeys
                keys = cmd[3:3 + cmd[2]]
                assert all(isinstance(k, str) for k in keys) and all(isinstance(a, str) for a in cmd[3 + cmd[2]:])
    assert len(f.requests[0]["body"]) == 2                                 # two EVALs batched into ONE round trip


def test_pipeline_is_not_assumed_atomic_partial_failure_is_reported_and_retry_is_safe():
    f = RedisBackedUpstash(); clock = Clock()
    q = JobQueue(make_remote(f, clock), QueueConfig(enabled=True), clock)
    real = f.handler
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        resp = real(req)
        if calls["n"] == 1:                                                # a middle command "fails" in the reply
            body = resp.json(); body[1] = {"error": "ERR something"}
            return httpx.Response(200, json=body)
        return resp
    q.remote._factory = lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    items = [("aaaaaaaaaaa", "en"), ("bbbbbbbbbbb", "en"), ("ccccccccccc", "en")]
    with pytest.raises(QueueUnavailable):
        asyncio.run(q.enqueue_many(items))                                # caller is told: NOT acknowledged
    res = asyncio.run(q.enqueue_many(items))                              # retry: no duplicates, nothing lost
    assert [r["state"] for r in res] == ["exists", "exists", "exists"] or sorted(r["state"] for r in res).count("created") <= 1
    assert asyncio.run(q.summary())["pending_total"] == 3


@pytest.mark.parametrize("status", [401, 403, 429, 500, 502, 503])
def test_http_errors_become_unavailable_never_success(status):
    rs = RemoteStore("https://x.example", "t", client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(status, text="no"))))
    with pytest.raises(RemoteUnavailable):
        asyncio.run(rs.exec([["PING"]]))


def test_malformed_replies_are_not_trusted():
    for bad in ({"result": "OK"}, [], [{"result": 1}, {"result": 2}], "text"):
        rs = RemoteStore("https://x.example", "t", client_factory=lambda b=bad: httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=b))))
        with pytest.raises(RemoteUnavailable):
            asyncio.run(rs.exec([["PING"]]))
    rs = RemoteStore("https://x.example", "t", client_factory=lambda: httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[{"error": "ERR bad"}]))))
    with pytest.raises(RemoteCommandError):
        asyncio.run(rs.exec([["PING"]]))


def test_estimated_command_cost_per_operation_is_documented_and_small():
    """Worst case assumes Upstash bills every command executed inside a script (its billing page does not say)."""
    f = RedisBackedUpstash(); f._install_counter(); clock = Clock()
    try:
        q = JobQueue(make_remote(f, clock), QueueConfig(enabled=True), clock)
        costs = {}

        def measure(name, coro):
            b = f.top_level + f.inner
            asyncio.run(coro)
            costs[name] = f.top_level + f.inner - b
        measure("enqueue", q.enqueue("aaaaaaaaaaa", "en"))
        holder = {}

        async def claim():
            holder["j"] = (await q.claim("w", 1, 300))[0]
        measure("claim", claim())
        measure("complete", q.complete("aaaaaaaaaaa:en", "w", holder["j"]["fence"], ["yt:c:k"], "v", 100))
        measure("idle_claim", q.claim("w", 1, 300))
    finally:
        f._restore()
    assert max(costs.values()) <= 16, costs
    print("per-operation commands (top-level + inside script):", costs)
