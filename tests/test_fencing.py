"""Order 11: worker ownership, fencing tokens, stale-report rejection, no cache/negative-cache poisoning.
Real Lua scripts on a Redis emulator; MOCK: the emulator is not Upstash (atomicity of EVAL is Redis/Upstash's documented
behaviour, not something these tests can prove for the live service)."""
import asyncio
import itertools
import random

import pytest
from starlette.testclient import TestClient

from job_queue import JobQueue, QueueConfig, QueueUnavailable, job_id
from tests.helpers import Clock
from tests.queue_harness import Stack, WORKER_TOKEN, make_result, main, VID, wc, Extractor, run_until_idle
from tests.redis_fake import RedisBackedUpstash, make_remote

A = "aaaaaaaaaaa"
K = ["yt:c:k"]


def mk(cfg=None):
    f = RedisBackedUpstash(); clock = Clock()
    return JobQueue(make_remote(f, clock), cfg or QueueConfig(enabled=True), clock), f, clock


async def two_claims(q, clock, lease=60):
    """A claims; lease expires; B claims after recovery. Returns (jobA, jobB)."""
    await q.enqueue(A, "en")
    (a,) = await q.claim("A", 1, lease)
    clock.advance(lease + 1); await q.claim("B", 1, lease); clock.advance(61)
    (b,) = await q.claim("B", 1, lease)
    return a, b


def snapshot(f):
    return {k: (f.redis.type(k), f.redis.hgetall(k) if f.redis.type(k) == "hash" else
                f.redis.zrange(k, 0, -1, withscores=True) if f.redis.type(k) == "zset" else f.redis.get(k)) for k in f.keys()}


# ── reproduction of the Order-11 bugs, now guarded (they FAILED on commit b340779) ──
async def test_stale_worker_cannot_overwrite_a_committed_transcript():
    q, f, clock = mk()
    a, b = await two_claims(q, clock)
    assert await q.complete(b["job_id"], "B", b["fence"], K, "GOOD", 100) == "completed"
    assert await q.complete(a["job_id"], "A", a["fence"], K, "STALE", 100) == "stale"
    assert f.redis.get("yt:c:k") == "GOOD"


async def test_stale_failure_cannot_poison_the_negative_cache():
    q, f, clock = mk()
    a, b = await two_claims(q, clock)
    await q.complete(b["job_id"], "B", b["fence"], K, "GOOD", 100)
    r = await q.fail(a["job_id"], "A", a["fence"], "no_captions", "x", negative=("yt:c:k", "NEG-A", 100))
    assert r["state"] == "stale" and f.redis.get("yt:c:k") == "GOOD"


async def test_stale_failure_while_new_owner_is_still_working_changes_nothing():
    q, f, clock = mk()
    a, b = await two_claims(q, clock)
    before = snapshot(f)
    r = await q.fail(a["job_id"], "A", a["fence"], "no_captions", "x", negative=("yt:c:k", "NEG-A", 100))
    assert r["state"] == "stale"
    after = snapshot(f)
    after_stats = dict(after["yt:q:stats"][1]); before_stats = dict(before["yt:q:stats"][1])
    after_stats.pop("stale_rejected"); before_stats.pop("stale_rejected", None)
    assert {k: v for k, v in after.items() if k != "yt:q:stats"} == {k: v for k, v in before.items() if k != "yt:q:stats"}
    assert after_stats == before_stats                                   # only the stale_rejected counter moved
    assert f.redis.get("yt:c:k") is None and (await q.get_many([b["job_id"]]))[b["job_id"]]["status"] == "processing"


# ── ownership rules ──
async def test_complete_requires_matching_worker_and_fence():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    assert await q.complete(j["job_id"], "B", j["fence"], K, "X", 100) == "stale"              # wrong worker
    assert await q.complete(j["job_id"], "A", j["fence"] + 1, K, "X", 100) == "stale"          # wrong fence
    assert await q.complete(j["job_id"], "A", 0, K, "X", 100) == "stale"
    assert f.redis.get("yt:c:k") is None
    assert await q.complete(j["job_id"], "A", j["fence"], K, "OK", 100) == "completed"


async def test_fence_from_another_job_is_useless():
    q, f, clock = mk()
    await q.enqueue_many([(A, "en"), ("bbbbbbbbbbb", "en")])
    ja, jb = await q.claim("A", 2, 300)
    assert await q.complete(ja["job_id"], "A", jb["fence"], K, "X", 100) == "stale"


async def test_late_but_unclaimed_completion_is_accepted_and_stored():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 60)
    clock.advance(61); await q.claim("B", 1, 60)                         # lease recovered to pending; nobody re-claimed
    assert (await q.get_many([j["job_id"]]))[j["job_id"]]["status"] == "pending"
    assert await q.complete(j["job_id"], "A", j["fence"], K, "LATE-BUT-VALID", 100) == "completed_late"
    assert f.redis.get("yt:c:k") == "LATE-BUT-VALID"
    clock.advance(1000)
    assert await q.claim("B", 1, 60) == []                               # job is done; not re-run


async def test_retry_after_lost_reply_is_idempotent_and_does_not_change_the_stored_value():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    assert await q.complete(j["job_id"], "A", j["fence"], K, "V1", 100) == "completed"
    assert await q.complete(j["job_id"], "A", j["fence"], K, "V1-retry", 100) == "already_completed"
    assert f.redis.get("yt:c:k") == "V1"                                 # retry cannot rewrite the value
    assert await q.complete(j["job_id"], "B", j["fence"], K, "OTHER", 100) == "stale"


async def test_terminal_failed_job_rejects_later_completion():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    assert (await q.fail(j["job_id"], "A", j["fence"], "no_captions", "", negative=("yt:c:k", "NEG", 100)))["state"] == "failed"
    assert await q.complete(j["job_id"], "A", j["fence"], K, "X", 100) == "stale"
    assert f.redis.get("yt:c:k") == "NEG"


async def test_negative_entry_never_replaces_an_existing_cache_entry():
    q, f, clock = mk()
    f.redis.set("yt:c:k", "POSITIVE-FROM-ELSEWHERE")                     # e.g. Render itself fetched it meanwhile
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    assert (await q.fail(j["job_id"], "A", j["fence"], "no_captions", "", negative=("yt:c:k", "NEG", 100)))["state"] == "failed"
    assert f.redis.get("yt:c:k") == "POSITIVE-FROM-ELSEWHERE"


async def test_non_definitive_failures_never_write_to_the_cache_even_if_a_value_is_supplied():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    await q.fail(j["job_id"], "A", j["fence"], "blocked", "x", negative=("yt:c:k", "NEG", 100))
    assert f.redis.get("yt:c:k") is None


async def test_renew_requires_the_current_fence():
    q, f, clock = mk()
    a, b = await two_claims(q, clock)
    assert (await q.renew(a["job_id"], "A", a["fence"], 60))["renewed"] is False
    assert (await q.renew(b["job_id"], "B", b["fence"], 60))["renewed"] is True


async def test_fences_are_unique_monotonic_and_never_reused_after_prune():
    q, f, clock = mk(QueueConfig(enabled=True, retention_days=1))
    seen = []
    for i in range(3):
        await q.enqueue(A, "en", force=True)
        (j,) = await q.claim("w", 1, 300); seen.append(j["fence"])
        await q.complete(j["job_id"], "w", j["fence"], K, "v", 100)
        clock.advance(3 * 86400); await q.prune()                        # record deleted, then the same job id is re-created
    assert seen == sorted(set(seen)) and len(seen) == 3


# ── Redis-side atomicity: assumptions that CAN be checked offline ──
async def test_many_concurrent_completers_exactly_one_value_is_stored():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    res = await asyncio.gather(*[q.complete(j["job_id"], "A" if i == 0 else f"W{i}", j["fence"], K, f"V{i}", 100) for i in range(30)])
    assert res.count("completed") == 1 and res.count("stale") == 29 and f.redis.get("yt:c:k") == "V0"


async def test_complete_and_claim_race_on_expired_lease_has_one_owner():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (a,) = await q.claim("A", 1, 60)
    clock.advance(61)
    r = await asyncio.gather(q.complete(a["job_id"], "A", a["fence"], K, "A-VALUE", 100), q.claim("B", 1, 60))
    # whichever script ran first wins; the outcome must be consistent in either order
    if r[0] == "completed":
        assert r[1] == [] and f.redis.get("yt:c:k") == "A-VALUE"
    else:
        assert r[0] == "stale" and f.redis.get("yt:c:k") is None


async def test_all_state_changes_of_complete_are_one_script_call():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("A", 1, 300)
    before = f.eval_calls
    await q.complete(j["job_id"], "A", j["fence"], K, "v", 100)
    assert f.eval_calls == before + 1 and f.http_requests >= 1          # cache write + job state + accounting in ONE EVAL


# ── model-based interleavings: invariants hold for every ordering ──
OPS = ["claimA", "claimB", "tick", "doneA", "doneB", "failA", "failB"]


async def run_sequence(seq):
    q, f, clock = mk(QueueConfig(enabled=True, requeue_base=1))
    await q.enqueue(A, "en")
    held, accepted = {}, []
    for op in seq:
        w = op[-1] if op[-1] in "AB" else None
        if op.startswith("claim"):
            js = await q.claim(w, 1, 60)
            if js:
                held[w] = js[0]
        elif op == "tick":
            clock.advance(62)
        elif op.startswith("done") and w in held:
            r = await q.complete(held[w]["job_id"], w, held[w]["fence"], K, f"VAL-{w}", 100)
            if r in ("completed", "completed_late"):
                accepted.append(("done", w))
        elif op.startswith("fail") and w in held:
            r = await q.fail(held[w]["job_id"], w, held[w]["fence"], "no_captions", "", negative=("yt:c:k", f"NEG-{w}", 100))
            if r["state"] == "failed":
                accepted.append(("fail", w))
    rec = (await q.get_many([job_id(A, "en")]))[job_id(A, "en")]
    cached = f.redis.get("yt:c:k")
    # invariants
    assert len(accepted) <= 1, (seq, accepted)                          # at most one terminal outcome
    if accepted:
        kind, w = accepted[0]
        assert cached == (f"VAL-{w}" if kind == "done" else f"NEG-{w}"), (seq, cached)
        assert rec["status"] == ("completed" if kind == "done" else "failed")
        assert rec["claimed_by"] == w
    else:
        assert cached is None and rec["status"] not in ("completed", "failed"), (seq, rec["status"], cached)
    s = await q.summary()
    assert s["counters"].get("completed_total", 0) <= 1


async def test_every_short_interleaving_of_two_workers_keeps_ownership_invariants():
    for n in (1, 2, 3, 4):
        for seq in itertools.product(OPS, repeat=n):
            await run_sequence(seq)


async def test_random_long_interleavings_keep_ownership_invariants():
    rng = random.Random(2026)
    for _ in range(300):
        await run_sequence([rng.choice(OPS) for _ in range(rng.randint(5, 10))])


# ── through the worker API (HTTP shapes, 409, fences required) ──
H = {"Authorization": f"Bearer {WORKER_TOKEN}"}


@pytest.fixture
def stack(monkeypatch):
    s = Stack(monkeypatch); s.client = TestClient(main.app, raise_server_exceptions=False); return s


def _enq():
    asyncio.run(main.tool_queue_transcripts([VID(1)], "en"))


def _claim(stack, w, lease=60):
    return stack.client.post("/worker/claim", json={"worker_id": w, "max_jobs": 1, "lease_seconds": lease}, headers=H).json()["jobs"]


def _body(job, w, text):
    r = make_result(25); r["full_transcript"] = text * 10
    return {"worker_id": w, "job_id": job["job_id"], "fence": job["fence"], "result": r}


def _two(stack):
    _enq(); (a,) = _claim(stack, "laptop-A")
    stack.clock.advance(61); _claim(stack, "laptop-B"); stack.clock.advance(61)
    (b,) = _claim(stack, "laptop-B")
    return a, b


def _shared_text(stack):
    stack.svc.store.purge(10 ** 12, 0)                                    # read the SHARED store, not the local layer
    r = asyncio.run(stack.svc.cached_result(VID(1), "en"))
    return None if r is None else r["full_transcript"]


def test_api_stale_complete_is_409_and_keeps_the_good_transcript(stack):
    a, b = _two(stack)
    assert stack.client.post("/worker/complete", json=_body(b, "laptop-B", "GOOD "), headers=H).json()["state"] == "completed"
    r = stack.client.post("/worker/complete", json=_body(a, "laptop-A", "STALE "), headers=H)
    assert r.status_code == 409 and r.json()["error"] == "stale_job"
    assert _shared_text(stack).startswith("GOOD")


def test_api_stale_fail_is_409_and_does_not_poison_the_cache(stack):
    a, b = _two(stack)
    stack.client.post("/worker/complete", json=_body(b, "laptop-B", "GOOD "), headers=H)
    r = stack.client.post("/worker/fail", json={"worker_id": "laptop-A", "job_id": a["job_id"], "fence": a["fence"],
                                                "error_class": "no_captions", "message": "x"}, headers=H)
    assert r.status_code == 409
    assert _shared_text(stack).startswith("GOOD")
    assert asyncio.run(main.tool_get_video_transcript(VID(1), "en"))["status"] == "ok"


def test_api_requires_a_fence_and_rejects_forged_ones(stack):
    _enq(); (j,) = _claim(stack, "laptop-A")
    body = _body(j, "laptop-A", "X ")
    del body["fence"]
    assert stack.client.post("/worker/complete", json=body, headers=H).status_code == 422
    body["fence"] = 0
    assert stack.client.post("/worker/complete", json=body, headers=H).status_code == 422
    body["fence"] = j["fence"] + 7
    assert stack.client.post("/worker/complete", json=body, headers=H).status_code == 409
    assert _shared_text(stack) is None


def test_api_same_claim_retry_after_lost_response_is_idempotent(stack):
    _enq(); (j,) = _claim(stack, "laptop-A")
    first = stack.client.post("/worker/complete", json=_body(j, "laptop-A", "ONE "), headers=H)
    again = stack.client.post("/worker/complete", json=_body(j, "laptop-A", "ONE "), headers=H)
    assert first.json()["state"] == "completed" and again.status_code == 200 and again.json()["state"] == "already_completed"


def test_api_complete_acknowledged_only_if_shared_store_holds_the_value(stack):
    _enq(); (j,) = _claim(stack, "laptop-A")
    stack.fake.down = True; stack.svc.remote._open_until = 0
    assert stack.client.post("/worker/complete", json=_body(j, "laptop-A", "ONE "), headers=H).status_code == 503
    stack.fake.down = False; stack.svc.remote._open_until = 0
    assert _shared_text(stack) is None
    assert stack.client.post("/worker/complete", json=_body(j, "laptop-A", "ONE "), headers=H).json()["state"] == "completed"
    assert _shared_text(stack).startswith("ONE")


def test_api_definitive_failure_with_valid_fence_writes_negative_cache_once(stack):
    _enq(); (j,) = _claim(stack, "laptop-A")
    r = stack.client.post("/worker/fail", json={"worker_id": "laptop-A", "job_id": j["job_id"], "fence": j["fence"],
                                                "error_class": "no_captions", "message": "disabled"}, headers=H)
    assert r.json()["state"] == "failed"
    assert asyncio.run(main.tool_get_video_transcript(VID(1), "en"))["status"] == "no_captions"


# ── worker-side: stale local outbox is discarded, newer result survives ──
def test_worker_discards_stale_outbox_item_and_never_overwrites_the_newer_result(stack, tmp_path):
    _enq()
    sA = stack.settings(state_dir=str(tmp_path / "A"), worker_id="laptop-A", lease_seconds=60)
    wA = stack.worker(sA, Extractor(), flaky=lambda: False)
    api = stack.api(sA)
    (jobA,) = api.claim(1)
    # A finishes its work but cannot deliver (Render briefly unreachable): result parked in A's outbox with A's fence
    state = wc.State(sA.state_dir, stack.clock)
    state.outbox_put(jobA["job_id"], {"kind": "complete", "payload": make_result(25), "fence": jobA["fence"]})
    stack.clock.advance(61)
    wB = stack.worker(stack.settings(state_dir=str(tmp_path / "B"), worker_id="laptop-B"), Extractor())
    run_until_idle(wB); stack.clock.advance(61); run_until_idle(wB)       # B recovers the lease and completes with its own result
    assert asyncio.run(main.get_transcript_service().queue.summary())["completed_retained"] == 1
    before = _shared_text(stack)
    wA.flush_outbox()                                                      # A's laptop restarts and retries delivery
    assert list((tmp_path / "A" / "outbox").iterdir()) == []              # 409 -> final: item discarded, not retried forever
    assert _shared_text(stack) == before


def test_worker_outbox_delivers_a_valid_result_after_restart_exactly_once(stack, tmp_path):
    _enq()
    s = stack.settings(state_dir=str(tmp_path / "A"), worker_id="laptop-A")
    (job,) = stack.api(s).claim(1)
    wc.State(s.state_dir, stack.clock).outbox_put(job["job_id"], {"kind": "complete", "payload": make_result(25), "fence": job["fence"]})
    w2 = stack.worker(s, Extractor())
    w2.flush_outbox(); w2.flush_outbox()
    assert asyncio.run(main.get_transcript_service().queue.summary())["completed_retained"] == 1
    assert w2.stats["resubmitted"] == 1
