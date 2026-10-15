"""Queue semantics: the REAL Lua scripts executing on a Redis emulator (fakeredis). MOCK: the emulator is not Upstash."""
import asyncio

import httpx
import pytest

from job_queue import (ERROR_CLASS_RE, JOB_ID_RE, JobQueue, QueueConfig, QueueUnavailable, job_id, kind_for, sanitize_message)
from tests.helpers import Clock
from tests.redis_fake import RedisBackedUpstash, make_remote

A, B, C = "aaaaaaaaaaa", "bbbbbbbbbbb", "ccccccccccc"


def mk(cfg=None, clock=None):
    f = RedisBackedUpstash()
    clock = clock or Clock()
    q = JobQueue(make_remote(f, clock), cfg or QueueConfig(enabled=True), clock)
    return q, f, clock


async def status(q, vid, lang="en"):
    return (await q.get_many([job_id(vid, lang)]))[job_id(vid, lang)]


# ── unique jobs / duplicate prevention ──
async def test_enqueue_unique_and_duplicates_collapse():
    q, f, _ = mk()
    assert (await q.enqueue(A, "en"))["state"] == "created"
    d = await q.enqueue(A, "en")
    assert d["state"] == "exists" and d["existing_status"] == "pending"
    assert (await q.enqueue(A, "es"))["state"] == "created"          # different language = different job
    s = await q.summary()
    assert s["pending_total"] == 2 and s["counters"]["created"] == 2 and s["counters"]["dup_enqueues"] == 1


async def test_concurrent_duplicate_enqueues_create_exactly_one_job():
    q, f, _ = mk()
    res = await asyncio.gather(*[q.enqueue(A, "en") for _ in range(25)])
    assert sorted(r["state"] for r in res).count("created") == 1 and (await q.summary())["pending_total"] == 1


async def test_enqueue_many_batch_single_round_trip():
    q, f, _ = mk()
    before = f.http_requests
    res = await q.enqueue_many([(A, "en"), (B, "en"), (A, "en")])
    assert [r["state"] for r in res] == ["created", "created", "exists"] and f.http_requests == before + 1


async def test_backpressure_when_queue_full():
    q, f, _ = mk(QueueConfig(enabled=True, max_pending=2))
    r = await q.enqueue_many([(A, "en"), (B, "en"), (C, "en")])
    assert [x["state"] for x in r] == ["created", "created", "full"]


# ── atomic claiming ──
async def test_claim_marks_processing_counts_attempt_and_sets_lease():
    q, f, clock = mk()
    await q.enqueue(A, "en")
    jobs = await q.claim("w1", 5, 300)
    assert len(jobs) == 1 and jobs[0]["attempt"] == 1 and jobs[0]["lease_until"] == int(clock()) + 300
    rec = await status(q, A)
    assert rec["status"] == "processing" and rec["owner"] == "w1"
    assert await q.claim("w2", 5, 300) == []                          # nothing else due


async def test_concurrent_workers_never_claim_the_same_job():
    q, f, _ = mk()
    await q.enqueue_many([(f"v{i:010d}"[-11:], "en") for i in range(40)])
    batches = await asyncio.gather(*[q.claim(f"w{i}", 5, 300) for i in range(12)])
    ids = [j["job_id"] for b in batches for j in b]
    assert len(ids) == len(set(ids)) == 40 or len(ids) == len(set(ids))   # no duplicates ever
    assert len(ids) == 40 - (await q.summary())["pending_total"]
    rest = await asyncio.gather(*[q.claim(f"x{i}", 5, 300) for i in range(12)])
    all_ids = ids + [j["job_id"] for b in rest for j in b]
    assert len(all_ids) == len(set(all_ids)) == 40


async def test_claim_respects_not_before():
    q, f, clock = mk()
    await q.enqueue(A, "en")
    (j,) = await q.claim("w1", 1, 300)
    await q.fail(j["job_id"], "w1", "network_error", "x")            # transient -> backoff
    assert await q.claim("w1", 1, 300) == []
    clock.advance(61)
    assert len(await q.claim("w1", 1, 300)) == 1


# ── leases ──
async def test_lease_renewal_extends_and_only_owner_can_renew():
    q, f, clock = mk()
    await q.enqueue(A, "en")
    (j,) = await q.claim("w1", 1, 120)
    clock.advance(100)
    r = await q.renew(j["job_id"], "w1", 120)
    assert r["renewed"] and r["lease_until"] == int(clock()) + 120
    assert (await q.renew(j["job_id"], "w2", 120))["renewed"] is False
    assert (await q.renew("zzzzzzzzzzz:en", "w1", 120))["state"] == "unknown"


async def test_expired_lease_is_recovered_requeued_with_backoff_and_attempt_counted():
    q, f, clock = mk()
    await q.enqueue(A, "en")
    await q.claim("w1", 1, 120)
    clock.advance(121)                                                 # worker vanished (laptop shut down)
    assert await q.claim("w2", 1, 120) == []                           # recovered, but backoff (60 s) first
    rec = await status(q, A)
    assert rec["status"] == "pending" and rec["attempts"] == 1
    clock.advance(61)
    (j,) = await q.claim("w2", 1, 120)
    assert j["attempt"] == 2 and (await q.summary())["counters"]["lease_recoveries"] == 1


async def test_late_completion_after_reclaim_is_idempotent():
    q, f, clock = mk()
    await q.enqueue(A, "en")
    (j1,) = await q.claim("w1", 1, 120)
    clock.advance(200); await q.claim("w2", 1, 120); clock.advance(61)
    await q.claim("w2", 1, 120)                                        # w2 now owns attempt 2
    assert await q.complete(j1["job_id"], "w1") == "completed_late"    # w1 finally finishes: accepted
    assert await q.complete(j1["job_id"], "w2") == "already_completed" # w2 finishes too: harmless
    s = await q.summary()
    assert s["processing"] == 0 and s["completed_retained"] == 1 and s["counters"]["completed_total"] == 1


async def test_poison_job_stops_after_max_attempts_via_lease_expiry():
    q, f, clock = mk(QueueConfig(enabled=True, max_attempts=3))
    await q.enqueue(A, "en")
    for _ in range(10):
        clock.advance(3700)
        await q.claim("w1", 1, 60)
    rec = await status(q, A)
    assert rec["status"] == "failed" and rec["fail_class"] == "lease_expired_max_attempts" and rec["attempts"] == 3


# ── completion / failure classification ──
async def test_complete_is_idempotent_and_removes_from_queues():
    q, f, _ = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("w1", 1, 300)
    assert await q.complete(j["job_id"], "w1") == "completed"
    assert await q.complete(j["job_id"], "w1") == "already_completed"
    assert (await q.enqueue(A, "en"))["state"] == "completed"           # re-asking never re-queues
    s = await q.summary()
    assert s["pending_total"] == 0 and s["processing"] == 0
    assert await q.complete("zzzzzzzzzzz:en", "w1") == "unknown"


@pytest.mark.parametrize("cls", ["no_captions", "video_unavailable", "age_restricted", "language_unavailable", "invalid_video_id"])
async def test_definitive_failures_are_terminal_and_not_retried(cls):
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("w1", 1, 300)
    assert (await q.fail(j["job_id"], "w1", cls, "x"))["state"] == "failed"
    clock.advance(10 ** 6)
    assert await q.claim("w1", 1, 300) == []
    rec = await status(q, A)
    assert rec["status"] == "failed" and rec["fail_class"] == cls
    assert (await q.enqueue(A, "en"))["state"] == "failed"              # duplicate request does not resurrect it
    assert (await q.enqueue(A, "en", force=True))["state"] == "created" # explicit retry does


async def test_blocked_defers_with_exponential_delay_and_does_not_burn_attempts():
    q, f, clock = mk(QueueConfig(enabled=True, defer_base=1800, defer_max=21600, max_attempts=3))
    await q.enqueue(A, "en")
    delays = []
    for _ in range(6):                                                   # far more blocks than max_attempts
        clock.advance(30000)
        (j,) = await q.claim("w1", 1, 300)
        r = await q.fail(j["job_id"], "w1", "blocked", "bot check")
        assert r["state"] == "deferred"
        delays.append(r["retry_at"] - int(clock()))
    assert delays == [1800, 3600, 7200, 14400, 21600, 21600]            # doubled, capped
    rec = await status(q, A)
    assert rec["status"] == "deferred" and rec["attempts"] == 0 and rec["defers"] == 6


async def test_transient_failures_retry_with_backoff_then_fail_at_max_attempts():
    q, f, clock = mk(QueueConfig(enabled=True, max_attempts=3, requeue_base=60))
    await q.enqueue(A, "en")
    waits = []
    for i in range(3):
        clock.advance(5000)
        (j,) = await q.claim("w1", 1, 300)
        r = await q.fail(j["job_id"], "w1", "timeout", "slow")
        waits.append(r.get("retry_at", 0) - int(clock()) if r["state"] == "pending" else r["state"])
    assert waits == [60, 120, "failed"]
    rec = await status(q, A)
    assert rec["fail_class"] == "max_attempts:timeout" and rec["attempts"] == 3


async def test_interrupted_returns_job_immediately_without_consuming_attempt():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("w1", 1, 300)
    assert (await q.fail(j["job_id"], "w1", "interrupted", "shutdown"))["state"] == "pending"
    (j2,) = await q.claim("w2", 1, 300)
    assert j2["attempt"] == 1


async def test_stale_failure_report_is_ignored():
    q, f, clock = mk()
    await q.enqueue(A, "en"); (j,) = await q.claim("w1", 1, 300)
    assert (await q.fail(j["job_id"], "w2", "no_captions", "x"))["state"] == "stale"      # not the owner
    assert (await status(q, A))["status"] == "processing"


async def test_job_expires_after_max_age():
    q, f, clock = mk(QueueConfig(enabled=True, max_age_days=2))
    await q.enqueue(A, "en")
    clock.advance(3 * 86400)                                            # laptop offline for 3 days
    assert await q.claim("w1", 1, 300) == []
    rec = await status(q, A)
    assert rec["status"] == "failed" and rec["fail_class"] == "expired"


async def test_prune_removes_old_finished_records_and_keeps_counts_consistent():
    q, f, clock = mk(QueueConfig(enabled=True, retention_days=1))
    for v in (A, B):
        await q.enqueue(v, "en")
    for j in await q.claim("w1", 2, 300):
        await q.complete(j["job_id"], "w1")
    await q.enqueue(C, "en")
    clock.advance(2 * 86400)
    assert await q.prune() == 2
    s = await q.summary()
    assert s["completed_retained"] == 0 and s["records_retained"] == 0 and (await status(q, A)) is None


async def test_pruned_or_expired_record_does_not_wedge_the_queue():
    q, f, clock = mk()
    await q.enqueue_many([(A, "en"), (B, "en")])
    f.redis.hdel("yt:q:jobs", job_id(A, "en"))                          # record vanished (manual cleanup / TTL)
    (j,) = await q.claim("w1", 5, 300)
    assert j["video_id"] == B and (await q.summary())["pending_total"] == 0


# ── progress / depth ──
async def test_summary_reports_depth_and_progress():
    q, f, clock = mk()
    await q.enqueue_many([(f"v{i:010d}"[-11:], "en") for i in range(5)])
    js = await q.claim("w1", 2, 300)
    await q.complete(js[0]["job_id"], "w1"); await q.fail(js[1]["job_id"], "w1", "no_captions", "")
    clock.advance(30)
    s = await q.summary()
    assert (s["pending_total"], s["ready_now"], s["processing"], s["completed_retained"], s["failed_retained"]) == (3, 3, 0, 1, 1)
    assert s["oldest_pending_age_seconds"] == 30


# ── durability / outage behaviour ──
async def test_nothing_is_acknowledged_when_store_is_down_and_recovery_works():
    q, f, clock = mk()
    f.down = True
    with pytest.raises(QueueUnavailable):
        await q.enqueue(A, "en")
    with pytest.raises(QueueUnavailable):
        await q.claim("w1", 1, 300)
    f.down = False; q.remote._open_until = 0
    assert (await q.summary())["pending_total"] == 0                    # the failed enqueue left no trace
    assert (await q.enqueue(A, "en"))["state"] == "created"


async def test_reply_lost_after_write_is_safe_to_retry():
    q, f, clock = mk()
    real = f.handler
    state = {"drop": True}

    def handler(req):
        resp = real(req)                                                 # executed on the server...
        if state["drop"]:
            state["drop"] = False
            return httpx.Response(502, text="gateway")                   # ...but the reply never reached us
        return resp
    q.remote._factory = lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(QueueUnavailable):                                # caller is told "not acknowledged"
        await q.enqueue(A, "en")
    assert (await q.enqueue(A, "en"))["state"] == "exists"              # retry finds it: no duplicate
    assert (await q.summary())["pending_total"] == 1


async def test_jobs_survive_a_render_restart_and_a_new_queue_object():
    q, f, clock = mk()
    await q.enqueue_many([(A, "en"), (B, "en")]); await q.claim("w1", 1, 300)
    q2 = JobQueue(make_remote(f, clock), QueueConfig(enabled=True), clock)   # "Render restarted"
    s = await q2.summary()
    assert s["pending_total"] == 1 and s["processing"] == 1


async def test_script_error_is_reported_not_swallowed(monkeypatch):
    import job_queue
    q, f, _ = mk()
    monkeypatch.setattr(job_queue, "ENQUEUE", "return redis.call('NOPE')")
    with pytest.raises(QueueUnavailable):
        await q.enqueue(A, "en")


# ── helpers / validation ──
def test_validators_and_helpers():
    assert JOB_ID_RE.match("dQw4w9WgXcQ:en") and JOB_ID_RE.match("dQw4w9WgXcQ:pt-br") and JOB_ID_RE.match("dQw4w9WgXcQ:any")
    assert not JOB_ID_RE.match("short:en") and not JOB_ID_RE.match("dQw4w9WgXcQ:EN") and not JOB_ID_RE.match("dQw4w9WgXcQ:en|x")
    assert ERROR_CLASS_RE.match("max_attempts:timeout") and not ERROR_CLASS_RE.match("Bad Class")
    assert kind_for("no_captions") == "definitive" and kind_for("blocked") == "blocked" and kind_for("interrupted") == "interrupted"
    assert kind_for("timeout") == "transient" and kind_for("weird") == "transient"
    assert "|" not in sanitize_message("a|b\nc\x00") and len(sanitize_message("x" * 999)) == 160


async def test_scripts_only_touch_declared_keys_and_no_cjson():
    import job_queue
    for name in ("ENQUEUE", "CLAIM", "RENEW", "COMPLETE", "FAIL", "PRUNE"):
        src = getattr(job_queue, name)
        assert "cjson" not in src and "redis.call('KEYS'" not in src and "SCAN" not in src
        import re
        used = set(re.findall(r"KEYS\[(\d)\]", src))
        assert used <= {"1", "2", "3", "4", "5"}
        for call in re.findall(r"redis\.call\('([A-Z]+)', ([^,)]+)", src):
            assert call[1].startswith("KEYS["), f"{name}: {call} uses an undeclared key"


async def test_expired_jobs_ahead_in_line_do_not_starve_fresh_jobs():
    q, f, clock = mk(QueueConfig(enabled=True, max_age_days=2))
    await q.enqueue_many([(f"o{i:010d}"[-11:], "en") for i in range(30)])        # old backlog
    clock.advance(3 * 86400)
    await q.enqueue(A, "en")                                                    # fresh job queued behind 30 expired ones
    (j,) = await q.claim("w1", 1, 300)
    assert j["video_id"] == A
    s = await q.summary()
    assert s["failed_retained"] == 30 and s["processing"] == 1
