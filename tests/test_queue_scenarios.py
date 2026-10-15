"""Workload / failure scenarios (OFFLINE). Real: worker, API, Lua scripts (on a Redis emulator), byte sizes, command counts.
Simulated: YouTube, Upstash, Render, time. None of this is evidence about the live services."""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))
import queue_sim as qs  # noqa: E402
from tests.queue_harness import E, Extractor, Stack, VID, main, make_result, run_until_idle  # noqa: E402


@pytest.mark.parametrize("per_day", [50, 100])
def test_daily_workload_three_days(tmp_path, per_day):
    r = qs.steady(tmp_path, per_day, days=3)
    assert r["completed"] == per_day * 3 and r["failed"] == 0
    assert all(d["pending_after"] == 0 for d in r["daily"]) and all(d["virtual_hours_to_finish"] < 8 for d in r["daily"])
    assert r["extractions"] == per_day * 3                                   # every video extracted exactly once
    assert r["paid_calls"] == 0 and r["paid_usage_keys"] == 0                # zero paid-provider usage
    assert r["commands_per_transcript_worst_case"] < 60                      # measured ~35; guard against regressions
    assert r["cache_bytes_per_transcript"] < 40_000


@pytest.mark.parametrize("per_day", [50, 100])
def test_laptop_offline_three_days_then_catches_up_without_loss(tmp_path, per_day):
    r = qs.offline_then_catch_up(tmp_path, per_day)
    assert r["peak_backlog"] == per_day * 3 and r["completed"] == per_day * 3 and r["failed"] == 0
    assert r["pending_after_each_online_day"][-1] == 0
    assert r["pending_after_each_online_day"][0] == per_day * 3 - 120        # daily cap 120 paces the catch-up
    assert r["paid_calls"] == 0


def test_laptop_offline_longer_than_job_max_age_expires_jobs_visibly(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(5)])
    st.clock.advance(8 * 86400)
    w, _ = qs.worker_for(st, Extractor())
    run_until_idle(w)
    s = qs._summary()
    assert s["failed_retained"] == 5 and s["completed_retained"] == 0
    rec = asyncio.run(main.get_transcript_service().queue.get_many([f"{VID(0)}:en"]))[f"{VID(0)}:en"]
    assert rec["fail_class"] == "expired"                                    # visible, not silently lost
    r = asyncio.run(main.tool_queue_transcripts([VID(0)], "en", retry_failed=True))
    assert r["results"][0]["state"] == "queued"                              # and can be re-opened deliberately
    qs.close(st)


def test_concurrent_workers_never_extract_the_same_video_twice(tmp_path):
    r = qs.concurrent_workers(tmp_path, jobs=48, workers=4)
    assert r["completed"] == 48 and r["extractions"] == 48 and r["max_extractions_of_one_video"] == 1 and r["claims"] == 48


def test_large_three_hour_transcripts(tmp_path):
    r = qs.large_transcripts(tmp_path, minutes=180, n=3)
    assert r["completed"] == 3 and r["segments_returned"] == 200 and r["total_segments"] > 3000 and r["chars"] > 100_000
    assert r["cache_bytes_each"] < 200_000


def test_render_restart_in_the_middle_of_a_run_loses_nothing(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(40)])
    ex = Extractor()
    w, _ = qs.worker_for(st, ex, hourly_cap=15)
    run_until_idle(w)                                                         # 15 done
    st.restart_render()                                                       # Render redeploys / spins down: memory + local DB gone
    w.api = qs.worker_for(st, ex)[0].api                                      # worker keeps running against the "new" Render
    st.clock.advance(3601); run_until_idle(w); st.clock.advance(3601); run_until_idle(w)
    s = qs._summary()
    assert s["completed_retained"] == 40 and ex.total == 40
    assert asyncio.run(main.tool_get_video_transcript(VID(3), "en"))["cached"] is True
    qs.close(st)


def test_upstash_outage_mid_run_nothing_lost_nothing_falsely_acknowledged(tmp_path):
    st = qs.make_stack(tmp_path)
    r1 = asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(10)], "en"))
    assert r1["summary"] == {"queued": 10}
    ex = Extractor()
    w, _ = qs.worker_for(st, ex, hourly_cap=1000, daily_cap=1000)
    st.fake.down = True; st.svc.remote._open_until = 0
    r2 = asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(10, 20)], "en"))
    assert r2["status"] == "queue_unavailable" and r2["queued"] == 0           # told the truth: NOT queued
    assert w.run_cycle() == "offline" and ex.total == 0
    st.fake.down = False; st.svc.remote._open_until = 0
    assert asyncio.run(main.tool_transcript_queue_status([VID(15)]))["videos"][0]["state"] == "not_queued"
    run_until_idle(w)
    assert qs._summary()["completed_retained"] == 10 and ex.total == 10
    qs.close(st)


def test_upstash_outage_while_a_result_is_ready_keeps_it_locally_then_delivers(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(1)])
    ex = Extractor()
    w, _ = qs.worker_for(st, ex)
    orig = w.extract
    def die_after_work(v, l):
        r = orig(v, l); st.fake.down = True; st.svc.remote._open_until = 0; return r
    w.extract = die_after_work
    w.run_cycle()
    assert len(os.listdir(os.path.join(st.dir, "outbox"))) == 1 and qs._summary.__name__            # kept locally
    st.fake.down = False; st.svc.remote._open_until = 0; st.clock.advance(10)
    w.extract = orig
    w.run_cycle()
    assert qs._summary()["completed_retained"] == 1 and ex.total == 1
    qs.close(st)


def test_slow_extraction_longer_than_lease_with_a_second_worker_is_idempotent(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(1)])
    ex1, ex2 = Extractor(clock=st.clock, seconds_per_call=400), Extractor()            # worker 1 takes 400 s (> 300 s lease)
    w1, _ = qs.worker_for(st, ex1, worker_id="laptop-1", renew_interval=10_000)
    w2, _ = qs.worker_for(st, ex2, worker_id="laptop-2")
    orig = w1.extract
    def slow_then_other_worker_takes_over(v, l):
        r = orig(v, l)                                                         # clock advanced 400 s inside
        run_until_idle(w2); st.clock.advance(61); run_until_idle(w2)         # w2 recovers the expired lease and finishes
        return r
    w1.extract = slow_then_other_worker_takes_over
    w1.run_cycle()
    s = qs._summary()
    assert s["completed_retained"] == 1 and s["processing"] == 0 and s["pending_total"] == 0
    assert ex1.total == 1 and ex2.total == 1                                   # duplicated work happened, results did not
    assert s["counters"]["completed_total"] == 1 and s["counters"].get("dup_completions", 0) + s["counters"].get("late_completions", 0) >= 1
    qs.close(st)


def test_youtube_blocking_all_day_does_not_hammer_youtube_or_lose_jobs(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(50)])
    ex = Extractor(lambda v, n: E("blocked", "bot check"))
    w, _ = qs.worker_for(st, ex)
    for _ in range(96):                                                       # 24 h in 15-min steps
        run_until_idle(w); st.clock.advance(900)
    assert ex.total <= 8                                                       # exponential pauses: a handful of probes per day
    s = qs._summary()
    assert s["pending_total"] == 50 and s["failed_retained"] == 0 and s["completed_retained"] == 0
    ex.behavior = None                                                         # YouTube lets the laptop through again
    for _ in range(40):
        run_until_idle(w); st.clock.advance(1800)
    assert qs._summary()["completed_retained"] == 50
    qs.close(st)


def test_mixed_outcomes_are_all_classified_and_visible(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(6)])
    plan = {VID(0): "ok", VID(1): E("no_captions"), VID(2): E("video_unavailable"), VID(3): E("age_restricted"),
            VID(4): "ok", VID(5): E("language_unavailable")}
    ex = Extractor(lambda v, n: plan[v])
    w, _ = qs.worker_for(st, ex)
    run_until_idle(w)
    s = qs._summary()
    assert (s["completed_retained"], s["failed_retained"], s["pending_total"]) == (2, 4, 0)
    rows = asyncio.run(main.tool_transcript_queue_status([VID(i) for i in range(6)]))["videos"]
    assert [r["state"] for r in rows] == ["completed", "failed", "failed", "failed", "completed", "failed"]
    assert {r["fail_class"] for r in rows if r["state"] == "failed"} == {"no_captions", "video_unavailable", "age_restricted", "language_unavailable"}
    qs.close(st)


def test_duplicate_requests_across_tools_create_one_job_and_one_extraction(tmp_path):
    st = qs.make_stack(tmp_path)
    for _ in range(5):
        asyncio.run(main.tool_queue_transcripts([VID(1), f"https://youtu.be/{VID(1)}"], "en"))
        asyncio.run(main.tool_get_video_transcript(VID(1), "en"))               # Render itself is "blocked" here -> auto-queue
    assert qs._summary()["counters"]["created"] == 1
    ex = Extractor(); w, _ = qs.worker_for(st, ex); run_until_idle(w)
    assert ex.total == 1
    r = asyncio.run(main.tool_queue_transcripts([VID(1)], "en"))
    assert r["results"][0]["state"] == "cached"
    qs.close(st)


def test_backpressure_keeps_the_queue_bounded(tmp_path):
    st = qs.make_stack(tmp_path, env={"QUEUE_MAX_PENDING": "20"})
    r = asyncio.run(main.tool_queue_transcripts([VID(i) for i in range(30)], "en"))
    assert r["summary"] == {"queued": 20, "full": 10}
    qs.close(st)


def test_idle_render_poll_is_cheap_and_diagnostics_show_queue_health(tmp_path):
    r = qs.idle_poll_cost(tmp_path)
    assert r["render_idle_claim_commands"] <= 8
    st = qs.make_stack(tmp_path / "d")
    qs._enqueue([VID(1)])
    d = asyncio.run(main.get_transcript_service().diagnostics())
    assert d["job_queue"]["pending_total"] == 1 and d["remote_storage"]["limit_mb"] == 256
    st.clock.advance(7 * 3600)
    d = asyncio.run(main.get_transcript_service().diagnostics())
    assert "queue_backlog_older_than_6h_worker_offline_or_stalled" in d["alerts"]
    qs.close(st)


def test_storage_threshold_alerts_at_70_and_90_percent(tmp_path):
    st = qs.make_stack(tmp_path, env={"REMOTE_STORAGE_LIMIT_MB": "1"})
    svc = main.get_transcript_service()
    asyncio.run(svc.remote.put_cache("k1", {"x": "y" * 10}, "positive", 100))
    st.fake.redis.hset("yt:cw", st.clock.__class__.__name__ and __import__("time").strftime("%Y-%m-%d", __import__("time").gmtime(st.clock())), 750_000)
    assert "remote_storage_70pct" in asyncio.run(svc.diagnostics())["alerts"]
    st.fake.redis.hset("yt:cw", __import__("time").strftime("%Y-%m-%d", __import__("time").gmtime(st.clock())), 950_000)
    assert "remote_storage_90pct" in asyncio.run(svc.diagnostics())["alerts"]
    qs.close(st)


def test_capacity_projection_for_1500_and_3000_per_month_fits_free_tier(tmp_path):
    s = qs.steady(tmp_path, 50, days=2)
    p = qs.capacity_projection(s["commands_per_transcript_worst_case"], s["cache_bytes_per_transcript"], 5)
    for k, v in p["commands"].items():
        assert v["pct_of_500k"] < 60, k                                          # >= 40 % head-room even at 3,000/month
    assert p["storage"]["3000/month"]["pct_of_256MB"] < 50
