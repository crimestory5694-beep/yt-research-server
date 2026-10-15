"""Backlog visibility and policies: expiry is visible, never silent; re-opening is intentional and bounded (OFFLINE)."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))
import queue_sim as qs  # noqa: E402
from tests.queue_harness import E, Extractor, VID, main, run_until_idle  # noqa: E402


def tool(name, **kw):
    return asyncio.run(getattr(main, name)(**kw))


def test_expired_jobs_are_visible_listed_and_reopened_only_on_request(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(5)])
    st.clock.advance(8 * 86400)                                               # laptop away longer than the 7-day policy
    w, _ = qs.worker_for(st, Extractor())
    run_until_idle(w)
    s = qs._summary()
    assert s["expired_total"] == 5 and s["expired_today"] == 5 and s["failed_retained"] == 5 and s["completed_retained"] == 0
    st_out = tool("tool_transcript_queue_status", show="expired")
    assert len(st_out["jobs"]) == 5 and {j["fail_class"] for j in st_out["jobs"]} == {"expired"} and st_out["queue"]["expired_total"] == 5
    again = tool("tool_queue_transcripts", video_ids=[VID(0)])                  # asking again does NOT resurrect it
    assert again["results"][0]["state"] == "failed"
    re_open = tool("tool_queue_transcripts", reopen_expired=True)               # deliberate action
    assert re_open["summary"] == {"queued": 5}
    run_until_idle(w)
    assert qs._summary()["completed_retained"] == 5
    qs.close(st)


def test_reopen_expired_is_bounded_and_language_scoped(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(60)], "en"); qs._enqueue([VID(i) for i in range(60, 120)], "en")      # tool limit: 100 per call
    qs._enqueue([VID(i) for i in range(500, 503)], "es")
    st.clock.advance(8 * 86400)
    w, _ = qs.worker_for(st, Extractor(), hourly_cap=1)
    for _ in range(30):                                                         # one claim expires many jobs per call
        w.run_cycle()
    out = tool("tool_queue_transcripts", reopen_expired=True, language="en")
    assert out["summary"]["queued"] == 100                                       # bounded to 100 per call
    es = tool("tool_transcript_queue_status", show="expired", language="es")
    assert all(j["language"] in ("en", "es") for j in es["jobs"])
    qs.close(st)


def test_job_max_age_policy_is_configurable(tmp_path):
    st = qs.make_stack(tmp_path, env={"QUEUE_JOB_MAX_AGE_DAYS": "14"})
    qs._enqueue([VID(1)])
    st.clock.advance(8 * 86400)
    w, _ = qs.worker_for(st, Extractor())
    run_until_idle(w)
    s = qs._summary()
    assert s["completed_retained"] == 1 and s["expired_total"] == 0
    qs.close(st)


def test_summary_reports_throughput_and_drain_estimate(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(i) for i in range(30)])
    w, _ = qs.worker_for(st, Extractor(), hourly_cap=1000, daily_cap=1000)
    run_until_idle(w)
    qs._enqueue([VID(i) for i in range(100, 160)])
    s = qs._summary()
    assert s["completed_today"] == 30 and s["completed_last_3_days"] == 30 and s["pending_total"] == 60
    assert s["estimated_drain_days"] == 6.0 and s["enqueued_today"] == 90
    qs.close(st)


def test_failed_listing_shows_real_failure_classes(tmp_path):
    st = qs.make_stack(tmp_path)
    qs._enqueue([VID(1), VID(2)])
    plan = {VID(1): E("no_captions"), VID(2): E("age_restricted")}
    w, _ = qs.worker_for(st, Extractor(lambda v, n: plan[v]))
    run_until_idle(w)
    out = tool("tool_transcript_queue_status", show="failed")
    assert {j["fail_class"] for j in out["jobs"]} == {"no_captions", "age_restricted"}
    qs.close(st)


def test_new_jobs_arriving_after_reconnection_are_processed_oldest_first_without_starvation(tmp_path):
    st = qs.make_stack(tmp_path)
    ex = Extractor()
    order = []
    orig = ex.__call__
    w, _ = qs.worker_for(st, ex, hourly_cap=20, daily_cap=120, min_delay=20, max_delay=45)
    for d in range(3):                                                           # offline: backlog of 150
        qs._enqueue([VID(d * 50 + i) for i in range(50)]); st.clock.advance(86400)
    done_order = []
    for day in range(4):                                                         # online 6 h/day, 50 new jobs arrive every day
        qs._enqueue([VID(1000 + day * 50 + i) for i in range(50)])
        end = st.clock() + 6 * 3600
        while st.clock() < end:
            r = w.run_cycle()
            if r != "worked":
                st.clock.advance(min(w.state.wait_for_caps(20, 120) + 1 if r == "capped" else 300, max(end - st.clock(), 1)))
        st.clock.advance(86400 - 6 * 3600)
    first = [int(v[3:]) for v in list(ex.calls)[:50]]
    assert first == sorted(first) and max(first) < 150                           # backlog (oldest) was served first
    s = qs._summary()
    assert s["expired_total"] == 0 and s["completed_retained"] == 350 and s["pending_total"] == 0   # 150 backlog + 4 x 50 new: all served
    qs.close(st)
