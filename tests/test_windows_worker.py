"""Local worker core against the REAL ASGI app and Lua queue (everything network-side in-process).
MOCK: YouTube extraction. Not covered: Windows Task Scheduler, real HTTPS, real YouTube."""
import json
import logging
import os
import threading

import pytest

from tests.queue_harness import (E, Extractor, Stack, VID, WORKER_TOKEN, make_result, run_until_idle, wc, main)
import asyncio


def enq(ids, lang="en"):
    return asyncio.run(main.tool_queue_transcripts(ids, lang))


def qsum():
    return asyncio.run(main.get_transcript_service().queue.summary())


@pytest.fixture
def st(monkeypatch, tmp_path):
    s = Stack(monkeypatch)
    s.dir = str(tmp_path / "worker")
    return s


def mkworker(st, extract, **kw):
    s = st.settings(state_dir=st.dir, **{k: v for k, v in kw.items() if k not in ("flaky", "stop")})
    return st.worker(s, extract, flaky=kw.get("flaky"), stop=kw.get("stop")), s


# ── settings / security ──
def test_settings_require_https_and_a_long_token():
    ok = dict(RENDER_URL="https://x.onrender.com", WORKER_TOKEN="t" * 32)
    assert wc.Settings.from_env(ok).base_url == "https://x.onrender.com"
    for bad in ({**ok, "RENDER_URL": "http://x.onrender.com"}, {**ok, "WORKER_TOKEN": "short"}, {**ok, "RENDER_URL": ""},
                {**ok, "UPSTASH_READONLY_URL": "http://insecure"}):
        with pytest.raises(ValueError):
            wc.Settings.from_env(bad)
    assert wc.Settings.from_env({**ok, "RENDER_URL": "http://localhost:8000", "ALLOW_INSECURE_LOCALHOST": "1"})


def test_token_is_not_in_repr_or_str_and_pacing_defaults_are_conservative():
    s = wc.Settings.from_env({"RENDER_URL": "https://x.onrender.com", "WORKER_TOKEN": "SECRET" + "t" * 30})
    assert "SECRET" not in repr(s) and "SECRET" not in str(s)
    assert (s.min_delay, s.max_delay, s.hourly_cap, s.daily_cap, s.poll_min) == (20, 45, 20, 120, 300)
    hot = wc.Settings.from_env({"RENDER_URL": "https://x.onrender.com", "WORKER_TOKEN": "t" * 32, "MIN_DELAY_SECONDS": "0", "HOURLY_CAP": "99999"})
    assert hot.min_delay >= 5 and hot.hourly_cap <= 120                   # cannot be configured into aggressive behaviour


def test_log_filter_scrubs_token_and_bearer_strings():
    f = wc.RedactingFilter(["tok-123456789"])
    rec = logging.LogRecord("x", logging.INFO, "", 0, "auth Bearer abc.def and tok-123456789 here", (), None)
    f.filter(rec)
    assert "tok-123456789" not in rec.getMessage() and "abc.def" not in rec.getMessage()


def test_worker_code_has_no_paid_provider_or_audio_download_paths():
    root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "windows_worker")
    src = "".join(open(os.path.join(root, f), encoding="utf-8").read() for f in os.listdir(root) if f.endswith((".py", ".ps1")))
    for banned in ("supadata", "transcriptapi", "whisper", "bestaudio", "cookiefile", "cookies_from_browser", "download=true", "download=1"):
        assert banned not in src.lower(), banned


# ── a normal day ──
def test_worker_processes_jobs_end_to_end_and_results_are_cached(st):
    enq([VID(i) for i in range(5)])
    ex = Extractor()
    w, _ = mkworker(st, ex)
    out = run_until_idle(w)
    assert out.count("worked") >= 1 and out[-1] == "idle" and ex.total == 5
    s = qsum()
    assert s["completed_retained"] == 5 and s["pending_total"] == 0 and s["processing"] == 0
    r = asyncio.run(main.tool_get_video_transcript(VID(0), "en"))
    assert r["status"] == "ok" and r["cached"] is True
    assert st.paid_calls == 0


def test_pacing_between_extractions_and_hourly_cap_are_enforced(st):
    enq([VID(i) for i in range(30)])
    w, s = mkworker(st, Extractor(), min_delay=30, max_delay=30, hourly_cap=10)
    out = run_until_idle(w)
    assert out[-1] == "capped" and qsum()["completed_retained"] == 10        # stops at the hourly cap
    assert all(x >= 30 for x in w.sleeps[:9])                               # >= min delay between extractions
    st.clock.advance(3601)
    run_until_idle(w)
    assert qsum()["completed_retained"] == 20


def test_pacing_state_survives_worker_restart_so_caps_cannot_be_reset_by_rebooting(st):
    enq([VID(i) for i in range(30)])
    w, s = mkworker(st, Extractor(), hourly_cap=5)
    run_until_idle(w)
    w2, _ = mkworker(st, Extractor(), hourly_cap=5)                         # laptop rebooted: new process, same state dir
    assert run_until_idle(w2) == ["capped"] and qsum()["completed_retained"] == 5


# ── failure handling ──
def test_captions_unavailable_is_terminal_and_not_retried(st):
    enq([VID(1)])
    ex = Extractor(lambda v, n: E("no_captions", "disabled"))
    w, _ = mkworker(st, ex)
    run_until_idle(w); st.clock.advance(10 ** 6); run_until_idle(w)
    assert ex.total == 1 and qsum()["failed_retained"] == 1
    assert asyncio.run(main.tool_get_video_transcript(VID(1), "en"))["status"] == "no_captions"


def test_blocked_pauses_the_worker_and_defers_the_job_exponentially(st):
    enq([VID(i) for i in range(10)])
    ex = Extractor(lambda v, n: E("blocked", "bot check"))
    w, _ = mkworker(st, ex, block_pause_base=900)
    out = run_until_idle(w)
    assert out[-1] == "paused" and ex.total == 1                             # ONE attempt, then it stops hammering
    assert qsum()["counters"]["blocked_deferrals"] == 1
    for _ in range(3):                                                       # keep being blocked after each pause
        st.clock.advance(w.state.paused_for() + 1)
        run_until_idle(w)
    assert ex.total <= 4 and w.state.data["blocks_in_row"] == ex.total
    pauses = [900 * 2 ** i for i in range(4)]
    assert int(w.state.paused_for()) <= pauses[min(ex.total - 1, 3)]
    assert qsum()["completed_retained"] == 0 and qsum()["failed_retained"] == 0       # nothing lost, nothing failed


def test_recovers_after_block_when_youtube_allows_again(st):
    enq([VID(1), VID(2)])
    first = {"done": False}
    def block_once(v, n):
        if not first["done"]:
            first["done"] = True; return E("blocked")
        return "ok"
    ex = Extractor(block_once)
    w, _ = mkworker(st, ex)
    run_until_idle(w)
    assert qsum()["completed_retained"] == 0
    st.clock.advance(1800 + 900 + 5)                                         # past job defer (30 min) and worker pause (15 min)
    run_until_idle(w)
    assert qsum()["completed_retained"] == 2 and w.state.data["blocks_in_row"] == 0


def test_transient_error_retried_by_queue_with_backoff(st):
    enq([VID(1)])
    ex = Extractor(lambda v, n: E("network_error", "x") if n < 3 else "ok")
    w, _ = mkworker(st, ex)
    for _ in range(6):
        run_until_idle(w); st.clock.advance(200)
    assert ex.total == 3 and qsum()["completed_retained"] == 1


# ── interruption / recovery ──
def test_worker_killed_mid_transcript_job_is_recovered_by_lease_expiry(st):
    enq([VID(1)])
    api = st.api(st.settings(state_dir=st.dir))
    (job,) = api.claim(1)                                                    # worker 1 claims... then the laptop shuts down
    assert qsum()["processing"] == 1
    assert run_until_idle(mkworker(st, Extractor(), worker_id="laptop-2")[0])[-1] == "idle"   # lease still valid: untouched
    st.clock.advance(301)                                                    # lease expired
    w2 = mkworker(st, ex := Extractor(), worker_id="laptop-2")[0]
    assert run_until_idle(w2)[-1] == "idle" and ex.total == 0                # recovered into the queue, short backoff first
    assert qsum()["pending_total"] == 1 and qsum()["processing"] == 0
    st.clock.advance(61)
    run_until_idle(w2)
    assert ex.total == 1 and qsum()["completed_retained"] == 1
    rec = asyncio.run(main.get_transcript_service().queue.get_many([job["job_id"]]))[job["job_id"]]
    assert rec["status"] == "completed" and rec["attempts"] == 2


def test_original_worker_waking_up_late_cannot_duplicate_or_corrupt(st):
    enq([VID(1)])
    s1 = st.settings(state_dir=st.dir, worker_id="laptop-1")
    api1 = st.api(s1)
    (job,) = api1.claim(1)
    st.clock.advance(400)                                                    # laptop slept; lease expired
    w2 = mkworker(st, Extractor(), worker_id="laptop-2")[0]
    run_until_idle(w2); st.clock.advance(61); run_until_idle(w2)             # laptop-2 recovers and finishes the job
    code, body = api1.complete(job["job_id"], job["fence"], make_result(25))   # laptop-1 wakes and delivers its old result
    assert code == 409 and body["error"] == "stale_job"                       # rejected, stores nothing
    assert qsum()["completed_retained"] == 1


def test_graceful_stop_requested_during_pacing_exits_promptly(st):
    enq([VID(i) for i in range(5)])
    stop = threading.Event()
    ex = Extractor()
    w, _ = mkworker(st, ex, stop=stop)
    orig = w.extract
    def extract_then_stop(v, l):
        r = orig(v, l); stop.set(); return r
    w.extract = extract_then_stop
    assert w.run_cycle() == "worked" and ex.total == 1                      # finished the current job, then stopped
    assert w.run_forever() == 0 and ex.total == 1                            # run_forever returns immediately


def test_result_is_kept_in_outbox_when_render_is_down_and_delivered_later(st):
    enq([VID(1)])
    down = {"v": False}
    ex = Extractor()
    w, _ = mkworker(st, ex, flaky=lambda: down["v"])
    orig = w.extract
    def extract_then_render_dies(v, l):
        r = orig(v, l); down["v"] = True; return r                           # Render restarts right after the work is done
    w.extract = extract_then_render_dies
    w.run_cycle()
    assert qsum()["completed_retained"] == 0
    assert len(os.listdir(os.path.join(st.dir, "outbox"))) == 1             # nothing lost: stored locally
    down["v"] = False
    w2, _ = mkworker(st, Extractor())                                        # laptop restarted too
    w2.run_cycle()
    assert qsum()["completed_retained"] == 1 and os.listdir(os.path.join(st.dir, "outbox")) == []
    assert ex.total == 1                                                      # delivered, not re-extracted


def test_offline_server_backs_off_without_hammering(st):
    enq([VID(1)])
    w, _ = mkworker(st, Extractor(), flaky=lambda: True)
    assert w.run_cycle() == "offline"
    assert qsum()["pending_total"] == 1


def test_rejected_token_stops_the_worker_instead_of_retrying_forever(st):
    s = st.settings(state_dir=st.dir, token="x" * 40)
    w = st.worker(s, Extractor())
    assert w.run_cycle() == "auth" and w.run_forever() == 3


def test_upstash_outage_during_claim_is_survivable(st):
    enq([VID(1)])
    st.fake.down = True; st.svc.remote._open_until = 0
    w, _ = mkworker(st, Extractor())
    assert w.run_cycle() == "offline"
    st.fake.down = False; st.svc.remote._open_until = 0
    assert run_until_idle(w)[-1] == "idle" and qsum()["completed_retained"] == 1


# ── slow extraction & lease renewal ──
def test_lease_is_renewed_while_a_slow_extraction_runs(st):
    import time
    enq([VID(1)])
    calls = {"renews": 0}
    w, s = mkworker(st, Extractor(), lease_seconds=60, renew_interval=0.05)
    real_renew = w.api.renew
    w.api.renew = lambda j, f: (calls.__setitem__("renews", calls["renews"] + 1), real_renew(j, f))[1]
    def slow(v, l):
        time.sleep(0.4); return make_result(10)
    w.extract = slow
    w.run_cycle()
    assert calls["renews"] >= 3 and qsum()["completed_retained"] == 1


# ── logging hygiene ──
def test_no_token_or_transcript_text_is_ever_logged(st, caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.addFilter(wc.RedactingFilter([WORKER_TOKEN]))
    enq([VID(1), VID(2)])
    ex = Extractor(lambda v, n: E("no_captions", "disabled") if v == VID(2) else "ok")
    w, _ = mkworker(st, ex)
    run_until_idle(w)
    text = caplog.text
    assert WORKER_TOKEN not in text and "w1 w2 w3" not in text and "full_transcript" not in text
    assert "ok" in text                                                      # it did log job outcomes (ids/classes only)


# ── doorbell (optional read-only Upstash check that avoids waking Render) ──
def test_doorbell_skips_render_when_nothing_is_waiting_and_falls_back_when_unknown(st):
    s = st.settings(state_dir=st.dir)
    api_calls = {"n": 0}
    w, _ = mkworker(st, Extractor())
    real_claim = w.api.claim
    w.api.claim = lambda n=1: (api_calls.__setitem__("n", api_calls["n"] + 1), real_claim(n))[1]
    w.doorbell = lambda: 0
    assert w.run_cycle() == "idle" and api_calls["n"] == 0                   # Render never contacted
    enq([VID(1)])
    w.doorbell = lambda: 1
    assert w.run_cycle() == "worked" and api_calls["n"] == 1
    enq([VID(2)])
    w.doorbell = lambda: None                                                # doorbell unreachable -> ask Render anyway
    assert w.run_cycle() == "worked"


def test_make_doorbell_builds_read_only_zcount_pipeline_and_survives_errors():
    import httpx
    seen = {}
    def handler(req):
        seen["auth"] = req.headers["authorization"]; seen["body"] = json.loads(req.content); seen["url"] = str(req.url)
        return httpx.Response(200, json=[{"result": 2}, {"result": 1}])
    s = wc.Settings(base_url="https://x", token="t" * 30, doorbell_url="https://db.upstash.io", doorbell_token="ro-token")
    ring = wc.make_doorbell(s, httpx.Client(transport=httpx.MockTransport(handler)), clock=lambda: 1000)
    assert ring() == 3 and seen["url"].endswith("/pipeline") and [c[0] for c in seen["body"]] == ["ZCOUNT", "ZCOUNT"]
    boom = wc.make_doorbell(s, httpx.Client(transport=httpx.MockTransport(lambda r: (_ for _ in ()).throw(httpx.ConnectError("x")))))
    assert boom() is None
    assert wc.make_doorbell(wc.Settings(base_url="https://x", token="t" * 30)) is None
