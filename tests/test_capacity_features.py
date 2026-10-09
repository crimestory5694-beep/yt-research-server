"""Queueing, retry/backoff, escalating block cooldown, cache purge/slimming, alerts. Providers are MOCKED."""
import asyncio
import time

import pytest

import transcript_service as ts
from tests.helpers import VID, Clock, E, FakeLibrary, PaidRecorder, good_data, make_service


class FastClock:
    """Wall clock running `speed`x faster, so a 60 s rate window elapses in ~0.1 s of test time."""

    def __init__(self, speed=600.0):
        self.t0, self.speed = time.monotonic(), speed

    def __call__(self):
        return 1_800_000_000.0 + (time.monotonic() - self.t0) * self.speed


def ids(n, p="q"):
    return [f"{p}{i:010d}"[-11:] for i in range(n)]


# ── bounded queue instead of instant rejection ──
async def test_burst_is_queued_not_rejected_when_slots_free_up_within_budget():
    svc = make_service({"TRANSCRIPT_FETCHES_PER_MINUTE": "5", "TRANSCRIPT_QUEUE_WAIT_SECONDS": "5"},
                       lib=FakeLibrary(good_data()), clock=FastClock())
    res = await asyncio.gather(*[svc.get_transcript(v) for v in ids(12)])
    assert all(r["status"] == "ok" for r in res)
    assert svc._counters["queued"] >= 7 and svc._waiters == 0


async def test_queue_budget_exhausted_returns_rate_limited_with_retry_after():
    svc = make_service({"TRANSCRIPT_FETCHES_PER_MINUTE": "2", "TRANSCRIPT_QUEUE_WAIT_SECONDS": "1"},
                       lib=FakeLibrary(good_data()))          # virtual clock never frees a slot
    res = [await svc.get_transcript(v) for v in ids(3)]
    assert [r["status"] for r in res] == ["ok", "ok", "rate_limited"] and res[2]["retry_after_seconds"] >= 1
    assert svc.diagnostics and "rate_limit_rejections_since_start" in (await svc.diagnostics())["alerts"]


async def test_queue_max_bounds_waiters():
    svc = make_service({"TRANSCRIPT_FETCHES_PER_MINUTE": "1", "TRANSCRIPT_QUEUE_WAIT_SECONDS": "2", "TRANSCRIPT_QUEUE_MAX": "2"},
                       lib=FakeLibrary(good_data()))
    t0 = time.monotonic()
    res = await asyncio.gather(*[svc.get_transcript(v) for v in ids(8)])
    st = [r["status"] for r in res]
    assert st.count("ok") == 1 and st.count("rate_limited") == 7
    assert time.monotonic() - t0 < 4        # at most 2 waited the budget; the other 5 were rejected immediately


async def test_paid_providers_do_not_consume_fetch_slots_and_reservation_not_wasted():
    paid = PaidRecorder((200, {"content": "hello world " * 20}))
    env = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "ALLOW_PAID_WITH_EPHEMERAL_STATE": "true", "SUPADATA_API_KEY": "k_SECRET",
           "TRANSCRIPT_FETCHES_PER_MINUTE": "1", "TRANSCRIPT_PROVIDER_ORDER": "youtube_direct,supadata"}
    svc = make_service(env, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    r1 = await svc.get_transcript(ids(1)[0])
    r2 = await svc.get_transcript(ids(2)[1])     # free slot exhausted -> rate_limited BEFORE any paid reservation
    assert r1["status"] == "ok" and r2["status"] == "rate_limited" and len(paid.requests) == 1
    assert svc.store.usage("supadata", time.strftime("%Y-%m", time.gmtime(svc._clock()))) == 1


# ── retry with exponential backoff ──
async def test_transient_errors_are_retried_with_backoff_then_succeed():
    lib = FakeLibrary(E(ts.NETWORK_ERROR), E(ts.TIMEOUT), good_data())
    svc = make_service({"TRANSCRIPT_RETRIES": "2", "TRANSCRIPT_BACKOFF_MS": "40"}, lib=lib)
    t0 = time.monotonic()
    r = await svc.get_transcript(VID)
    dt = time.monotonic() - t0
    assert r["status"] == "ok" and len(lib.calls) == 3 and svc._counters["retries"] == 2
    assert dt >= 0.04 + 0.08 - 0.01                                         # 40ms then 80ms (exponential)
    assert [a.get("retry") for a in r["attempts"] if a["provider"] == "youtube_direct"] == [1, 2, None]


@pytest.mark.parametrize("status", [ts.BLOCKED, ts.PARSE_ERROR, ts.NO_CAPTIONS, ts.PROXY_ERROR, ts.INVALID_VIDEO_ID])
async def test_non_transient_errors_are_never_retried(status):
    lib = FakeLibrary(E(status))
    svc = make_service({"TRANSCRIPT_RETRIES": "3", "TRANSCRIPT_BACKOFF_MS": "5"}, lib=lib)
    await svc.get_transcript(VID)
    assert len(lib.calls) == 1


async def test_retries_are_bounded_and_paid_calls_never_retried():
    lib = FakeLibrary(E(ts.NETWORK_ERROR))
    svc = make_service({"TRANSCRIPT_RETRIES": "2", "TRANSCRIPT_BACKOFF_MS": "5"}, lib=lib)
    r = await svc.get_transcript(VID)
    assert len(lib.calls) == 3 and r["status"] == "network_error"
    paid = PaidRecorder((500, "boom"))
    env = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "ALLOW_PAID_WITH_EPHEMERAL_STATE": "true", "SUPADATA_API_KEY": "k_SECRET",
           "TRANSCRIPT_RETRIES": "3", "TRANSCRIPT_BACKOFF_MS": "5", "TRANSCRIPT_PROVIDER_ORDER": "supadata"}
    await make_service(env, lib=FakeLibrary(good_data()), paid=paid).get_transcript(VID)
    assert len(paid.requests) == 1


# ── escalating block cooldown ──
async def test_block_cooldown_escalates_exponentially_caps_and_resets_on_success():
    clock = Clock()
    lib = FakeLibrary(E(ts.BLOCKED), E(ts.BLOCKED), E(ts.BLOCKED), E(ts.BLOCKED), good_data())
    svc = make_service({"TRANSCRIPT_BLOCK_COOLDOWN_SECONDS": "300", "TRANSCRIPT_BLOCK_COOLDOWN_MAX_SECONDS": "1000",
                        "TRANSCRIPT_PROVIDER_ORDER": "youtube_direct"}, lib=lib, clock=clock)
    waits = []
    for i in range(4):
        await svc.get_transcript(ids(5)[i])
        cd = svc.store.cooldown("youtube_direct", clock())
        waits.append(int(cd[0] - clock()))
        clock.advance(waits[-1] + 1)
    assert waits == [300, 600, 1000, 1000]                                  # doubled, then capped
    r = await svc.get_transcript(ids(5)[4])
    assert r["status"] == "ok" and svc._block_streak == {}
    lib.script = [E(ts.BLOCKED)]
    await svc.get_transcript(ids(6)[5])
    assert int(svc.store.cooldown("youtube_direct", clock())[0] - clock()) == 300      # streak reset


async def test_sustained_block_limits_requests_reaching_youtube():
    clock = Clock()
    lib = FakeLibrary(E(ts.BLOCKED))
    svc = make_service({"TRANSCRIPT_PROVIDER_ORDER": "youtube_direct"}, lib=lib, clock=clock)
    for i in range(288):                         # one request every 5 min for a day
        clock.advance(300)
        await svc.get_transcript(f"b{i:010d}"[-11:])
    assert len(lib.calls) <= 30                  # ~27 (ramp 5-10-20-40 min then hourly); was 288 with a flat 5-minute cooldown


# ── storage footprint ──
async def test_cached_payload_keeps_only_returned_segments_but_reports_truncation():
    svc = make_service(lib=FakeLibrary(good_data(n=3000)))
    first = await svc.get_transcript(VID)
    hit = await svc.get_transcript(VID)
    assert hit["cached"] and len(hit["segments"]) == 200 and hit["segments_truncated"] is True and hit["total_segments"] == 3000
    assert hit["full_transcript"] == first["full_transcript"]
    stored, _ = svc.store.get(f"{VID}|en|0", svc._clock())
    assert len(stored["segments"]) == 200


def test_purge_removes_expired_then_oldest_until_under_limit():
    st = ts.TranscriptStore(":memory:")
    for i in range(10):
        st.put(f"k{i}", {"x": "y" * 1000}, "positive", 100, now=i)
    st.put("old", {"x": "y"}, "positive", 1, now=0)       # expires at 1
    deleted = st.purge(now=50, max_bytes=3500)
    assert st.get("old", 50) is None and st.size_bytes() <= 3500 and deleted >= 8
    assert st.get("k9", 50) is not None                  # newest survives


async def test_local_cache_is_purged_during_operation():
    svc = make_service({"TRANSCRIPT_LOCAL_CACHE_MAX_MB": "1", "TRANSCRIPT_FETCHES_PER_MINUTE": "100000"}, lib=FakeLibrary(good_data(n=3000)))
    for v in ids(120, "p"):
        await svc.get_transcript(v)
    assert svc.store.size_bytes() <= 1_000_000 + 25 * 40000 and svc._counters["purged_rows"] > 0


# ── alerts & diagnostics ──
async def test_alerts_for_blocked_everything_failing_and_non_persistent_cache():
    svc = make_service({"TRANSCRIPT_PROVIDER_ORDER": "youtube_direct"}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)))
    for v in ids(10, "f"):
        await svc.get_transcript(v)
    d = await svc.diagnostics()
    assert "all_free_providers_failing" in d["alerts"] and "cache_not_persistent_across_restarts" in d["alerts"]
    assert d["outcomes_since_start"]["upstream_error"] == 10 and d["queue"]["max"] == 50
    blocked = make_service({"TRANSCRIPT_PROVIDER_ORDER": "youtube_direct"}, lib=FakeLibrary(E(ts.BLOCKED)))
    await blocked.get_transcript(VID)
    assert "youtube_blocked" in (await blocked.diagnostics())["alerts"]


async def test_definitive_answers_do_not_trigger_health_alerts():
    svc = make_service(lib=FakeLibrary(E(ts.NO_CAPTIONS)))
    for v in ids(12, "n"):
        await svc.get_transcript(v)
    assert "all_free_providers_failing" not in (await svc.diagnostics())["alerts"]


async def test_paid_cap_alerts_80pct_and_reached():
    paid = PaidRecorder((200, {"content": "hello world " * 20}))
    env = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "ALLOW_PAID_WITH_EPHEMERAL_STATE": "true", "SUPADATA_API_KEY": "k_SECRET",
           "TRANSCRIPT_PROVIDER_ORDER": "supadata", "SUPADATA_DAILY_LIMIT": "5", "SUPADATA_MONTHLY_LIMIT": "50"}
    svc = make_service(env, lib=FakeLibrary(good_data()), paid=paid)
    for v in ids(4, "c"):
        await svc.get_transcript(v)
    assert "paid_supadata_day_cap_80pct" in (await svc.diagnostics())["alerts"]
    await svc.get_transcript(ids(5, "c")[4])
    d = await svc.diagnostics()
    assert "paid_supadata_day_cap_reached" in d["alerts"] and d["providers"]["supadata"]["used_today"] == 5


async def test_alert_logging_is_rate_limited(caplog):
    import logging
    caplog.set_level(logging.WARNING, logger="yt.transcripts")
    svc = make_service({"TRANSCRIPT_PROVIDER_ORDER": "youtube_direct"}, lib=FakeLibrary(E(ts.BLOCKED)))
    for v in ids(3, "l"):
        await svc.get_transcript(v)
        svc.store.set_cooldown("youtube_direct", 0, "x")
    assert sum("TRANSCRIPT_ALERT youtube_blocked" in m for m in caplog.messages) == 1
