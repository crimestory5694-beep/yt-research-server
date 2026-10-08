"""Persistent shared cache + enforceable caps. The remote store is a MOCK of Upstash's REST protocol."""
import json

import pytest

import transcript_service as ts
from remote_store import RemoteStore, decode_value, encode_value
from tests.helpers import (VID, Clock, E, FakeLibrary, FakeUpstash, PaidRecorder, good_data, make_remote,
                           make_service_remote)

PAID = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "SUPADATA_API_KEY": "sd_SECRET_654321",
        "TRANSCRIPT_API_KEY": "tk_SECRET_123456"}
LONG = "hello world " * 20


# ── cache survives a "Render restart" ──
async def test_cache_survives_restart_via_remote():
    fake = FakeUpstash()
    lib1 = FakeLibrary(good_data(n=50))
    await make_service_remote(fake, lib=lib1).get_transcript(VID)
    lib2 = FakeLibrary(E(ts.BLOCKED))                       # a restarted, blocked instance: new empty local DB
    svc2 = make_service_remote(fake, lib=lib2)
    r = await svc2.get_transcript(VID)
    assert r["status"] == "ok" and r["cached"] is True and lib2.calls == []
    assert svc2._counters["remote_cache_hits"] == 1
    r2 = await svc2.get_transcript(VID)                     # now served from the warmed local layer
    assert r2["cached"] and svc2._counters["remote_cache_hits"] == 1


async def test_negative_cache_shared_via_remote():
    fake = FakeUpstash()
    await make_service_remote(fake, lib=FakeLibrary(E(ts.NO_CAPTIONS))).get_transcript(VID)
    lib2 = FakeLibrary(good_data())
    r = await make_service_remote(fake, lib=lib2).get_transcript(VID)
    assert r["status"] == "no_captions" and lib2.calls == []


async def test_remote_values_are_compressed_with_ttl_and_roundtrip():
    fake = FakeUpstash()
    await make_service_remote(fake, lib=FakeLibrary(good_data(n=3000))).get_transcript(VID)
    (k, v), = [(k, v) for k, v in fake.data.items() if ":c:" in k]
    assert fake.ttl[k] == 30 * 86400 and len(v) < len(json.dumps(good_data(n=3000))) // 4
    payload, kind = decode_value(v)
    assert kind == "positive" and payload["total_segments"] == 3000


async def test_oversized_value_drops_segments_but_keeps_text(monkeypatch):
    import remote_store
    fake = FakeUpstash()
    rs = make_remote(fake)
    big = {"status": "ok", "full_transcript": "x" * 100, "segments": [{"text": "t" * 50, "start": i} for i in range(20000)]}
    monkeypatch.setattr(remote_store, "MAX_VALUE_BYTES", 5000)
    assert await rs.put_cache("k", big, "positive", 60) is True
    got, _ = await rs.get_cache("k")
    assert got["segments"] == [] and got["full_transcript"] == "x" * 100


# ── remote failure never breaks transcripts; breaker stops hammering ──
async def test_remote_down_degrades_to_local_and_still_serves():
    fake = FakeUpstash(); fake.down = True
    lib = FakeLibrary(good_data())
    svc = make_service_remote(fake, lib=lib)
    r = await svc.get_transcript(VID)
    assert r["status"] == "ok" and (await svc.get_transcript(VID))["cached"] is True
    assert svc.remote.stats["errors"] >= 1


async def test_circuit_breaker_short_circuits_then_recovers():
    fake = FakeUpstash(); fake.down = True
    clock = Clock()
    rs = make_remote(fake, clock, breaker_failures=2, breaker_seconds=60)
    for _ in range(2):
        await rs.get_cache("a")
    n = len(fake.requests)
    await rs.get_cache("a"); await rs.get_cache("b")
    assert len(fake.requests) == n and rs.stats["short_circuited"] == 2 and not rs.available
    fake.down = False; clock.advance(61)
    assert await rs.put_cache("a", {"x": 1}, "positive", 10) is True and rs.available


async def test_bad_token_and_http_errors_are_contained():
    fake = FakeUpstash(token="right")
    rs = RemoteStore("https://x", "wrong", client_factory=fake.factory())
    assert await rs.get_cache("k") is None and "401" in rs.last_error
    fake.status = 500
    rs2 = make_remote(fake)
    assert await rs2.get_cache("k") is None


async def test_roundtrip_verifies_protocol_and_cleans_up():
    fake = FakeUpstash()
    assert (await make_remote(fake).roundtrip())["ok"] is True
    assert not [k for k in fake.data if "probe" in k]
    fake.down = True
    assert (await make_remote(fake).roundtrip())["ok"] is False


# ── caps survive restarts (the Render-Free problem) ──
async def test_daily_cap_persists_across_restarts_and_blocks_paid():
    fake = FakeUpstash()
    env = {**PAID, "SUPADATA_DAILY_LIMIT": "2", "TRANSCRIPTAPI_DAILY_LIMIT": "0", "SUPADATA_MONTHLY_LIMIT": "50"}
    paid = PaidRecorder((200, {"content": LONG}))
    clock = Clock()
    ids = ["aaaaaaaaaa1", "aaaaaaaaaa2", "aaaaaaaaaa3"]
    out = []
    for vid in ids:       # each iteration = a brand-new instance (restart): local counters would reset every time
        svc = make_service_remote(fake, env, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid, clock=clock)
        out.append(await svc.get_transcript(vid))
    assert [o["status"] for o in out[:2]] == ["ok", "ok"] and out[2]["status"] != "ok"
    assert len(paid.requests) == 2
    assert {"provider": "supadata", "outcome": "skipped", "reason": "daily_limit_reached"} in out[2]["attempts"]


async def test_zero_limit_means_provider_cannot_be_used():
    fake = FakeUpstash()
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service_remote(fake, {**PAID, "SUPADATA_MONTHLY_LIMIT": "0", "TRANSCRIPTAPI_MONTHLY_LIMIT": "0"},
                              lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    r = await svc.get_transcript(VID)
    assert paid.requests == [] and r["status"] != "ok"


async def test_paid_fails_closed_when_remote_unavailable():
    fake = FakeUpstash(); fake.down = True
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service_remote(fake, PAID, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    r = await svc.get_transcript(VID)
    assert paid.requests == [] and any(a.get("reason") == "state_unavailable" for a in r["attempts"])


async def test_reservation_is_released_when_over_limit_so_counter_is_exact():
    fake = FakeUpstash()
    rs = make_remote(fake)
    assert await rs.reserve("supadata", "2026-10", "2026-10-08", 1, 5) is None
    assert await rs.reserve("supadata", "2026-10", "2026-10-08", 1, 5) == "monthly_limit_reached"
    assert fake.data["yt:u:supadata:2026-10"] == 1 and fake.data["yt:u:supadata:2026-10-08"] == 1


async def test_quota_cooldown_persists_across_restarts():
    fake = FakeUpstash()
    paid = PaidRecorder((402, {"error": "quota"}))
    clock = Clock()
    env = {**PAID, "TRANSCRIPT_API_KEY": ""}
    await make_service_remote(fake, env, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid, clock=clock).get_transcript(VID)
    n = len(paid.requests)
    r = await make_service_remote(fake, env, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid, clock=clock).get_transcript("bbbbbbbbbbb")
    assert len(paid.requests) == n and any("cooldown:provider_quota_exhausted" in a.get("reason", "") for a in r["attempts"])


async def test_paid_allowed_with_remote_even_on_ephemeral_fs_and_default_off_otherwise():
    fake = FakeUpstash()
    paid = PaidRecorder((200, {"content": LONG}))
    on = make_service_remote(fake, {**PAID, "TRANSCRIPT_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    assert (await on.get_transcript(VID))["status"] == "ok"
    off = make_service_remote(fake, {"SUPADATA_API_KEY": "k_SECRET", "TRANSCRIPT_API_KEY": "t_SECRET"},
                              lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=PaidRecorder((200, {"content": LONG})))
    r = await off.get_transcript("ccccccccccc")
    assert all(a.get("reason") == "paid_apis_disabled" for a in r["attempts"] if a["provider"] in ts.PAID_PROVIDERS)


def test_default_caps_are_conservative():
    cfg = ts.TranscriptConfig.from_env({})
    assert cfg.limits_for("supadata") == (25, 5) and cfg.limits_for("transcriptapi") == (25, 5)
    cfg = ts.TranscriptConfig.from_env({"SUPADATA_MONTHLY_LIMIT": "80", "PAID_TRANSCRIPT_DAILY_LIMIT": "3"})
    assert cfg.limits_for("supadata") == (80, 3) and cfg.limits_for("transcriptapi") == (25, 3)


async def test_local_reserve_daily_and_monthly():
    st = ts.TranscriptStore(":memory:")
    assert st.reserve("p", "2026-10", "2026-10-08", 3, 2) is None
    assert st.reserve("p", "2026-10", "2026-10-08", 3, 2) is None
    assert st.reserve("p", "2026-10", "2026-10-08", 3, 2) == "daily_limit_reached"
    assert st.reserve("p", "2026-10", "2026-10-09", 3, 2) is None
    assert st.reserve("p", "2026-10", "2026-10-10", 3, 2) == "monthly_limit_reached"


async def test_diagnostics_show_remote_state_without_secrets():
    fake = FakeUpstash()
    svc = make_service_remote(fake, PAID, lib=FakeLibrary(good_data()))
    await svc.get_transcript(VID)
    d = await svc.diagnostics()
    assert d["cache"]["remote"]["configured"] and d["cache"]["remote"]["errors"] == 0
    assert "SECRET" not in str(d) and "fake-upstash" not in str(d)
    assert d["providers"]["supadata"]["monthly_limit"] == 25 and d["providers"]["supadata"]["used_today"] == 0


async def test_any_language_result_with_matching_language_serves_strict_requests_too():
    fake = FakeUpstash()
    await make_service_remote(fake, lib=FakeLibrary(good_data("en"))).get_transcript(VID, "en", any_language=True)
    lib = FakeLibrary(E(ts.BLOCKED))
    assert (await make_service_remote(fake, lib=lib).get_transcript(VID, "en"))["cached"] is True and lib.calls == []


async def test_any_language_fallback_result_does_not_pose_as_exact_match():
    fake = FakeUpstash()
    await make_service_remote(fake, lib=FakeLibrary(good_data("de"))).get_transcript(VID, "en", any_language=True)
    lib = FakeLibrary(E(ts.LANGUAGE_UNAVAILABLE, "none"))
    r = await make_service_remote(fake, lib=lib).get_transcript(VID, "en")      # strict request must not get German
    assert r["status"] == "language_unavailable" and len(lib.calls) == 1
    again = await make_service_remote(fake, lib=FakeLibrary(E(ts.BLOCKED))).get_transcript(VID, "en", any_language=True)
    assert again["cached"] is True and again["language"] == "de"
