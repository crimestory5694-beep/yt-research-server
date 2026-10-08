"""Service-level tests. The YouTube library and paid providers are MOCKED (no network)."""
import asyncio

import pytest

import transcript_service as ts
from tests.helpers import VID, Clock, E, FakeLibrary, PaidRecorder, good_data, make_service

PAID_ENV = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "ALLOW_PAID_WITH_EPHEMERAL_STATE": "true", "TRANSCRIPT_API_KEY": "tk_SECRET_123456",
            "SUPADATA_API_KEY": "sd_SECRET_654321"}
LONG = "hello world " * 20  # > 50 chars for paid parsers


# ── success, formatting, backward-compatible shape ──
async def test_success_shape_is_backward_compatible():
    svc = make_service(lib=FakeLibrary(good_data(n=3)))
    r = await svc.get_transcript(VID, "en")
    for k in ("video_id", "language", "total_segments", "full_transcript", "segments", "source", "proxy_used"):
        assert k in r  # keys the old tool returned
    assert r["status"] == "ok" and r["source"] == "youtube-transcript-api" and r["proxy_used"] is False
    assert r["full_transcript"] == "word0 word1 word2" and r["total_segments"] == 3
    assert r["segments"][0] == {"text": "word0", "start": 0.0, "duration": 1.0}
    assert r["caption_type"] == "manual" and r["cached"] is False and r["segments_truncated"] is False


async def test_long_video_segments_truncated_but_full_text_complete():
    svc = make_service(lib=FakeLibrary(good_data(n=5000)))
    r = await svc.get_transcript(VID)
    assert len(r["segments"]) == 200 and r["segments_truncated"] is True
    assert r["total_segments"] == 5000 and r["full_transcript"].count("word") == 5000


async def test_auto_generated_flag():
    svc = make_service(lib=FakeLibrary(good_data(generated=True)))
    assert (await svc.get_transcript(VID))["caption_type"] == "auto_generated"


# ── identifiers ──
@pytest.mark.parametrize("bad", ["", "abc", "x" * 30, "https://example.com/watch?v=dQw4w9WgXcQ", None, 123])
async def test_invalid_ids_rejected_without_any_fetch(bad):
    lib = FakeLibrary(good_data())
    r = await make_service(lib=lib).get_transcript(bad)
    assert r["status"] == "invalid_video_id" and r["retryable"] is False and lib.calls == []


@pytest.mark.parametrize("url", [
    f"https://www.youtube.com/watch?v={VID}&t=5s", f"https://youtu.be/{VID}?si=abc",
    f"https://www.youtube.com/shorts/{VID}", f"https://www.youtube.com/embed/{VID}",
    f"youtube.com/watch?v={VID}", f"https://www.youtube.com/live/{VID}"])
async def test_url_forms_normalized(url):
    lib = FakeLibrary(good_data())
    r = await make_service(lib=lib).get_transcript(url)
    assert r["status"] == "ok" and lib.calls[0]["video_id"] == VID


# ── language selection ──
def test_parse_languages():
    assert ts.parse_languages("en") == ["en"]
    assert ts.parse_languages("es") == ["es", "en"]
    assert ts.parse_languages("es, pt") == ["es", "pt", "en"]
    assert ts.parse_languages("") == ["en"]


def test_pick_transcript_prefers_manual_then_primary_subtag():
    av = [("en", True, "auto-en"), ("en", False, "man-en"), ("pt-BR", False, "pt"), ("en-GB", False, "gb")]
    assert ts.pick_transcript(av, ["en"])[2] == "man-en"
    assert ts.pick_transcript(av, ["pt"])[2] == "pt"          # pt matches pt-BR
    assert ts.pick_transcript(av, ["fr", "en-GB"])[2] == "gb"
    assert ts.pick_transcript(av, ["fr"]) is None


async def test_language_passed_and_en_fallback_appended():
    lib = FakeLibrary(good_data("es"))
    r = await make_service(lib=lib).get_transcript(VID, "es")
    assert lib.calls[0]["languages"] == ["es", "en"] and r["language"] == "es" and r["requested_language"] == "es"


async def test_language_unavailable_lists_available_and_does_not_call_paid():
    err = E(ts.LANGUAGE_UNAVAILABLE, "none", available_languages=[{"code": "de", "name": "German", "auto_generated": False}])
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service(PAID_ENV, lib=FakeLibrary(err), paid=paid)
    r = await svc.get_transcript(VID, "fr")
    assert r["status"] == "language_unavailable" and r["available_languages"][0]["code"] == "de"
    assert paid.requests == []


async def test_any_language_flag_forwarded():
    lib = FakeLibrary(good_data("de"))
    await make_service(lib=lib).get_transcript(VID, "fr", any_language=True)
    assert lib.calls[0]["any_language"] is True


# ── missing captions: definitive, never reaches paid providers, negative-cached ──
async def test_no_captions_is_definitive_and_never_spends_credits():
    paid = PaidRecorder((200, {"content": LONG}))
    lib = FakeLibrary(E(ts.NO_CAPTIONS, "disabled"))
    svc = make_service(PAID_ENV, lib=lib, paid=paid)
    r = await svc.get_transcript(VID)
    assert r["status"] == "no_captions" and r["retryable"] is False
    assert paid.requests == [] and len(lib.calls) == 1
    r2 = await svc.get_transcript(VID)               # negative cache
    assert r2["cached"] is True and len(lib.calls) == 1


async def test_negative_cache_expires():
    clock = Clock()
    lib = FakeLibrary(E(ts.NO_CAPTIONS, "disabled"))
    svc = make_service(lib=lib, clock=clock)
    await svc.get_transcript(VID)
    clock.advance(7 * 3600)
    await svc.get_transcript(VID)
    assert len(lib.calls) == 2


async def test_video_unavailable_and_age_restricted_are_definitive():
    for st in (ts.VIDEO_UNAVAILABLE, ts.AGE_RESTRICTED, ts.VIDEO_UNPLAYABLE):
        paid = PaidRecorder((200, {"content": LONG}))
        r = await make_service(PAID_ENV, lib=FakeLibrary(E(st)), paid=paid).get_transcript(VID)
        assert r["status"] == st and paid.requests == []


# ── cache + de-duplication ──
async def test_cache_hit_and_miss():
    lib = FakeLibrary(good_data())
    svc = make_service(lib=lib)
    a = await svc.get_transcript(VID); b = await svc.get_transcript(VID)
    assert a["cached"] is False and b["cached"] is True and len(lib.calls) == 1
    await svc.get_transcript(VID, "es")               # different language key = miss
    assert len(lib.calls) == 2


async def test_cache_is_persistent_across_service_instances(tmp_path):
    path = str(tmp_path / "c.sqlite3")
    env = {"TRANSCRIPT_CACHE_PATH": path}
    lib1 = FakeLibrary(good_data())
    cfg = ts.TranscriptConfig.from_env(env)
    s1 = ts.TranscriptService(cfg, store=ts.TranscriptStore(path), library_fetch=lib1)
    await s1.get_transcript(VID)
    lib2 = FakeLibrary(good_data())
    s2 = ts.TranscriptService(cfg, store=ts.TranscriptStore(path), library_fetch=lib2)
    r = await s2.get_transcript(VID)
    assert r["cached"] is True and lib2.calls == []


async def test_failures_are_not_cached():
    lib = FakeLibrary(E(ts.NETWORK_ERROR, "boom"), good_data())
    svc = make_service(lib=lib)
    assert (await svc.get_transcript(VID))["status"] != "ok"
    assert (await svc.get_transcript(VID))["status"] == "ok"


async def test_simultaneous_identical_requests_share_one_fetch():
    import time
    calls = []

    def slow(video_id, languages, any_language, proxy, timeout, secrets=()):
        calls.append(1); time.sleep(0.2); return good_data()
    svc = make_service(lib=slow)
    results = await asyncio.gather(*[svc.get_transcript(VID) for _ in range(8)])
    assert len(calls) == 1 and all(r["status"] == "ok" for r in results)
    assert sum(1 for r in results if r.get("deduplicated")) == 7


# ── blocked IP / proxy ──
async def test_blocked_without_proxy_gives_actionable_error_and_cooldown():
    lib = FakeLibrary(E(ts.BLOCKED, "blocked"))
    svc = make_service(lib=lib)
    r = await svc.get_transcript(VID)
    assert r["status"] == "blocked" and r["retryable"] is True and "proxy" in r["error"].lower()
    r2 = await svc.get_transcript("aaaaaaaaaaa")      # different video, direct is cooling down
    assert r2["status"] == "blocked" and len(lib.calls) == 1   # not hammering a blocked IP
    assert any(a.get("reason", "").startswith("cooldown") for a in r2["attempts"])


async def test_blocked_direct_falls_back_to_webshare_proxy():
    lib = FakeLibrary(E(ts.BLOCKED), good_data())
    svc = make_service({"WEBSHARE_USER": "u_secret", "WEBSHARE_PASS": "p_secret"}, lib=lib)
    r = await svc.get_transcript(VID)
    assert r["status"] == "ok" and r["proxy_used"] == "webshare"
    assert [c["proxy"] for c in lib.calls] == [None, "WebshareProxyConfig"]


async def test_generic_proxy_reports_true_and_cooldown_routes_straight_to_proxy():
    lib = FakeLibrary(E(ts.BLOCKED), good_data())
    svc = make_service({"PROXY_URL": "http://user:pw@proxy.example:8080"}, lib=lib)
    r = await svc.get_transcript(VID)
    assert r["proxy_used"] is True and lib.calls[1]["proxy"] == "GenericProxyConfig"
    r2 = await svc.get_transcript("bbbbbbbbbbb")      # direct cooling down -> proxy only
    assert len(lib.calls) == 3 and lib.calls[2]["proxy"] == "GenericProxyConfig"


async def test_proxy_not_used_when_direct_works():
    lib = FakeLibrary(good_data())
    await make_service({"PROXY_URL": "http://p:1"}, lib=lib).get_transcript(VID)
    assert [c["proxy"] for c in lib.calls] == [None]


async def test_proxy_error_and_misconfig_classified_and_reported():
    lib = FakeLibrary(E(ts.BLOCKED), E(ts.PROXY_ERROR, "Proxy connection failed"))
    r = await make_service({"PROXY_URL": "http://p:1"}, lib=lib).get_transcript(VID)
    assert r["status"] == "blocked" and "proxy" in r["error"].lower()
    assert [a["outcome"] for a in r["attempts"] if a["provider"].startswith("youtube")] == ["blocked", "proxy_error"]


# ── network errors / timeouts ──
async def test_library_network_error_then_success_via_proxy():
    lib = FakeLibrary(E(ts.NETWORK_ERROR), good_data())
    r = await make_service({"PROXY_URL": "http://p:1"}, lib=lib).get_transcript(VID)
    assert r["status"] == "ok"


async def test_hung_provider_times_out_without_hanging_server():
    import time
    def hang(*a, **k):
        time.sleep(3); return good_data()
    svc = make_service({"TRANSCRIPT_TOTAL_TIMEOUT_SECONDS": "1"}, lib=hang)
    t0 = asyncio.get_event_loop().time()
    r = await svc.get_transcript(VID)
    assert r["status"] == "timeout" and asyncio.get_event_loop().time() - t0 < 2.5


# ── paid providers: default OFF, caps, quota vs no-captions ──
async def test_paid_apis_disabled_by_default_even_with_keys():
    paid = PaidRecorder((200, {"content": LONG}))
    env = {k: v for k, v in PAID_ENV.items() if k != "ENABLE_PAID_TRANSCRIPT_APIS"}
    r = await make_service(env, lib=FakeLibrary(E(ts.BLOCKED)), paid=paid).get_transcript(VID)
    assert paid.requests == [] and r["status"] == "blocked"
    assert {"provider": "transcriptapi", "outcome": "skipped", "reason": "paid_apis_disabled"} in r["attempts"]


async def test_paid_enabled_but_free_succeeds_spends_nothing():
    paid = PaidRecorder((200, {"content": LONG}))
    await make_service(PAID_ENV, lib=FakeLibrary(good_data()), paid=paid).get_transcript(VID)
    assert paid.requests == []


async def test_paid_fallback_success_transcriptapi():
    paid = PaidRecorder((200, {"segments": [{"text": "hello world " * 10, "start": 0, "duration": 2}]}))
    r = await make_service(PAID_ENV, lib=FakeLibrary(E(ts.BLOCKED)), paid=paid).get_transcript(VID)
    assert r["status"] == "ok" and r["source"] == "transcriptapi.com" and r["proxy_used"] is False
    assert paid.requests[0].headers["authorization"].startswith("Bearer ")


async def test_quota_exhausted_is_distinct_from_no_captions_and_triggers_cooldown():
    paid = PaidRecorder((402, {"error": "quota exceeded"}))
    svc = make_service(PAID_ENV, lib=FakeLibrary(E(ts.UPSTREAM_ERROR, "yt flake")), paid=paid)
    r = await svc.get_transcript(VID)
    assert r["status"] == "provider_quota_exhausted" and r["status"] != "no_captions" and r["retryable"] is True
    n = len(paid.requests)
    assert n == 2                                     # tried both providers once
    r2 = await svc.get_transcript("ccccccccccc")
    assert len(paid.requests) == n                    # cooldown: no repeat spend/latency
    assert any("cooldown:provider_quota_exhausted" in a.get("reason", "") for a in r2["attempts"])


async def test_quota_cooldown_expires():
    clock = Clock()
    paid = PaidRecorder((429, "Monthly credit limit exceeded"))
    svc = make_service(PAID_ENV, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid, clock=clock)
    await svc.get_transcript(VID)
    clock.advance(13 * 3600)
    n = len(paid.requests)
    await svc.get_transcript("ddddddddddd")
    assert len(paid.requests) > n


@pytest.mark.parametrize("status,body,expect", [
    (402, "", "provider_quota_exhausted"), (429, "credits exhausted", "provider_quota_exhausted"),
    (429, "slow down", "provider_rate_limited"), (401, "bad key", "provider_auth_failed"),
    (403, "forbidden", "provider_auth_failed"), (404, "nope", "provider_no_result"),
    (500, "oops", "upstream_error")])
def test_classify_paid_http(status, body, expect):
    assert ts.classify_paid_http(status, body)[0] == expect


async def test_auth_failure_cools_down_for_a_day():
    paid = PaidRecorder((401, "invalid key"))
    svc = make_service({**PAID_ENV, "SUPADATA_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    r = await svc.get_transcript(VID)
    assert "provider_auth_failed" in [a["outcome"] for a in r["attempts"]]
    await svc.get_transcript("eeeeeeeeeee")
    assert len(paid.requests) == 1


async def test_monthly_cap_enforced():
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service({**PAID_ENV, "PAID_TRANSCRIPT_MONTHLY_LIMIT": "2", "SUPADATA_API_KEY": ""},
                       lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    ids = ["aaaaaaaaaa1", "aaaaaaaaaa2", "aaaaaaaaaa3"]
    out = [await svc.get_transcript(i) for i in ids]
    assert [o["status"] for o in out[:2]] == ["ok", "ok"] and len(paid.requests) == 2
    assert out[2]["status"] != "ok"
    assert {"provider": "transcriptapi", "outcome": "skipped", "reason": "monthly_limit_reached"} in out[2]["attempts"]


async def test_monthly_cap_resets_next_month():
    clock = Clock()
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service({**PAID_ENV, "PAID_TRANSCRIPT_MONTHLY_LIMIT": "1", "SUPADATA_API_KEY": ""},
                       lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid, clock=clock)
    await svc.get_transcript("aaaaaaaaaa1")
    assert (await svc.get_transcript("aaaaaaaaaa2"))["status"] != "ok"
    clock.advance(32 * 86400)
    assert (await svc.get_transcript("aaaaaaaaaa3"))["status"] == "ok"


async def test_paid_result_is_cached_so_credit_spent_once():
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service({**PAID_ENV, "SUPADATA_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    await svc.get_transcript(VID); await svc.get_transcript(VID); await svc.get_transcript(VID)
    assert len(paid.requests) == 1


async def test_paid_concurrent_duplicates_spend_once():
    paid = PaidRecorder((200, {"content": LONG}))
    svc = make_service({**PAID_ENV, "SUPADATA_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid)
    await asyncio.gather(*[svc.get_transcript(VID) for _ in range(6)])
    assert len(paid.requests) == 1


async def test_paid_network_error_classified():
    import httpx
    def boom(request): raise httpx.ConnectError("down", request=request)
    from tests.helpers import make_service as ms
    svc = ms({**PAID_ENV, "SUPADATA_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)))
    svc._http_factory = lambda: httpx.AsyncClient(transport=httpx.MockTransport(boom))
    r = await svc.get_transcript(VID)
    assert "network_error" in [a["outcome"] for a in r["attempts"]]


# ── local rate limit ──
async def test_local_fetch_rate_limit_protects_upstream():
    lib = FakeLibrary(good_data())
    svc = make_service({"TRANSCRIPT_FETCHES_PER_MINUTE": "2"}, lib=lib)
    r = [await svc.get_transcript(f"vvvvvvvvvv{i}") for i in range(3)]
    assert [x["status"] for x in r] == ["ok", "ok", "rate_limited"] and r[2]["retry_after_seconds"] >= 1
    assert (await svc.get_transcript("vvvvvvvvvv0"))["cached"] is True   # cache hits are free


# ── secrets never leak ──
async def test_error_text_is_redacted():
    secret = "tk_SECRET_123456"
    paid = PaidRecorder((500, f"server said key={secret} Bearer {secret}"))
    r = await make_service({**PAID_ENV, "SUPADATA_API_KEY": ""}, lib=FakeLibrary(E(ts.UPSTREAM_ERROR)), paid=paid).get_transcript(VID)
    blob = str(r)
    assert secret not in blob and "SECRET" not in blob


def test_redact_patterns():
    assert "AIza" not in ts.redact("https://x?key=AIza" + "a" * 35)
    assert "pw" not in ts.redact("http://user:pw@host:80")
    assert "abcdef" not in ts.redact("Authorization: Bearer abcdef123")
    assert "mysecretvalue" not in ts.redact("x mysecretvalue y", secrets=["mysecretvalue"])


# ── config / diagnostics ──
def test_config_defaults_are_free_and_safe():
    cfg = ts.TranscriptConfig.from_env({})
    assert cfg.enable_paid is False and cfg.provider_order[:3] == ["youtube_direct", "ytdlp", "youtube_proxy"] and cfg.enable_ytdlp is False


def test_unknown_providers_in_order_ignored():
    assert ts.TranscriptConfig.from_env({"TRANSCRIPT_PROVIDER_ORDER": "bogus,youtube_direct"}).provider_order == ["youtube_direct"]


async def test_diagnostics_contains_no_secret_values():
    svc = make_service({**PAID_ENV, "WEBSHARE_USER": "u_SECRET", "WEBSHARE_PASS": "p_SECRET"}, lib=FakeLibrary(good_data()))
    await svc.get_transcript(VID)
    d = await svc.diagnostics()
    assert "SECRET" not in str(d) and d["providers"]["transcriptapi"]["configured"] is True
    assert d["counters"]["requests"] == 1


def test_unwritable_cache_path_degrades_to_memory(tmp_path):
    f = tmp_path / "file"; f.write_text("x")
    st = ts.TranscriptStore(str(f / "sub" / "c.db"))       # parent is a file -> cannot create
    assert st.persistent is False
    st.put("k", {"a": 1}, "positive", 10, 0)
    assert st.get("k", 1)[0] == {"a": 1}


# ── paid parsers ──
def test_parse_transcriptapi_variants():
    assert ts.parse_transcriptapi_response({"segments": [{"text": "a" * 60, "start": 1, "duration": 2}]})["segments"][0]["start"] == 1.0
    assert ts.parse_transcriptapi_response({"transcript": ["a" * 30, "b" * 30]})["full_transcript"].startswith("aaa")
    assert ts.parse_transcriptapi_response({"content": "plain text"})["full_transcript"] == "plain text"
    assert ts.parse_transcriptapi_response({"segments": [{"text": "short"}]}) is None
    assert ts.parse_transcriptapi_response([]) is None


def test_parse_supadata():
    r = ts.parse_supadata_response({"content": "c" * 60, "lang": "es", "availableLangs": ["es", "en"]})
    assert r["language_code"] == "es" and r["available_languages"] == ["es", "en"]
    assert ts.parse_supadata_response({"content": "tiny"}) is None


# ── ephemeral filesystem (Render free): paid calls need trustworthy persistent counters ──
EPH = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "TRANSCRIPT_API_KEY": "tk_SECRET_123456"}


async def test_paid_refused_on_default_ephemeral_state():
    paid = PaidRecorder((200, {"content": LONG}))
    cfg = ts.TranscriptConfig.from_env(EPH)                       # no TRANSCRIPT_CACHE_PATH -> default, not explicit
    cfg.cache_path = ":memory:"
    svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(":memory:"), library_fetch=FakeLibrary(E(ts.BLOCKED)),
                               http_client_factory=paid.factory())
    r = await svc.get_transcript(VID)
    assert paid.requests == [] and any(a.get("reason") == "ephemeral_state_paid_blocked" for a in r["attempts"])
    assert (await svc.diagnostics())["paid_apis_effective"] is False


async def test_paid_allowed_with_explicit_persistent_path(tmp_path):
    p = str(tmp_path / "state.sqlite3")
    cfg = ts.TranscriptConfig.from_env({**EPH, "TRANSCRIPT_CACHE_PATH": p})
    paid = PaidRecorder((200, {"content": LONG}))
    svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(p), library_fetch=FakeLibrary(E(ts.BLOCKED)),
                               http_client_factory=paid.factory())
    assert (await svc.get_transcript(VID))["status"] == "ok" and len(paid.requests) == 1


async def test_explicit_memory_path_is_not_persistent():
    cfg = ts.TranscriptConfig.from_env({**EPH, "TRANSCRIPT_CACHE_PATH": ":memory:"})
    svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(":memory:"), library_fetch=FakeLibrary(E(ts.BLOCKED)))
    assert svc._paid_state_trustworthy() is False


def test_default_cache_path_is_not_railway_specific(monkeypatch):
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", "/should/be/ignored"); monkeypatch.delenv("TRANSCRIPT_CACHE_PATH", raising=False)
    cfg = ts.TranscriptConfig.from_env()
    assert "should/be/ignored" not in cfg.cache_path and cfg.cache_path_explicit is False


def test_restart_loses_state_on_ephemeral_fs_but_not_on_persistent(tmp_path):
    """Simulates a Render restart: new process = new store object. File store survives, :memory: does not."""
    for path, survives in ((str(tmp_path / "s.db"), True), (":memory:", False)):
        a = ts.TranscriptStore(path); a.put("k", {"x": 1}, "positive", 100, 0); a.bump_usage("transcriptapi", "2026-10")
        b = ts.TranscriptStore(path)
        assert (b.get("k", 1) is not None) is survives and (b.usage("transcriptapi", "2026-10") == 1) is survives


def test_unwritable_default_path_degrades_to_memory_with_visible_flag(tmp_path, monkeypatch):
    ro = tmp_path / "ro"; ro.write_text("file, not dir")
    st = ts.TranscriptStore(str(ro / "x" / "c.db"))
    assert st.persistent is False
