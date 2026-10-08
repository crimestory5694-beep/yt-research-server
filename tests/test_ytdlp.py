"""yt-dlp extractor: parsers/selection/classification are real code under test; yt-dlp and YouTube are MOCKED.
Whether yt-dlp helps from Render is NOT established here - see the probe extractor matrix."""
import json

import pytest

import transcript_service as ts
import ytdlp_provider as yp
from tests.helpers import VID, Clock, E, FakeLibrary, good_data, make_service

JSON3 = json.dumps({"events": [{"tStartMs": 0, "dDurationMs": 1500, "segs": [{"utf8": "Hello"}, {"utf8": " world"}]},
                               {"tStartMs": 1500, "dDurationMs": 500}, {"tStartMs": 2000, "dDurationMs": 1000, "segs": [{"utf8": "\n"}]},
                               {"tStartMs": 3000, "dDurationMs": 2000, "segs": [{"utf8": "second\nline"}]}]})
VTT = """WEBVTT

00:00:00.000 --> 00:00:01.500
Hello <c>world</c>

00:00:01.500 --> 00:00:03.000
Hello world

00:00:03.000 --> 01:00:05.000
second line
"""


def test_parse_json3_and_vtt():
    s = yp.parse_json3(JSON3)
    assert s == [{"text": "Hello world", "start": 0.0, "duration": 1.5}, {"text": "second line", "start": 3.0, "duration": 2.0}]
    v = yp.parse_vtt(VTT)
    assert [x["text"] for x in v] == ["Hello world", "second line"] and v[1]["start"] == 3.0 and v[1]["duration"] == 3602.0


INFO = {"subtitles": {"nl": [{"ext": "vtt", "url": "u-nl-vtt"}, {"ext": "json3", "url": "u-nl-json"}]},
        "automatic_captions": {"en-orig": [{"ext": "json3", "url": "u-en-orig"}], "en": [{"ext": "json3", "url": "u-en"}],
                               "fr": [{"ext": "json3", "url": "u-fr-translated"}]}}


class FakeYDL:
    def __init__(self, info=INFO, exc=None):
        self.info, self.exc = info, exc

    def __call__(self, opts):
        self.opts = opts; return self

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def extract_info(self, url, download=False):
        assert download is False
        if self.exc: raise self.exc
        return self.info


def run(langs, info=INFO, exc=None, body=JSON3, any_language=False):
    ydl = FakeYDL(info, exc)
    fetched = []
    def get(url): fetched.append(url); return body
    return yp.fetch_with_ytdlp(VID, langs, any_language, 5, _ydl_factory=ydl, _http_get=get), ydl, fetched


def test_selects_manual_over_auto_and_prefers_json3():
    d, ydl, fetched = run(["nl", "en"])
    assert d["language_code"] == "nl" and d["caption_type"] == "manual" and fetched == ["u-nl-json"]
    assert ydl.opts["skip_download"] is True and ydl.opts["proxy"] == ""     # never downloads media; direct connection


def test_auto_translations_are_ignored_in_favour_of_original():
    d, _, fetched = run(["en"])
    assert d["caption_type"] == "auto_generated" and fetched == ["u-en-orig"]
    assert {a["code"] for a in d["available_languages"]} == {"nl", "en"}      # 'fr' machine translation not offered
    with pytest.raises(ts.TranscriptError) as e:
        run(["fr"])
    assert e.value.status == ts.PROVIDER_NO_RESULT and not e.value.definitive


def test_any_language_fallback():
    d, *_ = run(["ja"], any_language=True)
    assert d["language_code"] == "nl"                                           # manual preferred


@pytest.mark.parametrize("msg,expected", [
    ("ERROR: [youtube] x: Sign in to confirm you’re not a bot", ts.BLOCKED),
    ("HTTP Error 429: Too Many Requests", ts.BLOCKED),
    ("ERROR: Video unavailable. This video is private", ts.VIDEO_UNAVAILABLE),
    ("Sign in to confirm your age. This video may be inappropriate", ts.AGE_RESTRICTED),
    ("<urlopen error timed out>", ts.NETWORK_ERROR),
    ("something odd", ts.UPSTREAM_ERROR)])
def test_error_classification(msg, expected):
    with pytest.raises(ts.TranscriptError) as e:
        run(["en"], exc=RuntimeError(msg))
    assert e.value.status == expected


def test_no_subtitles_is_not_definitive_and_empty_body_is_parse_error():
    with pytest.raises(ts.TranscriptError) as e:
        run(["en"], info={"subtitles": {}, "automatic_captions": {}})
    assert e.value.status == ts.PROVIDER_NO_RESULT and not e.value.definitive
    with pytest.raises(ts.TranscriptError) as e:
        run(["nl"], body='{"events": []}')
    assert e.value.status == ts.PARSE_ERROR


# ── service integration ──
async def test_ytdlp_off_by_default_even_if_installed():
    lib = FakeLibrary(E(ts.BLOCKED)); calls = []
    svc = make_service(lib=lib)
    svc._ytdlp_fetch = lambda *a, **k: calls.append(1)
    r = await svc.get_transcript(VID)
    assert calls == [] and {"provider": "ytdlp", "outcome": "skipped", "reason": "ytdlp_disabled"} in r["attempts"]


async def test_ytdlp_rescues_when_direct_fails_and_is_cached():
    calls = []
    def yt(*a, **k):
        calls.append(1); return good_data("en", generated=True)
    svc = make_service({"ENABLE_YTDLP": "true"}, lib=FakeLibrary(E(ts.BLOCKED)))
    svc._ytdlp_fetch = yt
    r = await svc.get_transcript(VID)
    assert r["status"] == "ok" and r["source"] == "yt-dlp" and r["proxy_used"] is False
    assert (await svc.get_transcript(VID))["cached"] is True and calls == [1]


async def test_definitive_answer_from_free_library_skips_ytdlp():
    calls = []
    svc = make_service({"ENABLE_YTDLP": "true"}, lib=FakeLibrary(E(ts.NO_CAPTIONS)))
    svc._ytdlp_fetch = lambda *a, **k: calls.append(1)
    assert (await svc.get_transcript(VID))["status"] == "no_captions" and calls == []


async def test_ytdlp_enabled_but_not_installed_is_reported(monkeypatch):
    svc = make_service({"ENABLE_YTDLP": "true"}, lib=FakeLibrary(E(ts.BLOCKED)))
    monkeypatch.setattr(svc, "_ytdlp_available", lambda: False)
    r = await svc.get_transcript(VID)
    assert any(a.get("reason") == "ytdlp_not_installed" for a in r["attempts"])


async def test_ytdlp_blocked_gets_cooldown_like_other_free_providers():
    calls = []
    def yt(*a, **k):
        calls.append(1); raise E(ts.BLOCKED, "bot check")
    svc = make_service({"ENABLE_YTDLP": "true"}, lib=FakeLibrary(E(ts.BLOCKED)))
    svc._ytdlp_fetch = yt
    await svc.get_transcript(VID); await svc.get_transcript("ddddddddddd")
    assert calls == [1]
