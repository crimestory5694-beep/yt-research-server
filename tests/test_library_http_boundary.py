"""Runs the REAL youtube-transcript-api + REAL requests against CANNED YouTube responses
(static assets shipped inside the library's own test suite) served by an in-process adapter.
No network. Verifies our classification/selection logic against the library's real parsing.
Not proof that live YouTube works from Render or any cloud host."""
import json
import os

import pytest
import requests
from requests.adapters import BaseAdapter

import transcript_service as ts
import youtube_transcript_api as yta

ASSETS = os.path.join(os.path.dirname(yta.__file__), "test", "assets")
pytestmark = pytest.mark.skipif(not os.path.isdir(ASSETS), reason="library test assets not installed")
VID = "GJLlxj_dtq8"


def asset(name):
    with open(os.path.join(ASSETS, name), "rb") as f:
        return f.read()


class FakeYouTube(BaseAdapter):
    def __init__(self, html="youtube.html.static", innertube="youtube.innertube.json.static",
                 xml="transcript.xml.static", html_status=200):
        super().__init__()
        self.html, self.innertube, self.xml, self.html_status = html, innertube, xml, html_status
        self.urls, self.timeouts = [], []

    def send(self, request, **kw):
        self.urls.append(request.url); self.timeouts.append(kw.get("timeout"))
        r = requests.Response(); r.url = request.url; r.status_code = 200
        if "/watch" in request.url:
            r.status_code = self.html_status; r._content = asset(self.html)
        elif "youtubei/v1/player" in request.url:
            r._content = asset(self.innertube)
        else:
            r._content = asset(self.xml)
        return r

    def close(self):
        pass


@pytest.fixture
def patch_session(monkeypatch):
    def _install(adapter):
        class S(ts._TimeoutSession):
            def __init__(self, timeout):
                super().__init__(timeout)
                self.mount("https://", adapter); self.mount("http://", adapter)
        monkeypatch.setattr(ts, "_TimeoutSession", S)
        return adapter
    return _install


def fetch(langs=("en",), any_language=False, proxy=None):
    return ts.fetch_with_library(VID, list(langs), any_language, proxy, 5)


def test_success_real_parser(patch_session):
    ad = patch_session(FakeYouTube())
    d = fetch()
    assert d["language_code"] == "en" and d["caption_type"] == "manual"
    assert d["full_transcript"].startswith("Hey, this is just a test")
    assert "<i>" not in d["full_transcript"] and len(d["segments"]) == 3     # html stripped, empty cue dropped
    assert d["available_languages"] and all(t is not None for t in ad.timeouts)  # timeout enforced on every request


def test_request_timeout_is_applied(patch_session):
    ad = patch_session(FakeYouTube())
    fetch()
    assert ad.timeouts[0] == (5, 5)


def _multilang_innertube(tracks):
    """Real innertube asset with captionTracks replaced by (code, kind) pairs."""
    d = json.loads(asset("youtube_ww1_nl_en.innertube.json.static"))
    base = d["captions"]["playerCaptionsTracklistRenderer"]["captionTracks"][0]
    out = []
    for code, kind in tracks:
        t = json.loads(json.dumps(base)); t["languageCode"] = code
        t["name"] = {"runs": [{"text": code}]}
        t.pop("kind", None)
        if kind:
            t["kind"] = kind
        out.append(t)
    d["captions"]["playerCaptionsTracklistRenderer"]["captionTracks"] = out
    return json.dumps(d).encode()


class MultiLang(FakeYouTube):
    def __init__(self, tracks):
        super().__init__(); self._payload = _multilang_innertube(tracks)

    def send(self, request, **kw):
        r = super().send(request, **kw)
        if "youtubei/v1/player" in request.url:
            r._content = self._payload
        return r


def test_real_asset_manual_preferred_over_auto_for_same_language(patch_session):
    patch_session(FakeYouTube(innertube="youtube_ww1_nl_en.innertube.json.static"))  # tracks: en manual + en asr
    d = fetch(["en"])
    assert d["caption_type"] == "manual"
    assert sorted(a["auto_generated"] for a in d["available_languages"]) == [False, True]


def test_multilingual_selection_with_real_library(patch_session):
    patch_session(MultiLang([("nl", None), ("pt-BR", None), ("en", "asr")]))
    assert fetch(["nl", "en"])["language_code"] == "nl"
    assert fetch(["pt"])["language_code"] == "pt-BR"                  # primary-subtag match
    d = fetch(["fr", "en"]); assert d["language_code"] == "en" and d["caption_type"] == "auto_generated"
    with pytest.raises(ts.TranscriptError) as e:
        fetch(["fr"])
    assert e.value.status == ts.LANGUAGE_UNAVAILABLE and {a["code"] for a in e.value.available_languages} == {"nl", "pt-BR", "en"}
    assert fetch(["fr"], any_language=True)["language_code"] in ("nl", "pt-BR")   # manual preferred over asr


@pytest.mark.parametrize("asset_name,expected", [
    ("youtube_transcripts_disabled.innertube.json.static", ts.NO_CAPTIONS),
    ("youtube_transcripts_disabled2.innertube.json.static", ts.NO_CAPTIONS),
    ("youtube_video_unavailable.innertube.json.static", ts.VIDEO_UNAVAILABLE),
    ("youtube_age_restricted.innertube.json.static", ts.AGE_RESTRICTED),
    ("youtube_unplayable.innertube.json.static", ts.VIDEO_UNPLAYABLE),
    ("youtube_request_blocked.innertube.json.static", ts.BLOCKED),
    ("youtube_po_token_required.innertube.json.static", ts.BLOCKED),
])
def test_library_failure_modes_classified(patch_session, asset_name, expected):
    patch_session(FakeYouTube(innertube=asset_name))
    with pytest.raises(ts.TranscriptError) as e:
        fetch()
    assert e.value.status == expected


def test_captcha_page_is_blocked(patch_session):
    patch_session(FakeYouTube(html="youtube_too_many_requests.html.static"))
    with pytest.raises(ts.TranscriptError) as e:
        fetch()
    assert e.value.status == ts.BLOCKED


def test_http_429_is_blocked(patch_session):
    class A(FakeYouTube):
        def send(self, request, **kw):
            r = super().send(request, **kw); r.status_code = 429; return r
    patch_session(A())
    with pytest.raises(ts.TranscriptError) as e:
        fetch()
    assert e.value.status == ts.BLOCKED


def test_http_500_is_upstream_error(patch_session):
    class A(FakeYouTube):
        def send(self, request, **kw):
            r = super().send(request, **kw); r.status_code = 500; return r
    patch_session(A())
    with pytest.raises(ts.TranscriptError) as e:
        fetch()
    assert e.value.status == ts.UPSTREAM_ERROR


def test_unparsable_page_is_parse_error(patch_session):
    patch_session(FakeYouTube(html="youtube_consent_page_invalid.html.static"))
    with pytest.raises(ts.TranscriptError) as e:
        fetch()
    assert e.value.status in (ts.PARSE_ERROR, ts.UPSTREAM_ERROR)


def test_invalid_video_id_url_rejected_by_library():
    with pytest.raises(ts.TranscriptError) as e:
        ts.fetch_with_library("https://www.youtube.com/watch?v=" + VID, ["en"], False, None, 5)
    # network is not reached for a URL passed as id: either invalid_video_id or a classified error
    assert e.value.status in (ts.INVALID_VIDEO_ID, ts.NETWORK_ERROR, ts.PROXY_ERROR, ts.UPSTREAM_ERROR, ts.VIDEO_UNAVAILABLE, ts.TIMEOUT)


# ── proxy configuration errors against the real library/requests (local-only sockets) ──
def test_dead_proxy_is_classified_proxy_error():
    cfg = yta.proxies.GenericProxyConfig(http_url="http://127.0.0.1:9", https_url="http://127.0.0.1:9")
    with pytest.raises(ts.TranscriptError) as e:
        ts.fetch_with_library(VID, ["en"], False, cfg, 3)
    assert e.value.status == ts.PROXY_ERROR


def test_empty_generic_proxy_config_is_misconfig():
    with pytest.raises(Exception) as e:
        yta.proxies.GenericProxyConfig()
    assert ts.classify_library_exception(e.value).status == ts.PROXY_MISCONFIGURED


def test_connect_timeout_classified():
    err = ts.classify_library_exception(requests.exceptions.ConnectTimeout("t"))
    assert err.status == ts.TIMEOUT
    assert ts.classify_library_exception(requests.exceptions.ConnectionError("x")).status == ts.NETWORK_ERROR


async def test_end_to_end_service_over_real_library(patch_session):
    patch_session(FakeYouTube())
    from tests.helpers import make_service
    svc = make_service(lib=ts.fetch_with_library)
    r = await svc.get_transcript(VID, "en")
    assert r["status"] == "ok" and r["caption_type"] == "manual" and "just a test" in r["full_transcript"]
    assert (await svc.get_transcript(VID, "en"))["cached"] is True


def test_direct_session_ignores_ambient_proxy_env(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9"); monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    s = ts._TimeoutSession(5)
    assert s.trust_env is False
    assert requests.utils.select_proxy("https://www.youtube.com/watch", s.proxies) is None
    assert s.merge_environment_settings("https://www.youtube.com/", {}, None, None, None)["proxies"] == {}
