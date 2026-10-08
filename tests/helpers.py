import httpx

from transcript_service import (TranscriptConfig, TranscriptError, TranscriptService,
                                TranscriptStore)

VID = "dQw4w9WgXcQ"


class Clock:
    def __init__(self, t=1_800_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def good_data(code="en", generated=False, n=3):
    segs = [{"text": f"word{i}", "start": float(i), "duration": 1.0} for i in range(n)]
    return {"language_code": code, "caption_type": "auto_generated" if generated else "manual",
            "available_languages": [{"code": code, "name": code, "auto_generated": generated}],
            "segments": segs, "full_transcript": " ".join(s["text"] for s in segs)}


class FakeLibrary:
    """Stands in for fetch_with_library. `script` is a list of results/exceptions per call."""

    def __init__(self, *script):
        self.script = list(script)
        self.calls = []

    def __call__(self, video_id, languages, any_language, proxy_cfg, timeout, secrets=()):
        self.calls.append({"video_id": video_id, "languages": languages, "any_language": any_language,
                           "proxy": type(proxy_cfg).__name__ if proxy_cfg else None})
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if isinstance(item, Exception):
            raise item
        return item


class PaidRecorder:
    """httpx MockTransport factory: records requests, returns scripted (status, json/text)."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def handler(self, request: httpx.Request):
        self.requests.append(request)
        item = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        status, body = item
        if isinstance(body, (dict, list)):
            return httpx.Response(status, json=body)
        return httpx.Response(status, text=body)

    def factory(self):
        return lambda: httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


def make_service(env=None, lib=None, paid=None, clock=None):
    base = {"TRANSCRIPT_CACHE_PATH": ":memory:"}
    base.update(env or {})
    cfg = TranscriptConfig.from_env(base)
    return TranscriptService(cfg, store=TranscriptStore(":memory:"), clock=clock or Clock(),
                             http_client_factory=paid.factory() if paid else None,
                             library_fetch=lib or FakeLibrary(good_data()))


def E(status, msg="x", **kw):
    return TranscriptError(status, msg, **kw)
