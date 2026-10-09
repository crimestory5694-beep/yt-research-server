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
    base = {"TRANSCRIPT_CACHE_PATH": ":memory:", "TRANSCRIPT_RETRIES": "0", "TRANSCRIPT_QUEUE_WAIT_SECONDS": "0"}
    base.update(env or {})
    cfg = TranscriptConfig.from_env(base)
    return TranscriptService(cfg, store=TranscriptStore(":memory:"), clock=clock or Clock(),
                             http_client_factory=paid.factory() if paid else None,
                             library_fetch=lib or FakeLibrary(good_data()))


def E(status, msg="x", **kw):
    return TranscriptError(status, msg, **kw)


class FakeUpstash:
    """In-memory stand-in for the Upstash REST /pipeline endpoint (MOCK - not a real Upstash database).
    Supports the subset used by remote_store: GET SET(EX) DEL INCR DECR EXPIRE."""

    def __init__(self, token="tok_SECRET_up"):
        self.data, self.ttl, self.requests, self.token = {}, {}, [], token
        self.down = False
        self.status = 200

    def handler(self, request: httpx.Request):
        import json
        self.requests.append(request)
        if self.down:
            raise httpx.ConnectError("down", request=request)
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(401, json={"error": "Unauthorized"})
        if self.status != 200:
            return httpx.Response(self.status, text="err")
        out = []
        for cmd in json.loads(request.content):
            op, *a = cmd
            op = op.upper()
            if op == "GET":
                out.append({"result": self.data.get(a[0])})
            elif op == "SET":
                self.data[a[0]] = a[1]
                if len(a) >= 4 and str(a[2]).upper() == "EX":
                    self.ttl[a[0]] = a[3]
                out.append({"result": "OK"})
            elif op == "DEL":
                out.append({"result": int(self.data.pop(a[0], None) is not None)})
            elif op in ("INCR", "DECR"):
                self.data[a[0]] = int(self.data.get(a[0], 0)) + (1 if op == "INCR" else -1)
                out.append({"result": self.data[a[0]]})
            elif op == "EXPIRE":
                self.ttl[a[0]] = a[1]
                out.append({"result": 1})
            else:
                out.append({"error": f"ERR unknown command {op}"})
        return httpx.Response(200, json=out)

    def factory(self):
        return lambda: httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


def make_remote(fake, clock=None, **kw):
    from remote_store import RemoteStore
    return RemoteStore("https://fake-upstash.example", fake.token, client_factory=fake.factory(),
                       clock=clock or Clock(), **kw)


def make_service_remote(fake, env=None, lib=None, paid=None, clock=None, ytdlp=None):
    """Fresh service (fresh LOCAL sqlite = 'restarted Render instance') sharing one fake remote."""
    clock = clock or Clock()
    base = {"TRANSCRIPT_CACHE_PATH": ":memory:", "TRANSCRIPT_RETRIES": "0", "TRANSCRIPT_QUEUE_WAIT_SECONDS": "0"}
    base.update(env or {})
    cfg = TranscriptConfig.from_env(base)
    return TranscriptService(cfg, store=TranscriptStore(":memory:"), clock=clock,
                             http_client_factory=paid.factory() if paid else None,
                             library_fetch=lib or FakeLibrary(good_data()), ytdlp_fetch=ytdlp,
                             remote=make_remote(fake, clock))
