"""End-to-end OFFLINE harness: real worker code <-> real FastAPI app <-> real Lua queue scripts on a Redis emulator.
MOCKED: YouTube extraction (scripted), Upstash (fakeredis behind an Upstash-shaped REST fake), Render (in-process app).
NOT covered: real network, real Upstash, real Render, real YouTube, Windows."""
import os
import sys
import threading

from starlette.testclient import TestClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in (ROOT, os.path.join(ROOT, "windows_worker")):
    if p not in sys.path:
        sys.path.insert(0, p)

import main  # noqa: E402
import transcript_service as ts  # noqa: E402
import worker_core as wc  # noqa: E402
from tests.helpers import Clock, FakeLibrary, good_data  # noqa: E402
from tests.redis_fake import RedisBackedUpstash, make_remote  # noqa: E402

WORKER_TOKEN = "w" * 40
MCP_TOKEN = "m" * 40
VID = lambda i: f"vid{i:08d}"[-11:]


class Stack:
    """One 'deployment': fake Upstash (persistent across restarts), Render app/service (restartable), clock."""

    def __init__(self, monkeypatch, env=None, fake=None, clock=None):
        self.mp, self.clock = monkeypatch, clock or Clock()
        self.fake = fake or RedisBackedUpstash()
        self.env = {k: v for k, v in {"TRANSCRIPT_QUEUE_ENABLED": "true", "TRANSCRIPT_CACHE_PATH": ":memory:", "TRANSCRIPT_RETRIES": "0",
                    "TRANSCRIPT_QUEUE_WAIT_SECONDS": "0", "TRANSCRIPT_FETCHES_PER_MINUTE": "100000", **(env or {})}.items() if not k.startswith("_")}
        self.mp.setenv("WORKER_AUTH_TOKEN", WORKER_TOKEN)
        if (env or {}).get("_no_mcp_auth") != "1":
            self.mp.setenv("MCP_AUTH_TOKEN", MCP_TOKEN)           # the queue is fail-closed without authenticated MCP
        self.mp.setenv("WORKER_RATE_LIMIT_PER_MINUTE", "100000")
        self.mp.setenv("RATE_LIMIT_PER_MINUTE", "0")
        self.paid_calls = 0
        self.restart_render()

    def restart_render(self, sync_lib=None):
        """New service object = Render restart/spin-down: local SQLite and in-memory state are lost, Upstash is not."""
        cfg = ts.TranscriptConfig.from_env(self.env)
        self.svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(":memory:"), clock=self.clock,
                                        library_fetch=sync_lib or FakeLibrary(ts.TranscriptError(ts.BLOCKED, "render blocked")),
                                        remote=make_remote(self.fake, self.clock),
                                        http_client_factory=self._paid_guard)
        self.mp.setattr(main, "_transcript_service", self.svc)
        return self.svc

    def _paid_guard(self):
        self.paid_calls += 1
        raise AssertionError("a paid provider HTTP client was requested")

    def settings(self, **kw):
        base = dict(base_url="https://render.test", token=WORKER_TOKEN, worker_id=kw.pop("worker_id", "laptop-1"),
                    state_dir=kw.pop("state_dir"), min_delay=1, max_delay=1, poll_min=60, poll_max=60,
                    allow_insecure_localhost=True)
        base.update(kw)
        return wc.Settings(**base)

    def api(self, s, flaky=None):
        client = TestClient(main.app, base_url="https://render.test", raise_server_exceptions=False,
                            headers={"Authorization": f"Bearer {s.token}"})
        if flaky is not None:
            real = client.request
            def req(*a, **k):
                if flaky():
                    import httpx
                    raise httpx.ConnectError("render down")
                return real(*a, **k)
            client.request = req
        return wc.Api(s, client)

    def worker(self, s, extract, flaky=None, stop=None):
        sleeps = []
        def sleep(sec):
            sleeps.append(sec); self.clock.advance(sec); return False
        w = wc.Worker(s, self.api(s, flaky), wc.State(s.state_dir, self.clock), extract, stop=stop or threading.Event(),
                      clock=self.clock, sleep=sleep)
        w.sleeps = sleeps
        return w


class Extractor:
    """Scripted YouTube: behavior(video_id, call_no) -> 'ok' | TranscriptError | callable side effect."""

    def __init__(self, behavior=None, minutes=25, clock=None, seconds_per_call=0):
        self.behavior, self.calls, self.minutes, self.clock, self.spc = behavior, {}, minutes, clock, seconds_per_call
        self.lock = threading.Lock()

    def __call__(self, video_id, language):
        with self.lock:
            n = self.calls[video_id] = self.calls.get(video_id, 0) + 1
        if self.clock and self.spc:
            self.clock.advance(self.spc)
        out = self.behavior(video_id, n) if self.behavior else "ok"
        if isinstance(out, Exception):
            raise out
        return make_result(self.minutes, seed=hash(video_id) & 0xFFFF)

    @property
    def total(self):
        return sum(self.calls.values())


_VOCAB = None


def _vocab():
    """Deterministic pseudo-English: 3000 words, Zipf-distributed (like real speech), so zlib ratios are realistic
    (a repeating pattern would compress ~20x and flatter the storage numbers)."""
    global _VOCAB
    if _VOCAB is None:
        import random
        rng = random.Random(42)
        letters = "etaoinshrdlcumwfgypbvkjxqz"
        words = ["".join(rng.choice(letters[:rng.randint(8, 20)]) for _ in range(rng.choice((2, 3, 3, 4, 4, 5, 5, 6, 7, 8)))) for _ in range(3000)]
        weights = [1.0 / (i + 1) for i in range(3000)]
        _VOCAB = (words, weights)
    return _VOCAB


def make_result(minutes=25, code="en", seed=0):
    import random
    words, weights = _vocab()
    rng = random.Random(seed or minutes)
    n = minutes * 150
    toks = rng.choices(words, weights, k=n)
    segs = [{"text": " ".join(toks[i:i + 8]), "start": round(i / 2.5, 1), "duration": 3.0} for i in range(0, n, 8)]
    return {"language_code": code, "caption_type": "auto_generated", "available_languages": [{"code": code, "name": code, "auto_generated": True}],
            "segments": segs[:200], "total_segments": len(segs), "full_transcript": " ".join(s["text"] for s in segs), "source": "youtube-transcript-api"}


def E(status, msg="x"):
    return ts.TranscriptError(status, msg)


def run_until_idle(w, max_cycles=2000):
    """Run poll cycles until nothing is left to do right now. Returns the list of cycle outcomes."""
    out = []
    for _ in range(max_cycles):
        r = w.run_cycle()
        out.append(r)
        if r in ("idle", "paused", "capped", "offline", "auth"):
            break
    return out
