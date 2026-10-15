"""Local transcript worker - core logic (no Windows-specific code; unit-tested on Linux with mocks).

Outbound HTTPS only: the worker polls the Render coordinator (/worker/*) and never opens a listening port.
It never sees Upstash credentials (Render brokers the queue). It never logs tokens, cookies or transcript text.
Free path only: youtube-transcript-api (and yt-dlp subtitles if enabled). No audio download, no speech-to-text, no
paid API. Pacing is conservative and persisted so a reboot cannot reset the caps.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

import httpx

log = logging.getLogger("yt.worker")

ERROR_CLASSES_SAFE = re.compile(r"^[a-z][a-z_:]{2,40}$")


# ───────────────────────────── settings ─────────────────────────────
def _num(env, key, default, lo, hi, cast=int):
    try:
        v = cast(env.get(key, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


@dataclass
class Settings:
    base_url: str
    token: str = field(repr=False)                 # never printed
    worker_id: str = "worker"
    poll_min: int = 300                            # idle poll interval (seconds), backs off to poll_max
    poll_max: int = 1800
    min_delay: int = 20                            # pause between extractions (seconds, randomised up to max_delay)
    max_delay: int = 45
    hourly_cap: int = 20
    daily_cap: int = 120
    lease_seconds: int = 300
    claim_timeout: int = 100                       # Render Free may need ~1 minute to wake up
    block_pause_base: int = 900                    # 15 min, doubling per consecutive block, capped
    block_pause_max: int = 21600
    enable_ytdlp: bool = False
    doorbell_url: str = ""                         # OPTIONAL read-only Upstash REST URL/token: lets idle polls skip Render
    doorbell_token: str = field(default="", repr=False)
    queue_prefix: str = "yt:"
    state_dir: str = ""
    allow_insecure_localhost: bool = False
    renew_interval: float = 0                      # 0 = lease_seconds/3 (tests use small values)

    @classmethod
    def from_env(cls, env: dict) -> "Settings":
        url = (env.get("RENDER_URL") or "").strip().rstrip("/")
        s = cls(base_url=url, token=(env.get("WORKER_TOKEN") or "").strip(),
                worker_id=re.sub(r"[^A-Za-z0-9._-]", "-", (env.get("WORKER_ID") or os.environ.get("COMPUTERNAME") or "worker"))[:64] or "worker",
                poll_min=_num(env, "POLL_MIN_SECONDS", 300, 60, 3600), poll_max=_num(env, "POLL_MAX_SECONDS", 1800, 60, 7200),
                min_delay=_num(env, "MIN_DELAY_SECONDS", 20, 5, 600), max_delay=_num(env, "MAX_DELAY_SECONDS", 45, 5, 1200),
                hourly_cap=_num(env, "HOURLY_CAP", 20, 1, 120), daily_cap=_num(env, "DAILY_CAP", 120, 1, 1000),
                lease_seconds=_num(env, "LEASE_SECONDS", 300, 60, 900), enable_ytdlp=(env.get("ENABLE_YTDLP", "").lower() in ("1", "true", "yes")),
                doorbell_url=(env.get("UPSTASH_READONLY_URL") or "").strip().rstrip("/"), doorbell_token=(env.get("UPSTASH_READONLY_TOKEN") or "").strip(),
                queue_prefix=env.get("REMOTE_CACHE_PREFIX", "yt:"), state_dir=env.get("STATE_DIR", ""),
                allow_insecure_localhost=(env.get("ALLOW_INSECURE_LOCALHOST", "") == "1"))
        s.max_delay = max(s.max_delay, s.min_delay)
        s.poll_max = max(s.poll_max, s.poll_min)
        s.validate()
        return s

    def validate(self):
        from urllib.parse import urlparse
        u = urlparse(self.base_url)
        local = u.hostname in ("localhost", "127.0.0.1")
        if u.scheme != "https" and not (self.allow_insecure_localhost and local):
            raise ValueError("RENDER_URL must be an https:// URL")
        if not u.hostname:
            raise ValueError("RENDER_URL is missing")
        if len(self.token) < 24:
            raise ValueError("WORKER_TOKEN is missing or too short (use 32+ random characters)")
        if self.doorbell_url and not self.doorbell_url.startswith("https://"):
            raise ValueError("UPSTASH_READONLY_URL must be https")


class RedactingFilter(logging.Filter):
    """Belt and braces: scrub the token (and anything shaped like a bearer token) from every log record."""

    def __init__(self, secrets):
        super().__init__()
        self.secrets = [s for s in secrets if s]

    def filter(self, record):
        msg = record.getMessage()
        for s in self.secrets:
            msg = msg.replace(s, "***")
        msg = re.sub(r"(?i)bearer\s+\S+", "Bearer ***", msg)
        record.msg, record.args = msg, ()
        return True


# ───────────────────────────── persisted pacing state ─────────────────────────────
class State:
    def __init__(self, directory: str, clock: Callable[[], float] = time.time):
        self.dir, self._clock = directory, clock
        os.makedirs(os.path.join(directory, "outbox"), exist_ok=True)
        self.path = os.path.join(directory, "state.json")
        self.data = {"events": [], "blocks_in_row": 0, "pause_until": 0, "idle_polls": 0}
        try:
            with open(self.path, encoding="utf-8") as f:
                self.data.update(json.load(f))
        except (OSError, ValueError):
            pass

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f)
        os.replace(tmp, self.path)          # atomic: a crash cannot leave a half-written state file

    def _trim(self):
        cutoff = self._clock() - 86400
        self.data["events"] = [t for t in self.data["events"] if t > cutoff]

    def record_extraction(self):
        self._trim(); self.data["events"].append(self._clock()); self.save()

    def counts(self) -> tuple:
        self._trim(); now = self._clock()
        return sum(1 for t in self.data["events"] if t > now - 3600), len(self.data["events"])

    def wait_for_caps(self, hourly_cap, daily_cap) -> float:
        """Seconds until another extraction is allowed by the hourly/daily caps (0 = allowed now)."""
        self._trim(); now = self._clock(); ev = sorted(self.data["events"])
        waits = [0.0]
        hour = [t for t in ev if t > now - 3600]
        if len(hour) >= hourly_cap:
            waits.append(hour[len(hour) - hourly_cap] + 3600 - now)
        if len(ev) >= daily_cap:
            waits.append(ev[len(ev) - daily_cap] + 86400 - now)
        return max(waits)

    def paused_for(self) -> float:
        return max(0.0, self.data["pause_until"] - self._clock())

    def note_block(self, base, cap):
        n = self.data["blocks_in_row"] = self.data["blocks_in_row"] + 1
        self.data["pause_until"] = self._clock() + min(base * 2 ** (n - 1), cap); self.save()

    def note_success(self):
        if self.data["blocks_in_row"]:
            self.data["blocks_in_row"] = 0; self.save()

    # outbox: finished results awaiting acknowledgement (survives crash / reboot / offline Render)
    def outbox_put(self, job_id: str, body: dict):
        files = os.listdir(os.path.join(self.dir, "outbox"))
        if len(files) >= 50:
            raise RuntimeError("outbox full")
        p = os.path.join(self.dir, "outbox", re.sub(r"[^A-Za-z0-9_-]", "_", job_id) + ".json")
        with open(p + ".tmp", "w", encoding="utf-8") as f:
            json.dump({"job_id": job_id, "body": body}, f)
        os.replace(p + ".tmp", p)

    def outbox_items(self):
        d = os.path.join(self.dir, "outbox")
        for name in sorted(os.listdir(d)):
            if name.endswith(".json"):
                try:
                    with open(os.path.join(d, name), encoding="utf-8") as f:
                        yield name, json.load(f)
                except (OSError, ValueError):
                    os.remove(os.path.join(d, name))

    def outbox_done(self, name: str):
        try:
            os.remove(os.path.join(self.dir, "outbox", name))
        except OSError:
            pass


# ───────────────────────────── HTTP API client ─────────────────────────────
class AuthError(Exception):
    pass


class ServerBusy(Exception):
    def __init__(self, retry_after: int = 0, msg: str = ""):
        super().__init__(msg); self.retry_after = retry_after


class Api:
    def __init__(self, s: Settings, client: Optional[httpx.Client] = None):
        self.s = s
        self.http = client or httpx.Client(base_url=s.base_url, timeout=s.claim_timeout,
                                           headers={"Authorization": f"Bearer {s.token}", "User-Agent": "yt-research-worker/1"})

    def _call(self, method, path, **kw):
        try:
            r = self.http.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise ServerBusy(0, type(e).__name__) from None
        if r.status_code in (401, 403):
            raise AuthError("worker token rejected")
        if r.status_code == 429:
            raise ServerBusy(int(r.headers.get("retry-after", "60") or 60), "rate limited")
        if r.status_code >= 500:
            raise ServerBusy(0, f"HTTP {r.status_code}")
        if r.status_code >= 400:
            return r.status_code, r.json() if r.content else {}
        return r.status_code, r.json()

    def ping(self):
        return self._call("GET", "/worker/ping")[1]

    def claim(self, max_jobs=1):
        code, body = self._call("POST", "/worker/claim", json={"worker_id": self.s.worker_id, "max_jobs": max_jobs,
                                                                "lease_seconds": self.s.lease_seconds})
        return body.get("jobs", []) if code == 200 else []

    def renew(self, job_id, fence):
        return self._call("POST", "/worker/renew", json={"worker_id": self.s.worker_id, "job_id": job_id, "fence": fence,
                                                         "lease_seconds": self.s.lease_seconds}, timeout=30)[1]

    def complete(self, job_id, fence, result):
        return self._call("POST", "/worker/complete", json={"worker_id": self.s.worker_id, "job_id": job_id, "fence": fence,
                                                            "result": result}, timeout=60)

    def fail(self, job_id, fence, error_class, message=""):
        return self._call("POST", "/worker/fail", json={"worker_id": self.s.worker_id, "job_id": job_id, "fence": fence,
                                                        "error_class": error_class, "message": message[:200]}, timeout=60)


# ───────────────────────────── extraction ─────────────────────────────
def default_extractor(enable_ytdlp: bool = False):
    """Returns extract(video_id, language) -> result dict or raises transcript_service.TranscriptError."""
    import transcript_service as ts

    def extract(video_id: str, language: str) -> dict:
        languages, any_flag = (["en"], True) if language == "any" else (ts.parse_languages(language), False)
        try:
            data = ts.fetch_with_library(video_id, languages, any_flag, None, 20)
            source = "youtube-transcript-api"
        except ts.TranscriptError as e:
            if e.definitive or not enable_ytdlp:
                raise
            import ytdlp_provider
            data = ytdlp_provider.fetch_with_ytdlp(video_id, languages, any_flag, 20)
            source = "yt-dlp"
        segs = data["segments"]
        return {"language_code": data["language_code"], "caption_type": data.get("caption_type", "unknown"),
                "available_languages": [{"code": a.get("code", "")[:16], "name": str(a.get("name", ""))[:80],
                                         "auto_generated": bool(a.get("auto_generated"))} for a in data.get("available_languages", [])[:200]],
                "segments": segs[:200], "total_segments": len(segs), "full_transcript": data["full_transcript"], "source": source}
    return extract


# ───────────────────────────── the worker ─────────────────────────────
class Worker:
    def __init__(self, s: Settings, api: Api, state: State, extract: Callable[[str, str], dict],
                 stop: Optional[threading.Event] = None, clock: Callable[[], float] = time.time,
                 sleep: Optional[Callable[[float], None]] = None, doorbell: Optional[Callable[[], Optional[int]]] = None):
        self.s, self.api, self.state, self.extract, self._clock = s, api, state, extract, clock
        self.stop = stop or threading.Event()
        self._sleep = sleep or (lambda sec: self.stop.wait(sec))
        self.doorbell = doorbell
        self.stats = {"done": 0, "failed": 0, "blocked": 0, "resubmitted": 0}

    # -- helpers --
    def _classify(self, exc: Exception) -> tuple:
        cls = getattr(exc, "status", None)
        if not cls or not ERROR_CLASSES_SAFE.match(str(cls)):
            return "unknown_error", type(exc).__name__
        return str(cls), str(getattr(exc, "message", ""))[:160]

    def _submit(self, job_id: str, fence: int, kind: str, payload: dict) -> bool:
        """Deliver complete/fail; 3 spaced attempts. False = could not deliver (caller keeps it in the outbox)."""
        for i, wait in enumerate((0, 5, 15)):
            if wait and self._sleep(wait) is True and self.stop.is_set():
                break
            try:
                code, body = (self.api.complete(job_id, fence, payload) if kind == "complete"
                              else self.api.fail(job_id, fence, payload["error_class"], payload.get("message", "")))
                if code < 400:
                    return True
                if code in (404, 409, 422):    # job gone / STALE claim (someone else owns it) / rejected: final, never retried
                    log.warning("server rejected %s for %s (HTTP %s): discarding local result", kind, job_id, code)
                    return True
            except ServerBusy as e:
                log.warning("submit %s for %s deferred: %s", kind, job_id, e)
        return False

    def flush_outbox(self):
        for name, item in list(self.state.outbox_items()):
            if self.stop.is_set():
                return
            body = item["body"]
            if self._submit(item["job_id"], int(body["fence"]), body["kind"], body["payload"]):
                self.state.outbox_done(name); self.stats["resubmitted"] += 1
                log.info("resubmitted stored result for %s", item["job_id"])
            else:
                return                         # server unreachable: try again next cycle

    def _renewer(self, job_id: str, fence: int, done: threading.Event):
        interval = self.s.renew_interval or max(20, self.s.lease_seconds // 3)
        while not done.wait(interval):
            try:
                r = self.api.renew(job_id, fence)
                if not r.get("renewed"):
                    log.warning("lease for %s no longer ours (%s); will still try to deliver", job_id, r.get("state"))
                    return
            except (ServerBusy, AuthError):
                pass

    def process(self, job: dict):
        jid, vid, lang, fence = job["job_id"], job["video_id"], job["language"], int(job["fence"])
        done = threading.Event()
        t = threading.Thread(target=self._renewer, args=(jid, fence, done), daemon=True); t.start()
        self.state.record_extraction()
        try:
            result = self.extract(vid, lang)
            kind, payload = "complete", result
        except Exception as exc:               # classified; never includes transcript text
            cls, msg = self._classify(exc)
            kind, payload = "fail", {"error_class": cls, "message": msg}
        finally:
            done.set(); t.join(timeout=2)
        if kind == "fail" and payload["error_class"] == "blocked":
            self.state.note_block(self.s.block_pause_base, self.s.block_pause_max); self.stats["blocked"] += 1
            log.warning("YouTube blocked this machine; pausing %ss", int(self.state.paused_for()))
        elif kind == "complete":
            self.state.note_success()
        log.info("job %s -> %s", jid, payload.get("error_class", "ok") if kind == "fail" else "ok")
        if not self._submit(jid, fence, kind, payload):
            try:
                self.state.outbox_put(jid, {"kind": kind, "payload": payload, "fence": fence})
                log.warning("could not deliver result for %s; stored for retry", jid)
            except RuntimeError:
                log.error("outbox full; dropping local copy of %s (server lease will re-queue it)", jid)
        self.stats["done" if kind == "complete" else "failed"] += 1

    def has_work(self) -> bool:
        """Cheap check that does not wake Render: read-only Upstash 'doorbell' if configured, else assume yes."""
        if self.doorbell is None:
            return True
        n = self.doorbell()
        return True if n is None else n > 0

    def run_cycle(self) -> str:
        """One poll cycle. Returns 'paused' | 'capped' | 'idle' | 'worked' | 'offline' | 'auth'."""
        pause = self.state.paused_for()
        if pause > 0:
            return "paused"
        try:
            self.flush_outbox()
            wait = self.state.wait_for_caps(self.s.hourly_cap, self.s.daily_cap)
            if wait > 0:
                return "capped"
            if not self.has_work():
                return "idle"
            jobs = self.api.claim(1)
        except AuthError:
            log.error("worker token rejected by the server - stopping (fix WORKER_TOKEN)")
            return "auth"
        except ServerBusy as e:
            log.warning("server not available: %s", e)
            return "offline"
        if not jobs:
            return "idle"
        for job in jobs:
            self.process(job)
            if self.stop.is_set() or self.state.paused_for() > 0:
                break
            self._sleep(random.uniform(self.s.min_delay, self.s.max_delay))   # conservative pacing between extractions
        return "worked"

    def run_forever(self):
        idle = backoff = 0
        log.info("worker %s started", self.s.worker_id)
        while not self.stop.is_set():
            r = self.run_cycle()
            if r == "auth":
                return 3
            if r == "worked":
                idle = backoff = 0; continue          # more work may be waiting; caps/pacing still apply
            if r == "idle":
                idle += 1; delay = min(self.s.poll_min * 2 ** min(idle - 1, 6), self.s.poll_max)
            elif r == "offline":
                backoff += 1; delay = min(30 * 2 ** (backoff - 1), 900)
            elif r == "paused":
                delay = min(self.state.paused_for() + 1, 3600)
            else:  # capped
                delay = min(self.state.wait_for_caps(self.s.hourly_cap, self.s.daily_cap) + 1, 3600)
            self._sleep(delay * random.uniform(0.9, 1.1))
        log.info("worker stopping: %s", self.stats)
        return 0


def make_doorbell(s: Settings, client: Optional[httpx.Client] = None, clock: Callable[[], float] = time.time):
    if not (s.doorbell_url and s.doorbell_token):
        return None
    http = client or httpx.Client(timeout=15)

    def ring() -> Optional[int]:
        try:
            r = http.post(s.doorbell_url + "/pipeline", headers={"Authorization": f"Bearer {s.doorbell_token}"},
                          json=[["ZCOUNT", f"{s.queue_prefix}q:pending", "-inf", int(clock())],
                                ["ZCOUNT", f"{s.queue_prefix}q:leases", "-inf", int(clock())]])
            res = r.json()
            return int(res[0]["result"]) + int(res[1]["result"]) if r.status_code == 200 else None
        except Exception:
            return None                      # unknown -> caller falls back to asking Render
    return ring
