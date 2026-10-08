"""Transcript retrieval service.

Design goals (see README.md "Transcripts"):
  * free-first: cache -> youtube-transcript-api direct -> (optional) proxy
    -> (optional, default OFF) paid APIs
  * definitive answers ("no captions", "video unavailable", ...) stop the chain
    so paid providers are never called for videos that have no transcript
  * every failure is classified; nothing is silently swallowed
  * paid providers are hard-gated: explicit enable flag, key present, monthly
    cap, and a persisted cooldown after quota/auth errors
  * results (and definitive negatives) are cached; concurrent identical
    requests share one fetch

Nothing in this module logs or returns secret values.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

import httpx
import requests

try:  # pragma: no cover - import guard
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api import _errors as yt_errors
    from youtube_transcript_api.proxies import (
        GenericProxyConfig,
        InvalidProxyConfig,
        WebshareProxyConfig,
    )
    _LIB_IMPORT_ERROR: Optional[str] = None
except Exception as _e:  # pragma: no cover
    YouTubeTranscriptApi = None  # type: ignore
    yt_errors = None  # type: ignore
    GenericProxyConfig = WebshareProxyConfig = None  # type: ignore
    InvalidProxyConfig = Exception  # type: ignore
    _LIB_IMPORT_ERROR = type(_e).__name__

log = logging.getLogger("yt.transcripts")

# ─── status vocabulary ───────────────────────────────────────────────────────
OK = "ok"
INVALID_VIDEO_ID = "invalid_video_id"
NO_CAPTIONS = "no_captions"
LANGUAGE_UNAVAILABLE = "language_unavailable"
VIDEO_UNAVAILABLE = "video_unavailable"
AGE_RESTRICTED = "age_restricted"
VIDEO_UNPLAYABLE = "video_unplayable"
BLOCKED = "blocked"
TIMEOUT = "timeout"
NETWORK_ERROR = "network_error"
UPSTREAM_ERROR = "upstream_error"
PARSE_ERROR = "parse_error"
PROXY_ERROR = "proxy_error"
PROXY_MISCONFIGURED = "proxy_misconfigured"
QUOTA_EXHAUSTED = "provider_quota_exhausted"
PROVIDER_RATE_LIMITED = "provider_rate_limited"
PROVIDER_AUTH_FAILED = "provider_auth_failed"
PROVIDER_NO_RESULT = "provider_no_result"
LOCAL_RATE_LIMITED = "rate_limited"
ALL_FAILED = "all_providers_failed"

# Answers that are properties of the video, not of the provider. They end the
# provider chain (a paid API would not find captions that do not exist) and are
# safe to cache.
DEFINITIVE = {
    INVALID_VIDEO_ID, NO_CAPTIONS, LANGUAGE_UNAVAILABLE, VIDEO_UNAVAILABLE,
    AGE_RESTRICTED, VIDEO_UNPLAYABLE,
}
NEGATIVE_CACHEABLE = DEFINITIVE - {INVALID_VIDEO_ID}

PAID_PROVIDERS = ("transcriptapi", "supadata")
FREE_PROVIDERS = ("youtube_direct", "youtube_proxy")
DEFAULT_ORDER = "youtube_direct,youtube_proxy,transcriptapi,supadata"

_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


class TranscriptError(Exception):
    """A classified provider failure."""

    def __init__(self, status: str, message: str, *, available_languages=None,
                 retry_after: Optional[int] = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.available_languages = available_languages or []
        self.retry_after = retry_after

    @property
    def definitive(self) -> bool:
        return self.status in DEFINITIVE


# ─── configuration ───────────────────────────────────────────────────────────

def _bool(v: Optional[str], default=False) -> bool:
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _int(v: Optional[str], default: int) -> int:
    try:
        return int(v) if v not in (None, "") else default
    except ValueError:
        return default


@dataclass
class TranscriptConfig:
    provider_order: list = field(default_factory=lambda: DEFAULT_ORDER.split(","))
    enable_paid: bool = False
    transcript_api_key: str = ""
    supadata_api_key: str = ""
    webshare_user: str = ""
    webshare_pass: str = ""
    proxy_url: str = ""
    proxy_retries: int = 3
    cache_path: str = ""
    cache_path_explicit: bool = False
    allow_paid_with_ephemeral_state: bool = False
    cache_ttl_seconds: int = 30 * 86400
    negative_ttl_seconds: int = 6 * 3600
    request_timeout: int = 20
    total_timeout: int = 90
    max_concurrent_fetches: int = 4
    fetches_per_minute: int = 30
    block_cooldown_seconds: int = 300
    paid_monthly_limit: int = 50
    quota_cooldown_seconds: int = 12 * 3600
    auth_cooldown_seconds: int = 24 * 3600
    rate_cooldown_seconds: int = 900
    max_segments_returned: int = 200

    @classmethod
    def from_env(cls, env=None) -> "TranscriptConfig":
        e = os.environ if env is None else env
        order = [p.strip() for p in (e.get("TRANSCRIPT_PROVIDER_ORDER") or DEFAULT_ORDER).split(",") if p.strip()]
        known = set(FREE_PROVIDERS) | set(PAID_PROVIDERS)
        order = [p for p in order if p in known]
        # The default location is on the host's (usually ephemeral) filesystem. Only an explicit
        # TRANSCRIPT_CACHE_PATH is treated as an owner assertion that it sits on persistent storage.
        cache_path = e.get("TRANSCRIPT_CACHE_PATH", "")
        explicit = bool(cache_path)
        if not cache_path:
            cache_path = os.path.join(".cache", "transcript_cache.sqlite3")
        return cls(
            provider_order=order or DEFAULT_ORDER.split(","),
            enable_paid=_bool(e.get("ENABLE_PAID_TRANSCRIPT_APIS"), False),
            transcript_api_key=e.get("TRANSCRIPT_API_KEY", ""),
            supadata_api_key=e.get("SUPADATA_API_KEY", ""),
            webshare_user=e.get("WEBSHARE_USER", ""),
            webshare_pass=e.get("WEBSHARE_PASS", ""),
            proxy_url=e.get("PROXY_URL", ""),
            proxy_retries=_int(e.get("TRANSCRIPT_PROXY_RETRIES"), 3),
            cache_path=cache_path,
            cache_path_explicit=explicit,
            allow_paid_with_ephemeral_state=_bool(e.get("ALLOW_PAID_WITH_EPHEMERAL_STATE"), False),
            cache_ttl_seconds=_int(e.get("TRANSCRIPT_CACHE_TTL_DAYS"), 30) * 86400,
            negative_ttl_seconds=_int(e.get("TRANSCRIPT_NEGATIVE_TTL_HOURS"), 6) * 3600,
            request_timeout=_int(e.get("TRANSCRIPT_TIMEOUT_SECONDS"), 20),
            total_timeout=_int(e.get("TRANSCRIPT_TOTAL_TIMEOUT_SECONDS"), 90),
            max_concurrent_fetches=max(1, _int(e.get("TRANSCRIPT_MAX_CONCURRENCY"), 4)),
            fetches_per_minute=_int(e.get("TRANSCRIPT_FETCHES_PER_MINUTE"), 30),
            block_cooldown_seconds=_int(e.get("TRANSCRIPT_BLOCK_COOLDOWN_SECONDS"), 300),
            paid_monthly_limit=_int(e.get("PAID_TRANSCRIPT_MONTHLY_LIMIT"), 50),
        )

    def secrets(self) -> list:
        return [s for s in (self.transcript_api_key, self.supadata_api_key,
                            self.webshare_user, self.webshare_pass) if s]


# ─── redaction ───────────────────────────────────────────────────────────────
_REDACT_PATTERNS = [
    (re.compile(r"AIza[0-9A-Za-z_\-]{20,}"), "***"),
    (re.compile(r"(?i)(key|token|api[_-]?key|password)=([^&\s'\"]+)"), r"\1=***"),
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]+"), "Bearer ***"),
    (re.compile(r"://[^/\s:@]+:[^/\s@]+@"), "://***@"),
]


def redact(text: Any, secrets=()) -> str:
    s = str(text)
    for sec in secrets:
        if sec and len(sec) >= 4:
            s = s.replace(sec, "***")
    for pat, rep in _REDACT_PATTERNS:
        s = pat.sub(rep, s)
    return s


# ─── video id / language helpers ─────────────────────────────────────────────

def normalize_video_id(value: str) -> Optional[str]:
    """Accept a bare 11-char id or common YouTube URL shapes. None if invalid."""
    if not isinstance(value, str):
        return None
    v = value.strip()
    if _VIDEO_ID_RE.match(v):
        return v
    try:
        u = urlparse(v if "//" in v else "https://" + v)
    except ValueError:
        return None
    host = (u.hostname or "").lower()
    cand = None
    if host in ("youtu.be", "www.youtu.be"):
        cand = u.path.strip("/").split("/")[0]
    elif host.endswith("youtube.com") or host.endswith("youtube-nocookie.com"):
        parts = [p for p in u.path.split("/") if p]
        if parts and parts[0] == "watch":
            cand = (parse_qs(u.query).get("v") or [""])[0]
        elif len(parts) >= 2 and parts[0] in ("shorts", "embed", "live", "v"):
            cand = parts[1]
    return cand if cand and _VIDEO_ID_RE.match(cand) else None


def parse_languages(language: str, fallback_en: bool = True) -> list:
    """'es' -> ['es','en'];  'es,pt' -> ['es','pt','en'] (order = priority)."""
    langs = [l.strip() for l in (language or "en").replace(";", ",").split(",") if l.strip()]
    if not langs:
        langs = ["en"]
    if fallback_en and "en" not in langs:
        langs.append("en")
    return langs


def pick_transcript(available: list, languages: list):
    """available: [(code, is_generated, obj)] -> obj or None.

    Per requested language: exact code (manual before auto), then same primary
    subtag (e.g. 'en' matches 'en-GB').
    """
    for lang in languages:
        lang_l = lang.lower()
        for exact in (True, False):
            matches = [a for a in available
                       if (a[0].lower() == lang_l if exact
                           else a[0].lower().split("-")[0] == lang_l.split("-")[0])]
            matches.sort(key=lambda a: a[1])  # manual (False) first
            if matches:
                return matches[0]
    return None


# ─── persistence: cache, provider state, usage ───────────────────────────────

class TranscriptStore:
    def __init__(self, path: str):
        self.requested_path = path
        self.persistent = path != ":memory:"
        self._lock = threading.Lock()
        try:
            if path != ":memory:":
                d = os.path.dirname(path)
                if d:
                    os.makedirs(d, exist_ok=True)
            self._db = sqlite3.connect(path, check_same_thread=False)
            self._init()
        except Exception as e:  # unwritable fs -> degrade to memory, say so
            log.warning("transcript cache path unusable (%s); using in-memory store", type(e).__name__)
            self.persistent = False
            self._db = sqlite3.connect(":memory:", check_same_thread=False)
            self._init()

    def _init(self):
        with self._lock:
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                    kind TEXT NOT NULL, created REAL NOT NULL, expires REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS provider_state(provider TEXT PRIMARY KEY,
                    cooldown_until REAL NOT NULL, reason TEXT);
                CREATE TABLE IF NOT EXISTS usage(provider TEXT, month TEXT, count INTEGER NOT NULL,
                    PRIMARY KEY(provider, month));
            """)
            self._db.commit()

    # cache
    def get(self, key: str, now: float) -> Optional[tuple]:
        with self._lock:
            row = self._db.execute("SELECT payload, kind, expires FROM cache WHERE key=?", (key,)).fetchone()
            if not row:
                return None
            if row[2] <= now:
                self._db.execute("DELETE FROM cache WHERE key=?", (key,))
                self._db.commit()
                return None
            return json.loads(row[0]), row[1]

    def put(self, key: str, payload: dict, kind: str, ttl: int, now: float):
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO cache VALUES(?,?,?,?,?)",
                             (key, json.dumps(payload, ensure_ascii=False), kind, now, now + ttl))
            self._db.commit()

    def stats(self, now: float) -> dict:
        with self._lock:
            rows = self._db.execute("SELECT kind, COUNT(*) FROM cache WHERE expires>? GROUP BY kind", (now,)).fetchall()
        return {k: n for k, n in rows}

    # provider cooldown
    def cooldown(self, provider: str, now: float) -> Optional[tuple]:
        with self._lock:
            row = self._db.execute("SELECT cooldown_until, reason FROM provider_state WHERE provider=?", (provider,)).fetchone()
        if row and row[0] > now:
            return row[0], row[1]
        return None

    def set_cooldown(self, provider: str, until: float, reason: str):
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO provider_state VALUES(?,?,?)", (provider, until, reason))
            self._db.commit()

    # usage
    def usage(self, provider: str, month: str) -> int:
        with self._lock:
            row = self._db.execute("SELECT count FROM usage WHERE provider=? AND month=?", (provider, month)).fetchone()
        return row[0] if row else 0

    def bump_usage(self, provider: str, month: str) -> int:
        with self._lock:
            self._db.execute("INSERT INTO usage VALUES(?,?,1) ON CONFLICT(provider,month) DO UPDATE SET count=count+1",
                             (provider, month))
            self._db.commit()
            return self._db.execute("SELECT count FROM usage WHERE provider=? AND month=?", (provider, month)).fetchone()[0]


# ─── YouTube (free) provider via youtube-transcript-api ──────────────────────

class _TimeoutSession(requests.Session):
    """youtube-transcript-api 1.2.4 sets no request timeout; enforce one."""

    def __init__(self, timeout: float):
        super().__init__()
        # Ignore ambient HTTP(S)_PROXY env vars: proxies are used only when explicitly configured
        # (WEBSHARE_* / PROXY_URL), so "direct" really is direct and failures are attributable.
        self.trust_env = False
        self._default_timeout = (min(5, timeout), timeout)

    def request(self, *args, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self._default_timeout
        return super().request(*args, **kwargs)


def classify_library_exception(exc: BaseException, secrets=()) -> TranscriptError:
    """Map youtube-transcript-api / requests exceptions to TranscriptError."""
    if isinstance(exc, TranscriptError):
        return exc
    msg = redact(str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__, secrets)
    E = yt_errors
    if E is not None:
        if isinstance(exc, E.InvalidVideoId):
            return TranscriptError(INVALID_VIDEO_ID, "Invalid video id")
        if isinstance(exc, E.VideoUnavailable):
            return TranscriptError(VIDEO_UNAVAILABLE, "Video is unavailable (private, deleted or region-locked)")
        if isinstance(exc, E.TranscriptsDisabled):
            return TranscriptError(NO_CAPTIONS, "Captions are disabled for this video")
        if isinstance(exc, E.NoTranscriptFound):
            return TranscriptError(LANGUAGE_UNAVAILABLE, "No transcript in the requested language(s)")
        if isinstance(exc, E.AgeRestricted):
            return TranscriptError(AGE_RESTRICTED, "Video is age-restricted; transcript needs a signed-in session")
        if isinstance(exc, E.VideoUnplayable):
            return TranscriptError(VIDEO_UNPLAYABLE, "Video is unplayable: " + msg)
        if isinstance(exc, (E.RequestBlocked, E.PoTokenRequired)):  # IpBlocked subclasses RequestBlocked
            return TranscriptError(BLOCKED, "YouTube blocked this request (common for cloud/datacenter IPs)")
        if isinstance(exc, E.YouTubeRequestFailed):
            if "429" in str(exc):
                return TranscriptError(BLOCKED, "YouTube rate-limited this request (HTTP 429)")
            return TranscriptError(UPSTREAM_ERROR, "YouTube request failed: " + msg)
        if isinstance(exc, (E.YouTubeDataUnparsable, E.FailedToCreateConsentCookie)):
            return TranscriptError(PARSE_ERROR, "Could not parse YouTube response (library may need an upgrade): " + type(exc).__name__)
    if isinstance(exc, InvalidProxyConfig):
        return TranscriptError(PROXY_MISCONFIGURED, "Proxy configuration invalid: " + msg)
    if isinstance(exc, requests.exceptions.ProxyError):
        return TranscriptError(PROXY_ERROR, "Proxy connection failed (check proxy credentials/plan): " + msg)
    if isinstance(exc, requests.exceptions.Timeout):
        return TranscriptError(TIMEOUT, "Request timed out")
    if isinstance(exc, requests.exceptions.RequestException):
        return TranscriptError(NETWORK_ERROR, "Network error: " + type(exc).__name__)
    if isinstance(exc, (ValueError, KeyError, IndexError)) or type(exc).__name__ == "ParseError":
        return TranscriptError(PARSE_ERROR, "Unexpected response format: " + type(exc).__name__)
    return TranscriptError(UPSTREAM_ERROR, f"{type(exc).__name__}: {msg}")


def fetch_with_library(video_id: str, languages: list, any_language: bool,
                       proxy_config, timeout: float, secrets=()) -> dict:
    """Blocking. Returns normalized transcript dict or raises TranscriptError."""
    if YouTubeTranscriptApi is None:
        raise TranscriptError(UPSTREAM_ERROR, "youtube-transcript-api is not importable: " + str(_LIB_IMPORT_ERROR))
    try:
        api = YouTubeTranscriptApi(proxy_config=proxy_config, http_client=_TimeoutSession(timeout))
        tlist = api.list(video_id)
        available = [(t.language_code, bool(t.is_generated), t) for t in tlist]
        avail_info = [{"code": c, "name": getattr(t, "language", c), "auto_generated": g}
                      for c, g, t in available]
        chosen = pick_transcript(available, languages)
        if chosen is None and any_language and available:
            chosen = sorted(available, key=lambda a: a[1])[0]
        if chosen is None:
            raise TranscriptError(
                LANGUAGE_UNAVAILABLE if available else NO_CAPTIONS,
                "No transcript in requested language(s); available: " +
                (", ".join(a["code"] for a in avail_info) or "none"),
                available_languages=avail_info)
        code, generated, tr = chosen
        fetched = tr.fetch()
        segments = [{"text": s.text, "start": round(s.start, 1), "duration": round(s.duration, 1)}
                    for s in fetched]
    except TranscriptError:
        raise
    except Exception as e:
        raise classify_library_exception(e, secrets) from None
    if not segments:
        raise TranscriptError(NO_CAPTIONS, "Transcript was empty")
    return {
        "language_code": code,
        "caption_type": "auto_generated" if generated else "manual",
        "available_languages": avail_info,
        "segments": segments,
        "full_transcript": " ".join(s["text"] for s in segments),
    }


# ─── paid provider response parsing (pure functions, unit-tested) ────────────

def parse_transcriptapi_response(data: Any) -> Optional[dict]:
    """Mirrors the pre-existing parser. Returns {'full_transcript', 'segments'?} or None."""
    if not isinstance(data, dict):
        return None
    segs = data.get("segments", data.get("transcript", []))
    if isinstance(segs, list) and segs:
        texts, structured = [], []
        for seg in segs:
            if isinstance(seg, dict):
                t = seg.get("text", "")
                texts.append(t)
                if "start" in seg:
                    structured.append({"text": t, "start": round(float(seg.get("start") or 0), 1),
                                       "duration": round(float(seg.get("duration") or seg.get("dur") or 0), 1)})
            elif isinstance(seg, str):
                texts.append(seg)
        full = " ".join(t for t in texts if t)
        if len(full) > 50:
            out = {"full_transcript": full}
            if len(structured) == len([t for t in texts if t is not None]):
                out["segments"] = structured
            return out
    elif data.get("content"):
        return {"full_transcript": str(data["content"])}
    return None


def parse_supadata_response(data: Any) -> Optional[dict]:
    if not isinstance(data, dict):
        return None
    content = data.get("content", "")
    if isinstance(content, str) and len(content) > 50:
        return {"full_transcript": content, "language_code": data.get("lang"),
                "available_languages": data.get("availableLangs", [])}
    return None


_QUOTA_WORDS = re.compile(r"(?i)quota|credit|insufficient|limit exceeded|exceeded|billing|payment|subscription|upgrade")


def classify_paid_http(status: int, body_text: str) -> Optional[tuple]:
    """Return (status_label, cooldown_kind) for a non-success provider response."""
    quota_hint = bool(_QUOTA_WORDS.search(body_text or ""))
    if status == 402:
        return QUOTA_EXHAUSTED, "quota"
    if status in (401, 403):
        return (QUOTA_EXHAUSTED, "quota") if quota_hint else (PROVIDER_AUTH_FAILED, "auth")
    if status == 429:
        return (QUOTA_EXHAUSTED, "quota") if quota_hint else (PROVIDER_RATE_LIMITED, "rate")
    if status in (404, 422):
        return PROVIDER_NO_RESULT, None
    if status == 202:
        return PROVIDER_NO_RESULT, None  # async job; not supported by this server
    return UPSTREAM_ERROR, None


# ─── the service ─────────────────────────────────────────────────────────────

@dataclass
class _Ctx:
    video_id: str
    languages: list
    any_language: bool
    requested_language: str


class TranscriptService:
    def __init__(self, config: Optional[TranscriptConfig] = None, store: Optional[TranscriptStore] = None,
                 clock: Callable[[], float] = time.time,
                 http_client_factory: Optional[Callable[[], httpx.AsyncClient]] = None,
                 library_fetch: Optional[Callable[..., dict]] = None):
        self.cfg = config or TranscriptConfig.from_env()
        self.store = store or TranscriptStore(self.cfg.cache_path)
        self._clock = clock
        self._http_factory = http_client_factory or (lambda: httpx.AsyncClient(timeout=self.cfg.request_timeout))
        self._library_fetch = library_fetch or fetch_with_library
        self._inflight: dict = {}
        self._sem: Optional[asyncio.Semaphore] = None
        self._fetch_times: deque = deque()
        self._counters = {"requests": 0, "cache_hits": 0, "deduplicated": 0, "network_fetches": 0}

    # -- public ------------------------------------------------------------
    async def get_transcript(self, video_id: str, language: str = "en", any_language: bool = False) -> dict:
        self._counters["requests"] += 1
        vid = normalize_video_id(video_id)
        if vid is None:
            return self._error_result(str(video_id)[:40], TranscriptError(
                INVALID_VIDEO_ID, "Invalid video id: expected an 11-character YouTube video id or a YouTube URL"), [])
        languages = parse_languages(language)
        ctx = _Ctx(vid, languages, bool(any_language), language or "en")
        key = f"{vid}|{','.join(l.lower() for l in languages)}|{int(ctx.any_language)}"

        hit = self.store.get(key, self._clock())
        if hit:
            self._counters["cache_hits"] += 1
            payload, kind = hit
            return self._present(payload, ctx, cached=True)

        pending = self._inflight.get(key)
        if pending is not None:
            self._counters["deduplicated"] += 1
            payload = await asyncio.shield(pending)
            return self._present(payload, ctx, cached=False, deduplicated=True)

        fut = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        payload = None
        try:
            try:
                payload = await asyncio.wait_for(self._run_chain(ctx), timeout=self.cfg.total_timeout)
            except asyncio.TimeoutError:
                payload = self._error_payload(ctx, TranscriptError(TIMEOUT, "Overall transcript timeout reached"), [])
            except asyncio.CancelledError:
                payload = self._error_payload(ctx, TranscriptError(UPSTREAM_ERROR, "Request cancelled"), [])
                raise
            except Exception as e:  # never let a bug escape as an unclassified 500
                log.exception("transcript chain crashed")
                payload = self._error_payload(ctx, TranscriptError(UPSTREAM_ERROR, "Internal error: " + type(e).__name__), [])
            now = self._clock()
            if payload.get("status") == OK:
                self.store.put(key, payload, "positive", self.cfg.cache_ttl_seconds, now)
            elif payload.get("status") in NEGATIVE_CACHEABLE:
                ttl = 3600 if payload["status"] == LANGUAGE_UNAVAILABLE else self.cfg.negative_ttl_seconds
                self.store.put(key, payload, "negative", ttl, now)
            return self._present(payload, ctx, cached=False)
        finally:
            self._inflight.pop(key, None)
            if not fut.done():
                fut.set_result(payload or self._error_payload(ctx, TranscriptError(UPSTREAM_ERROR, "Request aborted"), []))

    def diagnostics(self) -> dict:
        now = self._clock()
        month = time.strftime("%Y-%m", time.gmtime(now))
        providers = {}
        for p in self.cfg.provider_order:
            info = {"configured": self._configured(p)}
            if p in PAID_PROVIDERS:
                info["enabled"] = self.cfg.enable_paid
                info["used_this_month"] = self.store.usage(p, month)
                info["monthly_limit"] = self.cfg.paid_monthly_limit
            cd = self.store.cooldown(p, now)
            if cd:
                info["cooldown_seconds_remaining"] = int(cd[0] - now)
                info["cooldown_reason"] = cd[1]
            providers[p] = info
        return {
            "provider_order": self.cfg.provider_order,
            "paid_apis_enabled": self.cfg.enable_paid,
            "paid_apis_effective": self.cfg.enable_paid and self._paid_state_trustworthy(),
            "providers": providers,
            "cache": {"persistent": self.store.persistent, "entries": self.store.stats(now)},
            "counters": dict(self._counters),
            "library_available": YouTubeTranscriptApi is not None,
        }

    # -- internals ---------------------------------------------------------
    def _configured(self, p: str) -> bool:
        if p == "youtube_direct":
            return True
        if p == "youtube_proxy":
            return bool((self.cfg.webshare_user and self.cfg.webshare_pass) or self.cfg.proxy_url)
        if p == "transcriptapi":
            return bool(self.cfg.transcript_api_key)
        if p == "supadata":
            return bool(self.cfg.supadata_api_key)
        return False

    def _proxy_config(self):
        if self.cfg.webshare_user and self.cfg.webshare_pass:
            return WebshareProxyConfig(proxy_username=self.cfg.webshare_user,
                                       proxy_password=self.cfg.webshare_pass,
                                       retries_when_blocked=self.cfg.proxy_retries), "webshare"
        if self.cfg.proxy_url:
            return GenericProxyConfig(http_url=self.cfg.proxy_url, https_url=self.cfg.proxy_url), True
        return None, False

    def _sem_get(self) -> asyncio.Semaphore:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self.cfg.max_concurrent_fetches)
        return self._sem

    def _local_rate_ok(self, now: float) -> Optional[int]:
        """Global cap on outbound (uncached) fetches/minute. Returns retry_after or None."""
        if self.cfg.fetches_per_minute <= 0:
            return None
        while self._fetch_times and self._fetch_times[0] <= now - 60:
            self._fetch_times.popleft()
        if len(self._fetch_times) >= self.cfg.fetches_per_minute:
            return max(1, int(self._fetch_times[0] + 60 - now))
        self._fetch_times.append(now)
        return None

    async def _run_chain(self, ctx: _Ctx) -> dict:
        attempts: list = []
        errors: list = []
        for name in self.cfg.provider_order:
            now = self._clock()
            skip = self._skip_reason(name, now)
            if skip:
                attempts.append({"provider": name, "outcome": "skipped", "reason": skip})
                continue
            retry = self._local_rate_ok(now)
            if retry is not None:
                err = TranscriptError(LOCAL_RATE_LIMITED, "Server-side transcript fetch rate limit reached", retry_after=retry)
                attempts.append({"provider": name, "outcome": LOCAL_RATE_LIMITED})
                errors.append(err)
                break
            self._counters["network_fetches"] += 1
            try:
                data, source, proxy_used = await self._call_provider(name, ctx)
            except TranscriptError as e:
                self._after_failure(name, e)
                attempts.append({"provider": name, "outcome": e.status, "detail": redact(e.message, self.cfg.secrets())[:200]})
                errors.append(e)
                if e.definitive:
                    return self._error_payload(ctx, e, attempts)
                continue
            except asyncio.TimeoutError:
                e = TranscriptError(TIMEOUT, f"{name} timed out")
                attempts.append({"provider": name, "outcome": TIMEOUT})
                errors.append(e)
                continue
            attempts.append({"provider": name, "outcome": OK})
            return self._success_payload(ctx, data, source, proxy_used, attempts)
        return self._error_payload(ctx, self._summarize(errors, attempts), attempts)

    def _skip_reason(self, name: str, now: float) -> Optional[str]:
        if name in PAID_PROVIDERS:
            if not self.cfg.enable_paid:
                return "paid_apis_disabled"
            if not self._configured(name):
                return "not_configured"
            if not self._paid_state_trustworthy():
                return "ephemeral_state_paid_blocked"
            month = time.strftime("%Y-%m", time.gmtime(now))
            if self.cfg.paid_monthly_limit <= 0 or self.store.usage(name, month) >= self.cfg.paid_monthly_limit:
                return "monthly_limit_reached"
        elif not self._configured(name):
            return "not_configured"
        cd = self.store.cooldown(name, now)
        if cd:
            return f"cooldown:{cd[1]}:{int(cd[0] - now)}s"
        return None

    def _paid_state_trustworthy(self) -> bool:
        """Monthly caps/cooldowns live in the SQLite store. On an ephemeral filesystem (e.g. Render free:
        wiped on every restart/spin-down/deploy) they would silently reset, so paid calls are refused unless the
        owner pointed TRANSCRIPT_CACHE_PATH at persistent storage or explicitly accepted the risk."""
        if self.cfg.allow_paid_with_ephemeral_state:
            return True
        return bool(self.cfg.cache_path_explicit and self.store.persistent)

    def _after_failure(self, name: str, e: TranscriptError):
        now = self._clock()
        until, reason = None, e.status
        if e.status == BLOCKED:
            until = now + self.cfg.block_cooldown_seconds
        elif e.status == QUOTA_EXHAUSTED:
            until = now + self.cfg.quota_cooldown_seconds
        elif e.status == PROVIDER_AUTH_FAILED:
            until = now + self.cfg.auth_cooldown_seconds
        elif e.status == PROVIDER_RATE_LIMITED:
            until = now + self.cfg.rate_cooldown_seconds
        elif e.status in (PROXY_ERROR, PROXY_MISCONFIGURED):
            until = now + 60
        if until:
            self.store.set_cooldown(name, until, reason)

    async def _call_provider(self, name: str, ctx: _Ctx):
        if name in FREE_PROVIDERS:
            proxy_cfg, proxy_used = (None, False)
            if name == "youtube_proxy":
                try:
                    proxy_cfg, proxy_used = self._proxy_config()
                except Exception as e:
                    raise classify_library_exception(e, self.cfg.secrets()) from None
            async with self._sem_get():
                data = await asyncio.wait_for(
                    asyncio.to_thread(self._library_fetch, ctx.video_id, ctx.languages, ctx.any_language,
                                      proxy_cfg, self.cfg.request_timeout, self.cfg.secrets()),
                    timeout=self.cfg.request_timeout * 2 + 10)
            return data, "youtube-transcript-api", proxy_used
        return await self._call_paid(name, ctx)

    async def _call_paid(self, name: str, ctx: _Ctx):
        month = time.strftime("%Y-%m", time.gmtime(self._clock()))
        self.store.bump_usage(name, month)  # count before the call: failures may still be billed
        secrets = self.cfg.secrets()
        if name == "transcriptapi":
            req = dict(url="https://transcriptapi.com/api/v2/youtube/transcript",
                       params={"video_url": ctx.video_id, "send_metadata": "true"},
                       headers={"Authorization": f"Bearer {self.cfg.transcript_api_key}"})
            parser, source = parse_transcriptapi_response, "transcriptapi.com"
        else:
            params = {"url": f"https://www.youtube.com/watch?v={ctx.video_id}", "text": "true"}
            if ctx.languages and ctx.languages[0] != "en":
                params["lang"] = ctx.languages[0]
            req = dict(url="https://api.supadata.ai/v1/transcript", params=params,
                       headers={"x-api-key": self.cfg.supadata_api_key})
            parser, source = parse_supadata_response, "supadata.ai"
        try:
            async with self._http_factory() as client:
                r = await client.get(**req)
        except httpx.TimeoutException:
            raise TranscriptError(TIMEOUT, f"{name} timed out") from None
        except httpx.HTTPError as e:
            raise TranscriptError(NETWORK_ERROR, f"{name} network error: {type(e).__name__}") from None
        if r.status_code != 200:
            label, _ = classify_paid_http(r.status_code, r.text[:500])
            raise TranscriptError(label, f"{name} returned HTTP {r.status_code}: " + redact(r.text[:120], secrets))
        try:
            parsed = parser(r.json())
        except ValueError:
            parsed = None
        if not parsed:
            raise TranscriptError(PROVIDER_NO_RESULT, f"{name} returned no usable transcript")
        parsed.setdefault("language_code", None)
        parsed.setdefault("caption_type", "unknown")
        parsed.setdefault("available_languages", [])
        return parsed, source, False

    def _summarize(self, errors: list, attempts: list) -> TranscriptError:
        if not errors:
            reasons = {a.get("reason", "") for a in attempts}
            hint = ("No transcript provider could run. Free YouTube access is the default; "
                    "check provider order/config. Skipped: " + ", ".join(sorted(r for r in reasons if r)))
            blocked_cd = any(str(a.get("reason", "")).startswith("cooldown:blocked") for a in attempts)
            if blocked_cd:
                return TranscriptError(BLOCKED, "YouTube is blocking this server's IP; temporarily not retrying. " + self._block_hint())
            return TranscriptError(ALL_FAILED, hint)
        by = [e.status for e in errors]
        if LOCAL_RATE_LIMITED in by:
            return next(e for e in errors if e.status == LOCAL_RATE_LIMITED)
        parts = list(dict.fromkeys(e.message for e in errors if e.status not in (BLOCKED, QUOTA_EXHAUSTED)))
        if BLOCKED in by:
            parts.insert(0, "YouTube blocked the request. " + self._block_hint())
        if QUOTA_EXHAUSTED in by:
            parts.append("Paid transcript provider quota is exhausted (captions may still exist).")
        msg = " ".join(parts)[:400]
        if BLOCKED in by:
            return TranscriptError(BLOCKED, msg)
        if QUOTA_EXHAUSTED in by:
            return TranscriptError(QUOTA_EXHAUSTED, msg)
        return TranscriptError(by[-1] if len(set(by)) == 1 else ALL_FAILED, msg)

    def _block_hint(self) -> str:
        if self._configured("youtube_proxy"):
            return "A proxy is configured but also failed or is cooling down."
        return ("This is typical for cloud IPs. Options: configure a residential proxy "
                "(WEBSHARE_USER/WEBSHARE_PASS or PROXY_URL), or set ENABLE_PAID_TRANSCRIPT_APIS=true with a provider key.")

    # -- payload shaping ---------------------------------------------------
    def _success_payload(self, ctx: _Ctx, data: dict, source: str, proxy_used, attempts: list) -> dict:
        segments = data.get("segments") or []
        return {
            "status": OK,
            "video_id": ctx.video_id,
            "language_code": data.get("language_code"),
            "requested_language": ctx.requested_language,
            "total_segments": len(segments) if segments else None,
            "full_transcript": data["full_transcript"],
            "segments": segments,
            "caption_type": data.get("caption_type", "unknown"),
            "available_languages": data.get("available_languages", []),
            "source": source,
            "proxy_used": proxy_used,
            "attempts": attempts,
        }

    def _error_payload(self, ctx: _Ctx, err: TranscriptError, attempts: list) -> dict:
        p = {
            "status": err.status,
            "video_id": ctx.video_id,
            "error": f"Transcript unavailable: {redact(err.message, self.cfg.secrets())}",
            "source": "none",
            "retryable": err.status not in DEFINITIVE,
            "attempts": attempts,
        }
        if err.available_languages:
            p["available_languages"] = err.available_languages
        if err.retry_after:
            p["retry_after_seconds"] = err.retry_after
        return p

    def _error_result(self, video_id: str, err: TranscriptError, attempts: list) -> dict:
        return self._error_payload(_Ctx(video_id, [], False, ""), err, attempts)

    def _present(self, payload: dict, ctx: _Ctx, cached: bool, deduplicated: bool = False) -> dict:
        """Backward-compatible response (keys of the pre-existing tool) + new fields."""
        out = dict(payload)
        out["cached"] = cached
        if deduplicated:
            out["deduplicated"] = True
        if out.get("status") != OK:
            return out
        segments = out.pop("segments", []) or []
        cap = self.cfg.max_segments_returned
        out["language"] = out.get("language_code") or ctx.requested_language
        if segments:
            out["segments"] = segments[:cap]
            out["segments_truncated"] = len(segments) > cap
        else:
            out.pop("total_segments", None)
        if out.get("total_segments") is None:
            out.pop("total_segments", None)
        return out
