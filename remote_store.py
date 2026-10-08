"""Optional shared persistent store over the Upstash Redis REST API (works from Render Free, which has an
ephemeral filesystem and spins down when idle).

Holds three things so that they survive restarts/redeploys and are shared across instances:
  * transcript cache (positive + negative), zlib+base64 JSON, with Redis TTLs
  * paid-provider cooldowns
  * paid-provider usage counters (atomic INCR reservations, per month and per day)

Every failure is non-fatal: reads degrade to a cache miss, and paid reservations FAIL CLOSED
("state_unavailable") because a cap that cannot be checked is not a cap.

Protocol (Upstash docs): POST {url}/pipeline with Bearer token and body [[cmd, args...], ...] ->
[{"result": ...} | {"error": "..."}]. NOT yet verified against a live Upstash database (needs the owner's free
account) - the probe's remote-store round trip is the verification.
"""
from __future__ import annotations

import base64
import json
import logging
import time
import zlib
from typing import Any, Callable, Optional

import httpx

log = logging.getLogger("yt.remote_store")

MAX_VALUE_BYTES = 8_000_000  # Upstash free tier max request size is 10 MB
UNAVAILABLE = object()


def encode_value(kind: str, payload: dict) -> str:
    raw = json.dumps({"k": kind, "p": payload}, ensure_ascii=False, separators=(",", ":")).encode()
    return base64.b64encode(zlib.compress(raw, 6)).decode()


def decode_value(s: str) -> tuple:
    obj = json.loads(zlib.decompress(base64.b64decode(s)))
    return obj["p"], obj["k"]


class RemoteStore:
    def __init__(self, url: str, token: str, prefix: str = "yt:", timeout: float = 5.0,
                 client_factory: Optional[Callable[[], httpx.AsyncClient]] = None,
                 clock: Callable[[], float] = time.time, breaker_failures: int = 3, breaker_seconds: int = 60):
        self.url = url.rstrip("/")
        self.token = token
        self.prefix = prefix
        self._factory = client_factory or (lambda: httpx.AsyncClient(timeout=timeout))
        self._clock = clock
        self._fails = 0
        self._open_until = 0.0
        self._breaker_failures, self._breaker_seconds = breaker_failures, breaker_seconds
        self.stats = {"commands": 0, "errors": 0, "short_circuited": 0}
        self.last_error: Optional[str] = None

    @property
    def available(self) -> bool:
        return self._clock() >= self._open_until

    async def _pipeline(self, commands: list):
        """Returns list of results, or UNAVAILABLE. Never raises."""
        if not self.available:
            self.stats["short_circuited"] += 1
            return UNAVAILABLE
        try:
            async with self._factory() as client:
                r = await client.post(self.url + "/pipeline", json=commands,
                                      headers={"Authorization": f"Bearer {self.token}"})
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            body = r.json()
            if not isinstance(body, list) or len(body) != len(commands):
                raise RuntimeError("unexpected response shape")
            errs = [x.get("error") for x in body if isinstance(x, dict) and x.get("error")]
            if errs:
                raise RuntimeError("command error")  # message deliberately not retained (may echo data)
            self.stats["commands"] += len(commands)
            self._fails = 0
            return [x.get("result") for x in body]
        except Exception as e:
            self.stats["errors"] += 1
            self.last_error = type(e).__name__ + (f": {e}" if isinstance(e, RuntimeError) else "")
            self._fails += 1
            if self._fails >= self._breaker_failures:
                self._open_until = self._clock() + self._breaker_seconds
                self._fails = 0
            return UNAVAILABLE

    # ── cache ──
    async def get_cache(self, key: str):
        res = await self._pipeline([["GET", f"{self.prefix}c:{key}"]])
        if res is UNAVAILABLE or res[0] is None:
            return None
        try:
            return decode_value(res[0])
        except Exception:
            return None  # corrupt/foreign value = miss

    async def put_cache(self, key: str, payload: dict, kind: str, ttl: int) -> bool:
        enc = encode_value(kind, payload)
        if len(enc) > MAX_VALUE_BYTES and payload.get("segments"):
            enc = encode_value(kind, {**payload, "segments": []})  # keep the text, drop timestamps
        if len(enc) > MAX_VALUE_BYTES:
            return False
        res = await self._pipeline([["SET", f"{self.prefix}c:{key}", enc, "EX", int(max(ttl, 1))]])
        return res is not UNAVAILABLE

    # ── cooldowns ──
    async def get_cooldown(self, provider: str, now: float):
        res = await self._pipeline([["GET", f"{self.prefix}cd:{provider}"]])
        if res is UNAVAILABLE or not res[0]:
            return None
        try:
            until, reason = str(res[0]).split("|", 1)
            until = float(until)
        except ValueError:
            return None
        return (until, reason) if until > now else None

    async def set_cooldown(self, provider: str, until: float, reason: str, now: float) -> bool:
        ttl = int(max(until - now, 1))
        res = await self._pipeline([["SET", f"{self.prefix}cd:{provider}", f"{until}|{reason}", "EX", ttl]])
        return res is not UNAVAILABLE

    # ── usage caps (atomic reservation) ──
    async def reserve(self, provider: str, month: str, day: str, month_limit: int, day_limit: int) -> Optional[str]:
        """None = reserved. Otherwise the reason string. Fails closed when the store is unreachable."""
        if month_limit <= 0:
            return "monthly_limit_reached"
        if day_limit <= 0:
            return "daily_limit_reached"
        mk, dk = f"{self.prefix}u:{provider}:{month}", f"{self.prefix}u:{provider}:{day}"
        res = await self._pipeline([["INCR", mk], ["EXPIRE", mk, 40 * 86400], ["INCR", dk], ["EXPIRE", dk, 2 * 86400]])
        if res is UNAVAILABLE:
            return "state_unavailable"
        m, d = int(res[0]), int(res[2])
        if m > month_limit or d > day_limit:
            await self._pipeline([["DECR", mk], ["DECR", dk]])  # give the slot back; best effort
            return "monthly_limit_reached" if m > month_limit else "daily_limit_reached"
        return None

    async def usage(self, provider: str, period: str) -> Optional[int]:
        res = await self._pipeline([["GET", f"{self.prefix}u:{provider}:{period}"]])
        if res is UNAVAILABLE:
            return None
        return int(res[0] or 0)

    # ── verification ──
    async def roundtrip(self) -> dict:
        """Write/read/delete one tiny key. Used by the probe to verify credentials and the REST protocol."""
        k = f"{self.prefix}probe:{int(self._clock())}"
        res = await self._pipeline([["SET", k, "ok", "EX", 60], ["GET", k], ["DEL", k]])
        if res is UNAVAILABLE:
            return {"ok": False, "error": self.last_error}
        return {"ok": res[1] == "ok", "error": None if res[1] == "ok" else "readback_mismatch"}

    def diagnostics(self) -> dict:
        return {"configured": True, "available": self.available, **self.stats, "last_error": self.last_error}
