"""Optional bearer-token auth and per-client rate limiting (pure ASGI middleware).

Defaults preserve existing client access:
  * MCP_AUTH_TOKEN(S) unset  -> auth DISABLED (open, as before). Set it to enforce.
  * RATE_LIMIT_PER_MINUTE    -> 120 requests/min per client on protected paths.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from collections import defaultdict, deque
from typing import Optional
from urllib.parse import parse_qs

PROTECTED_PREFIXES = ("/mcp", "/messages", "/sse", "/health/transcripts")
WORKER_PREFIX = "/worker"


def load_tokens(env=None) -> list:
    e = os.environ if env is None else env
    raw = ",".join([e.get("MCP_AUTH_TOKENS", ""), e.get("MCP_AUTH_TOKEN", "")])
    return [t.strip() for t in raw.split(",") if t.strip()]


def load_worker_tokens(env=None) -> list:
    """Tokens that may call /worker/* ONLY. Distinct from the MCP tokens (least privilege): a leaked worker token
    cannot use MCP tools, and an MCP token cannot claim/complete jobs. One token per worker allows revocation."""
    e = os.environ if env is None else env
    raw = ",".join([e.get("WORKER_AUTH_TOKENS", ""), e.get("WORKER_AUTH_TOKEN", "")])
    return [t.strip() for t in raw.split(",") if t.strip()]


class _TooLarge(Exception):
    pass


class SlidingWindowLimiter:
    def __init__(self, limit: int, window: float = 60.0, clock=time.monotonic):
        self.limit, self.window, self._clock = limit, window, clock
        self._hits: dict = defaultdict(deque)

    def check(self, key: str) -> Optional[int]:
        """None if allowed, else seconds until a slot frees up."""
        if self.limit <= 0:
            return None
        now = self._clock()
        q = self._hits[key]
        while q and q[0] <= now - self.window:
            q.popleft()
        if len(q) >= self.limit:
            return max(1, int(q[0] + self.window - now) + 1)
        q.append(now)
        if len(self._hits) > 10000:  # bound memory
            for k in [k for k, v in self._hits.items() if not v][:5000]:
                self._hits.pop(k, None)
        return None


class SecurityMiddleware:
    def __init__(self, app, tokens=None, rate_limit_per_minute: Optional[int] = None, worker_tokens=None):
        self.app = app
        self.tokens = load_tokens() if tokens is None else tokens
        if rate_limit_per_minute is None:
            try:
                rate_limit_per_minute = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "120"))
            except ValueError:
                rate_limit_per_minute = 120
        self.limiter = SlidingWindowLimiter(rate_limit_per_minute)
        self._worker_tokens = worker_tokens
        self.worker_limiter = SlidingWindowLimiter(int(os.environ.get("WORKER_RATE_LIMIT_PER_MINUTE", "60") or 60))
        try:
            self.worker_max_body = int(os.environ.get("WORKER_MAX_BODY_BYTES", str(4_000_000)))
        except ValueError:
            self.worker_max_body = 4_000_000

    @property
    def auth_enabled(self) -> bool:
        return bool(self.tokens)

    def _presented_token(self, scope) -> str:
        headers = {k.decode("latin1").lower(): v.decode("latin1") for k, v in scope.get("headers", [])}
        auth = headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        if headers.get("x-api-key"):
            return headers["x-api-key"].strip()
        qs = parse_qs(scope.get("query_string", b"").decode("latin1"))
        return (qs.get("token") or [""])[0]

    def _client_key(self, scope, token: str) -> str:
        if token:
            return "t:" + hashlib.sha256(token.encode()).hexdigest()[:16]
        headers = {k.decode("latin1").lower(): v.decode("latin1") for k, v in scope.get("headers", [])}
        hdr = os.environ.get("CLIENT_IP_HEADER", "").strip().lower()  # e.g. true-client-ip, if your proxy sets it
        if hdr and headers.get(hdr):
            return "ip:" + headers[hdr].split(",")[0].strip()
        xff = headers.get("x-forwarded-for", "")
        if xff:  # rightmost entry is the one appended by the nearest (trusted) proxy
            return "ip:" + xff.split(",")[-1].strip()
        client = scope.get("client")
        return "ip:" + (client[0] if client else "unknown")

    async def _reject(self, send, status: int, message: str, retry_after: Optional[int] = None):
        body = json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32001, "message": message}}).encode()
        headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
        if retry_after:
            headers.append((b"retry-after", str(retry_after).encode()))
        if status == 401:
            headers.append((b"www-authenticate", b"Bearer"))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    async def _worker(self, scope, receive, send):
        """/worker/*: separate bearer tokens (header only - never in URLs), disabled unless configured, body-size
        capped, rate limited per token."""
        tokens = load_worker_tokens() if self._worker_tokens is None else self._worker_tokens
        if not tokens:
            return await self._reject(send, 503, "Worker API disabled: no WORKER_AUTH_TOKEN configured")
        headers = {k.decode("latin1").lower(): v.decode("latin1") for k, v in scope.get("headers", [])}
        auth = headers.get("authorization", "")
        presented = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if not presented or not any(hmac.compare_digest(presented.encode(), t.encode()) for t in tokens):
            return await self._reject(send, 401, "Unauthorized")
        try:                                           # re-read so the limit can be tuned without a code change
            self.worker_limiter.limit = int(os.environ.get("WORKER_RATE_LIMIT_PER_MINUTE", "60") or 60)
        except ValueError:
            pass
        wait = self.worker_limiter.check("w:" + hashlib.sha256(presented.encode()).hexdigest()[:16])
        if wait is not None:
            return await self._reject(send, 429, "Rate limit exceeded", wait)
        cl = headers.get("content-length")
        if cl and cl.isdigit() and int(cl) > self.worker_max_body:
            return await self._reject(send, 413, "Request body too large")
        seen = {"n": 0}

        async def limited_receive():
            msg = await receive()
            if msg["type"] == "http.request":
                seen["n"] += len(msg.get("body", b""))
                if seen["n"] > self.worker_max_body:
                    raise _TooLarge()
            return msg
        try:
            return await self.app(scope, limited_receive, send)
        except _TooLarge:
            return await self._reject(send, 413, "Request body too large")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path = scope.get("path", "")
        if path == WORKER_PREFIX or path.startswith(WORKER_PREFIX + "/"):
            return await self._worker(scope, receive, send)
        if scope.get("method") == "OPTIONS" or not path.startswith(PROTECTED_PREFIXES):
            return await self.app(scope, receive, send)

        presented = self._presented_token(scope)
        if self.auth_enabled:
            ok = any(hmac.compare_digest(presented.encode(), t.encode()) for t in self.tokens)
            if not ok:
                return await self._reject(send, 401, "Unauthorized")
        # only POSTs do work; long-lived GET streams are not counted
        if scope.get("method") == "POST":
            wait = self.limiter.check(self._client_key(scope, presented if self.auth_enabled else ""))
            if wait is not None:
                return await self._reject(send, 429, "Rate limit exceeded", wait)
        return await self.app(scope, receive, send)
