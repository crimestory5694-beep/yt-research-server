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


def load_tokens(env=None) -> list:
    e = os.environ if env is None else env
    raw = ",".join([e.get("MCP_AUTH_TOKENS", ""), e.get("MCP_AUTH_TOKEN", "")])
    return [t.strip() for t in raw.split(",") if t.strip()]


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
    def __init__(self, app, tokens=None, rate_limit_per_minute: Optional[int] = None):
        self.app = app
        self.tokens = load_tokens() if tokens is None else tokens
        if rate_limit_per_minute is None:
            try:
                rate_limit_per_minute = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "120"))
            except ValueError:
                rate_limit_per_minute = 120
        self.limiter = SlidingWindowLimiter(rate_limit_per_minute)

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

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path = scope.get("path", "")
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
