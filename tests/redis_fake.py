"""Upstash-REST-shaped fake backed by fakeredis (a real Redis command/Lua semantics emulator, in-process).
MOCK: it is not Upstash. It lets the exact Lua scripts run, and counts commands (top-level and inside scripts)."""
import json

import fakeredis
import httpx
from fakeredis._socket import _base as _fr_base

from tests.helpers import Clock


def to_resp2(x):
    """redis-py post-processes some replies (HGETALL -> dict, ZRANGE WITHSCORES -> [(member, float)]). Upstash returns the
    raw RESP2 shapes (flat arrays, scores as strings), so undo that to keep the production parsing code honest."""
    if isinstance(x, dict):
        return [to_resp2(i) for kv in x.items() for i in kv]
    if isinstance(x, tuple):
        return [to_resp2(i) for i in x]
    if isinstance(x, list):
        if x and all(isinstance(i, (list, tuple)) and len(i) == 2 and isinstance(i[1], float) for i in x):
            return [to_resp2(e) for i in x for e in i]
        return [to_resp2(i) for i in x]
    if isinstance(x, float):
        return str(int(x)) if x == int(x) else repr(x)
    return x


class RedisBackedUpstash:
    def __init__(self, token="tok_SECRET_up"):
        self.server = fakeredis.FakeServer()
        self.redis = fakeredis.FakeRedis(server=self.server, decode_responses=True)
        self.token = token
        self.down = False          # transport outage
        self.http_requests = 0
        self.top_level = 0         # commands received over the (fake) REST pipeline
        self.inner = 0             # commands executed inside Lua scripts
        self.eval_calls = 0
        self.current_keys = set()      # keys declared by the EVAL currently running
        self.undeclared = []           # (command, key) touched inside a script but NOT declared in KEYS
        self.requests = []             # raw REST request bodies, for protocol-shape assertions

    # command accounting: count every command fakeredis executes, split by origin
    def _install_counter(self):
        outer = self
        orig = _fr_base.BaseFakeSocket._run_command

        def counting(sock, func, sig, args, from_script):
            if from_script:
                outer.inner += 1
                if args:
                    k = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
                    if k not in outer.current_keys:
                        outer.undeclared.append((getattr(func, "__name__", "?"), k))
            return orig(sock, func, sig, args, from_script)
        _fr_base.BaseFakeSocket._run_command = counting
        self._restore = lambda: setattr(_fr_base.BaseFakeSocket, "_run_command", orig)

    def handler(self, request: httpx.Request):
        self.http_requests += 1
        if self.down:
            raise httpx.ConnectError("down", request=request)
        if request.headers.get("authorization") != f"Bearer {self.token}":
            return httpx.Response(401, json={"error": "Unauthorized"})
        out = []
        body = json.loads(request.content)
        self.requests.append({"url": str(request.url), "body": body})
        for cmd in body:
            self.top_level += 1
            if str(cmd[0]).upper() == "EVAL":
                self.eval_calls += 1
                self.current_keys = {str(k) for k in cmd[3:3 + int(cmd[2])]}
            try:
                res = self.redis.execute_command(*cmd)
                out.append({"result": to_resp2(res)})
            except Exception as e:
                out.append({"error": f"ERR {e}"})
        return httpx.Response(200, json=out)

    def factory(self):
        return lambda: httpx.AsyncClient(transport=httpx.MockTransport(self.handler))

    # ── introspection helpers ──
    def used_bytes_cache(self):
        return sum(len(self.redis.get(k) or "") for k in self.redis.scan_iter(match="yt:c:*"))

    def keys(self, pattern="*"):
        return sorted(self.redis.scan_iter(match=pattern))

    @property
    def billed_commands_worst_case(self):
        return self.top_level + self.inner


def make_remote(fake, clock=None, **kw):
    from remote_store import RemoteStore
    return RemoteStore("https://fake-upstash.example", fake.token, client_factory=fake.factory(), clock=clock or Clock(), **kw)
