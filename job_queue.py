"""Durable transcript job queue on Upstash Redis (REST), coordinated by the Render service.

Why Lua: claiming a job is read-decide-write (pick a due job, lease it, count the attempt). Upstash documents EVAL
over REST and runs a script as one atomic step; it also asks that every key a script touches is passed in KEYS. The
five keys below are therefore the ONLY keys any script touches (job records live as fields of one hash).
Not verified against a live Upstash database yet - see docs/WORKER_AND_QUEUE.md "live verification".

Data model (prefix P = "yt:q:"):
  P jobs     HASH   field = job_id ("<video_id>:<language>"), value = "status|video|lang|attempts|defers|created|
                    updated|owner|lease_until|fail_class|message|not_before"   (no JSON: cjson is not documented
                    for Upstash, so records are split/joined in plain Lua)
  P pending  ZSET   job_id -> not_before   (pending AND deferred jobs; claimable when score <= now)
  P leases   ZSET   job_id -> lease_until  (processing jobs)
  P stats    HASH   counters and n:<status> gauges
  P done     ZSET   job_id -> finished time (completed/failed records, pruned after retention)

Status machine: pending -> processing -> completed | failed | pending (retry) | deferred (blocked, retry later).
Guarantees: a job is acknowledged to the caller only after the EVAL that stored it returned; a claimed job whose worker
disappears is re-queued when its lease expires; completion is idempotent and also accepted late; attempts are bounded.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional

from remote_store import RemoteCommandError, RemoteStore, RemoteUnavailable

log = logging.getLogger("yt.queue")

DEFINITIVE_CLASSES = {"invalid_video_id", "no_captions", "language_unavailable", "video_unavailable",
                      "age_restricted", "video_unplayable"}
ERROR_CLASS_RE = re.compile(r"^[a-z][a-z_:]{2,40}$")
LANG_RE = re.compile(r"^(any|[a-z]{2,3}(-[a-z0-9]{2,8})?)$")
VIDEO_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}:(any|[a-z]{2,3}(-[a-z0-9]{2,8})?)$")


class QueueUnavailable(Exception):
    """The durable store could not be reached: nothing was queued or changed."""


def _int(v, d):
    try:
        return int(v) if v not in (None, "") else d
    except ValueError:
        return d


@dataclass
class QueueConfig:
    enabled: bool = False
    auto_queue: bool = True              # sync tool queues a job when extraction failed for a retryable reason
    max_attempts: int = 5
    lease_default: int = 300
    lease_min: int = 60
    lease_max: int = 900
    requeue_base: int = 60               # backoff after an expired lease / transient failure
    defer_base: int = 1800               # first re-try delay after YouTube blocked the worker
    defer_max: int = 21600
    max_age_days: int = 7
    retention_days: int = 14
    max_pending: int = 2000
    claim_max: int = 5
    storage_limit_mb: int = 256
    prefix: str = "yt:"

    @classmethod
    def from_env(cls, env=None) -> "QueueConfig":
        e = __import__("os").environ if env is None else env
        on = (e.get("TRANSCRIPT_QUEUE_ENABLED", "") or "").strip().lower() in ("1", "true", "yes", "on")
        auto = (e.get("TRANSCRIPT_AUTO_QUEUE", "true") or "true").strip().lower() in ("1", "true", "yes", "on")
        return cls(enabled=on, auto_queue=auto,
                   max_attempts=max(1, _int(e.get("QUEUE_MAX_ATTEMPTS"), 5)),
                   lease_default=_int(e.get("QUEUE_LEASE_SECONDS"), 300),
                   defer_base=_int(e.get("QUEUE_BLOCK_DEFER_SECONDS"), 1800),
                   defer_max=_int(e.get("QUEUE_BLOCK_DEFER_MAX_SECONDS"), 21600),
                   max_age_days=_int(e.get("QUEUE_JOB_MAX_AGE_DAYS"), 7),
                   retention_days=_int(e.get("QUEUE_RETENTION_DAYS"), 14),
                   max_pending=_int(e.get("QUEUE_MAX_PENDING"), 2000),
                   storage_limit_mb=_int(e.get("REMOTE_STORAGE_LIMIT_MB"), 256),
                   prefix=e.get("REMOTE_CACHE_PREFIX", "yt:"))


def job_id(video_id: str, language: str) -> str:
    return f"{video_id}:{language}"


def split_job_id(jid: str) -> tuple:
    vid, lang = jid.split(":", 1)
    return vid, lang


def kind_for(error_class: str) -> str:
    if error_class in DEFINITIVE_CLASSES:
        return "definitive"
    if error_class == "blocked":
        return "blocked"
    if error_class == "interrupted":
        return "interrupted"
    return "transient"


def sanitize_message(msg: str) -> str:
    return re.sub(r"[|\r\n\t\x00-\x1f]", " ", str(msg or ""))[:160]


# ─────────────────────────── Lua (Redis 5.1 dialect) ───────────────────────────
_PRELUDE = """
local function split(s)
  local t, i = {}, 1
  while true do
    local j = string.find(s, '|', i, true)
    if not j then t[#t+1] = string.sub(s, i); break end
    t[#t+1] = string.sub(s, i, j - 1); i = j + 1
  end
  return t
end
local function join(t) return table.concat(t, '|') end
local function cnt(f, d) redis.call('HINCRBY', KEYS[4], f, d) end
local function move(old, new)
  if old then cnt('n:' .. old, -1) end
  if new then cnt('n:' .. new, 1) end
end
local function terminal(jid, t, cls, now, msg)
  local old = t[1]
  t[1] = 'failed'; t[7] = tostring(now); t[8] = ''; t[9] = '0'; t[10] = cls; t[11] = msg or ''
  redis.call('HSET', KEYS[1], jid, join(t))
  redis.call('ZREM', KEYS[2], jid); redis.call('ZREM', KEYS[3], jid)
  redis.call('ZADD', KEYS[5], now, jid)
  move(old, 'failed'); cnt('failed_total', 1)
end
"""

ENQUEUE = _PRELUDE + """
local jid, video, lang, now, force, maxpending = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5], tonumber(ARGV[6])
local cur = redis.call('HGET', KEYS[1], jid)
local old = nil
if cur then
  local t = split(cur); old = t[1]
  if old == 'completed' then cnt('dup_enqueues', 1); return {'completed'} end
  if old == 'pending' or old == 'deferred' or old == 'processing' then cnt('dup_enqueues', 1); return {'exists', old} end
  if old == 'failed' and force ~= '1' then return {'failed', t[10]} end
end
if redis.call('ZCARD', KEYS[2]) >= maxpending then cnt('rejected_full', 1); return {'full'} end
if old == 'failed' then redis.call('ZREM', KEYS[5], jid) end
redis.call('HSET', KEYS[1], jid, join({'pending', video, lang, '0', '0', tostring(now), tostring(now), '', '0', '', '', tostring(now)}))
redis.call('ZADD', KEYS[2], now, jid)
move(old, 'pending'); cnt('created', 1)
return {'created'}
"""

CLAIM = _PRELUDE + """
local now, lease, worker = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]
local maxjobs, maxatt, base, maxage = tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7])
local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 50)
for _, jid in ipairs(expired) do
  redis.call('ZREM', KEYS[3], jid)
  local rec = redis.call('HGET', KEYS[1], jid)
  if rec then
    local t = split(rec)
    if t[1] == 'processing' then
      cnt('lease_recoveries', 1)
      local att = tonumber(t[4])
      if att >= maxatt then terminal(jid, t, 'lease_expired_max_attempts', now, '')
      elseif now - tonumber(t[6]) > maxage then terminal(jid, t, 'expired', now, '')
      else
        local nb = now + math.min(base * 2 ^ (att - 1), 3600)
        t[1] = 'pending'; t[7] = tostring(now); t[8] = ''; t[9] = '0'; t[12] = tostring(nb)
        redis.call('HSET', KEYS[1], jid, join(t)); redis.call('ZADD', KEYS[2], nb, jid); move('processing', 'pending')
      end
    end
  end
end
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, maxjobs + 100)
local out = {}
for _, jid in ipairs(ids) do
  if #out >= maxjobs then break end
  redis.call('ZREM', KEYS[2], jid)
  local rec = redis.call('HGET', KEYS[1], jid)
  if rec then
    local t = split(rec)
    if now - tonumber(t[6]) > maxage then terminal(jid, t, 'expired', now, '')
    else
      local old = t[1]
      t[1] = 'processing'; t[4] = tostring(tonumber(t[4]) + 1); t[7] = tostring(now); t[8] = worker
      t[9] = tostring(now + lease); t[12] = '0'
      redis.call('HSET', KEYS[1], jid, join(t)); redis.call('ZADD', KEYS[3], now + lease, jid)
      move(old, 'processing'); cnt('claims', 1)
      out[#out + 1] = {jid, t[2], t[3], t[4], t[9]}
    end
  end
end
return out
"""

RENEW = _PRELUDE + """
local jid, worker, now, lease = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
local rec = redis.call('HGET', KEYS[1], jid)
if not rec then return {'unknown'} end
local t = split(rec)
if t[1] ~= 'processing' or t[8] ~= worker then return {'lost', t[1]} end
t[9] = tostring(now + lease); t[7] = tostring(now)
redis.call('HSET', KEYS[1], jid, join(t)); redis.call('ZADD', KEYS[3], now + lease, jid)
cnt('renewals', 1)
return {'renewed', t[9]}
"""

COMPLETE = _PRELUDE + """
local jid, worker, now = ARGV[1], ARGV[2], tonumber(ARGV[3])
local rec = redis.call('HGET', KEYS[1], jid)
if not rec then return {'unknown'} end
local t = split(rec)
local old = t[1]
if old == 'completed' then cnt('dup_completions', 1); return {'already_completed'} end
local late = (old ~= 'processing' or t[8] ~= worker)
t[1] = 'completed'; t[7] = tostring(now); t[8] = worker; t[9] = '0'; t[10] = ''; t[11] = ''
redis.call('HSET', KEYS[1], jid, join(t))
redis.call('ZREM', KEYS[2], jid); redis.call('ZREM', KEYS[3], jid); redis.call('ZADD', KEYS[5], now, jid)
move(old, 'completed'); cnt('completed_total', 1)
if late then cnt('late_completions', 1); return {'completed_late'} end
return {'completed'}
"""

FAIL = _PRELUDE + """
local jid, worker, now, kind, cls, msg = ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4], ARGV[5], ARGV[6]
local maxatt, dbase, dmax, maxage = tonumber(ARGV[7]), tonumber(ARGV[8]), tonumber(ARGV[9]), tonumber(ARGV[10])
local rbase = tonumber(ARGV[11])
local rec = redis.call('HGET', KEYS[1], jid)
if not rec then return {'unknown'} end
local t = split(rec)
if t[1] ~= 'processing' or t[8] ~= worker then return {'stale', t[1]} end
redis.call('ZREM', KEYS[3], jid)
local att = tonumber(t[4])
t[7] = tostring(now); t[8] = ''; t[9] = '0'; t[10] = cls; t[11] = msg
local expired = (now - tonumber(t[6])) > maxage
if kind == 'definitive' then terminal(jid, t, cls, now, msg); return {'failed'} end
if expired then terminal(jid, t, 'expired', now, msg); return {'failed'} end
local nb
if kind == 'interrupted' then
  t[4] = tostring(math.max(att - 1, 0)); nb = now; t[1] = 'pending'; cnt('interruptions', 1)
elseif kind == 'blocked' then
  local d = tonumber(t[5]) + 1
  t[5] = tostring(d); t[4] = tostring(math.max(att - 1, 0))
  nb = now + math.min(dbase * 2 ^ (d - 1), dmax); t[1] = 'deferred'; cnt('blocked_deferrals', 1)
else
  if att >= maxatt then terminal(jid, t, 'max_attempts:' .. cls, now, msg); return {'failed'} end
  nb = now + math.min(rbase * 2 ^ (att - 1), 3600); t[1] = 'pending'; cnt('transient_retries', 1)
end
t[12] = tostring(nb)
redis.call('HSET', KEYS[1], jid, join(t)); redis.call('ZADD', KEYS[2], nb, jid)
move('processing', t[1])
return {t[1], tostring(nb)}
"""

PRUNE = _PRELUDE + """
local now, cutoff, limit = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local old = redis.call('ZRANGEBYSCORE', KEYS[5], '-inf', cutoff, 'LIMIT', 0, limit)
for _, jid in ipairs(old) do
  local rec = redis.call('HGET', KEYS[1], jid)
  if rec then move(split(rec)[1], nil) end
  redis.call('HDEL', KEYS[1], jid); redis.call('ZREM', KEYS[5], jid)
end
return #old
"""


def _parse_record(raw: Optional[str]) -> Optional[dict]:
    if not raw:
        return None
    t = raw.split("|")
    if len(t) < 12:
        return None
    return {"status": t[0], "video_id": t[1], "language": t[2], "attempts": int(t[3]), "defers": int(t[4]),
            "created": int(float(t[5])), "updated": int(float(t[6])), "owner": t[7] or None,
            "lease_until": int(float(t[8])) or None, "fail_class": t[9] or None, "message": t[10] or None,
            "not_before": int(float(t[11]))}


class JobQueue:
    def __init__(self, remote: RemoteStore, cfg: Optional[QueueConfig] = None, clock: Callable[[], float] = time.time):
        self.remote, self.cfg, self._clock = remote, cfg or QueueConfig(), clock
        p = (cfg.prefix if cfg else "yt:") + "q:"
        self.keys = [p + "jobs", p + "pending", p + "leases", p + "stats", p + "done"]
        self._last_prune = 0.0

    # ── plumbing ──
    def _eval_cmd(self, script: str, args: list) -> list:
        return ["EVAL", script, len(self.keys), *self.keys, *[str(a) for a in args]]

    async def _run(self, commands: list) -> list:
        try:
            return await self.remote.exec(commands)
        except RemoteUnavailable as e:
            raise QueueUnavailable(str(e)) from None
        except RemoteCommandError as e:
            log.error("queue script error: %s", e)
            raise QueueUnavailable("script error") from None

    def _now(self) -> int:
        return int(self._clock())

    # ── producer side (Render MCP tools) ──
    async def enqueue_many(self, items: list, force: bool = False) -> list:
        """items: [(video_id, language)] -> [{'state': ..}] in the same order. Raises QueueUnavailable BEFORE any
        acknowledgement if the store is unreachable."""
        now = self._now()
        cmds = [self._eval_cmd(ENQUEUE, [job_id(v, l), v, l, now, "1" if force else "0", self.cfg.max_pending]) for v, l in items]
        res = await self._run(cmds)
        out = []
        for (v, l), r in zip(items, res):
            state = r[0]
            out.append({"video_id": v, "language": l, "job_id": job_id(v, l), "state": state,
                        **({"existing_status": r[1]} if state in ("exists", "failed") and len(r) > 1 else {})})
        return out

    async def enqueue(self, video_id: str, language: str, force: bool = False) -> dict:
        return (await self.enqueue_many([(video_id, language)], force))[0]

    # ── worker side (Render /worker endpoints) ──
    def clamp_lease(self, seconds: Optional[int]) -> int:
        s = seconds or self.cfg.lease_default
        return max(self.cfg.lease_min, min(self.cfg.lease_max, int(s)))

    async def claim(self, worker: str, max_jobs: int = 1, lease_seconds: Optional[int] = None) -> list:
        now = self._now()
        n = max(1, min(self.cfg.claim_max, int(max_jobs)))
        res = await self._run([self._eval_cmd(CLAIM, [now, self.clamp_lease(lease_seconds), worker, n, self.cfg.max_attempts,
                                                      self.cfg.requeue_base, self.cfg.max_age_days * 86400])])
        return [{"job_id": j[0], "video_id": j[1], "language": j[2], "attempt": int(j[3]), "lease_until": int(float(j[4]))}
                for j in (res[0] or [])]

    async def renew(self, jid: str, worker: str, lease_seconds: Optional[int] = None) -> dict:
        r = (await self._run([self._eval_cmd(RENEW, [jid, worker, self._now(), self.clamp_lease(lease_seconds)])]))[0]
        return {"renewed": r[0] == "renewed", "state": r[0], **({"lease_until": int(float(r[1]))} if r[0] == "renewed" else {})}

    async def complete(self, jid: str, worker: str) -> str:
        return (await self._run([self._eval_cmd(COMPLETE, [jid, worker, self._now()])]))[0][0]

    async def fail(self, jid: str, worker: str, error_class: str, message: str = "") -> dict:
        if not ERROR_CLASS_RE.match(error_class or ""):
            error_class = "unknown_error"
        c = self.cfg
        r = (await self._run([self._eval_cmd(FAIL, [jid, worker, self._now(), kind_for(error_class), error_class,
                                                    sanitize_message(message), c.max_attempts, c.defer_base, c.defer_max,
                                                    c.max_age_days * 86400, c.requeue_base])]))[0]
        return {"state": r[0], **({"retry_at": int(float(r[1]))} if len(r) > 1 and r[0] in ("pending", "deferred") else {})}

    async def prune(self) -> int:
        now = self._now()
        self._last_prune = self._clock()
        cutoff = now - self.cfg.retention_days * 86400
        return int((await self._run([self._eval_cmd(PRUNE, [now, cutoff, 100])]))[0])

    async def maybe_prune(self):
        if self._clock() - self._last_prune > 6 * 3600:
            try:
                await self.prune()
            except QueueUnavailable:
                pass

    # ── reporting ──
    async def get_many(self, jids: list) -> dict:
        if not jids:
            return {}
        res = (await self._run([["HMGET", self.keys[0], *jids]]))[0]
        return {j: _parse_record(r) for j, r in zip(jids, res)}

    async def summary(self) -> dict:
        now = self._now()
        z, ready, lease, stats, oldest, done = await self._run([
            ["ZCARD", self.keys[1]], ["ZCOUNT", self.keys[1], "-inf", now], ["ZCARD", self.keys[2]],
            ["HGETALL", self.keys[3]], ["ZRANGE", self.keys[1], 0, 0, "WITHSCORES"], ["ZCARD", self.keys[4]]])
        st = dict(zip(stats[0::2], stats[1::2])) if isinstance(stats, list) else dict(stats or {})
        n = lambda k: max(int(st.get(k, 0)), 0)
        oldest_age = None
        if isinstance(oldest, list) and len(oldest) == 2:
            oldest_age = max(0, now - int(float(oldest[1])))
        return {"pending_total": int(z), "ready_now": int(ready), "deferred_or_waiting": int(z) - int(ready),
                "processing": int(lease), "completed_retained": n("n:completed"), "failed_retained": n("n:failed"),
                "oldest_pending_age_seconds": oldest_age, "records_retained": int(done),
                "counters": {k: int(v) for k, v in st.items() if not k.startswith("n:")}}
