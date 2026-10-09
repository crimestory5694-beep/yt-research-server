"""OFFLINE capacity/reliability simulation of the transcript engine. No network, no real YouTube, no provider credits.

    python scripts/capacity_sim.py [--quick]        # prints JSON; ~1-2 minutes

What is real: our TranscriptService code (cache, dedup, rate limiter, cooldowns, caps, queue, retries, compression,
SQLite), CPU cost and memory of that code on this machine, byte sizes of cached entries.
What is simulated: YouTube / providers (fake library with scripted outcomes and ~2 ms latency), Upstash (in-memory fake),
the clock (days pass instantly). Numbers therefore describe OUR code's behaviour and footprint, NOT Render's network,
YouTube's blocking decisions, or Render Free's real CPU speed (0.1 vCPU is much slower than this machine).
"""
import asyncio
import glob
import json
import os
import random
import resource
import sys
import time
import tracemalloc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import transcript_service as ts  # noqa: E402
from tests.helpers import Clock, FakeUpstash, make_remote  # noqa: E402

_WORDS = None


def words():
    global _WORDS
    if _WORDS is None:
        txt = " ".join(open(p, errors="ignore").read() for p in sorted(glob.glob("/usr/share/common-licenses/*")) if os.path.isfile(p))
        _WORDS = [w.lower() for w in txt.split() if w.isascii()] or ["lorem", "ipsum"]
    return _WORDS


def make_data(minutes: int, rng: random.Random, code="en") -> dict:
    """~150 spoken words/minute in ~8-word caption segments (typical of YouTube auto captions)."""
    w = words()
    start = rng.randrange(len(w) - 100)
    n = minutes * 150
    toks = [w[(start + i) % len(w)] for i in range(n)]
    segs = [{"text": " ".join(toks[i:i + 8]), "start": round(i / 2.5, 1), "duration": 3.0} for i in range(0, n, 8)]
    return {"language_code": code, "caption_type": "auto_generated",
            "available_languages": [{"code": code, "name": code, "auto_generated": True}],
            "segments": segs, "full_transcript": " ".join(s["text"] for s in segs)}


def pick_minutes(rng):
    r = rng.random()
    return 10 if r < .2 else 25 if r < .7 else 45 if r < .95 else 90


class SimLib:
    """Fake youtube-transcript-api. behavior(video_id, call_no) -> 'ok' | exception instance."""

    def __init__(self, behavior=None, latency=0.002, seed=1):
        self.behavior, self.latency, self.calls, self.per_video, self.cpu = behavior, latency, 0, {}, 0.0
        self.rng = random.Random(seed)
        self.pool = {m: make_data(m, self.rng) for m in (10, 25, 45, 90)}

    def __call__(self, video_id, languages, any_language, proxy, timeout, secrets=()):
        c0 = time.process_time()
        try:
            return self._call(video_id)
        finally:
            self.cpu += time.process_time() - c0      # harness cost (copying fake payloads) is subtracted from CPU figures

    def _call(self, video_id):
        self.calls += 1
        n = self.per_video[video_id] = self.per_video.get(video_id, 0) + 1
        time.sleep(self.latency)
        out = self.behavior(video_id, n) if self.behavior else "ok"
        if isinstance(out, Exception):
            raise out
        m = (sum(map(ord, video_id)) * 7919) % 100
        mins = 10 if m < 20 else 25 if m < 70 else 45 if m < 95 else 90
        d = self.pool[mins]
        return {**d, "segments": [dict(s) for s in d["segments"]]}


def vid(i: int) -> str:
    return f"v{i:010d}"[-11:]


def E(status, msg="sim"):
    return ts.TranscriptError(status, msg)


def build(env=None, lib=None, clock=None, fake=None, paid=None):
    clock = clock or Clock()
    base = {"TRANSCRIPT_CACHE_PATH": ":memory:", "TRANSCRIPT_FETCHES_PER_MINUTE": "30"}
    base.update(env or {})
    cfg = ts.TranscriptConfig.from_env(base)
    svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(":memory:"), clock=clock, library_fetch=lib or SimLib(),
                               http_client_factory=paid, remote=make_remote(fake, clock) if fake else None)
    return svc, clock


def counts(results):
    c = {}
    for r in results:
        c[r["status"]] = c.get(r["status"], 0) + 1
    return c


def remote_bytes(fake):
    return sum(len(v) for k, v in fake.data.items() if ":c:" in k)


# ───────────────────────── scenarios ─────────────────────────
async def steady(per_day, days=3, repeat=2, env=None, trace=False):
    """Uniques spread evenly over each virtual day; each unique asked `repeat` times (e.g. by different channels)."""
    fake = FakeUpstash()
    lib = SimLib()
    svc, clock = build(env, lib, fake=fake)
    if trace:
        tracemalloc.start()
    cpu0, t0 = time.process_time(), time.time()
    res, step = [], 86400 / (per_day * repeat)
    for d in range(days):
        ids = [vid(d * per_day + i) for i in range(per_day)]
        for rep in range(repeat):
            for i in ids:
                clock.advance(step)
                res.append(await svc.get_transcript(i))
    cpu, wall = time.process_time() - cpu0 - lib.cpu, time.time() - t0
    peak = 0
    if trace:
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    n = len(res)
    uniq = per_day * days
    entry = remote_bytes(fake) / max(uniq, 1)
    local = svc.store._db.execute("SELECT COALESCE(SUM(LENGTH(payload)),0), COUNT(*) FROM cache").fetchone()
    return {"requests": n, "statuses": counts(res), "network_fetches": lib.calls, "cache_hit_pct": round(100 * sum(r.get("cached", False) for r in res) / n, 1),
            "cpu_ms_per_request": round(1000 * cpu / n, 2), "wall_s": round(wall, 1), "py_heap_peak_mb": round(peak / 1e6, 1),
            "remote_avg_entry_kb": round(entry / 1024, 1), "local_avg_entry_kb": round(local[0] / max(local[1], 1) / 1024, 1),
            "remote_commands": svc.remote.stats["commands"],
            "projected_remote_mb_30d": round(entry * per_day * 30 / 1e6, 1), "projected_local_mb_30d": round(local[0] / max(local[1], 1) * per_day * 30 / 1e6, 1)}


async def burst(n, env=None):
    svc, clock = build(env, SimLib())
    t0 = time.time()
    res = await asyncio.gather(*[svc.get_transcript(vid(i)) for i in range(n)])
    return {"requested": n, "statuses": counts(res), "served_pct": round(100 * sum(r["status"] == "ok" for r in res) / n, 1),
            "wall_s": round(time.time() - t0, 2), "max_wait_hint": max([r.get("retry_after_seconds", 0) for r in res] + [0])}


async def blocked_day(per_day, days=2, env=None):
    """YouTube blocks the server for the whole period; no proxy, no paid."""
    lib = SimLib(lambda v, n: E(ts.BLOCKED, "bot check"))
    svc, clock = build(env, lib)
    res, step = [], 86400 / per_day
    for d in range(days):
        for i in range(per_day):
            clock.advance(step)
            res.append(await svc.get_transcript(vid(i + d * per_day)))
    return {"requests": len(res), "statuses": counts(res), "attempts_that_reached_youtube": lib.calls,
            "per_day_to_youtube": round(lib.calls / days, 1), "sample_error": res[-1].get("error", "")[:140]}


async def transient(n, fail_rate=0.10, env=None):
    rng = random.Random(7)
    lib = SimLib(lambda v, k: E(ts.NETWORK_ERROR) if rng.random() < fail_rate else "ok")
    svc, clock = build({"TRANSCRIPT_FETCHES_PER_MINUTE": "100000", **(env or {})}, lib)
    res = []
    for i in range(n):
        clock.advance(60)
        res.append(await svc.get_transcript(vid(i)))
    return {"requests": n, "ok_pct": round(100 * sum(r["status"] == "ok" for r in res) / n, 1), "provider_calls": lib.calls, "statuses": counts(res)}


async def restarts(unique=100, restart_every=25, env=None):
    """Service object recreated (= Render restart / spin-down; local SQLite lost) while the shared store persists."""
    fake = FakeUpstash()
    lib = SimLib()
    clock = Clock()
    ids = [vid(i) for i in range(unique)]
    svc, _ = build(env, lib, clock, fake)
    for k, i in enumerate(ids):
        if k and k % restart_every == 0:
            svc, _ = build(env, lib, clock, fake)
        await svc.get_transcript(i)
    first = lib.calls
    svc, _ = build(env, lib, clock, fake)                       # one more restart, then re-request everything
    res = [await svc.get_transcript(i) for i in ids]
    return {"unique": unique, "restarts": unique // restart_every + 1, "fetches_first_pass": first,
            "extra_fetches_after_restarts": lib.calls - first, "second_pass_cached_pct": round(100 * sum(r.get("cached", False) for r in res) / unique, 1)}


async def all_free_fail_paid_capped(per_day=500, days=3, env=None):
    import httpx
    paid_calls = {"n": 0, "by_day": {}}
    clock = Clock()

    def handler(req):
        paid_calls["n"] += 1
        d = time.strftime("%Y-%m-%d", time.gmtime(clock()))
        paid_calls["by_day"][d] = paid_calls["by_day"].get(d, 0) + 1
        return httpx.Response(200, json={"content": "word " * 40})
    fake = FakeUpstash()
    lib = SimLib(lambda v, n: E(ts.UPSTREAM_ERROR, "yt down"))
    paid = lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))
    e = {"ENABLE_PAID_TRANSCRIPT_APIS": "true", "SUPADATA_API_KEY": "k_sim_1", "TRANSCRIPT_API_KEY": "k_sim_2",
         "TRANSCRIPT_FETCHES_PER_MINUTE": "100000", **(env or {})}
    svc, _ = build(e, lib, clock, fake, paid)
    res = []
    for d in range(days):
        for i in range(per_day):
            clock.advance(86400 / per_day)
            res.append(await svc.get_transcript(vid(i + d * per_day)))
    diag = await svc.diagnostics()
    return {"requests": len(res), "statuses": counts(res), "paid_http_calls": paid_calls["n"],
            "paid_cap_per_utc_day_total": 10, "paid_calls_by_utc_day": paid_calls["by_day"],
            "max_paid_calls_in_a_day": max(paid_calls["by_day"].values()), "alerts": diag.get("alerts")}


def rss_mb():
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


async def main(quick=False):
    out = {}
    for n in ((25, 100) if quick else (25, 100, 500)):
        r = await steady(n, days=2 if n >= 500 else 3)                 # untraced: honest CPU figure
        r["py_heap_peak_mb"] = (await steady(n, days=1, trace=True))["py_heap_peak_mb"]   # traced: memory only
        out[f"steady_{n}_per_day"] = r
    for n in (25, 100, 500):
        out[f"burst_{n}"] = await burst(n, {"TRANSCRIPT_QUEUE_WAIT_SECONDS": "2"})
    for n in (25, 100, 500):
        out[f"blocked_{n}_per_day"] = await blocked_day(n)
    out["transient_10pct_500"] = await transient(500)
    out["restarts"] = await restarts()
    out["all_free_fail_paid_capped"] = await all_free_fail_paid_capped(500 if not quick else 100)
    out["process_maxrss_mb"] = rss_mb()
    return out


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main("--quick" in sys.argv)), indent=1))
