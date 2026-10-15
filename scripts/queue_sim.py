"""OFFLINE end-to-end simulation of the hybrid queue (worker <-> Render app <-> Lua queue on a Redis emulator).

    python scripts/queue_sim.py            # bounded: ~10-30 s, prints JSON
What is REAL: our worker, API, security middleware, Lua scripts (executed by a Redis emulator), compression, byte sizes.
What is SIMULATED: YouTube (scripted), Upstash itself (fakeredis behind an Upstash-shaped REST fake), Render (in-process),
time (virtual clock). Command counts are therefore exact for OUR code; they assume Upstash bills each command run inside a
Lua script separately (worst case - Upstash's billing page does not say).
"""
import json
import sys
import threading
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pytest  # noqa: E402

from tests.queue_harness import E, Extractor, Stack, VID, main, make_result, run_until_idle  # noqa: E402
import asyncio  # noqa: E402


def _enqueue(ids, lang="en"):
    return asyncio.run(main.tool_queue_transcripts(ids, lang))


def _summary():
    return asyncio.run(main.get_transcript_service().queue.summary())


def _cmds(stack):
    f = stack.fake
    return {"http": f.http_requests, "top": f.top_level, "inner": f.inner, "evals": f.eval_calls, "worst_case_billed": f.top_level + f.inner}


def make_stack(tmp, **kw):
    mp = pytest.MonkeyPatch()
    st = Stack(mp, **kw)
    st.fake._install_counter()
    st.dir = str(tmp)
    st._mp = mp
    return st


def close(st):
    st.fake._restore(); st._mp.undo()


def worker_for(st, ex, **kw):
    s = st.settings(state_dir=st.dir, **kw)
    return st.worker(s, ex), s


def drain(st, w, hours=30):
    """Let the worker work for up to `hours` of virtual time; returns outcome counts."""
    seen = {}
    end = st.clock() + hours * 3600
    while st.clock() < end:
        out = run_until_idle(w)
        for o in out:
            seen[o] = seen.get(o, 0) + 1
        if _summary()["pending_total"] == 0 and _summary()["processing"] == 0:
            break
        st.clock.advance(300)
    return seen


def steady(tmp, per_day, days=3, minutes=25):
    st = make_stack(tmp)
    ex = Extractor(minutes=minutes)
    w, _ = worker_for(st, ex)
    daily = []
    for d in range(days):
        ids = [VID(d * per_day + i) for i in range(per_day)]
        _enqueue(ids)
        t0 = st.clock()
        drain(st, w)
        s = _summary()
        daily.append({"enqueued": per_day, "pending_after": s["pending_total"], "virtual_hours_to_finish": round((st.clock() - t0) / 3600, 1)})
        st.clock.advance(86400 - (st.clock() - t0) % 86400)
    n = per_day * days
    c = _cmds(st)
    s = _summary()
    out = {"transcripts": n, "completed": s["completed_retained"], "failed": s["failed_retained"], "daily": daily,
           "extractions": ex.total, "paid_calls": st.paid_calls, "paid_usage_keys": len(st.fake.keys("yt:u:*")),
           "commands": c, "commands_per_transcript_worst_case": round(c["worst_case_billed"] / n, 1),
           "commands_per_transcript_top_level": round(c["top"] / n, 1),
           "cache_bytes_per_transcript": round(st.fake.used_bytes_cache() / n), "queue_record_bytes": len(st.fake.redis.hget("yt:q:jobs", f"{VID(0)}:en") or "")}
    close(st)
    return out


def idle_poll_cost(tmp):
    st = make_stack(tmp)
    w, _ = worker_for(st, Extractor())
    b = _cmds(st)["worst_case_billed"]
    w.run_cycle()
    a = _cmds(st)["worst_case_billed"]
    close(st)
    return {"render_idle_claim_commands": a - b}


def offline_then_catch_up(tmp, per_day, offline_days=3):
    st = make_stack(tmp)
    ex = Extractor()
    peak = 0
    for d in range(offline_days):                       # laptop is OFF: jobs just accumulate durably
        _enqueue([VID(d * per_day + i) for i in range(per_day)])
        peak = max(peak, _summary()["pending_total"])
        st.clock.advance(86400)
    st.restart_render()                                  # Render also restarted/slept meanwhile
    w, _ = worker_for(st, ex)
    day_end = []
    for d in range(3):
        drain(st, w, hours=24)
        day_end.append(_summary()["pending_total"])
        st.clock.advance(86400 - 0)
    s = _summary()
    out = {"enqueued": per_day * offline_days, "peak_backlog": peak, "pending_after_each_online_day": day_end,
           "completed": s["completed_retained"], "failed": s["failed_retained"], "expired": s["counters"].get("failed_total", 0),
           "paid_calls": st.paid_calls}
    close(st)
    return out


def concurrent_workers(tmp, jobs=60, workers=4):
    st = make_stack(tmp)
    _enqueue([VID(i) for i in range(jobs)])
    ex = Extractor()
    ws = []
    for i in range(workers):
        d = os.path.join(str(tmp), f"w{i}")
        s = st.settings(state_dir=d, worker_id=f"laptop-{i}", hourly_cap=1000, daily_cap=1000)
        ws.append(st.worker(s, ex))
    ths = [threading.Thread(target=lambda w=w: [w.run_cycle() for _ in range(40)]) for w in ws]
    [t.start() for t in ths]; [t.join() for t in ths]
    s = _summary()
    out = {"jobs": jobs, "workers": workers, "completed": s["completed_retained"], "extractions": ex.total,
           "max_extractions_of_one_video": max(ex.calls.values()), "claims": s["counters"].get("claims"), "paid_calls": st.paid_calls}
    close(st)
    return out


def large_transcripts(tmp, minutes=180, n=5):
    st = make_stack(tmp)
    _enqueue([VID(i) for i in range(n)])
    ex = Extractor(minutes=minutes)
    w, _ = worker_for(st, ex)
    drain(st, w)
    r = asyncio.run(main.tool_get_video_transcript(VID(0), "en"))
    out = {"minutes": minutes, "completed": _summary()["completed_retained"], "chars": len(r["full_transcript"]),
           "segments_returned": len(r["segments"]), "total_segments": r["total_segments"],
           "cache_bytes_each": round(st.fake.used_bytes_cache() / n)}
    close(st)
    return out


def capacity_projection(per_transcript_cmds_worst, per_transcript_bytes, idle_claim_cmds):
    """Monthly Upstash usage. Free tier (as reported on Upstash's pricing page): 500,000 commands/month, 256 MB."""
    rows = {}
    for monthly in (1500, 3000):
        for polls_per_day, label in ((0, "doorbell (no Render polling)"), (48, "Render polled every 30 min, laptop on 24h"),
                                     (12, "Render polled every 30 min, laptop on 6h/day")):
            idle = 30 * polls_per_day * idle_claim_cmds
            doorbell = 30 * (48 if polls_per_day == 0 else 0)          # 1 pipeline of 2 commands per poll
            reads_and_status = monthly * 3 * 2 + 30 * 20 * 7        # each transcript read ~3x (2 cmds); ~20 status checks/day (7 cmds)
            cmds = monthly * per_transcript_cmds_worst + idle + doorbell * 2 + reads_and_status
            rows[f"{monthly}/month | {label}"] = {"commands": int(cmds), "pct_of_500k": round(100 * cmds / 500_000, 1)}
    # status polling by the user / MCP client: 20 polls per day x 7 commands
    store = {}
    for monthly in (1500, 3000):
        live = monthly * per_transcript_bytes                           # 30-day cache TTL keeps ~1 month live
        store[f"{monthly}/month"] = {"live_cache_MB": round(live / 1e6, 1), "pct_of_256MB": round(100 * live / 256e6, 1),
                                     "plus_backlog_and_metadata_MB": round((live + 2000 * 600 + monthly * 400) / 1e6, 1)}
    return {"commands": rows, "storage": store}


def main_cli():
    import tempfile
    out = {}
    with tempfile.TemporaryDirectory() as d:
        out["steady_50_per_day_x3"] = steady(os.path.join(d, "a"), 50)
        out["steady_100_per_day_x3"] = steady(os.path.join(d, "b"), 100)
        out["idle_poll"] = idle_poll_cost(os.path.join(d, "c"))
        out["offline_3_days_50_per_day"] = offline_then_catch_up(os.path.join(d, "e"), 50)
        out["offline_3_days_100_per_day"] = offline_then_catch_up(os.path.join(d, "f"), 100)
        out["concurrent_4_workers_60_jobs"] = concurrent_workers(os.path.join(d, "g"))
        out["large_3h_transcripts"] = large_transcripts(os.path.join(d, "h"))
        s = out["steady_100_per_day_x3"]
        out["projection"] = capacity_projection(s["commands_per_transcript_worst_case"], s["cache_bytes_per_transcript"],
                                                out["idle_poll"]["render_idle_claim_commands"])
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main_cli()
