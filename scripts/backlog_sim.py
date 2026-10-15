"""OFFLINE backlog / uptime simulation with the REAL worker defaults (20 extractions/hour, 120/day, 20-45 s pacing,
jobs expire after 7 days). Real: worker, API, Lua queue, caps, expiry. SIMULATED: YouTube (scripted, ~8 s per extraction),
Upstash (Redis emulator), time (virtual).  "capacity_estimate" is arithmetic; "completed" is what the simulation achieved.

    python scripts/backlog_sim.py            # bounded; prints one JSON line per scenario (about 1-3 minutes)
Nothing here raises extraction rates or retries; caps are the shipped defaults.
"""
import json
import os
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import queue_sim as qs  # noqa: E402
from tests.queue_harness import E, Extractor, VID, main  # noqa: E402

DEFAULTS = dict(min_delay=20, max_delay=45, hourly_cap=20, daily_cap=120, poll_min=300, poll_max=1800)
DAY = 86400


def online_window(st, w, hours):
    end = st.clock() + hours * 3600
    while st.clock() < end:
        r = w.run_cycle()
        if r == "worked":
            continue
        gap = end - st.clock()
        if r == "idle":
            st.clock.advance(min(300, gap))
        elif r == "capped":
            st.clock.advance(min(w.state.wait_for_caps(w.s.hourly_cap, w.s.daily_cap) + 1, gap))
        elif r == "paused":
            st.clock.advance(min(w.state.paused_for() + 1, gap))
        else:
            st.clock.advance(min(60, gap))


def scenario(tmp, arrivals, hours, days=12, offline_days=0, block_days=(), start_online_day=0):
    st = qs.make_stack(tmp)
    blocked_now = {"v": False}
    ex = Extractor(lambda v, n: E("blocked", "bot check") if blocked_now["v"] else "ok", clock=st.clock, seconds_per_call=8)
    w, _ = qs.worker_for(st, ex, **DEFAULTS)
    rows, n_id, prev_done, prev_ext = [], 0, 0, 0
    for d in range(days):
        day0 = st.clock()
        blocked_now["v"] = d in block_days
        per_batch = [arrivals // 4 + (1 if i < arrivals % 4 else 0) for i in range(4)]
        for b in range(4):                                     # arrivals at 00:00, 06:00, 12:00, 18:00
            ids = [VID(n_id + i) for i in range(per_batch[b])]; n_id += len(ids)
            qs._enqueue(ids)
            if b < 3:
                st.clock.advance(6 * 3600)
        if d >= start_online_day and hours > 0:
            online_window(st, w, hours)
        s = qs._summary()
        rows.append({"day": d + 1, "arrived": arrivals, "completed": s["counters"].get("completed_total", 0) - prev_done,
                     "extraction_attempts": ex.total - prev_ext, "pending_end": s["pending_total"],
                     "oldest_pending_age_days": round((s["oldest_pending_age_seconds"] or 0) / DAY, 1), "expired_total": s["expired_total"]})
        prev_done, prev_ext = s["counters"].get("completed_total", 0), ex.total
        st.clock.advance(day0 + DAY - st.clock())
    s = qs._summary()
    out = {"arrivals_per_day": arrivals, "laptop_online_hours_per_day": hours, "days": days, "daily": rows,
           "total_arrived": arrivals * days, "total_completed": s["counters"].get("completed_total", 0), "total_expired": s["expired_total"],
           "pending_at_end": s["pending_total"], "paid_calls": st.paid_calls}
    qs.close(st)
    return out


def summarize(sc, capacity_per_day):
    last = sc["daily"][-4:]
    avg_done = sum(r["completed"] for r in last) / len(last)
    stable = (all(r["expired_total"] == sc["daily"][-5]["expired_total"] for r in last)
              and last[-1]["pending_end"] <= last[0]["pending_end"] + 2 and avg_done >= 0.95 * sc["arrivals_per_day"])
    return {"arrivals_per_day": sc["arrivals_per_day"], "online_hours_per_day": sc["laptop_online_hours_per_day"],
            "capacity_estimate_per_day": capacity_per_day, "measured_completed_per_day_last4": round(avg_done, 1),
            "pending_end": sc["pending_at_end"], "oldest_pending_age_days_end": last[-1]["oldest_pending_age_days"],
            "expired_total": sc["total_expired"], "completed_total": sc["total_completed"], "of_arrived": sc["total_arrived"],
            "sustainable": bool(stable)}


def emit(obj):
    print(json.dumps(obj), flush=True)


def main_cli(days=12):
    t0 = time.time()
    with tempfile.TemporaryDirectory() as d:
        i = 0
        for arrivals in (50, 100):
            for hours in (1, 2, 3, 6):
                i += 1
                sc = scenario(os.path.join(d, f"s{i}"), arrivals, hours, days=days)
                emit({"matrix": summarize(sc, min(120, 20 * hours))})
        for arrivals in (50, 100):
            for hours in (3, 6):
                i += 1
                sc = scenario(os.path.join(d, f"s{i}"), arrivals, hours, days=14, start_online_day=3)    # laptop offline days 1-3
                rows = sc["daily"]
                rec = next((r["day"] for r in rows if r["day"] > 3 and r["pending_end"] <= arrivals and r["oldest_pending_age_days"] < 1.5), None)
                emit({"offline_3_days_then_online": {"arrivals_per_day": arrivals, "online_hours": hours, "pending_after_day3": rows[2]["pending_end"],
                      "peak_oldest_age_days": max(r["oldest_pending_age_days"] for r in rows), "recovered_on_day": rec,
                      "expired_total": sc["total_expired"], "completed_total": sc["total_completed"], "of_arrived": sc["total_arrived"],
                      "pending_end": sc["pending_at_end"], "daily_pending": [r["pending_end"] for r in rows]}})
        for blk in ((3, 4), (3, 4, 5, 6)):
            i += 1
            sc = scenario(os.path.join(d, f"s{i}"), 50, 6, days=16, block_days=blk)
            rows = sc["daily"]
            emit({"youtube_blocks_days": [b + 1 for b in blk], "arrivals_per_day": 50, "online_hours": 6,
                  "extraction_attempts_on_blocked_days": [rows[b]["extraction_attempts"] for b in blk],
                  "daily_pending": [r["pending_end"] for r in rows], "daily_completed": [r["completed"] for r in rows],
                  "peak_oldest_age_days": max(r["oldest_pending_age_days"] for r in rows), "expired_total": sc["total_expired"],
                  "completed_total": sc["total_completed"], "of_arrived": sc["total_arrived"], "pending_end": sc["pending_at_end"]})
    emit({"elapsed_s": round(time.time() - t0, 1)})


if __name__ == "__main__":
    main_cli()
