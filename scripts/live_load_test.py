"""BOUNDED live test of the transcript engine through the real MCP endpoint of a TEST deployment.
Uses only the free path (the test service must have no provider keys / ENABLE_PAID_TRANSCRIPT_APIS). Not run by CI.

    python scripts/live_load_test.py --url https://yt-probe-test.onrender.com --ids ids.txt --phase 1
    (phase 1: 5 videos @10s | phase 2: 25 videos @10s | phase 3: 100 videos @30s; each then re-asks the first 5 to
     verify caching.)  Token, if the service has MCP_AUTH_TOKEN, is read from the environment variable MCP_AUTH_TOKEN.

Safety stops (hard): 3 consecutive `blocked`, 3 `rate_limited`, any provider_* status (a paid path should be impossible),
or more requests than the phase allows. Prints statuses/latencies only - no transcript text, no secrets.
"""
import argparse
import asyncio
import json
import os
import statistics
import sys
import time

import httpx

PHASES = {1: (5, 10.0), 2: (25, 10.0), 3: (100, 30.0)}
STOP_STATUSES = ("provider_quota_exhausted", "provider_rate_limited", "provider_auth_failed", "provider_no_result")


async def call(client, base, vid, headers):
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "yt_get_video_transcript", "arguments": {"video_id": vid, "language": "en", "any_language": True}}}
    t0 = time.time()
    r = await client.post(base + "/mcp", json=body, headers=headers, timeout=120)
    dt = time.time() - t0
    res = json.loads(r.json()["result"]["content"][0]["text"])
    return res, dt


async def run(client, base, ids, phase, sleep=asyncio.sleep, headers=None):
    n, interval = PHASES[phase]
    ids = ids[:n]
    rows, stop = [], None
    consecutive_blocked = rate_limited = 0
    for i, vid in enumerate(ids + ids[:5]):                    # second pass over the first 5 = cache check
        res, dt = await call(client, base, vid, headers or {})
        st = res.get("status")
        rows.append({"i": i, "status": st, "seconds": round(dt, 1), "cached": res.get("cached", False), "source": res.get("source"),
                     "chars": len(res.get("full_transcript", ""))})
        consecutive_blocked = consecutive_blocked + 1 if st == "blocked" else 0
        rate_limited += st == "rate_limited"
        if consecutive_blocked >= 3:
            stop = "3 consecutive blocked"
        elif rate_limited >= 3:
            stop = "3 rate_limited"
        elif st in STOP_STATUSES:
            stop = f"unexpected provider status {st}"
        if stop:
            break
        if i < len(ids) - 1 + 5:
            await sleep(interval if i < len(ids) - 1 else 1.0)
    first = [r for r in rows if r["i"] < len(ids)]
    second = [r for r in rows if r["i"] >= len(ids)]
    lat = sorted(r["seconds"] for r in first if not r["cached"]) or [0]
    health = None
    try:
        h = await client.get(base + "/health/transcripts", headers=headers or {}, timeout=30)
        d = h.json()
        health = {k: d.get(k) for k in ("alerts", "outcomes_since_start", "counters", "block_streaks")}
    except Exception as e:
        health = {"error": type(e).__name__}
    return {"phase": phase, "requested": len(ids), "stopped_early": stop,
            "ok_pct": round(100 * sum(r["status"] == "ok" for r in first) / max(len(first), 1), 1),
            "statuses": {s: sum(r["status"] == s for r in rows) for s in {r["status"] for r in rows}},
            "latency_s_p50_p95_uncached": [statistics.median(lat), lat[int(0.95 * (len(lat) - 1))]],
            "second_pass_cached": f"{sum(r['cached'] for r in second)}/{len(second)}", "server_health": health, "rows": rows}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True); ap.add_argument("--ids", required=True); ap.add_argument("--phase", type=int, choices=(1, 2, 3), required=True)
    a = ap.parse_args(argv)
    ids = [l.strip() for l in open(a.ids) if l.strip() and not l.startswith("#")]
    need = PHASES[a.phase][0]
    if len(set(ids)) < need:
        print(f"need at least {need} distinct video ids in {a.ids}", file=sys.stderr); return 2
    tok = os.environ.get("MCP_AUTH_TOKEN", "")
    headers = {"Authorization": f"Bearer {tok}"} if tok else {}

    async def go():
        async with httpx.AsyncClient() as c:
            return await run(c, a.url.rstrip("/"), list(dict.fromkeys(ids)), a.phase, headers=headers)
    out = asyncio.run(go())
    print(json.dumps(out, indent=1))
    return 0 if out["ok_pct"] >= 90 and not out["stopped_early"] else 1


if __name__ == "__main__":
    sys.exit(main())
