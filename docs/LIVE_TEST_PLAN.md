# Bounded live test plan (free; needs the owner's approval before anything is deployed or run)

Purpose: replace simulated numbers with measurements from Render's real outbound IP, without any paid credit.

## Setup (separate throwaway service, as in DEPLOY_STEPS.md part A)
* Free web service from branch `claude/charming-babbage-mo3ags`, **same region as production**, Auto-Deploy off.
* Env: only `MCP_AUTH_TOKEN` (a throwaway random string you invent) — and optionally the Upstash pair for the persistence step.
  **Never** set `YOUTUBE_API_KEY` (not needed), `SUPADATA_API_KEY`, `TRANSCRIPT_API_KEY`, `ENABLE_PAID_TRANSCRIPT_APIS`.
  With no provider keys a paid call is impossible, so the test cannot consume credits.
* `ids.txt`: at least 100 distinct public video IDs, one per line, mixed lengths (10 / 30 / 60+ min) and a few non-English.
  Prefer videos from your own channels or the channels you research.

## Phases (run from your computer; each gate must pass before the next)
| Phase | Command (token from env var `MCP_AUTH_TOKEN`) | Requests | Pace | Gate to continue |
|---|---|---|---|---|
| 0 | free probe (`RUN_TRANSCRIPT_PROBE_ON_STARTUP`) | 4 lookups | 2 s | verdict `FREE_EXTRACTION_WORKS` |
| 1 | `python scripts/live_load_test.py --url https://<svc>.onrender.com --ids ids.txt --phase 1` | 5 + 5 cache re-asks | 10 s | ok >= 90 %, second pass 5/5 cached |
| 2 | `... --phase 2` | 25 + 5 | 10 s (~5 min) | ok >= 90 %, no `blocked` |
| 3 | `... --phase 3` | 100 + 5 | 30 s (~55 min) | ok >= 90 %; `/health/transcripts` alerts empty |
| 4 | restart check: Render Dashboard -> Manual Deploy -> "Restart"; re-run phase 1 | 5 | 10 s | Needs Upstash: 5/5 cached with 0 new fetches. Without Upstash: expect 0/5 cached (proves the ephemeral-disk limit). |

Hard stops built into the runner: 3 consecutive `blocked`, 3 `rate_limited`, any `provider_*` status, or more requests than
the phase allows. Maximum YouTube lookups in the whole plan: ~140 plus at most one retry each.

## What each phase measures
* ok %, p50/p95 latency of uncached fetches, `blocked` onset (after how many requests, if ever), cache re-ask hit rate,
  `/health/transcripts` (`alerts`, `outcomes_since_start`, `counters.retries`, `queue`, `block_streaks`).
* Render cold start: first request after >15 min idle (note the delay), and whether your MCP client times out.
* Memory: Render Dashboard -> Metrics (RSS) during phase 3; Free has 512 MB.

## Costs and permissions
* Expected cost $0 (Free instance, no provider keys). Phase 3 keeps the service awake ~1 h against the workspace's
  free instance-hours (reported 750 h/month, shared by all free services - confirm on the Render billing page).
* Needs from you: permission to create the test service (Render), a throwaway `MCP_AUTH_TOKEN`, the `ids.txt`, and
  pasting back only the runner's JSON output. Delete the service afterwards.

## Do not
* Do not run phase 3 if phases 1-2 show `blocked`; stop and decide on options instead.
* Do not point this at the production service.
