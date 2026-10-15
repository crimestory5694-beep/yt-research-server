# Backlog capacity with the shipped worker defaults (offline simulation)

Defaults (unchanged): 20 extractions/hour, 120/day, 20-45 s pacing, jobs expire after 7 days, oldest-first.
**REAL** in the simulation: worker, API, Lua queue, caps, expiry. **SIMULATED**: YouTube (always succeeds unless a scenario blocks it;
8 s per extraction), Upstash (Redis emulator), the clock. "Capacity estimate" = arithmetic (`min(120, 20 x online hours)`);
"measured" = transcripts the simulation actually completed per day (last 4 days of a 12-day run). **No real transcript was fetched.**
Reproduce: `python scripts/backlog_sim.py` (about 2.5 minutes, prints JSON lines).

## Steady state (12 days, arrivals spread over the day, laptop online N contiguous hours/day)
| New jobs/day | Online h/day | Capacity estimate/day | Measured completed/day | Pending at day 12 | Oldest pending | Expired | Sustainable? |
|---|---|---|---|---|---|---|---|
| 50 | 1 | 20 | 20 | 331 | 6.5 d | 29 (more coming) | **No** |
| 50 | 2 | 40 | 40 | 120 (+10/day) | 2.3 d | 0 so far | **No** (backlog grows 10/day; expiry after ~day 20) |
| 50 | 3 | 60 | 50 | 0 | 0 | 0 | **Yes** (20 % headroom) |
| 50 | 6 | 120 | 50 | 0 | 0 | 0 | **Yes** (large headroom) |
| 100 | 1 | 20 | 20 | 681 | 6.8 d | 279 | **No** |
| 100 | 2 | 40 | 40 | 661 | 6.6 d | 59 | **No** |
| 100 | 3 | 60 | 60 | 480 | 4.9 d | 0 so far (expiry imminent) | **No** |
| 100 | 6 | 120 | 100 | 0 | 0 | 0 | **Yes**, but only 20 % headroom |

**Minimum uptime:** 50/day needs >= 2.5 h of worker time per day (plan 3 h); 100/day needs >= 5 h (plan 6 h). The daily cap (120) is the
absolute ceiling; 100/day uses 83 % of it, so 100/day is sustainable only when the laptop is on ~6 h EVERY day.

## Laptop offline 3 days, new jobs keep arriving (14 days)
| New jobs/day | Online h/day after return | Backlog after day 3 | Peak oldest age | Backlog by day 14 | Expired | Completed / arrived | Recovery |
|---|---|---|---|---|---|---|---|
| 50 | 3 | 150 | 2.9 d | 40 (draining 10/day) | 0 | 660 / 700 | backlog <= 1 day of arrivals on day 13; fully clear ~day 18 |
| 50 | 6 | 150 | 2.8 d | 0 | 0 | 700 / 700 | fully clear on day 6 (3 days after the laptop returns) |
| 100 | 3 | 300 | 6.4 d | 641 (growing 40/day) | **99** | 660 / 1400 | **never** at 3 h/day |
| 100 | 6 | 300 | 3.0 d | 80 (draining 20/day) | 0 | 1320 / 1400 | fully clear ~day 18 (about 2 weeks after return) |

## YouTube blocking the worker for several days (50/day, 6 h online, 16 days)
| Blocked days | Extraction attempts on blocked days (no hammering) | Peak backlog | Peak oldest age | Expired | Recovery |
|---|---|---|---|---|---|
| 4-5 (2 days) | 5, 1 | 100 | 2.0 d | 0 | backlog cleared on day 7; 800 / 800 completed |
| 4-7 (4 days) | 5, 1, 1, 1 | 200 | 4.0 d | 0 | cleared on day 10; 800 / 800 completed |
(The first blocked day shows 5 probes because the worker pauses 15 -> 30 -> 60 -> 120 -> 240 min; later days probe once. Jobs are
deferred 30 min doubling to 6 h and attempts are not consumed by blocks. A block longer than the 7-day job age would expire jobs - raise
`QUEUE_JOB_MAX_AGE_DAYS` if you expect that.)

## Reading these numbers honestly
* They show the QUEUE and PACING behave as designed. Whether YouTube lets your home IP fetch 20/hour is unknown until the live pilot;
  if it blocks sooner, real capacity is lower than every "measured" figure above.
* Real extraction can take longer than 8 s, but at 20/hour the hourly cap (not extraction time) is the limit.
* Defaults were NOT raised and retries were NOT made more aggressive to make the table look better. If 100/day with 20 % headroom is
  too tight, the options are more online hours (best), a later and evidence-based increase of `HOURLY_CAP` (never above what the pilot
  shows is tolerated), or accepting that some requests wait/expire. Expired jobs are listed and re-opened deliberately (see WORKER_AND_QUEUE.md).
