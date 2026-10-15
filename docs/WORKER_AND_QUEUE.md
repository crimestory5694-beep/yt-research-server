# Hybrid transcript system: Render coordinator + durable queue + local Windows worker

```
 MCP client ──► Render (FastAPI, free)  ──► Upstash Redis (free)  ◄── Render  ◄── Windows worker (your laptop)
   yt_queue_transcripts          queue + cache + counters         /worker/* over HTTPS, outbound only
   yt_get_video_transcript       (Lua scripts, atomic claims)     youtube-transcript-api from YOUR home IP
```
* Render never needs a background process: it only answers requests. The worker pulls jobs; nothing connects to your laptop.
* The worker has **no Upstash credentials** (Render brokers everything) and a token that can only call `/worker/*`.
* Finished transcripts are written to the same shared cache the synchronous tool reads, so `yt_get_video_transcript`
  returns them instantly (`cached: true`). No new "retrieval" tool is needed.
* Paid transcript APIs are untouched and stay **off**; nothing in the worker or queue can call them.

## Job lifecycle
`pending -> processing (leased) -> completed | failed`; `deferred` = pending with a later start time (YouTube blocked the worker).
* **Unique** by `video_id:language` (`any` = best available track); asking again returns the existing job.
* **Atomic claim** (Lua) leases a job to one worker for 60-900 s (default 300); the worker renews while extracting.
  Every claim also receives a globally unique **fencing token**. Renew/complete/fail must present `(worker_id, fence)`;
  a report from an older claim is rejected (HTTP 409 `stale_job`) and changes nothing. The transcript (or the negative
  "no captions" entry) is written to the shared cache **inside the same atomic script** that verifies ownership, so a
  stale worker can neither overwrite another worker's transcript nor poison the negative cache, and `200` means
  "durably stored AND attached to the authorized job". Negative entries use `SET NX` and never replace an existing entry.
  A retry of the same claim after a lost reply returns `already_completed` (idempotent). A completion that arrives after
  the lease expired but before anyone re-claimed the job is still accepted (`completed_late`).
* **Lease expiry**: if the laptop sleeps/shuts down, the next claim re-queues the job (60 s backoff, doubling, 1 h cap);
  attempts are bounded (default 5), so a poison job cannot loop forever.
* **Idempotent**: completing the same claim twice is harmless; a superseded claim's result is discarded by the worker
  (its outbox item is dropped on a 409).
* **Failure classes**: `no_captions / video_unavailable / age_restricted / language_unavailable / invalid_video_id` are final
  (and cached as negative answers); `blocked` defers 30 min, doubling to 6 h, and does **not** consume attempts;
  other errors retry with backoff up to the attempt limit; `interrupted` returns the job immediately.
* Jobs older than 7 days fail visibly as `expired`; finished records are pruned after 14 days.
* If Upstash is unreachable, queueing tools say `queue_unavailable` and **nothing is acknowledged**.

## Render environment variable names (values go ONLY into Render's Environment page)
| Name | Purpose | Default |
|---|---|---|
| `TRANSCRIPT_QUEUE_ENABLED` | turn the queue + 2 extra MCP tools on. **Fail-closed: also requires `MCP_AUTH_TOKEN(S)` and Upstash**, otherwise the queue stays off (tools hidden, `/worker/*` answers 503, auto-queue off, alert `queue_blocked_*`) | off |
| `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN` | the Upstash database (required for the queue) | - |
| `WORKER_AUTH_TOKEN` (or `WORKER_AUTH_TOKENS`, comma list = one per worker) | lets a worker call `/worker/*` only. `/worker/*` is disabled while unset | - |
| `MCP_AUTH_TOKEN` | optional, protects `/mcp` (separate from the worker token) | off |
| `TRANSCRIPT_AUTO_QUEUE` | `yt_get_video_transcript` also queues a job when extraction failed for a retryable reason | true |
| `QUEUE_MAX_ATTEMPTS` `QUEUE_LEASE_SECONDS` `QUEUE_BLOCK_DEFER_SECONDS` `QUEUE_BLOCK_DEFER_MAX_SECONDS` `QUEUE_JOB_MAX_AGE_DAYS` `QUEUE_RETENTION_DAYS` `QUEUE_MAX_PENDING` | tuning | 5 / 300 / 1800 / 21600 / 7 / 14 / 2000 |
| `REMOTE_STORAGE_LIMIT_MB` | capacity used for the 70 % / 90 % alerts | 256 |
| `QUEUE_MAX_ENQUEUE_PER_DAY` | global cap on NEW jobs per UTC day (anti-flood, on top of `QUEUE_MAX_PENDING`) | 500 |
| `WORKER_RATE_LIMIT_PER_MINUTE`, `WORKER_MAX_BODY_BYTES` | per-token request limit and body cap | 60 / 4000000 |
Paid-API variables (`ENABLE_PAID_TRANSCRIPT_APIS`, `SUPADATA_API_KEY`, `TRANSCRIPT_API_KEY`) stay unset/false.

## Upstash free database (about 5 minutes, you do this - I cannot create accounts)
1. upstash.com -> sign up (no card should be requested; **stop if it asks for payment**).
2. Create Database -> type **Redis**, any region close to your Render region, plan **Free**. Leave "Eviction" off.
3. Open the database -> section **REST API** -> copy `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`
   into Render -> your service -> Environment. (For the optional doorbell also copy the **read-only** token into the
   worker's `worker.env` as `UPSTASH_READONLY_TOKEN`; never put the full token on the laptop.)
4. Note the page that shows commands used this month; keep it open during the live test.

## Windows worker, beginner steps
1. Install Python 3.12 from python.org (tick **"Add python.exe to PATH"** and keep the "py launcher").
2. On GitHub open the repo, branch `claude/charming-babbage-mo3ags` -> Code -> Download ZIP. Extract it to
   `C:\yt-research-server` (the scheduled task points at this folder; moving it later means re-running the installer).
3. Open **PowerShell** (normal, not admin): `cd C:\yt-research-server` then
   `powershell -ExecutionPolicy Bypass -File windows_worker\install_worker.ps1`
   It asks for your Render URL and the worker token (hidden input), stores them in `%APPDATA%\yt-worker\worker.env`
   (readable only by your Windows user), builds a private Python environment and registers the task
   **"YT Transcript Worker"** (starts when you sign in, hidden, no admin rights).
4. Check status without any network: `%APPDATA%\yt-worker\venv\Scripts\python.exe windows_worker\worker_main.py --status`
5. **Connectivity test (needs your approval, see below).** Until you run it, nothing touches YouTube.
6. Start: sign out/in, or `Start-ScheduledTask -TaskName "YT Transcript Worker"`. Logs: `%APPDATA%\yt-worker\worker.log`.
7. Remove: `powershell -File windows_worker\uninstall_worker.ps1` (add `-RemoveData` to delete settings/logs too).
Shutting down is safe: an interrupted job's lease expires and it is retried; results that could not be delivered are kept in
`%APPDATA%\yt-worker\outbox` and sent after the next start. Pacing counters persist, so rebooting does not reset the caps.

## Safe live test plan (nothing here has been run; each step needs the owner's explicit OK)
| Step | What | Cost / risk |
|---|---|---|
| L0 | Create the free Upstash database (above). | $0 |
| L1 | Deploy this branch to a **separate** free Render test service with `TRANSCRIPT_QUEUE_ENABLED=true`, the Upstash pair and a throwaway `WORKER_AUTH_TOKEN`. No transcript-provider keys. | $0 (free hours) |
| L2 | `GET /worker/ping` with the token (curl or the worker `--self-test` first part). Verifies auth + that Lua scripts run on **real Upstash** (`queue_enabled: true`). Then call `yt_queue_transcripts` with 3 ids and `yt_transcript_queue_status`. | ~100 commands |
| L3 | `worker_main.py --self-test --i-approve-live-test --ids <3 ids>`: 1 ping, 1 reachability GET, 3 lookups >= 10 s apart from your home IP. Tells you if YouTube serves your IP. | 3 YouTube requests |
| L4 | Worker `--once` on 3 queued ids, then `yt_get_video_transcript` for them (expect `cached: true`, source `youtube-transcript-api`). | 3 lookups |
| L5 | Interrupt test: start the worker, kill it (Task Manager) mid-job, wait for the lease (<= 5 min) and confirm another run completes it. Restart the Render test service; confirm queue + cache intact. | 1-2 lookups |
| L6 | 25-video day with the Task Scheduler task; compare Upstash "commands used" with `docs` projections; check `/health/transcripts` alerts. | 25 lookups |
| L7 | Only if L3-L6 pass: 3 days at 50/day. Stop at the first `blocked`. | ~150 lookups |
Hard stops: 3 consecutive `blocked`, any `queue_unavailable` that persists > 1 h, Upstash usage > 50 % of the monthly allowance.

## Security model when the queue is on
* `MCP_AUTH_TOKEN` is **required**: without it the queue is refused (nothing is written; the original nine tools behave exactly as before).
* Separate credentials: MCP tokens work only on `/mcp`, `/messages`, `/sse`, `/health/transcripts`; worker tokens work only on `/worker/*`
  (header only, never in a URL). Neither is accepted on the other surface (regression-tested).
* Job status (`yt_transcript_queue_status`, `/worker/jobs/*`) and retrieval (`yt_get_video_transcript`, `/worker/result/*`) require the matching credential.
* Anti-flood: authenticated callers only, 100 ids per call, `QUEUE_MAX_PENDING`, `QUEUE_MAX_ENQUEUE_PER_DAY`, per-token request rate limit.
* Logs: tokens, Upstash credentials and transcript text are never logged (tested); `httpx` request logging is silenced so the Upstash host is not printed.
* Paid transcript APIs stay disabled by default and are never reachable from the worker or the queue (tested end-to-end with provider keys present).

## Backlog policy and visibility
* Order is oldest-first (FIFO). Jobs older than `QUEUE_JOB_MAX_AGE_DAYS` (default 7) become `failed` with class `expired` - **visibly**:
  `yt_transcript_queue_status` shows `expired_total`, `expired_today`, `oldest_pending_age_seconds`, `completed_today`,
  `completed_last_3_days`, `estimated_drain_days`, and `show: "expired"` / `"failed"` lists the jobs. `/health/transcripts` raises
  `queue_backlog_older_than_6h_worker_offline_or_stalled` and `queue_depth_high`.
* Nothing is re-opened automatically. To retry on purpose: `yt_queue_transcripts` with `retry_failed: true` for listed ids, or
  `reopen_expired: true` (at most 100 per call, one language per call).
* Safe knobs: raise `QUEUE_JOB_MAX_AGE_DAYS` (e.g. 14) if you are away more than a week; lower the arrival rate; keep the worker caps
  (`HOURLY_CAP`/`DAILY_CAP`) at their defaults until a live pilot shows YouTube tolerates more. Do not respond to a backlog with more retries.
