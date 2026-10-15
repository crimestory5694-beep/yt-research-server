# Upstash protocol review (desk review; NO live database has been contacted)

Sources: Upstash documentation as quoted in search-result excerpts (the docs site itself could not be opened from the
build sandbox): REST API page (`/docs/redis/features/restapi`), EVAL page (`/docs/redis/commands/scripting/eval`),
billing page (`/docs/redis/overall/billing`), pricing page (`/pricing/redis`), "Lua scripting on Upstash Redis" blog post,
ACL/REST-token page. Re-read the pages before relying on any row marked LIVE.

| Question | What the docs say (as excerpted) | Our usage | Status |
|---|---|---|---|
| `/pipeline` request shape | `POST {url}/pipeline`, body = JSON array of command arrays; reply = array, one `{"result": ..}` or `{"error": ".."}` per command, same order | `RemoteStore.exec` sends exactly this; test `test_rest_request_shape_matches_documented_pipeline_format` | matches docs; LIVE: confirm with a real call |
| Pipeline atomicity | **Not atomic**: other clients' commands may interleave | Nothing relies on it: every state change is ONE `EVAL`; pipelines only batch independent operations; a partly failed batch is reported as "not acknowledged" and retried idempotently (`test_pipeline_is_not_assumed_atomic...`) | OK by design |
| Transactions | `/multi-exec` is atomic (no WATCH) | not used (scripts give conditional logic) | n/a |
| EVAL support | Supported; "runs as a single atomic step"; `numkeys`, then keys (`KEYS`), then args (`ARGV`); SDKs take (script, keys[], args[]) | `["EVAL", script, numkeys, keys..., args...]` inside `/pipeline` | LIVE: exact REST body for EVAL and EVAL-inside-pipeline were not shown in the excerpts |
| Declared keys | Keys a script touches should be passed in `KEYS` (undeclared keys are read from disk under the lock = slower) | All six scripts touch only declared keys: static regex test + a dynamic test that executes a full lifecycle and records every key touched inside scripts (`test_scripts_touch_only_declared_keys_when_actually_executed`: 0 violations over 50+ script commands) | OK offline; LIVE: confirm no warnings/latency |
| Lua dialect | Standard Redis Lua; scripts should be short and deterministic (no clock/random) | Lua 5.1 syntax verified with the real 5.1 compiler; only `redis.call`, `string.find/sub`, `table.concat`, `math.floor/min/max`, `tostring/tonumber`, `ipairs`; time is passed via ARGV; no `cjson` (not documented for Upstash) | compile-checked offline |
| Commands used inside scripts | (no restriction list found) | HGET HSET HINCRBY HDEL ZADD ZREM ZCARD ZRANGEBYSCORE SET(EX/NX) EXPIRE - all ordinary commands | LIVE: confirm none is blocked |
| `SET k v EX n NX` in a script | standard Redis | used for negative-cache entries | LIVE |
| Script size / large ARGV | request size limit 10 MB (free tier), record 100 MB | cache values capped at 8 MB (`MAX_VALUE_BYTES`) | LIVE: try a 100 KB and a 5 MB value |
| Command counting | "each command ... is one billable request" (REST or Redis protocol). **Pipelines and Lua are not mentioned.** | Worst case assumed: every command inside a script bills. Measured (top-level + inside script): enqueue 9, claim 11, complete 13, idle claim 3 | LIVE: compare the console counter with the test's known command count |
| Free plan | 500,000 commands/month, 256 MB, 10 GB bandwidth/month, 10,000 commands/s, one free database, no card | capacity plan keeps usage at 13-27 % of commands and 8-16 % of storage at 1,500-3,000 transcripts/month | verify on the live pricing page; behaviour when a limit is hit is undocumented -> we fail visibly |
| Read-only token | Console switch gives a token for read commands only | optional worker "doorbell" uses `ZCOUNT` only | LIVE: confirm `ZCOUNT` works with the read-only token |

## Script-by-script compatibility notes
* **ENQUEUE** (HGET, ZCARD, HGET, HSET, ZADD, HINCRBY): all keys declared; no randomness; daily/pending caps are integer comparisons.
* **CLAIM** (ZRANGEBYSCORE x2 with `LIMIT`, HGET/HSET/ZADD/ZREM/HINCRBY): loop bounds are capped (50 expired leases + `maxjobs+100` due jobs per call) so a script cannot run long.
* **RENEW / COMPLETE / FAIL**: single HGET + HSET, plus (COMPLETE/FAIL) the cache `SET` on the declared cache key and the accounting hash.
* **PRUNE**: at most 100 jobs per call; HDEL of two old per-day counters.
* Returned values are arrays of strings only (no Lua booleans/floats), avoiding RESP conversion surprises.

## Minimal real-Upstash validation (owner approval required; see the checklist in the engineering report)
1. `PING`/`SET`/`GET` via `/pipeline` (protocol + token).
2. One `EVAL` inside `/pipeline` returning an array (`ENQUEUE` for a throw-away id).
3. Claim/renew/complete/fail with a stale fence -> expect `stale`; verify the cache key was NOT written.
4. 100 KB and 5 MB `complete` payloads.
5. Compare the Upstash console command counter before/after a known sequence (resolves script billing).
6. Read-only token `ZCOUNT`.
