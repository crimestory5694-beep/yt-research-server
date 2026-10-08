# YouTube Research MCP Server

FastAPI server exposing 9 MCP tools (Streamable HTTP at `/mcp`, legacy SSE at `/sse` + `/messages`).
Eight tools use the YouTube Data API v3; `yt_get_video_transcript` uses the transcript pipeline below.

> `openapi_plugin.yaml` is a leftover from an earlier REST design (`/channel/stats`, ...). Those routes do not exist in `main.py`.

## Transcript pipeline (`transcript_service.py`)

```
request -> normalize id/URL -> cache -> in-flight dedup -> provider chain -> cache result
chain:  youtube_direct  ->  youtube_proxy (only if configured)  ->  transcriptapi / supadata (only if explicitly enabled)
```

* **Free first.** `youtube-transcript-api` is called directly. A proxy is tried only if direct access fails with a
  retryable error (typically YouTube blocking a datacenter IP) and a proxy is configured.
* **Definitive answers stop the chain.** `no_captions`, `language_unavailable`, `video_unavailable`, `age_restricted`,
  `video_unplayable`, `invalid_video_id` never reach a paid provider and are cached (negative cache).
* **Paid APIs are OFF by default.** They run only when `ENABLE_PAID_TRANSCRIPT_APIS=true`, the key is set, the
  per-provider monthly cap (`PAID_TRANSCRIPT_MONTHLY_LIMIT`, default 50) is not reached, and no cooldown is active.
  Quota/billing (402, 429+quota wording) and auth errors start a persisted cooldown (12h / 24h) so credits and latency
  are not wasted on every request. Usage is counted *before* each call.
* **Result statuses:** `ok`, `no_captions`, `language_unavailable`, `video_unavailable`, `age_restricted`,
  `video_unplayable`, `invalid_video_id`, `blocked`, `timeout`, `network_error`, `upstream_error`, `parse_error`,
  `proxy_error`, `proxy_misconfigured`, `provider_quota_exhausted`, `provider_rate_limited`, `provider_auth_failed`,
  `provider_no_result`, `rate_limited`, `all_providers_failed`. Every response includes `attempts` (redacted).
* **Languages:** `language` may be `es` or a priority list `es,pt`; English is tried last. `any_language=true` returns
  any available track. Manual captions are preferred over auto-generated; `caption_type` and `available_languages`
  are returned. `en` also matches `en-GB` etc.
* **Cache:** SQLite at `TRANSCRIPT_CACHE_PATH` (default `./.cache/transcript_cache.sqlite3`). **On Render's free plan
  the filesystem is ephemeral** (wiped on every restart, redeploy and idle spin-down), so the cache then only helps while
  the instance stays awake. A persistent disk (paid plans only) is needed for durability. If the path is not writable
  the server falls back to memory and `/health/transcripts` reports `"persistent": false`.
* **Paid APIs and ephemeral storage:** monthly caps and cooldowns live in that SQLite file, so they would silently
  reset on an ephemeral filesystem. Therefore paid providers are **refused** unless `TRANSCRIPT_CACHE_PATH` is set
  explicitly (you assert it is on persistent storage) or `ALLOW_PAID_WITH_EPHEMERAL_STATE=true` (only sensible if the
  provider account itself has a hard cap, e.g. a free plan).
* **Limits:** at most `TRANSCRIPT_FETCHES_PER_MINUTE` uncached fetches/min server-wide (cache hits are free),
  `TRANSCRIPT_MAX_CONCURRENCY` parallel fetches, per-request and overall timeouts.

## Reducing paid-credit dependence

Order of defence, cheapest first: **shared persistent cache -> free `youtube-transcript-api` -> optional yt-dlp ->
optional proxy -> paid APIs (off by default)**.

* **Shared persistent cache (`remote_store.py`).** Set `UPSTASH_REDIS_REST_URL` / `UPSTASH_REDIS_REST_TOKEN` (an
  Upstash Redis database; reported free tier: 256 MB, 500K commands/month, no card - verify on their site). Transcripts
  (30 days), "no captions" answers (6 h), paid cooldowns and paid usage counters then survive Render restarts,
  spin-downs and redeploys and are shared between instances. Values are zlib-compressed. A cache hit costs 1
  Upstash command. If Upstash is unreachable, transcripts still work (local cache), but **paid calls fail closed**.
  Render's own free Key Value is memory-only and loses data on restart, and free Render Postgres expires after
  30 days, so neither is recommended.
* **Enforceable paid caps.** Per provider: `SUPADATA_MONTHLY_LIMIT`, `SUPADATA_DAILY_LIMIT`,
  `TRANSCRIPTAPI_MONTHLY_LIMIT`, `TRANSCRIPTAPI_DAILY_LIMIT` (defaults 25 / month and 5 / day via
  `PAID_TRANSCRIPT_MONTHLY_LIMIT` / `PAID_TRANSCRIPT_DAILY_LIMIT`; `0` = never use). A slot is reserved atomically
  *before* each paid call. Having the API keys in the environment does nothing unless
  `ENABLE_PAID_TRANSCRIPT_APIS=true`.
* **yt-dlp (optional, `ytdlp_provider.py`).** `ENABLE_YTDLP=true` + `pip install -r requirements-ytdlp.txt`. Reads subtitle
  tracks only (no media download). It shares the server's IP, so it only helps if YouTube's blocking is not purely
  IP-based; the probe's `extractors` matrix and `ytdlp_adds_value` field answer that on the real host. yt-dlp now
  expects an external JavaScript runtime (e.g. Deno) for full YouTube support, which Render's native Python image does
  not provide - another reason to measure before relying on it.
* **Prefetch from an unblocked machine.** `scripts/prefetch_transcripts.py ID...` fills the shared cache from your own
  computer (free providers only), so Render can serve those videos even if YouTube blocks Render.
* **Speech-to-text (Whisper) is deliberately not implemented**: it needs the video's audio, which means downloading
  media from YouTube (against its terms for third-party content, and it triggers the same IP blocks), and it cannot run
  on Render Free (0.1 CPU / 512 MB). It is only reasonable for audio you own or are licensed to use, on your own hardware.

## Authentication & rate limiting (`security.py`)

* `MCP_AUTH_TOKEN` (or comma-separated `MCP_AUTH_TOKENS`) unset -> **open, as before**. Set it to require
  `Authorization: Bearer <token>`, `X-API-Key: <token>`, or `?token=<token>` on `/mcp`, `/messages`, `/sse`,
  `/health/transcripts`. `GET /` stays public. Prefer headers: URLs end up in logs.
* `RATE_LIMIT_PER_MINUTE` (default 120) per client on `POST /mcp` and `/messages` (client = token, else IP).

## Tests

```
pip install -r requirements-dev.txt && pytest
```
All tests are offline: provider APIs are mocked, YouTube is simulated at the HTTP boundary.

## Live verification (free, no credits)

`python scripts/live_probe.py` (add `--with-proxy` only if a proxy is already configured) makes at most 1 reachability
GET and 4 YouTube lookups, with paid APIs force-disabled. Verdicts: `FREE_EXTRACTION_WORKS`, `YOUTUBE_IP_BLOCKED`,
`PROXY_CONFIG_FAILURE`, `NO_NETWORK_PATH_TO_YOUTUBE`, `INCONCLUSIVE_CAPTIONS_UNAVAILABLE_FOR_TESTED_VIDEOS`,
`PROVIDER_OR_PARSER_ERROR`. Exit code 0 only on success. Run it on the host you want to qualify, not locally: a laptop
result says nothing about Render's outbound IP. Without shell access (Render free) set
`RUN_TRANSCRIPT_PROBE_ON_STARTUP=true` on a throwaway service and read the `TRANSCRIPT_PROBE_RESULT` line in its Logs.
See `DEPLOY_STEPS.md`.
