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
* **Cache:** SQLite at `TRANSCRIPT_CACHE_PATH` (default `$RAILWAY_VOLUME_MOUNT_PATH/transcript_cache.sqlite3`, else
  `./.cache/`). Without a Railway Volume the cache and paid-usage counters reset on every deploy. If the path is not
  writable the server falls back to memory and `/health/transcripts` reports `"persistent": false`.
* **Limits:** at most `TRANSCRIPT_FETCHES_PER_MINUTE` uncached fetches/min server-wide (cache hits are free),
  `TRANSCRIPT_MAX_CONCURRENCY` parallel fetches, per-request and overall timeouts.

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
