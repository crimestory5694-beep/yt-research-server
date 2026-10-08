# Deploy & verify — YouTube Research MCP Server (Render)

Render ignores `Procfile`; the start command is whatever is set in the service's Settings. Use:
Build `pip install -r requirements.txt` · Start `uvicorn main:app --host 0.0.0.0 --port $PORT`.
`.python-version` pins Python 3.12 (the tested range is 3.11–3.13; an explicit `PYTHON_VERSION` env var overrides it).

## A. Real-IP test of the free transcript path (free, throwaway service)

Goal: learn whether YouTube serves transcripts to **Render's outbound IPs**. Render documents that outbound IP
ranges are shared by all services *in the same region*, so the test service must be in the **same region as your
production service** (Dashboard -> production service -> Settings -> Region).

1. Dashboard -> **New -> Web Service** -> same GitHub repo -> branch `claude/charming-babbage-mo3ags`.
2. Name `yt-probe-test`; **Region = same as production**; Runtime Python 3; Instance type **Free**;
   Build/Start commands as above; **Auto-Deploy: No**.
3. Environment: add exactly one variable: `RUN_TRANSCRIPT_PROBE_ON_STARTUP` = `true`.
   Add **no** API keys, no proxy variables, no transcript-provider keys. (The probe force-disables paid APIs anyway.)
4. Create. If Render asks for a payment method or shows any charge for Free: **stop and tell your engineer**.
5. Wait for "Live" (first build takes a few minutes), open **Logs**, find the line starting with
   `TRANSCRIPT_PROBE_RESULT` and copy it back (it contains no secrets and no transcript text).
6. Delete the test service (Settings -> Delete Service), or at least suspend it.

Reading the `verdict`: `FREE_EXTRACTION_WORKS` · `YOUTUBE_IP_BLOCKED` · `PROXY_CONFIG_FAILURE` ·
`NO_NETWORK_PATH_TO_YOUTUBE` · `INCONCLUSIVE_CAPTIONS_UNAVAILABLE_FOR_TESTED_VIDEOS` · `PROVIDER_OR_PARSER_ERROR`.
Free instances sleep after ~15 minutes idle and restart on the next request; each start re-runs the probe, which is fine.

## B. Production rollout (only after A, and only when you approve)

1. **Rotate the Google key first** (it is in git history): Google Cloud Console -> APIs & Services -> Credentials ->
   create a new key restricted to *YouTube Data API v3*, set it as `YOUTUBE_API_KEY`, delete the old one, check usage.
   The branch has no fallback key; YouTube tools fail with a clear error until the variable is set.
2. Optional: `MCP_AUTH_TOKEN` (clients must then send `Authorization: Bearer <token>`). Update clients first.
3. Keep `ENABLE_PAID_TRANSCRIPT_APIS` unset. Paid APIs additionally refuse to run on an ephemeral filesystem.
4. Deploy the branch to production only when you decide to.

Never paste real keys into this repository, issues, or chat.
