# Deploy Steps — YouTube Research MCP Server (Railway)

See `README.md` for the full configuration reference. Summary:

1. **Rotate the Google API key first.** The old key was committed to this repository's history
   and must be treated as compromised. In Google Cloud Console -> APIs & Services -> Credentials:
   create a new key, restrict it to *YouTube Data API v3*, then delete the old key.
2. Railway -> your service -> **Variables**: set `YOUTUBE_API_KEY` (new key). The server no longer has a
   built-in fallback key, so YouTube tools return a clear error until this is set.
3. Recommended: set `MCP_AUTH_TOKEN` (see README "Authentication") and update your MCP client to send it.
4. Optional: attach a Railway Volume so the transcript cache survives redeploys.
5. Deploy. Check `GET /` (status) and `GET /health/transcripts` (redacted diagnostics).

Never paste real keys into this repository, issues, or chat.
