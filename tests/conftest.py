import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Tests must never use real credentials or hit paid providers.
for k in ("YOUTUBE_API_KEY", "TRANSCRIPT_API_KEY", "SUPADATA_API_KEY", "WEBSHARE_USER",
          "WEBSHARE_PASS", "PROXY_URL", "ENABLE_PAID_TRANSCRIPT_APIS", "MCP_AUTH_TOKEN", "MCP_AUTH_TOKENS"):
    os.environ.pop(k, None)
os.environ["TRANSCRIPT_CACHE_PATH"] = ":memory:"
