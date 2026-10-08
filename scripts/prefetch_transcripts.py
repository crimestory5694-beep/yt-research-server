"""Fill the shared transcript cache from a machine YouTube does not block (e.g. your own computer/home IP).

    export UPSTASH_REDIS_REST_URL=...  UPSTASH_REDIS_REST_TOKEN=...      # same values as on Render
    python scripts/prefetch_transcripts.py VIDEO_ID_OR_URL [...] [--file ids.txt] [--lang es] [--delay 3] [--max 50]

Free providers only (paid APIs are force-disabled). Results land in the shared cache, so the Render server can
answer those videos even while YouTube blocks Render's IP. Prints statuses only - never transcript text or secrets.
Keep volumes modest: this is ordinary low-rate use of the same free library, not bulk scraping.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import transcript_service as ts  # noqa: E402


async def prefetch(ids, language="en", delay=3.0, max_items=50, service=None) -> list:
    if service is None:
        cfg = ts.TranscriptConfig.from_env()
        cfg.enable_paid = False
        cfg.transcript_api_key = cfg.supadata_api_key = ""
        cfg.provider_order = [p for p in cfg.provider_order if p in ts.FREE_PROVIDERS]
        cfg.fetches_per_minute = 20
        service = ts.TranscriptService(cfg)
    out = []
    for i, vid in enumerate(ids[:max_items]):
        r = await service.get_transcript(vid, language, any_language=True)
        out.append({"video_id": r.get("video_id", vid), "status": r["status"], "cached_already": r.get("cached", False),
                    "chars": len(r.get("full_transcript", ""))})
        if i < len(ids[:max_items]) - 1 and not r.get("cached"):
            await asyncio.sleep(delay)
    return out


def main(argv=None) -> int:
    a = list(sys.argv[1:] if argv is None else argv)

    def opt(name, default):
        if name in a:
            i = a.index(name); v = a[i + 1]; del a[i:i + 2]; return v
        return default
    lang, delay, mx, file = opt("--lang", "en"), float(opt("--delay", 3)), int(opt("--max", 50)), opt("--file", None)
    ids = list(a)
    if file:
        with open(file) as f:
            ids += [l.strip() for l in f if l.strip() and not l.startswith("#")]
    if not ids:
        print(__doc__); return 2
    if not (os.environ.get("UPSTASH_REDIS_REST_URL") and os.environ.get("UPSTASH_REDIS_REST_TOKEN")):
        print("WARNING: no UPSTASH_REDIS_REST_* set - results are cached only locally on this machine.", file=sys.stderr)
    res = asyncio.run(prefetch(ids, lang, delay, mx))
    print(json.dumps(res, indent=1))
    return 0 if all(r["status"] in ("ok",) or r["status"] in ts.NEGATIVE_CACHEABLE for r in res) else 1


if __name__ == "__main__":
    sys.exit(main())
