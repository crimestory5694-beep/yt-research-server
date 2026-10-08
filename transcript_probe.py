"""Minimal live probe: can THIS host fetch YouTube transcripts for free?

Three ways to run it, from the host you want to qualify (e.g. Render's outbound IP):
  * shell available:           python scripts/live_probe.py
  * no shell (Render free):    set RUN_TRANSCRIPT_PROBE_ON_STARTUP=true on a throwaway service and read the
                               "TRANSCRIPT_PROBE_RESULT {...}" line in the service Logs (see main.py startup hook)
Safety: paid providers are force-disabled and their keys ignored; the cache is in-memory; at most
5 YouTube page requests are made (plus 1 reachability GET); no transcript text and no env values are printed.
Add --with-proxy to also exercise youtube_proxy IF WEBSHARE_*/PROXY_URL are already set (never purchased here).
Add --with-ytdlp (or PROBE_YTDLP=true) to test yt-dlp as a SEPARATE extractor (needs requirements-ytdlp.txt installed):
the output then has an `extractors` matrix and `ytdlp_adds_value` (true only if yt-dlp works where the library does not).
If UPSTASH_REDIS_REST_URL/TOKEN are set, a tiny write/read/delete verifies the shared cache (`remote_store`).
"""
import asyncio
import json
import platform
import sys
import time

import requests

import transcript_service as ts

# Well-known, widely captioned videos (assumption, not guarantee) + a well-formed non-existent id as a control.
VIDEOS = [("jNQXAC9IVRw", "known-captioned?"), ("dQw4w9WgXcQ", "known-captioned?"),
          ("UF8uR6Z6KLc", "known-captioned?"), ("aaaaaaaaaaa", "control-nonexistent")]
SUCCESS, NO_CAPS = {ts.OK}, {ts.NO_CAPTIONS, ts.LANGUAGE_UNAVAILABLE, ts.VIDEO_UNAVAILABLE, ts.AGE_RESTRICTED, ts.VIDEO_UNPLAYABLE}


def reachability(timeout=10) -> dict:
    s = requests.Session(); s.trust_env = False
    try:
        r = s.get("https://www.youtube.com/", timeout=timeout, headers={"Accept-Language": "en-US"})
        body = r.text[:200000]
        return {"ok": True, "http_status": r.status_code, "captcha_page": 'class="g-recaptcha"' in body,
                "consent_page": "consent.youtube.com" in body}
    except Exception as e:
        return {"ok": False, "error": type(e).__name__}


def decide(reach: dict, results: list) -> dict:
    real = [r for r in results if r["label"] != "control-nonexistent"]
    st = [r["status"] for r in real]
    if any(s in SUCCESS for s in st):
        v = "FREE_EXTRACTION_WORKS"
    elif not reach.get("ok"):
        v = "NO_NETWORK_PATH_TO_YOUTUBE"
    elif any(s in (ts.PROXY_ERROR, ts.PROXY_MISCONFIGURED) for s in st):
        v = "PROXY_CONFIG_FAILURE"
    elif reach.get("captcha_page") or any(s == ts.BLOCKED for s in st):
        v = "YOUTUBE_IP_BLOCKED"
    elif st and all(s in NO_CAPS for s in st):
        v = "INCONCLUSIVE_CAPTIONS_UNAVAILABLE_FOR_TESTED_VIDEOS"
    else:
        v = "PROVIDER_OR_PARSER_ERROR"
    return {"verdict": v, "ok_count": sum(s in SUCCESS for s in st), "tested": len(st)}


async def _run_one(svc, delay) -> list:
    results = []
    for vid, label in VIDEOS:
        t0 = time.time()
        r = await svc.get_transcript(vid, "en", any_language=True)
        results.append({"video_id": vid, "label": label, "status": r["status"], "seconds": round(time.time() - t0, 1),
                        "providers": [(a["provider"], a["outcome"]) for a in r.get("attempts", [])],
                        "chars": len(r.get("full_transcript", "")), "caption_type": r.get("caption_type"),
                        "proxy_used": r.get("proxy_used")})
        await asyncio.sleep(delay)
    return results


def _probe_config(order, ytdlp=False):
    """Probe config: paid OFF + keys cleared, REMOTE CACHE OFF (a cached answer would fake a success)."""
    cfg = ts.TranscriptConfig.from_env()
    cfg.enable_paid = False
    cfg.transcript_api_key = cfg.supadata_api_key = ""
    cfg.remote_url = cfg.remote_token = ""
    cfg.enable_ytdlp = ytdlp
    cfg.provider_order = order
    cfg.cache_path = ":memory:"; cfg.fetches_per_minute = 10; cfg.block_cooldown_seconds = 0
    return cfg


async def remote_store_check() -> dict:
    """Verifies UPSTASH_REDIS_REST_* credentials + REST protocol with one tiny write/read/delete. Never prints them."""
    import os
    url, token = os.environ.get("UPSTASH_REDIS_REST_URL", ""), os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
    if not (url and token):
        return {"configured": False}
    from remote_store import RemoteStore
    rt = await RemoteStore(url, token).roundtrip()
    return {"configured": True, **rt}


async def run(service=None, with_proxy=False, delay=2.0, reach=None, with_ytdlp=None, check_remote=False) -> dict:
    import os
    reach = reachability() if reach is None else reach
    if service is not None:  # injected service (tests): single extractor path
        results = await _run_one(service, delay)
        return {"paid_apis": "disabled", "proxy_configured": service._configured("youtube_proxy"),
                "with_proxy": with_proxy, "reachability": reach, "results": results, **decide(reach, results)}

    if with_ytdlp is None:
        with_ytdlp = os.environ.get("PROBE_YTDLP", "").lower() in ("1", "true", "yes", "on")
    extractors = {"youtube_transcript_api": _probe_config(["youtube_direct"] + (["youtube_proxy"] if with_proxy else []))}
    if with_ytdlp:
        extractors["yt_dlp"] = _probe_config(["ytdlp"], ytdlp=True)
    matrix = {}
    for name, cfg in extractors.items():
        svc = ts.TranscriptService(cfg, store=ts.TranscriptStore(":memory:"))
        if name == "yt_dlp" and not svc._ytdlp_available():
            matrix[name] = {"verdict": "NOT_INSTALLED"}
            continue
        results = await _run_one(svc, delay)
        matrix[name] = {"results": results, **decide(reach, results)}
    primary = matrix["youtube_transcript_api"]
    working = [n for n, m in matrix.items() if m.get("verdict") == "FREE_EXTRACTION_WORKS"]
    out = {"paid_apis": "disabled", "proxy_configured": bool(extractors["youtube_transcript_api"].webshare_user
                                                              or extractors["youtube_transcript_api"].proxy_url),
           "with_proxy": with_proxy, "reachability": reach, "results": primary["results"],
           "extractors": {n: {k: v for k, v in m.items() if k != "results"} for n, m in matrix.items()},
           "working_extractors": working, **decide(reach, primary["results"])}
    if working:
        out["verdict"] = "FREE_EXTRACTION_WORKS"
    if "yt_dlp" in matrix:
        out["ytdlp_adds_value"] = ("yt_dlp" in working) and ("youtube_transcript_api" not in working)
    if check_remote:
        out["remote_store"] = await remote_store_check()
    return out


def environment_info() -> dict:
    """Non-secret hosting context so the log line is self-describing."""
    import os
    return {"platform": "render" if os.environ.get("RENDER") else "unknown",
            "git_branch": os.environ.get("RENDER_GIT_BRANCH"), "python": platform.python_version()}


async def run_and_log() -> dict:
    """Used by the optional startup hook: one line to stdout (Render Logs), never raises."""
    try:
        out = await run(check_remote=True)
        out["environment"] = environment_info()
    except Exception as e:  # a probe must never take the server down
        out = {"verdict": "PROBE_CRASHED", "error": type(e).__name__, "environment": environment_info()}
    print("TRANSCRIPT_PROBE_RESULT " + json.dumps(out, separators=(",", ":")), flush=True)
    return out


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    out = asyncio.run(run(with_proxy="--with-proxy" in argv, with_ytdlp=("--with-ytdlp" in argv) or None, check_remote=True))
    out["environment"] = environment_info()
    print(json.dumps(out, indent=2))
    return 0 if out["verdict"] == "FREE_EXTRACTION_WORKS" else 1
