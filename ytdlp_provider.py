"""Optional second free extractor: yt-dlp (subtitle metadata only; never downloads audio/video).

OFF by default (ENABLE_YTDLP=true plus `pip install -r requirements-ytdlp.txt`). yt-dlp uses different YouTube client
endpoints than youtube-transcript-api, but it presents the SAME source IP to YouTube, so it only adds availability if
YouTube's blocking is request-pattern based rather than IP based. Whether it helps from Render is exactly what the
probe measures (extractor matrix). Not verified against live YouTube.

Conservative classification: "no subtitles found" and "language missing" are NON-definitive here (provider_no_result):
a degraded yt-dlp extraction can omit subtitle lists, so it must not end the chain or poison the negative cache.
"""
from __future__ import annotations

import json
import re
from typing import Optional

import requests

import transcript_service as ts

EXT_PREFERENCE = ("json3", "vtt", "srv1")


def _import_ytdlp():
    try:
        import yt_dlp  # noqa: WPS433
        return yt_dlp
    except Exception:
        return None


def is_installed() -> bool:
    import importlib.util
    return importlib.util.find_spec("yt_dlp") is not None


# ── subtitle parsers (pure) ──
def parse_json3(text: str) -> list:
    data = json.loads(text)
    out = []
    for ev in data.get("events", []):
        segs = ev.get("segs")
        if not segs:
            continue
        t = "".join(s.get("utf8", "") for s in segs).replace("\n", " ").strip()
        if t:
            out.append({"text": t, "start": round(ev.get("tStartMs", 0) / 1000, 1),
                        "duration": round(ev.get("dDurationMs", 0) / 1000, 1)})
    return out


_VTT_TIME = re.compile(r"(?:(\d+):)?(\d{2}):(\d{2})[.,](\d{3})\s+-->\s+(?:(\d+):)?(\d{2}):(\d{2})[.,](\d{3})")


def _secs(h, m, s, ms) -> float:
    return int(h or 0) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000


def parse_vtt(text: str) -> list:
    out, prev = [], None
    for block in re.split(r"\n\s*\n", text.replace("\r\n", "\n")):
        lines = [l for l in block.split("\n") if l.strip()]
        for i, l in enumerate(lines):
            m = _VTT_TIME.search(l)
            if m:
                g = m.groups()
                start, end = _secs(*g[0:4]), _secs(*g[4:8])
                body = re.sub(r"<[^>]+>", "", " ".join(lines[i + 1:])).strip()
                if body and body != prev:  # YouTube auto-captions repeat rolling lines
                    out.append({"text": body, "start": round(start, 1), "duration": round(max(end - start, 0), 1)})
                    prev = body
                break
    return out


def parse_subtitle(ext: str, text: str) -> list:
    return parse_json3(text) if ext == "json3" else parse_vtt(text)


# ── track selection ──
def build_tracks(info: dict) -> list:
    """-> [(language_code, is_auto, entries)]. Auto tracks: keep only originals ('xx-orig') when present, because
    YouTube also lists machine translations of every language as 'automatic captions'."""
    tracks = [(c, False, e) for c, e in (info.get("subtitles") or {}).items() if e]
    auto = {c: e for c, e in (info.get("automatic_captions") or {}).items() if e}
    orig = {c[:-5]: e for c, e in auto.items() if c.endswith("-orig")}
    chosen = orig or auto
    tracks += [(c, True, e) for c, e in chosen.items()]
    return tracks


def pick_entry(entries: list) -> Optional[dict]:
    for ext in EXT_PREFERENCE:
        for e in entries:
            if e.get("ext") == ext and e.get("url"):
                return e
    return None


def classify_ytdlp_error(msg: str) -> ts.TranscriptError:
    m = (msg or "").lower()
    if "confirm your age" in m or "age-restricted" in m or "inappropriate for some users" in m:
        return ts.TranscriptError(ts.AGE_RESTRICTED, "Video is age-restricted")
    if "not a bot" in m or "http error 429" in m or "http error 403" in m or "too many requests" in m:
        return ts.TranscriptError(ts.BLOCKED, "YouTube blocked yt-dlp (bot check / 403 / 429)")
    if any(k in m for k in ("video unavailable", "private video", "has been removed", "is not available",
                            "no longer available", "terminated")):
        return ts.TranscriptError(ts.VIDEO_UNAVAILABLE, "Video is unavailable")
    if any(k in m for k in ("timed out", "timeout", "connection", "unable to download", "name or service", "ssl")):
        return ts.TranscriptError(ts.NETWORK_ERROR, "yt-dlp network error")
    return ts.TranscriptError(ts.UPSTREAM_ERROR, "yt-dlp error: " + ts.redact(msg or "unknown")[:160])


def fetch_with_ytdlp(video_id: str, languages: list, any_language: bool, timeout: float, secrets=(),
                     _ydl_factory=None, _http_get=None) -> dict:
    """Blocking. Same return shape as ts.fetch_with_library. Seams (_ydl_factory/_http_get) are for tests."""
    yt = _import_ytdlp() if _ydl_factory is None else None
    if _ydl_factory is None and yt is None:
        raise ts.TranscriptError(ts.UPSTREAM_ERROR, "yt-dlp is not installed")
    opts = {"quiet": True, "no_warnings": True, "skip_download": True, "noplaylist": True,
            "socket_timeout": timeout, "proxy": ""}  # "" = explicit direct connection (ignore env proxies)
    try:
        factory = _ydl_factory or yt.YoutubeDL
        with factory(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
    except ts.TranscriptError:
        raise
    except Exception as e:  # yt_dlp.utils.DownloadError / ExtractorError and friends
        raise classify_ytdlp_error(str(e)) from None
    if not isinstance(info, dict):
        raise ts.TranscriptError(ts.PARSE_ERROR, "yt-dlp returned no video info")

    tracks = build_tracks(info)
    avail = [{"code": c, "name": c, "auto_generated": a} for c, a, _ in tracks]
    if not tracks:
        raise ts.TranscriptError(ts.PROVIDER_NO_RESULT, "yt-dlp found no subtitle tracks (not conclusive)")
    chosen = ts.pick_transcript([(c, a, e) for c, a, e in tracks], languages)
    if chosen is None and any_language:
        chosen = sorted(tracks, key=lambda t: t[1])[0]
    if chosen is None:
        raise ts.TranscriptError(ts.PROVIDER_NO_RESULT, "yt-dlp: requested language not listed (not conclusive)",
                                 available_languages=avail)
    code, auto, entries = chosen
    entry = pick_entry(entries)
    if entry is None:
        raise ts.TranscriptError(ts.PARSE_ERROR, "yt-dlp: no supported subtitle format")
    try:
        get = _http_get or (lambda url: _plain_get(url, timeout))
        body = get(entry["url"])
        segments = parse_subtitle(entry["ext"], body)
    except ts.TranscriptError:
        raise
    except requests.exceptions.Timeout:
        raise ts.TranscriptError(ts.TIMEOUT, "subtitle download timed out") from None
    except requests.exceptions.RequestException as e:
        raise ts.TranscriptError(ts.NETWORK_ERROR, "subtitle download failed: " + type(e).__name__) from None
    except Exception as e:
        raise ts.TranscriptError(ts.PARSE_ERROR, "subtitle parse failed: " + type(e).__name__) from None
    if not segments:
        # YouTube serves an empty body when a PO token is required: treat as a provider problem, not "no captions"
        raise ts.TranscriptError(ts.PARSE_ERROR, "subtitle track was empty (possibly PO-token gated)")
    return {"language_code": code, "caption_type": "auto_generated" if auto else "manual",
            "available_languages": avail, "segments": segments,
            "full_transcript": " ".join(s["text"] for s in segments)}


def _plain_get(url: str, timeout: float) -> str:
    s = requests.Session()
    s.trust_env = False
    r = s.get(url, timeout=(min(5, timeout), timeout))
    if r.status_code in (403, 429):
        raise ts.TranscriptError(ts.BLOCKED, f"subtitle host returned HTTP {r.status_code}")
    r.raise_for_status()
    return r.text
