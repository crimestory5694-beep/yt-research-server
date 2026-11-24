"""
Research tools — web research, fact verification, and source discovery.
Ported from research-server (Node.js) to Python for unified deployment.
Provides 9 MCP tools for deep research, claim verification, primary source
discovery, historical context, content saturation analysis, cross-referencing,
story angle extraction, Wikipedia data, and API status.

Search engines (all optional — server works with whatever is configured):
  - Wikipedia API     — always free, no key needed
  - DuckDuckGo       — HTML scrape, free, no key needed
  - Google Custom Search — 100 free queries/day (needs GOOGLE_API_KEY + GOOGLE_CX)
  - Serper.dev        — Google SERP results (needs SERPER_API_KEY)
  - Tavily            — AI research search (needs TAVILY_API_KEY)
"""
import os
import re
import time
import asyncio
from typing import Any, Optional
from urllib.parse import quote

import httpx

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# ============ CONFIG ============
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY", "")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
GOOGLE_CX = os.environ.get("GOOGLE_CX", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
# gemini-2.0-flash and gemini-2.5-flash were both retired from the free tier
# (confirmed via the API's own deprecation error, which pointed at
# gemini-3.8-flash — a paid-tier model). gemini-3.6-flash is the current
# free-tier equivalent as of testing; override via GEMINI_MODEL if Google
# changes free-tier availability again.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
WIKI_UA = (
    "ResearchMCPServer/1.0 "
    "(https://github.com/crimestory5694-beep/yt-research-server; research-mcp-tool)"
)

# ── Wikipedia cache (10 min TTL) ──
_wiki_cache: dict = {}
_WIKI_CACHE_TTL = 600
_last_wiki_fetch = 0.0


def _wcache_get(key: str):
    entry = _wiki_cache.get(key)
    if not entry:
        return None
    if time.time() - entry["ts"] > _WIKI_CACHE_TTL:
        _wiki_cache.pop(key, None)
        return None
    return entry["value"]


def _wcache_set(key: str, value):
    if len(_wiki_cache) > 200:
        oldest = next(iter(_wiki_cache))
        _wiki_cache.pop(oldest, None)
    _wiki_cache[key] = {"value": value, "ts": time.time()}


async def _wiki_get(client: httpx.AsyncClient, url: str, **kwargs):
    """Rate-limited Wikipedia fetch (max 1 req / 200ms, retry on 429)."""
    global _last_wiki_fetch
    now = time.time()
    if now - _last_wiki_fetch < 0.2:
        await asyncio.sleep(0.2 - (now - _last_wiki_fetch))
    _last_wiki_fetch = time.time()

    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", WIKI_UA)
    headers.setdefault("Accept", "application/json")

    res = await client.get(url, headers=headers, timeout=12, **kwargs)
    if res.status_code == 429:
        await asyncio.sleep(2)
        _last_wiki_fetch = time.time()
        res = await client.get(url, headers=headers, timeout=12, **kwargs)
    return res


# ══════════════════════════════════════════════════════════════════════════════
#  SEARCH ENGINES
# ══════════════════════════════════════════════════════════════════════════════


async def _duckduckgo_search(query: str, max_results: int = 5) -> list:
    """DuckDuckGo HTML scrape (no API key needed)."""
    endpoints = [
        "https://html.duckduckgo.com/html/",
        "https://lite.duckduckgo.com/lite/",
    ]
    for url in endpoints:
        try:
            async with httpx.AsyncClient() as c:
                res = await c.post(
                    url,
                    data={"q": query, "kl": "us-en"},
                    headers={
                        "User-Agent": BROWSER_UA,
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Referer": "https://duckduckgo.com/",
                    },
                    timeout=12,
                )
            if res.status_code != 200:
                continue

            if not HAS_BS4:
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            results = []

            # Standard HTML endpoint
            for el in soup.select(".result"):
                if len(results) >= max_results:
                    break
                a = el.select_one(".result__a")
                snip = el.select_one(".result__snippet")
                if not a:
                    continue
                href = a.get("href", "")
                if "uddg=" in href:
                    try:
                        from urllib.parse import unquote
                        href = unquote(href.split("uddg=")[1].split("&")[0])
                    except Exception:
                        pass
                title = a.get_text(strip=True)
                if title and href.startswith("http"):
                    results.append({
                        "title": title,
                        "url": href,
                        "snippet": snip.get_text(strip=True) if snip else "",
                        "source": "duckduckgo",
                    })

            # Lite endpoint fallback
            if not results:
                for a in soup.select("a.result-link"):
                    if len(results) >= max_results:
                        break
                    href = a.get("href", "")
                    title = a.get_text(strip=True)
                    if title and href.startswith("http"):
                        results.append({"title": title, "url": href, "snippet": "", "source": "duckduckgo"})

            if results:
                return results
        except Exception as e:
            print(f"DuckDuckGo ({url}): {e}")
    return []


async def _google_search(query: str, max_results: int = 5, language: str = "en") -> list:
    """Google Custom Search API (100 free/day)."""
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        return []
    try:
        async with httpx.AsyncClient() as c:
            res = await c.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": GOOGLE_API_KEY, "cx": GOOGLE_CX, "q": query, "num": min(max_results, 10), "hl": language},
                timeout=12,
            )
        if res.status_code != 200:
            return []
        return [
            {"title": it.get("title", ""), "url": it.get("link", ""), "snippet": it.get("snippet", ""), "source": "google"}
            for it in res.json().get("items", [])[:max_results]
        ]
    except Exception as e:
        print(f"Google Custom Search: {e}")
        return []


async def _serper_search(query: str, max_results: int = 5, language: str = "en") -> list:
    """Serper.dev Google SERP (optional)."""
    if not SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient() as c:
            res = await c.post(
                "https://google.serper.dev/search",
                json={"q": query, "num": min(max_results, 10), "gl": "us", "hl": language},
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                timeout=12,
            )
        if res.status_code != 200:
            return []
        return [
            {"title": it.get("title", ""), "url": it.get("link", ""), "snippet": it.get("snippet", ""), "source": "serper"}
            for it in res.json().get("organic", [])[:max_results]
        ]
    except Exception as e:
        print(f"Serper: {e}")
        return []


async def _tavily_search(query: str, max_results: int = 5) -> list:
    """Tavily AI search (optional)."""
    if not TAVILY_API_KEY:
        return []
    try:
        async with httpx.AsyncClient() as c:
            res = await c.post(
                "https://api.tavily.com/search",
                json={"api_key": TAVILY_API_KEY, "query": query, "max_results": min(max_results, 10), "search_depth": "basic"},
                timeout=15,
            )
        if res.status_code != 200:
            return []
        return [
            {"title": it.get("title", ""), "url": it.get("url", ""), "snippet": it.get("content", ""), "source": "tavily"}
            for it in res.json().get("results", [])[:max_results]
        ]
    except Exception as e:
        print(f"Tavily: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  WIKIPEDIA API
# ══════════════════════════════════════════════════════════════════════════════


async def search_wikipedia(query: str, language: str = "en", max_results: int = 5) -> list:
    ck = f"ws:{language}:{query}:{max_results}"
    cached = _wcache_get(ck)
    if cached is not None:
        return cached

    async with httpx.AsyncClient() as c:
        # Full-text search
        try:
            res = await _wiki_get(c, f"https://{language}.wikipedia.org/w/api.php", params={
                "action": "query", "list": "search", "srsearch": query,
                "srlimit": str(max_results), "srprop": "snippet", "format": "json", "origin": "*",
            })
            if res.status_code == 200:
                items = res.json().get("query", {}).get("search", [])
                if items:
                    out = [{
                        "title": it["title"],
                        "url": f"https://{language}.wikipedia.org/wiki/{quote(it['title'].replace(' ', '_'))}",
                        "snippet": re.sub(r"<[^>]*>", "", it.get("snippet", "")).strip(),
                        "source": "wikipedia",
                    } for it in items]
                    _wcache_set(ck, out)
                    return out
        except Exception as e:
            print(f"Wiki full-text: {e}")

        # OpenSearch fallback
        try:
            res = await _wiki_get(c, f"https://{language}.wikipedia.org/w/api.php", params={
                "action": "opensearch", "search": query, "limit": str(max_results), "format": "json", "origin": "*",
            })
            if res.status_code == 200:
                d = res.json()
                if len(d) >= 4 and d[1]:
                    out = [{"title": d[1][i], "url": d[3][i], "snippet": d[2][i] if len(d[2]) > i else "", "source": "wikipedia"} for i in range(len(d[1]))]
                    _wcache_set(ck, out)
                    return out
        except Exception as e:
            print(f"Wiki opensearch: {e}")
    return []


async def get_wikipedia_article(title: str, language: str = "en"):
    ck = f"wa:{language}:{title}"
    cached = _wcache_get(ck)
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient() as c:
            res = await _wiki_get(c, f"https://{language}.wikipedia.org/w/api.php", params={
                "action": "query", "titles": title, "prop": "extracts|info|categories",
                "exlimit": "1", "explaintext": "1", "inprop": "url", "format": "json", "origin": "*",
            })
        if res.status_code != 200:
            return None
        pages = res.json().get("query", {}).get("pages", {})
        page = next(iter(pages.values()), None)
        if not page or "missing" in page:
            return None
        result = {
            "title": page.get("title", ""), "url": page.get("fullurl", ""),
            "content": page.get("extract", ""),
            "categories": [cat["title"].replace("Category:", "").replace("Kategorie:", "") for cat in page.get("categories", [])],
            "source": "wikipedia",
        }
        _wcache_set(ck, result)
        return result
    except Exception as e:
        print(f"Wiki article: {e}")
        return None


async def get_wikipedia_sections(title: str, language: str = "en"):
    ck = f"wsec:{language}:{title}"
    cached = _wcache_get(ck)
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient() as c:
            res = await _wiki_get(c, f"https://{language}.wikipedia.org/w/api.php", params={
                "action": "parse", "page": title, "prop": "sections|externallinks", "format": "json", "origin": "*",
            })
        if res.status_code != 200:
            return None
        data = res.json()
        if "error" in data:
            return None
        parse = data.get("parse", {})
        result = {
            "title": parse.get("title", ""),
            "sections": [{"heading": s["line"], "level": int(s["level"])} for s in parse.get("sections", [])],
            "externalLinks": parse.get("externallinks", []),
        }
        _wcache_set(ck, result)
        return result
    except Exception as e:
        print(f"Wiki sections: {e}")
        return None


async def get_wikipedia_references(title: str, language: str = "en") -> list:
    ck = f"wref:{language}:{title}"
    cached = _wcache_get(ck)
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient() as c:
            res = await _wiki_get(c, f"https://{language}.wikipedia.org/w/api.php", params={
                "action": "parse", "page": title, "prop": "externallinks", "format": "json", "origin": "*",
            })
        if res.status_code != 200:
            return []
        links = res.json().get("parse", {}).get("externallinks", [])
        _wcache_set(ck, links)
        return links
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  WEB SCRAPER
# ══════════════════════════════════════════════════════════════════════════════


async def fetch_page_content(url: str, max_length: int = 5000) -> Optional[str]:
    """Scrape a web page and return cleaned text."""
    try:
        async with httpx.AsyncClient(follow_redirects=True) as c:
            res = await c.get(url, headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }, timeout=15)
        if res.status_code != 200:
            return None
        ct = res.headers.get("content-type", "")
        if "text/html" not in ct and "text/plain" not in ct:
            return None

        if not HAS_BS4:
            # Fallback: strip tags with regex
            text = re.sub(r"<[^>]+>", " ", res.text)
            text = re.sub(r"[\t ]+", " ", text)
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            return text[:max_length]

        soup = BeautifulSoup(res.text, "html.parser")
        for tag in soup.select(
            "script,style,nav,header,footer,aside,iframe,"
            ".sidebar,.menu,.nav,.ad,.ads,.advertisement,"
            ".cookie-banner,.popup,noscript,svg,figure,img"
        ):
            tag.decompose()

        selectors = [
            "article", "main", '[role="main"]', ".post-content", ".entry-content",
            ".article-body", ".article-content", ".content", "#content", "#main-content",
            ".mw-parser-output", ".post-body", ".story-body",
        ]
        text = ""
        for sel in selectors:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) > 200:
                text = el.get_text(separator=" ", strip=True)
                break
        if not text:
            body = soup.find("body")
            text = body.get_text(separator=" ", strip=True) if body else ""

        text = re.sub(r"[\t ]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:max_length]
    except Exception as e:
        print(f"Scrape [{url}]: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  RELEVANCE FILTERING
#  Search engines (especially the Wikipedia full-text fallback used when no
#  paid engine is configured) can return results that are keyword-matched but
#  topically unrelated to the query. Tools that make a verdict (verify_claims,
#  check_content_saturation, cross_reference) must not treat an irrelevant hit
#  as support for that verdict — this was producing false VERIFIED/LOW results
#  off of totally unrelated pages (e.g. "The Three Stooges" "supporting" a
#  YouTube RPM claim). _is_relevant() is a cheap keyword-overlap heuristic to
#  reject those before they get counted.
# ══════════════════════════════════════════════════════════════════════════════

_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "of", "to", "in", "on", "for",
    "and", "or", "but", "than", "that", "this", "it", "its", "as", "by", "with",
    "be", "been", "has", "have", "had", "not", "no", "do", "does", "did", "will",
    "would", "can", "could", "about", "into", "than", "then", "so", "if", "you",
    "your", "they", "their", "what", "which", "who", "how", "why", "all", "more",
}


def _keywords(text: str) -> set:
    words = re.findall(r"[a-zA-Z]{3,}", (text or "").lower())
    return {w for w in words if w not in _STOPWORDS}


def _is_relevant(query: str, *texts: str, min_overlap: int = 1) -> bool:
    """Heuristic: does candidate text share enough meaningful keywords with the query
    to plausibly be about the same thing? This is NOT semantic understanding — it
    will not catch every irrelevant result — but it reliably rejects the kind of
    random-noise hits a full-text search fallback can return for long claim
    sentences or multi-word topics. Requires overlap scaled to query length so a
    short query still needs a real match, not just one common word."""
    q_kw = _keywords(query)
    if not q_kw:
        return True  # nothing to check against; don't block
    combined = " ".join(t for t in texts if t)
    t_kw = _keywords(combined)
    if not t_kw:
        return False
    overlap = q_kw & t_kw
    needed = max(min_overlap, min(2, len(q_kw)))
    return len(overlap) >= needed


def _filter_relevant(query: str, results: list, text_fields=("title", "snippet")) -> list:
    """Drop results that fail the relevance check against query."""
    return [r for r in results if _is_relevant(query, *[r.get(f, "") for f in text_fields])]


# ══════════════════════════════════════════════════════════════════════════════
#  LLM-BASED CLAIM JUDGING (Gemini)
#  The keyword-overlap relevance filter above is a cheap pre-filter, but it
#  cannot resolve genuine semantic ambiguity — e.g. a Wikipedia article about
#  Goofy mentioning his classic animated "shorts" will keyword-match a claim
#  about "YouTube Shorts" even though they are unrelated meanings of the same
#  word. Judging whether a source actually supports/refutes/doesn't address a
#  claim requires reading and understanding it, which needs an LLM call, not
#  more keyword rules. This is optional — if GEMINI_API_KEY isn't configured,
#  callers should fall back to the keyword heuristic.
# ══════════════════════════════════════════════════════════════════════════════


async def _gemini_judge(claim: str, source_title: str, source_content: str) -> dict:
    """Ask Gemini whether a source supports, refutes, or doesn't address a claim.
    Returns {"verdict": "SUPPORTS"|"REFUTES"|"IRRELEVANT"|"UNKNOWN", "reason": str}.
    UNKNOWN means the call failed/couldn't be parsed — callers should treat that
    like "no usable evidence", never like support."""
    if not GEMINI_API_KEY:
        return {"verdict": "UNKNOWN", "reason": "GEMINI_API_KEY not configured"}

    prompt = (
        "You are fact-checking a single claim against ONE source. Be strict: a source only "
        "\"SUPPORTS\" the claim if it actually contains information bearing on this specific "
        "claim's subject — not just because it shares a word with it (e.g. a source about "
        "Disney's animated \"shorts\" does NOT support a claim about \"YouTube Shorts\" even "
        "though both contain the word \"shorts\" — different meanings, different subject).\n\n"
        f'Claim: "{claim}"\n\n'
        f"Source title: {source_title}\n"
        f"Source content (excerpt):\n{(source_content or '')[:2000]}\n\n"
        "Reply in EXACTLY this format, nothing else:\n"
        "VERDICT: <SUPPORTS or REFUTES or IRRELEVANT>\n"
        "REASON: <one short sentence>"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 250},
    }
    # Google has retired Gemini free-tier model names more than once during
    # development of this tool (gemini-2.5-flash, then gemini-2.0-flash both
    # 404'd with "no longer available" errors pointing at paid-tier
    # replacements) — try the configured model, then one more known-current
    # free-tier name, before giving up.
    models_to_try = [GEMINI_MODEL] + (["gemini-3.6-flash"] if GEMINI_MODEL != "gemini-3.6-flash" else [])
    last_err = ""
    rate_limited = False
    try:
        async with httpx.AsyncClient() as c:
            res = None
            for model in models_to_try:
                res = await c.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                    params={"key": GEMINI_API_KEY},
                    json=body,
                    timeout=20,
                )
                if res.status_code == 200:
                    break
                if res.status_code == 429:
                    # Quota/rate limit — trying another model name won't help,
                    # it's the same key's quota. Stop here so the caller can
                    # surface this distinctly (so you know to rotate keys)
                    # rather than looking like a generic failure.
                    rate_limited = True
                    last_err = f"model={model} status=429 body={res.text[:300]}"
                    break
                last_err = f"model={model} status={res.status_code} body={res.text[:300]}"
        if rate_limited:
            return {"verdict": "UNKNOWN", "reason": f"RATE_LIMITED: Gemini free-tier quota exceeded — rotate to a different GEMINI_API_KEY. Detail: {last_err}"}
        if res is None or res.status_code != 200:
            return {"verdict": "UNKNOWN", "reason": f"Gemini API error: {last_err}"}
        data = res.json()
        text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        m = re.search(r"VERDICT:\s*(SUPPORTS|REFUTES|IRRELEVANT)", text, re.IGNORECASE)
        reason_m = re.search(r"REASON:\s*(.+)", text)
        verdict = m.group(1).upper() if m else "UNKNOWN"
        reason = reason_m.group(1).strip() if reason_m else text.strip()[:200]
        # Guard against two ways this can come out unreadable in a markdown
        # report: (1) Gemini occasionally echoes the prompt's own
        # "<one short sentence>" placeholder verbatim instead of writing a
        # real reason; (2) any literal "<"/">" in the reason gets swallowed
        # by markdown/HTML renderers (observed as a reason truncated to a
        # bare "<"). Strip angle brackets and fall back to a clear message
        # when nothing usable is left.
        reason = re.sub(r"[<>]", "", reason).strip()
        if not reason or reason.lower() in ("one short sentence", ""):
            reason = f"Gemini returned no usable reason text (raw: {text.strip()[:150]!r})" if text.strip() else "Gemini returned an empty response (no candidates — possibly blocked by safety filters or hit max output tokens)."
        return {"verdict": verdict, "reason": reason}
    except Exception as e:
        return {"verdict": "UNKNOWN", "reason": f"Gemini call failed: {type(e).__name__}: {e}" if str(e) else f"Gemini call failed: {type(e).__name__} (no further detail — likely a timeout)"}


# ══════════════════════════════════════════════════════════════════════════════
#  UNIFIED SEARCH
# ══════════════════════════════════════════════════════════════════════════════


async def unified_search(query: str, max_results: int = 5, language: str = "en", include_wikipedia: bool = True) -> list:
    """Run all configured engines in parallel, merge & deduplicate."""
    tasks = [_duckduckgo_search(query, max_results)]
    if include_wikipedia:
        tasks.append(search_wikipedia(query, language, 5))
    if GOOGLE_API_KEY and GOOGLE_CX:
        tasks.append(_google_search(query, max_results, language))
    if SERPER_API_KEY:
        tasks.append(_serper_search(query, max_results, language))
    if TAVILY_API_KEY:
        tasks.append(_tavily_search(query, max_results))

    all_arrays = await asyncio.gather(*tasks, return_exceptions=True)

    seen = set()
    merged = []
    for arr in all_arrays:
        if isinstance(arr, Exception):
            continue
        for r in arr:
            norm = r["url"].rstrip("/").replace("https://www.", "https://")
            if norm not in seen:
                seen.add(norm)
                merged.append(r)
    return merged[: max_results + 2]


async def multi_search(queries: list, max_results: int = 5, language: str = "en") -> list:
    """Run multiple queries in batches, deduplicate."""
    batch_size = 3
    all_results = []
    seen = set()
    for i in range(0, len(queries), batch_size):
        batch = queries[i : i + batch_size]
        batch_res = await asyncio.gather(
            *[unified_search(q, max_results, language) for q in batch],
            return_exceptions=True,
        )
        for arr in batch_res:
            if isinstance(arr, Exception):
                continue
            for r in arr:
                if r["url"] not in seen:
                    seen.add(r["url"])
                    all_results.append(r)
        if i + batch_size < len(queries):
            await asyncio.sleep(0.15)
    return all_results


# ══════════════════════════════════════════════════════════════════════════════
#  RESEARCH TOOL DEFINITIONS (MCP schema)
# ══════════════════════════════════════════════════════════════════════════════

RESEARCH_TOOLS = [
    {
        "name": "deep_research",
        "description": "Multi-source deep research on any topic. Searches web + Wikipedia, fetches page content, returns synthesized findings with citations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Research topic or question"},
                "num_sources": {"type": "integer", "description": "Number of sources to fetch (1-10, default 5)"},
                "language": {"type": "string", "description": "Language code (en, de, fr, etc.)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "verify_claims",
        "description": "Verify an array of factual claims against multiple web sources. Returns VERIFIED / PARTIALLY VERIFIED / UNVERIFIED for each claim with supporting sources.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "claims": {"type": "array", "items": {"type": "string"}, "description": "Array of factual claims to verify (1-20)"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["claims"],
        },
    },
    {
        "name": "find_primary_sources",
        "description": "Find primary, original, and academic sources for a historical topic. Prioritizes .gov, .edu, archives, museums, and scholarly sites.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Historical topic, event, or person"},
                "time_period": {"type": "string", "description": "Time period e.g. '1939-1945', 'WW1'"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "get_historical_context",
        "description": "Get comprehensive historical context: Wikipedia article, sections, timeline, categories, and supplementary web sources.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Person, event, or historical topic"},
                "language": {"type": "string", "description": "Language code (en, de, fr, etc.)"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "check_content_saturation",
        "description": "Check how saturated a topic is on the web and Wikipedia. Helps decide if a topic has room for new content or is already overdone.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Topic to check"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "cross_reference",
        "description": "Cross-reference a single claim across 3+ independent sources. Fetches full page content from each source for comparison.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "claim": {"type": "string", "description": "The claim or fact to cross-reference"},
                "min_sources": {"type": "integer", "description": "Minimum independent sources (2-6, default 3)"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["claim"],
        },
    },
    {
        "name": "extract_story_angles",
        "description": "Discover unexplored or underexplored angles for a topic by analyzing existing web and Wikipedia coverage, then identifying gaps.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Topic to find new angles for"},
                "existing_angles": {"type": "array", "items": {"type": "string"}, "description": "Angles already covered (to avoid repetition)"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "get_wikipedia_data",
        "description": "Get structured Wikipedia data: full article text, sections, categories, and external references. Supports any language (en, de, fr, etc.).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "Article title or search term"},
                "language": {"type": "string", "description": "Language code"},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "research_api_status",
        "description": "Check the status and availability of all configured research search engines (Wikipedia, DuckDuckGo, Google, Serper, Tavily).",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ══════════════════════════════════════════════════════════════════════════════
#  RESEARCH TOOL HANDLERS
# ══════════════════════════════════════════════════════════════════════════════


async def _h_deep_research(args: dict) -> dict:
    query = args["query"]
    num = min(int(args.get("num_sources", 5)), 10)
    lang = args.get("language", "en")

    results = await unified_search(query, max_results=num, language=lang)

    sources = []
    for r in results[:num]:
        content = await fetch_page_content(r["url"], 4000)
        sources.append({**r, "fullContent": content or r.get("snippet", "")})

    wiki = await search_wikipedia(query, lang, 1)
    if wiki:
        article = await get_wikipedia_article(wiki[0]["title"], lang)
        if article and not any(s["url"] == article["url"] for s in sources):
            sources.append({
                "title": article["title"], "url": article["url"],
                "snippet": (article.get("content") or "")[:300],
                "fullContent": (article.get("content") or "")[:4000],
                "source": "wikipedia",
            })

    out = f"## Deep Research: {query}\n\n**Sources Analyzed:** {len(sources)}\n**Language:** {lang}\n\n"
    for i, s in enumerate(sources):
        out += f"---\n### [{i+1}] {s['title']}\n**URL:** {s['url']}\n**Engine:** {s.get('source','unknown')}\n\n"
        out += (s.get("fullContent") or "No content extracted.") + "\n\n"
    out += "---\n## Source List\n"
    for i, s in enumerate(sources):
        out += f"[{i+1}] {s['title']} — {s['url']}\n"
    return {"report": out, "sources_count": len(sources)}


async def _check_one_source(claim: str, r: dict, use_llm: bool) -> dict:
    """Fetch + judge a single source for a single claim. Independent of every
    other source/claim, so callers run many of these concurrently instead of
    one at a time — the previous fully-sequential loop (fetch + Gemini call
    per source, one after another) was the main cause of verify_claims
    sometimes taking minutes on just a handful of claims."""
    content = await fetch_page_content(r["url"], 2000)
    has_content = bool(content and len(content) > 100)
    if use_llm:
        judged = await _gemini_judge(claim, r.get("title", ""), content or r.get("snippet", ""))
        verdict = judged["verdict"]
        reason = judged["reason"]
    else:
        # Fallback heuristic (keyword overlap only) — known to be fooled
        # by ambiguous terms; still better than nothing when no LLM key
        # is configured.
        is_relevant = _is_relevant(claim, r.get("title", ""), r.get("snippet", ""), content or "")
        verdict = "SUPPORTS" if (has_content and is_relevant) else "IRRELEVANT"
        reason = "No LLM configured — keyword-overlap heuristic only (less reliable)."
    return {
        "title": r["title"], "url": r["url"], "snippet": r.get("snippet", ""),
        "hasContent": has_content,
        "verdict": verdict,
        "reason": reason,
        "contentPreview": ((content or r.get("snippet") or "")[:500]),
    }


async def _check_one_claim(claim: str, lang: str, use_llm: bool) -> dict:
    raw_results = await unified_search(claim, max_results=5, language=lang, include_wikipedia=True)
    # Cheap keyword pre-filter to drop obvious noise before spending an LLM
    # call on it. This alone is NOT sufficient (see _gemini_judge's docstring
    # for why — ambiguous words like "shorts" defeat keyword matching), so
    # when GEMINI_API_KEY is set we additionally judge each surviving result
    # with an LLM read of its actual content.
    results = _filter_relevant(claim, raw_results)[:3]
    checked = list(await asyncio.gather(
        *[_check_one_source(claim, r, use_llm) for r in results],
        return_exceptions=False,
    )) if results else []
    supports = sum(1 for c in checked if c["verdict"] == "SUPPORTS")
    refutes = sum(1 for c in checked if c["verdict"] == "REFUTES")

    if not raw_results or not results:
        status, icon = "UNVERIFIED — no relevant sources found", "❌"
    elif refutes > 0 and refutes >= supports:
        status, icon = "CONTRADICTED", "🔴"
    elif supports >= 2:
        status, icon = "VERIFIED", "✅"
    elif supports == 1:
        status, icon = "PARTIALLY VERIFIED", "⚠️"
    else:
        status, icon = "UNVERIFIED", "❌"
    return {"claim": claim, "status": status, "icon": icon, "sources": checked}


async def _h_verify_claims(args: dict) -> dict:
    claims = args["claims"][:20]
    lang = args.get("language", "en")
    use_llm = bool(GEMINI_API_KEY)
    # Claims are independent of each other too, so run them concurrently in
    # small batches (same pattern as multi_search) rather than one at a time.
    report = []
    batch_size = 4
    for i in range(0, len(claims), batch_size):
        batch = claims[i:i + batch_size]
        batch_report = await asyncio.gather(*[_check_one_claim(c, lang, use_llm) for c in batch])
        report.extend(batch_report)

    verified = sum(1 for r in report if r["status"] == "VERIFIED")
    partial = sum(1 for r in report if r["status"] == "PARTIALLY VERIFIED")
    contradicted = sum(1 for r in report if r["status"] == "CONTRADICTED")
    unverified = sum(1 for r in report if r["status"].startswith("UNVERIFIED"))
    rate_limited = any(
        "RATE_LIMITED" in (s.get("reason") or "")
        for r in report for s in r["sources"]
    )

    out = f"## Claim Verification Report\n\n**Total Claims:** {len(claims)}\n"
    out += (f"✅ Verified: {verified} | ⚠️ Partial: {partial} | 🔴 Contradicted: {contradicted} | "
            f"❌ Unverified: {unverified}\n")
    if rate_limited:
        out += "\n🛑 **GEMINI RATE LIMITED:** Your GEMINI_API_KEY hit its free-tier quota during this check — the verdicts below are degraded (treated as UNKNOWN, not false positives). Rotate to a different GEMINI_API_KEY now, or call `research_api_status` to confirm.\n"
    if not use_llm:
        out += "\n⚠️ **Note:** GEMINI_API_KEY not configured — using a weaker keyword-overlap heuristic instead of real LLM judgment. Verdicts here are less reliable.\n"
    out += "\n"
    for r in report:
        out += f"### {r['icon']} {r['status']}\n**Claim:** \"{r['claim']}\"\n**Sources:**\n"
        for s in r["sources"]:
            out += f"- [{s['title']}]({s['url']}) — **{s['verdict']}**: {s.get('reason', '')}\n"
        out += "\n"
    return {"report": out, "verified": verified, "partial": partial, "contradicted": contradicted, "unverified": unverified}


async def _h_find_primary_sources(args: dict) -> dict:
    topic = args["topic"]
    tp = f" {args['time_period']}" if args.get("time_period") else ""
    lang = args.get("language", "en")

    queries = [
        f"{topic} primary source document{tp}",
        f"{topic} original records archive{tp}",
        f"{topic} historical documents{tp}",
    ]
    all_results = await multi_search(queries, max_results=4, language=lang)

    scored = []
    for r in all_results:
        score = 0
        u = r["url"].lower()
        if ".gov" in u: score += 4
        if ".edu" in u: score += 4
        if "archive.org" in u or "archives" in u: score += 3
        if "museum" in u: score += 3
        if "library" in u: score += 2
        if "jstor" in u or "scholar.google" in u: score += 3
        if "wikipedia" in u: score += 1
        if "bundesarchiv" in u or "nationalarchives" in u: score += 4
        scored.append({**r, "reliabilityScore": score})
    scored.sort(key=lambda x: x["reliabilityScore"], reverse=True)

    wiki = await search_wikipedia(topic, lang, 1)
    wiki_refs = []
    if wiki:
        wiki_refs = await get_wikipedia_references(wiki[0]["title"], lang)

    out = f"## Primary Sources: {topic}\n\n"
    if args.get("time_period"):
        out += f"**Time Period:** {args['time_period']}\n\n"
    out += "### Ranked by Source Reliability\n\n"
    for i, s in enumerate(scored[:12]):
        u = s["url"].lower()
        label = ("🏛️ Gov" if ".gov" in u else "🎓 Academic" if ".edu" in u else "📚 Archive" if "archive" in u
                 else "🏛️ Museum" if "museum" in u else "📖 Encyclopedia" if "wikipedia" in u else "🌐 Web")
        out += f"{i+1}. **{s['title']}** [{label}]\n   {s['url']}\n"
        if s.get("snippet"):
            out += f"   {s['snippet']}\n"
        out += "\n"
    if wiki_refs:
        out += f"### Wikipedia External References ({len(wiki_refs)} total)\n"
        for ref in wiki_refs[:15]:
            out += f"- {ref}\n"
    return {"report": out, "sources_count": len(scored[:12])}


async def _h_get_historical_context(args: dict) -> dict:
    topic = args["topic"]
    lang = args.get("language", "en")

    wiki = await search_wikipedia(topic, lang, 1)
    article, sections = None, None
    if wiki:
        article, sections = await asyncio.gather(
            get_wikipedia_article(wiki[0]["title"], lang),
            get_wikipedia_sections(wiki[0]["title"], lang),
        )

    web_results = await unified_search(f"{topic} timeline chronology", max_results=3, language=lang, include_wikipedia=False)
    web_content = []
    for r in web_results[:2]:
        content = await fetch_page_content(r["url"], 3000)
        if content:
            web_content.append({"title": r["title"], "url": r["url"], "content": content})

    out = f"## Historical Context: {topic}\n\n"
    if article:
        out += f"### Wikipedia — {article['title']}\n**URL:** {article['url']}\n\n"
        out += (article.get("content") or "No content.")[:5000] + "\n\n"
        if sections and sections.get("sections"):
            out += "### Article Structure\n"
            for s in sections["sections"]:
                out += f"{'  ' * max(0, s['level'] - 2)}- {s['heading']}\n"
            out += "\n"
        if article.get("categories"):
            out += "### Categories\n"
            for c in article["categories"][:12]:
                out += f"- {c}\n"
            out += "\n"
        if sections and sections.get("externalLinks"):
            out += f"### External Links ({len(sections['externalLinks'])})\n"
            for link in sections["externalLinks"][:10]:
                out += f"- {link}\n"
            out += "\n"
    else:
        out += f'*No Wikipedia article found for "{topic}" in {lang}.*\n\n'

    if web_content:
        out += "### Supplementary Web Sources\n\n"
        for wc in web_content:
            out += f"**[{wc['title']}]({wc['url']})**\n{wc['content'][:2000]}\n\n"
    return {"report": out}


async def _h_check_content_saturation(args: dict) -> dict:
    topic = args["topic"]
    lang = args.get("language", "en")

    web_results_raw, yt_results_raw, wiki_results_raw = await asyncio.gather(
        unified_search(topic, max_results=10, language=lang, include_wikipedia=False),
        unified_search(f"{topic} YouTube video", max_results=5, language=lang, include_wikipedia=False),
        search_wikipedia(topic, lang, 5),
    )

    # Only count results that are actually about the topic — the Wikipedia
    # full-text fallback in particular can return unrelated pages for a
    # multi-word topic phrase when no paid search engine is configured, which
    # previously inflated the "coverage" count with noise (e.g. unrelated
    # pages counted as 5 "results" for an unrelated niche).
    web_results = _filter_relevant(topic, web_results_raw)
    yt_results = _filter_relevant(topic, yt_results_raw)
    wiki_results = _filter_relevant(topic, wiki_results_raw)

    total = len(web_results) + len(yt_results) + len(wiki_results)
    engines_with_real_results = sum(1 for r in (web_results, yt_results) if r)
    data_quality_warning = ""

    if not web_results_raw and not yt_results_raw:
        # DuckDuckGo (and any paid engine, if configured) returned nothing at
        # all — the only signal available is Wikipedia, which is not a proxy
        # for YouTube content saturation. Don't issue a confident verdict off
        # of that alone.
        level = "UNKNOWN"
        advice = ("Could not get real web/YouTube search results for this topic (search engines returned "
                   "nothing — likely blocked, or SERPER_API_KEY/TAVILY_API_KEY/GOOGLE_API_KEY not configured). "
                   "Wikipedia coverage alone cannot tell you how saturated this niche is on YouTube. "
                   "Verify manually or configure a search API key and retry.")
    elif total >= 18:
        level, advice = "HIGH", "Topic is heavily covered. Need a unique angle, untold sub-story, or niche perspective to stand out."
    elif total >= 10:
        level, advice = "MEDIUM", "Some coverage exists but room for new content with a fresh angle."
    elif total >= 4:
        level, advice = "LOW", "Limited coverage — good opportunity for new content. Verify audience demand."
    else:
        level, advice = "VERY LOW", "Almost no coverage — either untapped opportunity or no audience demand. Research search volume."

    if engines_with_real_results == 0 and level != "UNKNOWN":
        data_quality_warning = ("\n⚠️ **Data quality note:** this verdict is based only on Wikipedia results "
                                 "(web/YouTube search engines returned nothing relevant) and may be unreliable.\n")

    out = f"## Content Saturation: {topic}\n\n**Saturation Level:** {level}\n**Total Results:** {total}\n**Assessment:** {advice}\n{data_quality_warning}\n"
    out += f"### Web Coverage ({len(web_results)} results)\n"
    for r in web_results[:5]:
        out += f"- [{r['title']}]({r['url']})\n"
    out += f"\n### YouTube Coverage ({len(yt_results)} results)\n"
    for r in yt_results[:5]:
        out += f"- [{r['title']}]({r['url']})\n"
    out += f"\n### Wikipedia Coverage ({len(wiki_results)} results)\n"
    for r in wiki_results:
        out += f"- [{r['title']}]({r['url']})\n"
    return {"report": out, "saturation": level, "total": total}


async def _h_cross_reference(args: dict) -> dict:
    claim = args["claim"]
    min_src = min(int(args.get("min_sources", 3)), 6)
    lang = args.get("language", "en")

    raw_results = await unified_search(claim, max_results=min_src + 4, language=lang)
    results = _filter_relevant(claim, raw_results)[: min_src + 2]
    sources = []
    for r in results:
        content = await fetch_page_content(r["url"], 2500)
        sources.append({
            "title": r["title"], "url": r["url"], "snippet": r.get("snippet", ""),
            "content": content or r.get("snippet") or "", "engine": r.get("source", "unknown"),
        })

    source_types = list({s["engine"] for s in sources})
    independence_warning = ""
    if len(source_types) <= 1 and sources:
        independence_warning = (f"\n⚠️ **Note:** all sources came from a single engine ({source_types[0]}) — "
                                 "this is not an independent cross-reference. Configure SERPER_API_KEY/"
                                 "TAVILY_API_KEY/GOOGLE_API_KEY for real source diversity.\n")

    out = f"## Cross-Reference Report\n\n**Claim:** \"{claim}\"\n**Sources Found:** {len(sources)}\n**Source Types:** {', '.join(source_types) or 'none'}\n{independence_warning}\n"
    for i, s in enumerate(sources):
        out += f"---\n### Source {i+1}: {s['title']}\n**URL:** {s['url']} *({s['engine']})*\n\n"
        out += s["content"][:1500] + "\n\n"
    out += f"---\n\n**Verification Notes:**\n- {len(sources)} sources checked for topical relevance\n- Source diversity: {len(source_types)} different engines\n- Review content above to confirm agreement or find contradictions\n"
    return {"report": out, "sources_count": len(sources)}


async def _h_extract_story_angles(args: dict) -> dict:
    topic = args["topic"]
    existing = args.get("existing_angles", [])
    lang = args.get("language", "en")

    queries = [topic, f"{topic} untold story", f"{topic} lesser known facts", f"{topic} different perspective", f"{topic} controversy"]
    all_results = await multi_search(queries, max_results=3, language=lang)

    with_content = []
    for r in all_results[:8]:
        content = await fetch_page_content(r["url"], 2000)
        with_content.append({"title": r["title"], "url": r["url"], "snippet": r.get("snippet", ""), "content": content or r.get("snippet") or ""})

    wiki = await search_wikipedia(topic, lang, 1)
    wiki_sections = None
    if wiki:
        wiki_sections = await get_wikipedia_sections(wiki[0]["title"], lang)

    out = f"## Story Angle Discovery: {topic}\n\n"
    if existing:
        out += "### Already Covered (avoid)\n"
        for a in existing:
            out += f"- ❌ {a}\n"
        out += "\n"
    if wiki_sections and wiki_sections.get("sections"):
        out += "### Wikipedia Sections (commonly covered angles)\n"
        for s in wiki_sections["sections"]:
            out += f"{'  ' * max(0, s['level'] - 2)}- {s['heading']}\n"
        out += "\n"
    out += f"### Sources Found ({len(with_content)})\n\n"
    for s in with_content:
        out += f"**[{s['title']}]({s['url']})**\n{(s['content'] or s['snippet'] or '')[:300]}\n\n"
    out += "### Search Strategy Used\n"
    for q in queries:
        out += f'- "{q}"\n'
    out += "\nAnalyze the sources above against the Wikipedia sections to spot underexplored angles.\n"
    return {"report": out, "sources_count": len(with_content)}


async def _h_get_wikipedia_data(args: dict) -> dict:
    topic = args["topic"]
    lang = args.get("language", "en")

    results = await search_wikipedia(topic, lang, 3)
    if not results:
        return {"topic": topic, "found": False, "report": f'No Wikipedia article found for "{topic}" in language "{lang}".'}

    best = results[0]
    article, sections, refs = await asyncio.gather(
        get_wikipedia_article(best["title"], lang),
        get_wikipedia_sections(best["title"], lang),
        get_wikipedia_references(best["title"], lang),
    )

    out = f"## Wikipedia: {best['title']}\n\n**URL:** {best['url']}\n**Language:** {lang}\n\n"
    if article:
        out += f"### Content\n\n{(article.get('content') or 'No content.')[:6000]}\n\n"
        if article.get("categories"):
            out += f"### Categories ({len(article['categories'])})\n"
            for c in article["categories"][:15]:
                out += f"- {c}\n"
            out += "\n"
    if sections and sections.get("sections"):
        out += "### Sections\n"
        for s in sections["sections"]:
            out += f"{'  ' * max(0, s['level'] - 2)}- {s['heading']}\n"
        out += "\n"
    if refs:
        out += f"### External References ({len(refs)} total, showing 20)\n"
        for ref in refs[:20]:
            out += f"- {ref}\n"
    if len(results) > 1:
        out += "\n### Related Articles\n"
        for r in results[1:]:
            out += f"- [{r['title']}]({r['url']}) — {r.get('snippet','')}\n"
    return {"topic": topic, "found": True, "report": out}


async def _gemini_health_check() -> dict:
    """Make one minimal, cheap generateContent call to find out Gemini's
    actual live status — not configured / working / rate-limited (quota hit,
    time to rotate keys) / model not found (deprecated) / other error."""
    if not GEMINI_API_KEY:
        return {
            "status": "➖ Not set",
            "note": "Optional — set GEMINI_API_KEY for real claim-verification (used by verify_claims)",
        }
    body = {
        "contents": [{"parts": [{"text": "Reply with exactly one word: OK"}]}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 5},
    }
    try:
        async with httpx.AsyncClient() as c:
            res = await c.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
                params={"key": GEMINI_API_KEY},
                json=body,
                timeout=15,
            )
        if res.status_code == 200:
            return {"status": f"✅ Working ({GEMINI_MODEL})", "note": "Live-checked just now — key is good and quota has room."}
        if res.status_code == 429:
            return {
                "status": "🛑 RATE_LIMITED",
                "note": f"Free-tier quota exceeded on this key ({GEMINI_MODEL}) — rotate to a different GEMINI_API_KEY now.",
            }
        if res.status_code == 404:
            return {
                "status": "⚠️ Model not found",
                "note": f"'{GEMINI_MODEL}' returned 404 — Google may have retired/renamed this model. Check which free-tier model name is current and set GEMINI_MODEL.",
            }
        if res.status_code in (401, 403):
            return {"status": "❌ Invalid key", "note": f"Status {res.status_code} — key may be wrong, disabled, or missing permissions. Detail: {res.text[:200]}"}
        return {"status": f"❌ Error {res.status_code}", "note": res.text[:200]}
    except Exception as e:
        # str(e) is empty for several common httpx exceptions (e.g. a bare
        # ReadTimeout/ConnectTimeout), which previously rendered as a
        # content-free "note": "" — always include the exception type so
        # there's something to act on (e.g. "ReadTimeout" => the call is
        # timing out, not necessarily a bad key).
        detail = f"{type(e).__name__}: {e}" if str(e) else f"{type(e).__name__} (no further detail — likely a timeout/connection issue reaching Gemini)"
        return {"status": "❌ Call failed", "note": detail}


async def _h_research_api_status(_args: dict) -> dict:
    engines = {
        "wikipedia": {"status": "✅ Always on", "note": "Free API, no key needed. Rate-limited + cached.", "cache_size": len(_wiki_cache)},
        "duckduckgo": {"status": "✅ Always on" if HAS_BS4 else "⚠️ beautifulsoup4 not installed", "note": "HTML scrape, no key needed. May be blocked from some server IPs."},
        "google_custom_search": {
            "status": "✅ Configured" if (GOOGLE_API_KEY and GOOGLE_CX) else "➖ Not set",
            "note": "100 free queries/day" if (GOOGLE_API_KEY and GOOGLE_CX) else "Set GOOGLE_API_KEY + GOOGLE_CX for Google results",
        },
        "serper": {
            "status": "✅ Configured" if SERPER_API_KEY else "➖ Not set",
            "note": "Google SERP via serper.dev" if SERPER_API_KEY else "Optional — set SERPER_API_KEY",
        },
        "tavily": {
            "status": "✅ Configured" if TAVILY_API_KEY else "➖ Not set",
            "note": "AI research search" if TAVILY_API_KEY else "Optional — set TAVILY_API_KEY",
        },
        "gemini": await _gemini_health_check(),
    }
    out = "## Research Server — API Status\n\n| Engine | Status | Details |\n|--------|--------|--------|\n"
    for name, info in engines.items():
        out += f"| {name} | {info['status']} | {info['note']} |\n"
    out += "\n⚠️ If `gemini` shows RATE_LIMITED, verify_claims will fall back to a less reliable keyword check until you add a fresh GEMINI_API_KEY.\n"
    return {"engines": engines, "report": out}


# ══════════════════════════════════════════════════════════════════════════════
#  DISPATCHER — called from main.py's call_tool()
# ══════════════════════════════════════════════════════════════════════════════

_RESEARCH_HANDLERS = {
    "deep_research": _h_deep_research,
    "verify_claims": _h_verify_claims,
    "find_primary_sources": _h_find_primary_sources,
    "get_historical_context": _h_get_historical_context,
    "check_content_saturation": _h_check_content_saturation,
    "cross_reference": _h_cross_reference,
    "extract_story_angles": _h_extract_story_angles,
    "get_wikipedia_data": _h_get_wikipedia_data,
    "research_api_status": _h_research_api_status,
}

RESEARCH_TOOL_NAMES = set(_RESEARCH_HANDLERS.keys())


async def handle_research_tool(name: str, arguments: dict) -> Any:
    """Dispatch a research tool call. Returns result dict or raises."""
    handler = _RESEARCH_HANDLERS.get(name)
    if not handler:
        return {"error": f"Unknown research tool: {name}"}
    try:
        return await handler(arguments)
    except Exception as e:
        return {"error": f"Research tool error ({name}): {str(e)}"}
