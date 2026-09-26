import asyncio
import json
import httpx
import os
from datetime import datetime, timezone, timedelta
from typing import Any
from statistics import median
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import uuid

app = FastAPI(title="YouTube Research MCP Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyCiKq99ECwtFX98T7dpTNM0BiOIpLXxBLE")
YT_BASE = "https://www.googleapis.com/youtube/v3"
PROXY_URL = os.environ.get("PROXY_URL", "")  # e.g. "http://user:pass@proxy.host:port"
WEBSHARE_USER = os.environ.get("WEBSHARE_USER", "")
WEBSHARE_PASS = os.environ.get("WEBSHARE_PASS", "")

# ─── MCP TOOL DEFINITIONS ────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_channel_stats",
        "description": "Get subscriber count, total views, video count and average views per video for a YouTube channel.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "YouTube channel ID"}
            },
            "required": ["channel_id"]
        }
    },
    {
        "name": "get_channel_videos",
        "description": "Get recent or popular videos from a channel (up to 100). Returns views, VHSP, outlier score, likes, comments, relative pace vs channel baseline, momentum band (STRONG/MODERATE/NORMAL/DECLINING), thumbnail URL, and 28-day flag. Includes channel subscriber count.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "YouTube channel ID"},
                "sort_by": {"type": "string", "enum": ["newest", "popular"], "default": "newest"},
                "max_results": {"type": "integer", "default": 50, "maximum": 100}
            },
            "required": ["channel_id"]
        }
    },
    {
        "name": "get_channel_outliers",
        "description": "Find videos that overperform relative to channel average. Returns outlier score, relative pace, momentum band, subscriber count. Use for Prompt A Bucket 1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "YouTube channel ID"},
                "min_outlier_score": {"type": "number", "default": 2.0},
                "within_days": {"type": "integer", "default": 28},
                "max_videos": {"type": "integer", "default": 50, "maximum": 100}
            },
            "required": ["channel_id"]
        }
    },
    {
        "name": "search_youtube",
        "description": "Search YouTube for videos matching a query. Returns videos with view counts, VHSP, likes, thumbnail. Use for Buckets 2, 4, 5.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "published_after_days": {"type": "integer", "default": 28},
                "max_results": {"type": "integer", "default": 25},
                "min_duration": {"type": "string", "enum": ["short", "medium", "long"], "default": "medium"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_video_comments",
        "description": "Get high-engagement comments filtered by minimum likes. Use for Bucket 3 gap mining — find requests and frustrations with 10+ likes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "video_id": {"type": "string", "description": "YouTube video ID"},
                "min_likes": {"type": "integer", "default": 10},
                "max_results": {"type": "integer", "default": 100}
            },
            "required": ["video_id"]
        }
    },
    {
        "name": "get_video_details",
        "description": "Get full stats for a specific video: views, likes, comments, duration, VHSP, tags, thumbnail.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "video_id": {"type": "string", "description": "YouTube video ID"}
            },
            "required": ["video_id"]
        }
    },
    {
        "name": "yt_keyword_research",
        "description": "Research a keyword for YouTube using YOUR OWN server (not vidIQ). Returns autocomplete suggestions, competition count (how many videos exist), top video views (demand proof), and a keyword score. FREE — no credits needed. Use before finalizing titles.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Keyword or phrase to research (e.g. 'cartel documentary', 'financial fraud')"},
                "max_suggestions": {"type": "integer", "default": 8, "description": "How many autocomplete suggestions to return"}
            },
            "required": ["keyword"]
        }
    },
    {
        "name": "yt_generate_titles",
        "description": "Generate optimized YouTube title variations for a topic. Analyzes top-performing videos on YouTube for that topic, extracts winning patterns, and generates 10 data-backed title options with scores. FREE — no credits needed.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "The subject/person/event to make a video about (e.g. 'Pablo Escobar', 'Baker Street Robbery', 'Viktor Bout')"},
                "niche": {"type": "string", "default": "documentary", "description": "Your channel niche for tone matching (e.g. 'crime documentary', 'psychology', 'horror', 'business')"},
                "max_titles": {"type": "integer", "default": 10, "description": "How many title variations to generate (max 15)"}
            },
            "required": ["topic"]
        }
    },
    {
        "name": "yt_get_video_transcript",
        "description": "Get the full transcript/captions of a YouTube video using YOUR OWN server (not NexLev). Returns the complete text. FREE — no credits or weekly limits. Useful for analyzing competitor scripts and content structure.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "video_id": {"type": "string", "description": "YouTube video ID"},
                "language": {"type": "string", "default": "en", "description": "Language code (default: en)"}
            },
            "required": ["video_id"]
        }
    }
]

# ─── YOUTUBE API HELPERS ─────────────────────────────────────────────────────

async def yt_get(endpoint: str, params: dict) -> dict:
    params["key"] = API_KEY
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{YT_BASE}/{endpoint}", params=params)
        r.raise_for_status()
        return r.json()

def calc_momentum_band(relative_pace: float) -> str:
    if relative_pace >= 3.0:
        return "STRONG MOMENTUM"
    elif relative_pace >= 1.5:
        return "MODERATE MOMENTUM"
    elif relative_pace >= 0.5:
        return "NORMAL"
    else:
        return "DECLINING"

def get_thumbnail(snippet: dict, video_id: str) -> str:
    thumbnails = snippet.get("thumbnails", {})
    return (
        thumbnails.get("maxres", {}).get("url") or
        thumbnails.get("high", {}).get("url") or
        thumbnails.get("medium", {}).get("url") or
        f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    )

async def tool_get_channel_stats(channel_id: str) -> dict:
    data = await yt_get("channels", {"part": "statistics,snippet", "id": channel_id})
    items = data.get("items", [])
    if not items:
        return {"error": "Channel not found"}
    item = items[0]
    stats = item["statistics"]
    return {
        "channel_id": channel_id,
        "title": item["snippet"]["title"],
        "subscribers": int(stats.get("subscriberCount", 0)),
        "total_views": int(stats.get("viewCount", 0)),
        "video_count": int(stats.get("videoCount", 0)),
        "avg_views_per_video": int(stats.get("viewCount", 0)) // max(int(stats.get("videoCount", 1)), 1)
    }

async def tool_get_channel_videos(channel_id: str, sort_by: str = "newest", max_results: int = 50) -> dict:
    # Fetch channel info — subscribers + uploads playlist
    ch = await yt_get("channels", {"part": "contentDetails,statistics,snippet", "id": channel_id})
    if not ch.get("items"):
        return {"error": "Channel not found"}
    ch_item = ch["items"][0]
    ch_stats = ch_item["statistics"]
    channel_avg = int(ch_stats.get("viewCount", 0)) // max(int(ch_stats.get("videoCount", 1)), 1)
    subscribers = int(ch_stats.get("subscriberCount", 0))
    channel_title = ch_item["snippet"]["title"]
    uploads_id = ch_item["contentDetails"]["relatedPlaylists"]["uploads"]

    # Pagination — fetch up to 100 videos (2 pages of 50)
    first_page_size = min(max_results, 50)
    pl = await yt_get("playlistItems", {
        "part": "contentDetails,snippet",
        "playlistId": uploads_id,
        "maxResults": first_page_size
    })
    video_ids = [i["contentDetails"]["videoId"] for i in pl.get("items", [])]

    # Second page if needed
    next_token = pl.get("nextPageToken")
    if next_token and max_results > 50:
        second_page_size = min(max_results - 50, 50)
        pl2 = await yt_get("playlistItems", {
            "part": "contentDetails,snippet",
            "playlistId": uploads_id,
            "maxResults": second_page_size,
            "pageToken": next_token
        })
        video_ids += [i["contentDetails"]["videoId"] for i in pl2.get("items", [])]

    if not video_ids:
        return {"videos": [], "channel_avg_views": channel_avg, "subscribers": subscribers, "channel_title": channel_title}

    # Fetch video stats — YouTube API allows max 50 IDs per call
    all_vid_items = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        vids = await yt_get("videos", {"part": "statistics,snippet,contentDetails", "id": ",".join(chunk)})
        all_vid_items.extend(vids.get("items", []))

    now = datetime.now(timezone.utc)
    results = []
    for v in all_vid_items:
        pub = datetime.fromisoformat(v["snippet"]["publishedAt"].replace("Z", "+00:00"))
        hours_old = max((now - pub).total_seconds() / 3600, 1)
        days_old = max(hours_old / 24, 0.1)
        views = int(v["statistics"].get("viewCount", 0))
        likes = int(v["statistics"].get("likeCount", 0))
        comment_count = int(v["statistics"].get("commentCount", 0))
        results.append({
            "video_id": v["id"],
            "title": v["snippet"]["title"],
            "published_at": v["snippet"]["publishedAt"],
            "days_old": int(days_old),
            "views": views,
            "likes": likes,
            "comment_count": comment_count,
            "vhsp": round(views / hours_old, 1),
            "daily_views": int(views / days_old),
            "outlier_score": round(views / channel_avg, 2) if channel_avg > 0 else 0,
            "duration": v["contentDetails"]["duration"],
            "within_28_days": int(days_old) <= 28,
            "thumbnail_url": get_thumbnail(v["snippet"], v["id"]),
            "video_url": f"https://www.youtube.com/watch?v={v['id']}"
        })

    if sort_by == "popular":
        results.sort(key=lambda x: x["views"], reverse=True)

    # Calculate baseline pace — median daily_views of last 10 videos (by publish date)
    by_date = sorted(results, key=lambda x: x["published_at"], reverse=True)
    baseline_videos = by_date[:10]
    baseline_daily_views_list = [v["daily_views"] for v in baseline_videos if v["daily_views"] > 0]
    baseline_pace = median(baseline_daily_views_list) if baseline_daily_views_list else 0

    # Add relative_pace and momentum_band to each video
    for v in results:
        if baseline_pace > 0:
            v["relative_pace"] = round(v["daily_views"] / baseline_pace, 2)
        else:
            v["relative_pace"] = 0
        v["momentum_band"] = calc_momentum_band(v["relative_pace"])

    return {
        "channel_id": channel_id,
        "channel_title": channel_title,
        "subscribers": subscribers,
        "channel_avg_views": channel_avg,
        "baseline_pace": int(baseline_pace),
        "baseline_note": "Median daily views of last 10 videos",
        "total_fetched": len(results),
        "videos": results
    }

async def tool_get_channel_outliers(channel_id: str, min_outlier_score: float = 2.0, within_days: int = 28, max_videos: int = 50) -> dict:
    data = await tool_get_channel_videos(channel_id=channel_id, sort_by="newest", max_results=max_videos)
    if "error" in data:
        return data
    channel_avg = data.get("channel_avg_views", 0)
    subscribers = data.get("subscribers", 0)
    channel_title = data.get("channel_title", "")
    baseline_pace = data.get("baseline_pace", 0)
    cutoff = datetime.now(timezone.utc) - timedelta(days=within_days)
    outliers = []
    for v in data.get("videos", []):
        pub = datetime.fromisoformat(v["published_at"].replace("Z", "+00:00"))
        if pub >= cutoff and v["outlier_score"] >= min_outlier_score:
            outliers.append(v)
    outliers.sort(key=lambda x: x["outlier_score"], reverse=True)
    return {
        "channel_id": channel_id,
        "channel_title": channel_title,
        "subscribers": subscribers,
        "channel_avg_views": channel_avg,
        "baseline_pace": baseline_pace,
        "outlier_threshold": min_outlier_score,
        "within_days": within_days,
        "outliers_found": len(outliers),
        "outliers": outliers
    }

async def tool_search_youtube(query: str, published_after_days: int = 28, max_results: int = 25, min_duration: str = "medium") -> dict:
    pub_after = (datetime.now(timezone.utc) - timedelta(days=published_after_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    results = await yt_get("search", {
        "part": "snippet",
        "q": query,
        "type": "video",
        "publishedAfter": pub_after,
        "maxResults": min(max_results, 50),
        "order": "viewCount",
        "videoDuration": min_duration
    })
    video_ids = [i["id"]["videoId"] for i in results.get("items", [])]
    if not video_ids:
        return {"query": query, "results": []}
    vids = await yt_get("videos", {"part": "statistics,contentDetails", "id": ",".join(video_ids)})
    stats_map = {v["id"]: v for v in vids.get("items", [])}
    now = datetime.now(timezone.utc)
    output = []
    for item in results.get("items", []):
        vid_id = item["id"]["videoId"]
        stats = stats_map.get(vid_id, {}).get("statistics", {})
        pub = datetime.fromisoformat(item["snippet"]["publishedAt"].replace("Z", "+00:00"))
        hours_old = max((now - pub).total_seconds() / 3600, 1)
        views = int(stats.get("viewCount", 0))
        output.append({
            "video_id": vid_id,
            "title": item["snippet"]["title"],
            "channel_id": item["snippet"]["channelId"],
            "channel_title": item["snippet"]["channelTitle"],
            "published_at": item["snippet"]["publishedAt"],
            "views": views,
            "vhsp": round(views / hours_old, 1),
            "likes": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "duration": stats_map.get(vid_id, {}).get("contentDetails", {}).get("duration", ""),
            "thumbnail_url": get_thumbnail(item["snippet"], vid_id),
            "video_url": f"https://www.youtube.com/watch?v={vid_id}"
        })
    output.sort(key=lambda x: x["views"], reverse=True)
    return {"query": query, "results": output}

async def tool_get_video_comments(video_id: str, min_likes: int = 10, max_results: int = 100) -> dict:
    try:
        data = await yt_get("commentThreads", {"part": "snippet", "videoId": video_id, "maxResults": min(max_results, 100), "order": "relevance"})
    except Exception:
        return {"video_id": video_id, "error": "Comments disabled or unavailable", "comments": []}
    comments = []
    for item in data.get("items", []):
        c = item["snippet"]["topLevelComment"]["snippet"]
        likes = c.get("likeCount", 0)
        if likes >= min_likes:
            comments.append({"comment_id": item["id"], "text": c["textDisplay"], "likes": likes, "published_at": c["publishedAt"], "author": c["authorDisplayName"]})
    comments.sort(key=lambda x: x["likes"], reverse=True)
    return {"video_id": video_id, "min_likes_filter": min_likes, "qualifying_comments": len(comments), "comments": comments}

async def tool_get_video_details(video_id: str) -> dict:
    data = await yt_get("videos", {"part": "statistics,snippet,contentDetails", "id": video_id})
    items = data.get("items", [])
    if not items:
        return {"error": "Video not found"}
    v = items[0]
    pub = datetime.fromisoformat(v["snippet"]["publishedAt"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    hours_old = max((now - pub).total_seconds() / 3600, 1)
    views = int(v["statistics"].get("viewCount", 0))
    tags = v["snippet"].get("tags", [])
    return {
        "video_id": video_id,
        "title": v["snippet"]["title"],
        "channel_id": v["snippet"]["channelId"],
        "channel_title": v["snippet"]["channelTitle"],
        "published_at": v["snippet"]["publishedAt"],
        "views": views,
        "likes": int(v["statistics"].get("likeCount", 0)),
        "comments": int(v["statistics"].get("commentCount", 0)),
        "duration": v["contentDetails"]["duration"],
        "vhsp": round(views / hours_old, 1),
        "hours_old": int(hours_old),
        "tags": tags,
        "thumbnail_url": get_thumbnail(v["snippet"], video_id),
        "video_url": f"https://www.youtube.com/watch?v={video_id}"
    }

async def tool_keyword_research(keyword: str, max_suggestions: int = 8) -> dict:
    # Step 1: YouTube Autocomplete — free, no API key needed
    autocomplete_url = "https://suggestqueries.google.com/complete/search"
    suggestions = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(autocomplete_url, params={
                "client": "youtube",
                "ds": "yt",
                "q": keyword,
                "hl": "en"
            }, headers={"User-Agent": "Mozilla/5.0"})
            raw = r.text
            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start != -1:
                data = json.loads(raw[start:end])
                if len(data) > 1 and isinstance(data[1], list):
                    for item in data[1][:max_suggestions]:
                        if isinstance(item, list) and item:
                            suggestions.append(item[0])
    except Exception:
        suggestions = []

    # Step 2: Competition count + top views for main keyword
    competition_count = 0
    top_views = []
    try:
        search_data = await yt_get("search", {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "maxResults": 10,
            "order": "relevance",
            "videoDuration": "medium"
        })
        video_ids = [i["id"]["videoId"] for i in search_data.get("items", [])]
        competition_count = search_data.get("pageInfo", {}).get("totalResults", 0)
        if video_ids:
            vids = await yt_get("videos", {"part": "statistics", "id": ",".join(video_ids)})
            views_list = [int(v["statistics"].get("viewCount", 0)) for v in vids.get("items", [])]
            views_list.sort(reverse=True)
            top_views = views_list[:3]
    except Exception:
        pass

    # Step 3: Score calculation
    avg_top_views = sum(top_views) / len(top_views) if top_views else 0
    if avg_top_views >= 1_000_000:
        demand = "HIGH"
        demand_pts = 40
    elif avg_top_views >= 200_000:
        demand = "MEDIUM"
        demand_pts = 25
    elif avg_top_views >= 50_000:
        demand = "LOW-MEDIUM"
        demand_pts = 15
    else:
        demand = "LOW"
        demand_pts = 5

    if competition_count < 1000:
        competition = "LOW"
        comp_pts = 40
    elif competition_count < 10000:
        competition = "MEDIUM"
        comp_pts = 25
    elif competition_count < 100000:
        competition = "HIGH"
        comp_pts = 10
    else:
        competition = "VERY HIGH"
        comp_pts = 0

    total_score = demand_pts + comp_pts
    if total_score >= 65:
        grade = "STRONG"
    elif total_score >= 40:
        grade = "MODERATE"
    else:
        grade = "WEAK"

    return {
        "keyword": keyword,
        "autocomplete_suggestions": suggestions,
        "competition": {"total_videos": competition_count, "level": competition},
        "demand": {"top_3_video_views": top_views, "avg_top_views": int(avg_top_views), "level": demand},
        "keyword_score": total_score,
        "grade": grade,
        "interpretation": f"Demand: {demand} | Competition: {competition} | Score: {total_score}/80"
    }

async def tool_generate_titles(topic: str, niche: str = "documentary", max_titles: int = 10) -> dict:
    import re
    max_titles = min(max_titles, 15)

    # Step 1: Search YouTube for this topic — get top performing videos
    search_data = await yt_get("search", {
        "part": "snippet",
        "q": topic,
        "type": "video",
        "maxResults": 15,
        "order": "viewCount",
        "videoDuration": "medium"
    })
    video_ids = [i["id"]["videoId"] for i in search_data.get("items", [])]

    top_titles = []
    top_tags = []
    if video_ids:
        vids = await yt_get("videos", {"part": "statistics,snippet", "id": ",".join(video_ids)})
        for v in vids.get("items", []):
            views = int(v["statistics"].get("viewCount", 0))
            title = v["snippet"]["title"]
            tags = v["snippet"].get("tags", [])
            top_titles.append({"title": title, "views": views})
            top_tags.extend(tags[:5])  # top 5 tags per video

    top_titles.sort(key=lambda x: x["views"], reverse=True)

    # Step 2: Analyze patterns in top titles
    all_titles_text = " ".join([t["title"] for t in top_titles])
    patterns_found = {
        "has_numbers": bool(re.search(r'\d+', all_titles_text)),
        "has_questions": any(t["title"].strip().endswith("?") or t["title"].lower().startswith(("why", "how", "what", "who")) for t in top_titles),
        "has_superlatives": any(word in all_titles_text.lower() for word in ["most", "worst", "deadliest", "biggest", "greatest", "richest", "dangerous"]),
        "has_emotional": any(word in all_titles_text.lower() for word in ["shocking", "untold", "incredible", "insane", "unbelievable", "terrifying", "dark", "secret", "hidden"]),
        "has_names": True,  # topic itself is usually a name
        "avg_title_length": round(sum(len(t["title"]) for t in top_titles) / max(len(top_titles), 1)),
    }

    # Step 3: Get autocomplete data for the topic
    suggestions = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://suggestqueries.google.com/complete/search", params={
                "client": "youtube", "ds": "yt", "q": topic, "hl": "en"
            }, headers={"User-Agent": "Mozilla/5.0"})
            raw = r.text
            start = raw.find("[")
            end = raw.rfind("]") + 1
            if start != -1:
                data = json.loads(raw[start:end])
                if len(data) > 1 and isinstance(data[1], list):
                    for item in data[1][:8]:
                        if isinstance(item, list) and item:
                            suggestions.append(item[0])
    except Exception:
        pass

    # Step 4: Generate titles using proven formulas
    # Extract useful fragments from autocomplete
    angle_words = []
    for s in suggestions:
        # Get the part after the topic name
        remainder = s.lower().replace(topic.lower(), "").strip()
        if remainder and len(remainder) > 2:
            angle_words.append(remainder)

    # Title formula templates — organized by type
    formulas = [
        # FORMULA 1: Name + Shocking Fact
        {"type": "shocking_fact", "template": f"{topic} — The Story No One Tells You", "score": 75},
        {"type": "shocking_fact", "template": f"The Untold Story of {topic}", "score": 72},
        {"type": "shocking_fact", "template": f"{topic} — What Really Happened", "score": 70},

        # FORMULA 2: Why/How Mystery
        {"type": "mystery", "template": f"Why {topic} Was Impossible to Catch", "score": 78},
        {"type": "mystery", "template": f"How {topic} Got Away With It for So Long", "score": 76},
        {"type": "mystery", "template": f"Why Nobody Could Stop {topic}", "score": 74},

        # FORMULA 3: Superlative + Niche
        {"type": "superlative", "template": f"The Most Dangerous {niche.split()[0].title()} in History — {topic}", "score": 73},
        {"type": "superlative", "template": f"{topic} — The {niche.split()[0].title()} That Shocked the World", "score": 71},

        # FORMULA 4: Number + Extreme
        {"type": "number", "template": f"{topic} — The Rise and Fall of a Criminal Empire", "score": 69},
        {"type": "number", "template": f"Inside {topic}'s Secret World", "score": 67},

        # FORMULA 5: Role Reveal / Identity
        {"type": "role_reveal", "template": f"Who Was {topic}? The Full Story", "score": 65},
        {"type": "role_reveal", "template": f"{topic} — From Nobody to the Most Wanted", "score": 74},

        # FORMULA 6: Emotional hook
        {"type": "emotional", "template": f"The Dark Truth About {topic}", "score": 77},
        {"type": "emotional", "template": f"{topic} — The Story That Will Change How You Think", "score": 68},
        {"type": "emotional", "template": f"The Real {topic} — What History Books Don't Tell You", "score": 76},
    ]

    # Add autocomplete-powered titles
    for angle in angle_words[:3]:
        formulas.append({
            "type": "autocomplete",
            "template": f"{topic} {angle.title()}",
            "score": 60
        })

    # Boost scores based on patterns found in top videos
    for f in formulas:
        if f["type"] == "number" and patterns_found["has_numbers"]:
            f["score"] += 5
        if f["type"] == "mystery" and patterns_found["has_questions"]:
            f["score"] += 5
        if f["type"] == "superlative" and patterns_found["has_superlatives"]:
            f["score"] += 5
        if f["type"] == "emotional" and patterns_found["has_emotional"]:
            f["score"] += 5
        # Penalize if title is too long (>70 chars)
        if len(f["template"]) > 70:
            f["score"] -= 5
        # Bonus for optimal length (40-65 chars)
        if 40 <= len(f["template"]) <= 65:
            f["score"] += 3

    # Sort by score and take top N
    formulas.sort(key=lambda x: x["score"], reverse=True)
    generated = formulas[:max_titles]

    # Assign grades
    for g in generated:
        if g["score"] >= 75:
            g["grade"] = "A"
        elif g["score"] >= 65:
            g["grade"] = "B"
        elif g["score"] >= 55:
            g["grade"] = "C"
        else:
            g["grade"] = "D"

    return {
        "topic": topic,
        "niche": niche,
        "titles_generated": len(generated),
        "generated_titles": [
            {
                "title": g["template"],
                "type": g["type"],
                "score": g["score"],
                "grade": g["grade"],
                "char_count": len(g["template"])
            }
            for g in generated
        ],
        "analysis": {
            "top_youtube_titles": top_titles[:5],
            "patterns_found": patterns_found,
            "autocomplete_angles": suggestions,
            "top_tags_from_competitors": list(set(top_tags))[:15]
        },
        "tip": "Grade A titles use patterns proven to get clicks in your niche. Combine the best title with a matching thumbnail for maximum CTR."
    }

async def tool_get_video_transcript(video_id: str, language: str = "en") -> dict:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api.proxies import WebshareProxyConfig, GenericProxyConfig

        # Priority: Webshare residential > Generic proxy > No proxy
        if WEBSHARE_USER and WEBSHARE_PASS:
            proxy_config = WebshareProxyConfig(
                proxy_username=WEBSHARE_USER,
                proxy_password=WEBSHARE_PASS,
            )
            api = YouTubeTranscriptApi(proxy_config=proxy_config)
        elif PROXY_URL:
            proxy_config = GenericProxyConfig(
                http_url=PROXY_URL,
                https_url=PROXY_URL,
            )
            api = YouTubeTranscriptApi(proxy_config=proxy_config)
        else:
            api = YouTubeTranscriptApi()

        transcript = await asyncio.to_thread(
            api.fetch, video_id, languages=[language, "en"]
        )
        segments = []
        for snippet in transcript:
            text = snippet.text if hasattr(snippet, 'text') else snippet.get('text', '')
            start = snippet.start if hasattr(snippet, 'start') else snippet.get('start', 0)
            duration = snippet.duration if hasattr(snippet, 'duration') else snippet.get('duration', 0)
            segments.append({"text": text, "start": round(start, 1), "duration": round(duration, 1)})
        full_text = " ".join([s["text"] for s in segments])
        return {
            "video_id": video_id,
            "language": language,
            "total_segments": len(segments),
            "full_transcript": full_text,
            "segments": segments[:200],
            "proxy_used": "webshare" if (WEBSHARE_USER and WEBSHARE_PASS) else bool(PROXY_URL)
        }
    except Exception as e:
        return {"video_id": video_id, "error": f"Transcript unavailable: {str(e)}", "proxy_used": "webshare" if (WEBSHARE_USER and WEBSHARE_PASS) else bool(PROXY_URL)}

async def call_tool(name: str, arguments: dict) -> Any:
    if name == "get_channel_stats":
        return await tool_get_channel_stats(**arguments)
    elif name == "get_channel_videos":
        return await tool_get_channel_videos(**arguments)
    elif name == "get_channel_outliers":
        return await tool_get_channel_outliers(**arguments)
    elif name == "search_youtube":
        return await tool_search_youtube(**arguments)
    elif name == "get_video_comments":
        return await tool_get_video_comments(**arguments)
    elif name == "get_video_details":
        return await tool_get_video_details(**arguments)
    elif name == "yt_keyword_research":
        return await tool_keyword_research(**arguments)
    elif name == "yt_generate_titles":
        return await tool_generate_titles(**arguments)
    elif name == "yt_get_video_transcript":
        return await tool_get_video_transcript(**arguments)
    else:
        return {"error": f"Unknown tool: {name}"}

# ─── MCP SSE ENDPOINTS ───────────────────────────────────────────────────────

def make_event(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

@app.get("/sse")
async def sse_endpoint(request: Request):
    async def event_stream():
        session_id = str(uuid.uuid4())
        yield make_event({
            "jsonrpc": "2.0",
            "method": "sse/endpoint",
            "params": {"uri": f"/messages?sessionId={session_id}"}
        })
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(15)
            yield ": keepalive\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.post("/messages")
async def messages_endpoint(request: Request):
    body = await request.json()
    method = body.get("method")
    req_id = body.get("id")
    params = body.get("params", {})

    if method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "yt-research-server", "version": "3.0.0"}
            }
        })

    elif method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"tools": TOOLS}
        })

    elif method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        try:
            result = await call_tool(tool_name, arguments)
            return JSONResponse({
                "jsonrpc": "2.0", "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                    "isError": False
                }
            })
        except Exception as e:
            return JSONResponse({
                "jsonrpc": "2.0", "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": str(e)}],
                    "isError": True
                }
            })

    elif method == "notifications/initialized":
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {}})

    return JSONResponse({
        "jsonrpc": "2.0", "id": req_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"}
    })

@app.post("/mcp")
async def mcp_streamable(request: Request):
    """Streamable HTTP MCP endpoint — required for ChatGPT plugin."""
    body = await request.json()
    method = body.get("method")
    req_id = body.get("id")
    params = body.get("params", {})

    if method == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "yt-research-server", "version": "3.0.0"}
        }
    elif method == "notifications/initialized":
        result = {}
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        try:
            tool_result = await call_tool(tool_name, arguments)
            result = {
                "content": [{"type": "text", "text": json.dumps(tool_result, ensure_ascii=False)}],
                "isError": False
            }
        except Exception as e:
            result = {
                "content": [{"type": "text", "text": str(e)}],
                "isError": True
            }
    else:
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        })

    return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": result})


@app.get("/mcp")
async def mcp_streamable_get(request: Request):
    """GET /mcp — SSE stream for Streamable HTTP transport."""
    async def event_stream():
        yield make_event({
            "jsonrpc": "2.0",
            "method": "sse/endpoint",
            "params": {"uri": "/mcp"}
        })
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(15)
            yield ": keepalive\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.get("/")
async def root():
    return {"status": "ok", "name": "yt-research-server", "version": "3.0.0", "protocol": "MCP Streamable HTTP + SSE", "tools": len(TOOLS), "endpoints": ["/mcp", "/sse"]}
