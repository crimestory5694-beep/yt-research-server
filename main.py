from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from typing import Optional
from datetime import datetime, timezone, timedelta

app = FastAPI(
    title="YouTube Research API",
    description="Proxy server for YouTube Data API v3 — powers Prompt A research workflow",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.environ.get("YOUTUBE_API_KEY", "AIzaSyCiKq99ECwtFX98T7dpTNM0BiOIpLXxBLE")
YT_BASE = "https://www.googleapis.com/youtube/v3"


async def yt_get(endpoint: str, params: dict) -> dict:
    params["key"] = API_KEY
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{YT_BASE}/{endpoint}", params=params)
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text)
        return r.json()


# ─── 1. CHANNEL STATS ────────────────────────────────────────────────────────

@app.get("/channel/stats")
async def channel_stats(channel_id: str):
    """Get subscriber count, total views, video count for a channel."""
    data = await yt_get("channels", {
        "part": "statistics,snippet",
        "id": channel_id
    })
    items = data.get("items", [])
    if not items:
        raise HTTPException(404, "Channel not found")
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


# ─── 2. CHANNEL VIDEOS ───────────────────────────────────────────────────────

@app.get("/channel/videos")
async def channel_videos(
    channel_id: str,
    sort_by: str = Query("newest", description="newest or popular"),
    max_results: int = Query(50, le=50)
):
    """Get recent or popular videos from a channel with view counts."""
    # Get uploads playlist ID
    ch = await yt_get("channels", {"part": "contentDetails,statistics", "id": channel_id})
    if not ch.get("items"):
        raise HTTPException(404, "Channel not found")

    ch_stats = ch["items"][0]["statistics"]
    channel_avg = int(ch_stats.get("viewCount", 0)) // max(int(ch_stats.get("videoCount", 1)), 1)
    uploads_id = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Get playlist items
    pl = await yt_get("playlistItems", {
        "part": "contentDetails,snippet",
        "playlistId": uploads_id,
        "maxResults": max_results
    })

    video_ids = [i["contentDetails"]["videoId"] for i in pl.get("items", [])]
    if not video_ids:
        return {"videos": [], "channel_avg_views": channel_avg}

    # Get video stats
    vids = await yt_get("videos", {
        "part": "statistics,snippet,contentDetails",
        "id": ",".join(video_ids)
    })

    now = datetime.now(timezone.utc)
    results = []
    for v in vids.get("items", []):
        pub = datetime.fromisoformat(v["snippet"]["publishedAt"].replace("Z", "+00:00"))
        hours_old = max((now - pub).total_seconds() / 3600, 1)
        days_old = hours_old / 24
        views = int(v["statistics"].get("viewCount", 0))
        vhsp = round(views / hours_old, 1)
        daily_views = round(views / days_old, 0)
        outlier_score = round(views / channel_avg, 2) if channel_avg > 0 else 0
        days_since = int(days_old)

        results.append({
            "video_id": v["id"],
            "title": v["snippet"]["title"],
            "published_at": v["snippet"]["publishedAt"],
            "days_old": days_since,
            "views": views,
            "vhsp": vhsp,
            "daily_views": int(daily_views),
            "outlier_score": outlier_score,
            "duration": v["contentDetails"]["duration"],
            "within_28_days": days_since <= 28
        })

    if sort_by == "popular":
        results.sort(key=lambda x: x["views"], reverse=True)

    return {
        "channel_avg_views": channel_avg,
        "videos": results
    }


# ─── 3. OUTLIER DETECTION ────────────────────────────────────────────────────

@app.get("/channel/outliers")
async def channel_outliers(
    channel_id: str,
    min_outlier_score: float = Query(2.0),
    within_days: int = Query(28),
    max_videos: int = Query(50, le=50)
):
    """Find outlier videos (overperforming relative to channel average)."""
    data = await channel_videos(channel_id=channel_id, sort_by="newest", max_results=max_videos)
    channel_avg = data["channel_avg_views"]

    cutoff = datetime.now(timezone.utc) - timedelta(days=within_days)

    outliers = []
    for v in data["videos"]:
        pub = datetime.fromisoformat(v["published_at"].replace("Z", "+00:00"))
        if pub >= cutoff and v["outlier_score"] >= min_outlier_score:
            outliers.append(v)

    outliers.sort(key=lambda x: x["outlier_score"], reverse=True)

    return {
        "channel_id": channel_id,
        "channel_avg_views": channel_avg,
        "outlier_threshold": min_outlier_score,
        "within_days": within_days,
        "outliers_found": len(outliers),
        "outliers": outliers
    }


# ─── 4. YOUTUBE SEARCH ───────────────────────────────────────────────────────

@app.get("/search")
async def youtube_search(
    query: str,
    published_after_days: int = Query(28, description="Published within last N days"),
    max_results: int = Query(25, le=50),
    min_duration: str = Query("medium", description="short (<4min), medium (4-20min), long (>20min)")
):
    """Search YouTube for videos matching query."""
    pub_after = (datetime.now(timezone.utc) - timedelta(days=published_after_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "publishedAfter": pub_after,
        "maxResults": max_results,
        "order": "viewCount",
        "videoDuration": min_duration
    }

    results = await yt_get("search", params)

    video_ids = [i["id"]["videoId"] for i in results.get("items", [])]
    if not video_ids:
        return {"results": []}

    # Get stats for results
    vids = await yt_get("videos", {
        "part": "statistics,contentDetails",
        "id": ",".join(video_ids)
    })

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
            "duration": stats_map.get(vid_id, {}).get("contentDetails", {}).get("duration", "")
        })

    output.sort(key=lambda x: x["views"], reverse=True)
    return {"query": query, "results": output}


# ─── 5. VIDEO COMMENTS ───────────────────────────────────────────────────────

@app.get("/video/comments")
async def video_comments(
    video_id: str,
    min_likes: int = Query(10, description="Minimum likes to include comment"),
    max_results: int = Query(100, le=100)
):
    """Mine comments for demand signals (requests, frustrations, gaps)."""
    try:
        data = await yt_get("commentThreads", {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results,
            "order": "relevance"
        })
    except HTTPException as e:
        if e.status_code == 403:
            return {"video_id": video_id, "error": "Comments disabled for this video", "comments": []}
        raise

    comments = []
    for item in data.get("items", []):
        c = item["snippet"]["topLevelComment"]["snippet"]
        likes = c.get("likeCount", 0)
        if likes >= min_likes:
            comments.append({
                "comment_id": item["id"],
                "text": c["textDisplay"],
                "likes": likes,
                "published_at": c["publishedAt"],
                "author": c["authorDisplayName"]
            })

    comments.sort(key=lambda x: x["likes"], reverse=True)
    return {
        "video_id": video_id,
        "min_likes_filter": min_likes,
        "qualifying_comments": len(comments),
        "comments": comments
    }


# ─── 6. VIDEO DETAILS ────────────────────────────────────────────────────────

@app.get("/video/details")
async def video_details(video_id: str):
    """Get full stats for a specific video."""
    data = await yt_get("videos", {
        "part": "statistics,snippet,contentDetails",
        "id": video_id
    })
    items = data.get("items", [])
    if not items:
        raise HTTPException(404, "Video not found")
    v = items[0]
    pub = datetime.fromisoformat(v["snippet"]["publishedAt"].replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    hours_old = max((now - pub).total_seconds() / 3600, 1)
    views = int(v["statistics"].get("viewCount", 0))

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
        "hours_old": int(hours_old)
    }


# ─── 7. HEALTH CHECK ─────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "message": "YouTube Research API is running"}
