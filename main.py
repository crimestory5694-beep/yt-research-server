import asyncio
import json
import httpx
import os
from datetime import datetime, timezone, timedelta
from typing import Any
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
        "description": "Get recent or popular videos from a channel with view counts, VHSP, outlier score, and 28-day flag.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "YouTube channel ID"},
                "sort_by": {"type": "string", "enum": ["newest", "popular"], "default": "newest"},
                "max_results": {"type": "integer", "default": 50, "maximum": 50}
            },
            "required": ["channel_id"]
        }
    },
    {
        "name": "get_channel_outliers",
        "description": "Find videos that overperform relative to channel average. Outlier score = video views / channel avg views. Use for Prompt A Bucket 1.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "YouTube channel ID"},
                "min_outlier_score": {"type": "number", "default": 2.0},
                "within_days": {"type": "integer", "default": 28},
                "max_videos": {"type": "integer", "default": 50}
            },
            "required": ["channel_id"]
        }
    },
    {
        "name": "search_youtube",
        "description": "Search YouTube for videos matching a query. Returns videos with view counts and VHSP. Use for Buckets 2, 4, 5.",
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
        "description": "Get full stats for a specific video: views, likes, comments, duration, VHSP.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "video_id": {"type": "string", "description": "YouTube video ID"}
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
    ch = await yt_get("channels", {"part": "contentDetails,statistics", "id": channel_id})
    if not ch.get("items"):
        return {"error": "Channel not found"}
    ch_stats = ch["items"][0]["statistics"]
    channel_avg = int(ch_stats.get("viewCount", 0)) // max(int(ch_stats.get("videoCount", 1)), 1)
    uploads_id = ch["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    pl = await yt_get("playlistItems", {"part": "contentDetails,snippet", "playlistId": uploads_id, "maxResults": min(max_results, 50)})
    video_ids = [i["contentDetails"]["videoId"] for i in pl.get("items", [])]
    if not video_ids:
        return {"videos": [], "channel_avg_views": channel_avg}
    vids = await yt_get("videos", {"part": "statistics,snippet,contentDetails", "id": ",".join(video_ids)})
    now = datetime.now(timezone.utc)
    results = []
    for v in vids.get("items", []):
        pub = datetime.fromisoformat(v["snippet"]["publishedAt"].replace("Z", "+00:00"))
        hours_old = max((now - pub).total_seconds() / 3600, 1)
        days_old = hours_old / 24
        views = int(v["statistics"].get("viewCount", 0))
        results.append({
            "video_id": v["id"],
            "title": v["snippet"]["title"],
            "published_at": v["snippet"]["publishedAt"],
            "days_old": int(days_old),
            "views": views,
            "vhsp": round(views / hours_old, 1),
            "daily_views": int(views / days_old),
            "outlier_score": round(views / channel_avg, 2) if channel_avg > 0 else 0,
            "duration": v["contentDetails"]["duration"],
            "within_28_days": int(days_old) <= 28
        })
    if sort_by == "popular":
        results.sort(key=lambda x: x["views"], reverse=True)
    return {"channel_avg_views": channel_avg, "videos": results}

async def tool_get_channel_outliers(channel_id: str, min_outlier_score: float = 2.0, within_days: int = 28, max_videos: int = 50) -> dict:
    data = await tool_get_channel_videos(channel_id=channel_id, sort_by="newest", max_results=max_videos)
    channel_avg = data.get("channel_avg_views", 0)
    cutoff = datetime.now(timezone.utc) - timedelta(days=within_days)
    outliers = []
    for v in data.get("videos", []):
        pub = datetime.fromisoformat(v["published_at"].replace("Z", "+00:00"))
        if pub >= cutoff and v["outlier_score"] >= min_outlier_score:
            outliers.append(v)
    outliers.sort(key=lambda x: x["outlier_score"], reverse=True)
    return {"channel_id": channel_id, "channel_avg_views": channel_avg, "outlier_threshold": min_outlier_score, "within_days": within_days, "outliers_found": len(outliers), "outliers": outliers}

async def tool_search_youtube(query: str, published_after_days: int = 28, max_results: int = 25, min_duration: str = "medium") -> dict:
    pub_after = (datetime.now(timezone.utc) - timedelta(days=published_after_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    results = await yt_get("search", {"part": "snippet", "q": query, "type": "video", "publishedAfter": pub_after, "maxResults": min(max_results, 50), "order": "viewCount", "videoDuration": min_duration})
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
        output.append({"video_id": vid_id, "title": item["snippet"]["title"], "channel_id": item["snippet"]["channelId"], "channel_title": item["snippet"]["channelTitle"], "published_at": item["snippet"]["publishedAt"], "views": views, "vhsp": round(views / hours_old, 1), "likes": int(stats.get("likeCount", 0)), "duration": stats_map.get(vid_id, {}).get("contentDetails", {}).get("duration", "")})
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
    return {"video_id": video_id, "title": v["snippet"]["title"], "channel_id": v["snippet"]["channelId"], "channel_title": v["snippet"]["channelTitle"], "published_at": v["snippet"]["publishedAt"], "views": views, "likes": int(v["statistics"].get("likeCount", 0)), "comments": int(v["statistics"].get("commentCount", 0)), "duration": v["contentDetails"]["duration"], "vhsp": round(views / hours_old, 1), "hours_old": int(hours_old)}

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
    else:
        return {"error": f"Unknown tool: {name}"}

# ─── MCP SSE ENDPOINTS ───────────────────────────────────────────────────────

def make_event(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

@app.get("/sse")
async def sse_endpoint(request: Request):
    async def event_stream():
        # Send endpoint event
        session_id = str(uuid.uuid4())
        yield make_event({
            "jsonrpc": "2.0",
            "method": "sse/endpoint",
            "params": {"uri": f"/messages?sessionId={session_id}"}
        })
        # Keep alive
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
                "serverInfo": {"name": "yt-research-server", "version": "1.0.0"}
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
            "serverInfo": {"name": "yt-research-server", "version": "1.0.0"}
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
    return {"status": "ok", "name": "yt-research-server", "protocol": "MCP Streamable HTTP + SSE", "tools": len(TOOLS), "endpoints": ["/mcp", "/sse"]}
