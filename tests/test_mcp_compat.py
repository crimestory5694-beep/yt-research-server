"""Existing MCP surface: tool names/schemas, JSON-RPC envelopes, and the 8 non-transcript tools
(YouTube Data API mocked with httpx.MockTransport - no network, no API key)."""
import json

import httpx
import pytest
from fastapi.testclient import TestClient

import main
from tests.helpers import VID, FakeLibrary, good_data, make_service

EXPECTED = ["get_channel_stats", "get_channel_videos", "get_channel_outliers", "search_youtube",
            "get_video_comments", "get_video_details", "yt_keyword_research", "yt_generate_titles",
            "yt_get_video_transcript"]
FAKE_KEY = "AIza" + "x" * 35


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(main, "_transcript_service", make_service(lib=FakeLibrary(good_data())))
    return TestClient(main.app)


def rpc(client, method, params=None, path="/mcp"):
    return client.post(path, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).json()


@pytest.mark.parametrize("path", ["/mcp", "/messages"])
def test_tools_list_unchanged_names_and_required_args(client, path):
    tools = rpc(client, "tools/list", path=path)["result"]["tools"]
    assert [t["name"] for t in tools] == EXPECTED
    by = {t["name"]: t for t in tools}
    assert by["yt_get_video_transcript"]["inputSchema"]["required"] == ["video_id"]
    assert set(by["yt_get_video_transcript"]["inputSchema"]["properties"]) == {"video_id", "language", "any_language"}
    assert by["get_channel_videos"]["inputSchema"]["required"] == ["channel_id"]


@pytest.mark.parametrize("path", ["/mcp", "/messages"])
def test_initialize_and_unknown_method(client, path):
    r = rpc(client, "initialize", path=path)
    assert r["result"]["serverInfo"]["name"] == "yt-research-server" and r["result"]["protocolVersion"]
    assert rpc(client, "nope", path=path)["error"]["code"] == -32601


def test_root_unchanged_fields(client):
    j = client.get("/").json()
    assert j["status"] == "ok" and j["tools"] == 9 and "/mcp" in j["endpoints"]


@pytest.mark.parametrize("path", ["/mcp", "/messages"])
def test_transcript_tool_call_through_mcp(client, path):
    r = rpc(client, "tools/call", {"name": "yt_get_video_transcript", "arguments": {"video_id": VID, "language": "en"}}, path)
    assert r["result"]["isError"] is False
    body = json.loads(r["result"]["content"][0]["text"])
    assert body["status"] == "ok" and body["full_transcript"] and body["source"] == "youtube-transcript-api"


def test_transcript_error_is_still_non_error_envelope_with_error_key(client):
    r = rpc(client, "tools/call", {"name": "yt_get_video_transcript", "arguments": {"video_id": "bad"}})
    body = json.loads(r["result"]["content"][0]["text"])
    assert r["result"]["isError"] is False and "error" in body and body["status"] == "invalid_video_id"


def test_unknown_tool(client):
    r = rpc(client, "tools/call", {"name": "zzz", "arguments": {}})
    assert "Unknown tool" in r["result"]["content"][0]["text"] or "Unknown tool" in json.dumps(r)


# ── the other tools, with YouTube Data API mocked ──
def yt_mock(monkeypatch, handler):
    monkeypatch.setattr(main, "API_KEY", FAKE_KEY)
    real = httpx.AsyncClient
    monkeypatch.setattr(main.httpx, "AsyncClient",
                        lambda **kw: real(transport=httpx.MockTransport(handler), **{k: v for k, v in kw.items() if k == "timeout"}))


def data_api(request: httpx.Request):
    p = request.url.path
    assert request.url.params["key"] == FAKE_KEY
    if p.endswith("/channels"):
        return httpx.Response(200, json={"items": [{"snippet": {"title": "Chan"}, "statistics": {"subscriberCount": "100", "viewCount": "10000", "videoCount": "10"},
                                                    "contentDetails": {"relatedPlaylists": {"uploads": "UU1"}}}]})
    if p.endswith("/playlistItems"):
        return httpx.Response(200, json={"items": [{"contentDetails": {"videoId": VID}}]})
    if p.endswith("/videos"):
        return httpx.Response(200, json={"items": [{"id": VID, "snippet": {"title": "T", "publishedAt": "2026-09-01T00:00:00Z", "channelId": "C", "channelTitle": "Chan", "tags": ["a"], "thumbnails": {}},
                                                    "statistics": {"viewCount": "5000", "likeCount": "10", "commentCount": "2"}, "contentDetails": {"duration": "PT10M"}}]})
    if p.endswith("/search"):
        return httpx.Response(200, json={"items": [{"id": {"videoId": VID}, "snippet": {"title": "T", "channelId": "C", "channelTitle": "Chan", "publishedAt": "2026-09-01T00:00:00Z", "thumbnails": {}}}], "pageInfo": {"totalResults": 500}})
    if p.endswith("/commentThreads"):
        return httpx.Response(200, json={"items": [{"id": "c1", "snippet": {"topLevelComment": {"snippet": {"likeCount": 20, "textDisplay": "hi", "publishedAt": "x", "authorDisplayName": "a"}}}}]})
    return httpx.Response(404, json={})


@pytest.mark.parametrize("name,args,key", [
    ("get_channel_stats", {"channel_id": "C"}, "subscribers"),
    ("get_channel_videos", {"channel_id": "C"}, "videos"),
    ("get_channel_outliers", {"channel_id": "C", "min_outlier_score": 0.1, "within_days": 99999}, "outliers"),
    ("search_youtube", {"query": "q"}, "results"),
    ("get_video_comments", {"video_id": VID}, "comments"),
    ("get_video_details", {"video_id": VID}, "vhsp"),
    ("yt_keyword_research", {"keyword": "k"}, "keyword_score"),
])
async def test_other_tools_still_work(monkeypatch, name, args, key):
    yt_mock(monkeypatch, data_api)
    res = await main.call_tool(name, args)
    assert key in res and "error" not in res


async def test_generate_titles_still_works(monkeypatch):
    yt_mock(monkeypatch, data_api)
    res = await main.call_tool("yt_generate_titles", {"topic": "Topic"})
    assert res["titles_generated"] > 0


# ── security regressions ──
async def test_missing_api_key_gives_clear_error_not_a_crash(monkeypatch):
    monkeypatch.setattr(main, "API_KEY", "")
    with pytest.raises(main.YouTubeAPIError, match="YOUTUBE_API_KEY"):
        await main.call_tool("get_channel_stats", {"channel_id": "C"})


def test_api_key_never_appears_in_mcp_error_output(monkeypatch, client):
    yt_mock(monkeypatch, lambda req: httpx.Response(403, json={"error": {"message": "quota", "errors": [{"reason": "quotaExceeded"}]}}))
    r = rpc(client, "tools/call", {"name": "get_channel_stats", "arguments": {"channel_id": "C"}})
    text = json.dumps(r)
    assert r["result"]["isError"] is True and FAKE_KEY not in text and "key=" not in text
    assert "403" in text and "quotaExceeded" in text


def test_no_hardcoded_google_key_in_tracked_sources():
    import pathlib, re
    root = pathlib.Path(main.__file__).parent
    pat = re.compile(r"AIza[0-9A-Za-z_\-]{30,}")
    for p in root.rglob("*"):
        if p.is_file() and ".git" not in p.parts and p.suffix in (".py", ".md", ".yaml", ".json", ".txt", ".toml", ".example", ""):
            if p.name.startswith(".venv"): continue
            found = bool(pat.search(p.read_text(errors="ignore")))
            assert not found, f"Google API key literal found in {p.name}"
