# Deploy Steps — YouTube Research API

## Step 1: Railway Account
1. railway.app → Sign up (free) → GitHub se login karo

## Step 2: Deploy
1. railway.app/new → "Deploy from GitHub repo" → apna repo select karo
   OR: agar GitHub nahi chahiye → "Deploy from template" → "Empty project"
2. "Add service" → agar GitHub se: apna repo
   OR drag-and-drop folder method: Railway CLI use karo (below)

## Step 3: Railway CLI Method (Easiest — no GitHub needed)
```bash
# Terminal mein:
npm install -g @railway/cli
railway login
cd yt-research-server
railway init
railway up
```

## Step 4: Environment Variable
Railway dashboard → Your service → Variables tab:
```
YOUTUBE_API_KEY = AIzaSyCiKq99ECwtFX98T7dpTNM0BiOIpLXxBLE
```
(Ye set karne ke baad main.py ki hardcoded key remove ho jayegi)

## Step 5: Get Your URL
Deploy ke baad Railway ek URL dega jaise:
https://yt-research-server-production-xxxx.railway.app

## Step 6: Update OpenAPI Schema
openapi_plugin.yaml mein ye line update karo:
```yaml
servers:
  - url: https://YOUR-RAILWAY-URL.railway.app  # <-- apna URL yahan
```

## Step 7: ChatGPT Plugin Add Karo
1. ChatGPT → Plugins → + → "Create plugin"
2. OpenAPI schema paste karo (openapi_plugin.yaml ka content)
3. Save → Test karo: "Get outliers for channel UCxKVonxttzOS2wjJESi-eEA"

## Test URLs (apna Railway URL se replace karo):
- https://YOUR-URL.railway.app/
- https://YOUR-URL.railway.app/channel/stats?channel_id=UCxKVonxttzOS2wjJESi-eEA
- https://YOUR-URL.railway.app/channel/outliers?channel_id=UCxKVonxttzOS2wjJESi-eEA&min_outlier_score=2.0
- https://YOUR-URL.railway.app/search?query=criminal+empire+documentary&published_after_days=28
