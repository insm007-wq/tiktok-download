# TikTok Video Download

Download TikTok videos by URL or video ID. Fast, reliable, and production-ready — typical run completes in ~10 seconds per video.

## Features

- **Just paste URLs** — no configuration, no tokens, no proxy setup. Everything runs on sensible defaults.
- **Flexible URL formats** — standard URLs (`https://www.tiktok.com/@user/video/123`), short URLs (`https://vm.tiktok.com/ZMxxx/`), and raw numeric IDs
- **Direct file delivery** — videos up to 30 MB are stored in Apify Key-Value Store with public download links
- **Large video fallback** — files over 30 MB return signed CDN URLs (valid 12–24 h)
- **Rich metadata** — author, stats, hashtags, duration, upload time
- **Residential proxy** — uses Apify RESIDENTIAL proxies automatically for reliability

## Input

Single required field. Everything else (proxy, concurrency, size limit, auth tokens) is handled automatically.

| Field | Type | Description |
|---|---|---|
| `videoUrls` | string[] | TikTok URLs or video IDs to download |

### Example input

```json
{
  "videoUrls": [
    "https://www.tiktok.com/@jtbcnews_official/video/7628806976562072852",
    "https://vm.tiktok.com/ZMxxx/",
    "7628806976562072852"
  ]
}
```

## Output

Each result is pushed to the dataset with the following shape:

```json
{
  "id": "7628806976562072852",
  "inputUrl": "https://www.tiktok.com/@user/video/7628806976562072852",
  "url": "https://www.tiktok.com/@user/video/7628806976562072852",
  "description": "Video caption...",
  "author": {
    "username": "user",
    "nickname": "Display Name",
    "url": "https://www.tiktok.com/@user"
  },
  "statistics": {
    "views": 123456,
    "likes": 1234,
    "comments": 56,
    "shares": 78
  },
  "duration": 15.2,
  "hashtags": ["fyp", "viral"],
  "uploadedAt": 1712345678,
  "downloadStatus": "success",
  "downloadUrl": "https://api.apify.com/v2/key-value-stores/.../records/video_7628806976562072852.mp4",
  "downloadUrls": ["https://api.apify.com/..."],
  "fileSizeBytes": 5784873,
  "chunkCount": 1,
  "storageType": "kv_store",
  "cdnUrl": "https://v16-webapp.tiktok.com/...",
  "playUrlCandidates": ["https://..."]
}
```

### Storage types

| `storageType` | Meaning |
|---|---|
| `kv_store` | Video stored as a single KV record (≤ 9 MB). Use `downloadUrl` |
| `kv_store_chunked` | Video split across multiple 9 MB chunks. Use `downloadUrls[]` |
| `cdn_url_only` | Video exceeded size limit. Use `cdnUrl` (TikTok CDN, valid 12–24 h) |
| `error` | Download failed. See `error` field |

## Notes

- This Actor uses Apify **RESIDENTIAL** proxies automatically. RESIDENTIAL proxy requires a **paid Apify plan** — on the Free plan the Actor will fail to acquire a proxy URL.
- TikTok may rate-limit the same IP; residential proxy mitigates this but IP quality varies. Re-run if a specific URL fails.
- Private videos and region-restricted content cannot be downloaded.
- CDN URLs are time-limited and should be downloaded promptly.

## Pricing

Charged only for **successful** downloads. Failed requests (invalid URL, private video, region block, CDN error) are logged but excluded from the dataset and **not billed**. Platform usage (compute, proxy bandwidth) is billed separately by Apify.

## Support

Report issues or request features via the Apify Store listing. Pull requests welcome.
