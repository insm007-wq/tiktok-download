# TikTok Video Download

Download TikTok videos by URL or video ID. Fast, reliable, and production-ready — typical run completes in ~10 seconds per video.

## Features

- **Flexible input** — standard URLs (`https://www.tiktok.com/@user/video/123`), short URLs (`https://vm.tiktok.com/ZMxxx/`), and raw numeric IDs
- **Direct file delivery** — videos up to 30 MB are stored in Apify Key-Value Store with public download links
- **Large video fallback** — files over 30 MB return signed CDN URLs (valid 12–24 h)
- **Rich metadata** — author, stats, hashtags, duration, upload time
- **Batch parallelism** — configurable concurrent downloads (up to 10)
- **Residential proxy** — uses Apify RESIDENTIAL proxies by default to avoid blocks

## Input

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `videoUrls` | string[] | ✅ | — | TikTok URLs or video IDs to download |
| `maxVideoSizeMb` | integer | | 30 | File-size limit (1–30 MB). Larger videos return CDN URL only |
| `maxConcurrentDownloads` | integer | | 3 | Parallel downloads (1–10) |
| `useProxy` | boolean | | true | Toggle Apify RESIDENTIAL proxy |
| `proxyConfiguration` | object | | RESIDENTIAL | Apify Proxy configuration |
| `msToken` | string | | — | Optional TikTok auth token. Leave empty for automatic generation |

### Example input

```json
{
  "videoUrls": [
    "https://www.tiktok.com/@jtbcnews_official/video/7628806976562072852",
    "https://vm.tiktok.com/ZMxxx/",
    "7628806976562072852"
  ],
  "maxVideoSizeMb": 30,
  "maxConcurrentDownloads": 3
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

- TikTok may rate-limit the same IP; RESIDENTIAL proxy mitigates this but residential IP quality varies. Re-run if a specific URL fails.
- Private videos and region-restricted content cannot be downloaded.
- CDN URLs are time-limited and should be downloaded promptly.
- Apify Store free plan does not include RESIDENTIAL proxies. Set `useProxy: false` or upgrade your plan if you see proxy errors.

## Pricing

Charged only for **successful** downloads. Failed requests (invalid URL, private video, region block, CDN error) are logged but excluded from the dataset and **not billed**. Platform usage (compute, proxy bandwidth) is billed separately by Apify.

## Support

Report issues or request features via the Apify Store listing. Pull requests welcome.
