import os
import sys
import json
import asyncio
import random
import re
from pathlib import Path
import httpx
from loguru import logger

# ---------------------------------------------------------
# 1. Configuration & Logging Setup
# ---------------------------------------------------------
CONFIG = {
    "base_dir": "tiktok_data",
    "download_media": True,
    "http2": False,
    "proxy": None,
    "timeout": 60.0,
    "delay_between_pages": (1.5, 3.0),
    "delay_between_videos": (2.0, 5.0)
}

logger.remove()
logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

# ---------------------------------------------------------
# 2. Utility Functions
# ---------------------------------------------------------
def clean_filename(text):
    return re.sub(r'[\\/*?:"<>|]', "", text).replace("\n", " ").strip()[:50]

async def download_file(client, url, path):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/"
        }
        resp = await client.get(url, headers=headers, timeout=60, follow_redirects=True)
        resp.raise_for_status()
        Path(path).write_bytes(resp.content)
        return True
    except Exception as e:
        logger.error(f"Download Error: {url[:50]}... -> {e}")
        return False

# ---------------------------------------------------------
# 3. The Main Scraper Engine
# ---------------------------------------------------------
class TikTokScraperV4:
    def __init__(self, config):
        self.cfg = config
        self.base_path = Path(config["base_dir"])
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.client = httpx.AsyncClient(
            http2=config["http2"],
            proxy=config.get("proxy"),
            timeout=config["timeout"]
        )

    async def get_video_meta(self, url):
        clean_url = url.replace("/photo/", "/video/")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://www.tiktok.com/"
        }
        logger.info(f"Fetching: {clean_url}")
        resp = await self.client.get(clean_url, headers=headers, follow_redirects=True)
        match = re.search(r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">([\s\S]*?)</script>', resp.text)
        if not match:
            return None
        data = json.loads(match.group(1))
        item = data.get("__DEFAULT_SCOPE__", {}).get("webapp.video-detail", {}).get("itemInfo", {}).get("itemStruct")
        if not item:
            item = data.get("__DEFAULT_SCOPE__", {}).get("webapp.image-detail", {}).get("itemInfo", {}).get("itemStruct")
        return item

    async def scrape_video(self, url):
        logger.info(f"{'-'*50}")
        logger.info(f"Starting: {url}")
        item = await self.get_video_meta(url)
        if not item:
            logger.error(f"Failed to get meta for {url}")
            return

        v_id = item["id"]
        author = item.get("author", {}).get("uniqueId", "unknown")
        desc_slug = clean_filename(item.get("desc", "no_desc"))
        file_prefix = f"{author}_{desc_slug}_{v_id}"

        v_path = self.base_path / file_prefix
        v_path.mkdir(exist_ok=True)

        # Save RAW Meta
        (v_path / f"RAW__meta__{file_prefix}.json").write_text(
            json.dumps(item, indent=2, ensure_ascii=False)
        )

        # Save Clean Meta
        clean_meta = {
            "post_info": {
                "id": v_id,
                "desc": item.get("desc"),
                "createTime": item.get("createTime"),
                "isAd": item.get("isAd", False)
            },
            "stats": item.get("statsV2", item.get("stats", {})),
            "author": {
                "uid": item.get("author", {}).get("id"),
                "username": item.get("author", {}).get("uniqueId"),
                "nickname": item.get("author", {}).get("nickname"),
                "avatar": item.get("author", {}).get("avatarLarger"),
                "verified": item.get("author", {}).get("verified")
            },
            "music": item.get("music", {}),
            "video_cdn": {
                "playAddr": item.get("video", {}).get("playAddr"),
                "downloadAddr": item.get("video", {}).get("downloadAddr"),
                "cover": item.get("video", {}).get("cover"),
                "duration": item.get("video", {}).get("duration"),
                "ratio": item.get("video", {}).get("ratio")
            }
        }
        (v_path / f"meta__{file_prefix}.json").write_text(
            json.dumps(clean_meta, indent=2, ensure_ascii=False)
        )

        # Save Caption
        caption_data = {
            "username": author,
            "account_id": item.get("author", {}).get("id"),
            "post_id": v_id,
            "post_url": url,
            "caption": item.get("desc", "")
        }
        (v_path / f"caption__{file_prefix}.json").write_text(
            json.dumps(caption_data, indent=2, ensure_ascii=False)
        )

        # Download Media
        if self.cfg.get("download_media", True):
            # Video
            video_data = item.get("video", {})
            play_url = None
            if isinstance(video_data.get("downloadAddr"), str):
                play_url = video_data.get("downloadAddr")
            elif isinstance(video_data.get("playAddr"), str):
                play_url = video_data.get("playAddr")
            elif isinstance(video_data.get("playAddr"), list) and video_data["playAddr"]:
                play_url = video_data["playAddr"][0]

            if play_url:
                await download_file(self.client, play_url, v_path / f"video__{file_prefix}.mp4")
            else:
                logger.info("No video URL (likely photo carousel)")

            # Images
            images = item.get("imagePost", {}).get("images", [])
            for i, img in enumerate(images):
                img_url = None
                if img.get("imageURL", {}).get("urlList"):
                    img_url = img["imageURL"]["urlList"][0]
                elif img.get("displayImage", {}).get("urlList"):
                    img_url = img["displayImage"]["urlList"][0]
                elif isinstance(img.get("displayAddr"), str):
                    img_url = img.get("displayAddr")
                if img_url:
                    await download_file(self.client, img_url, v_path / f"carousel_{i+1:03d}__{file_prefix}.jpg")

            # Audio
            music_data = item.get("music", {})
            play_url_data = music_data.get("playUrl")
            audio_url = None
            if isinstance(play_url_data, str):
                audio_url = play_url_data
            elif isinstance(play_url_data, dict) and play_url_data.get("urlList"):
                audio_url = play_url_data["urlList"][0]
            if audio_url:
                await download_file(self.client, audio_url, v_path / f"audio__{file_prefix}.mp3")

        await self.fetch_comments(v_id, v_path, file_prefix)

    async def fetch_comments(self, video_id, path, file_prefix):
        raw_comments = []
        clean_comments = []
        cursor, has_more, page = 0, 1, 1
        logger.info("Fetching comments...")

        while has_more:
            params = {"aweme_id": video_id, "cursor": cursor, "count": 50, "aid": "1988"}
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://www.tiktok.com/"
            }
            resp = await self.client.get(
                "https://www.tiktok.com/api/comment/list/",
                params=params, headers=headers
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            batch = data.get("comments") or []
            if not batch:
                break

            raw_comments.extend(batch)
            for c in batch:
                clean_comments.append({
                    "cid": c.get("cid"),
                    "text": c.get("text"),
                    "likes": c.get("digg_count"),
                    "reply_total": c.get("reply_comment_total"),
                    "create_time": c.get("create_time"),
                    "user": {
                        "uid": c.get("user", {}).get("uid"),
                        "username": c.get("user", {}).get("unique_id"),
                        "nickname": c.get("user", {}).get("nickname"),
                        "avatar": c.get("user", {}).get("avatar_thumb", {}).get("url_list", [""])[0]
                        if c.get("user", {}).get("avatar_thumb") else None
                    }
                })

            logger.info(f"Page {page}: {len(batch)} comments")
            has_more = data.get("has_more", 0)
            cursor = data.get("cursor", cursor + len(batch))
            page += 1

            if len(raw_comments) >= 10000:
                logger.info("Reached 10000 limit")
                break

            await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))

        (path / f"RAW__comments__{file_prefix}.json").write_text(
            json.dumps(raw_comments, indent=2, ensure_ascii=False)
        )
        (path / f"comments__{file_prefix}.json").write_text(
            json.dumps(clean_comments, indent=2, ensure_ascii=False)
        )
        logger.success(f"Comments saved: {len(raw_comments)}")

    async def close(self):
        await self.client.aclose()

# ---------------------------------------------------------
# 4. Execution Logic
# ---------------------------------------------------------
async def main():
    if not os.path.exists("links.txt"):
        print("Error: links.txt not found")
        return

    with open("links.txt", "r", encoding="utf-8") as f:
        URLS = [line.strip() for line in f if line.strip()]

    if not URLS:
        print("Error: links.txt is empty")
        return

    print(f"Found {len(URLS)} URLs. Starting...")

    scraper = TikTokScraperV4(CONFIG)
    try:
        for url in URLS:
            await scraper.scrape_video(url)
            await asyncio.sleep(random.uniform(*CONFIG["delay_between_videos"]))
    finally:
        await scraper.close()

if __name__ == "__main__":
    asyncio.run(main())
