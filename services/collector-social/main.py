"""
services/collector-social/main.py

PRIMARY-SOURCE SOCIAL & OSINT COLLECTOR
=======================================
Ingests real-time breaking social signals from curated accounts and channels:
1. Telegram Public Channels (Geopolitical OSINT & Maritime channels)
2. Reddit Subreddits (Geopolitical & Financial communities via JSON feeds)
3. X/Twitter Curated Feeds (High-signal breaking OSINT & TradFi accounts)

Publishes RawEvents with `source_type: "primary_social"` to Kafka topic: `events.raw.social`.
Deduplication via Redis sorted set with timestamp score and TTL.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.utils.logging import setup_sentinel_logging
logger = setup_sentinel_logging("collector.social", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task

POLL_INTERVAL_SEC = 60
DEDUP_TTL_DAYS = 14

# ── CURATED TELEGRAM PUBLIC CHANNELS ──────────────────────────────────────────
# High-signal OSINT, maritime tracking, and geopolitical breaking channels
TELEGRAM_CHANNELS = [
    ("OSINTtechnical", "geopolitical", 0.70),
    ("BellumActaNews", "geopolitical", 0.65),
    ("MaritimeExecutive", "maritime", 0.75),
    ("IntelDoge", "geopolitical", 0.65),
    ("ClashReport", "defense", 0.65),
    ("MiddleEastEye", "geopolitical", 0.60),
    ("warmonitors", "defense", 0.60),
]

# ── CURATED REDDIT SUBREDDITS ─────────────────────────────────────────────────
# Financial, options flow, commodities, and geopolitical communities
REDDIT_SUBREDDITS = [
    ("geopolitics", "geopolitical", 0.70),
    ("wallstreetbets", "financial", 0.50),
    ("options", "financial", 0.65),
    ("worldnews", "geopolitical", 0.60),
    ("stocks", "financial", 0.60),
    ("energy", "energy", 0.70),
    ("commodities", "commodities", 0.70),
]

# ── CURATED X/TWITTER BREAKING HANDLES ────────────────────────────────────────
TWITTER_HANDLES = [
    ("sentdefender", "geopolitical", 0.65),
    ("DeItaone", "financial", 0.80),
    ("FirstSquawk", "financial", 0.75),
    ("Schuldensuehn", "macro", 0.75),
    ("KobeissiLetter", "financial", 0.70),
    ("AuroraIntel", "geopolitical", 0.65),
]


class SocialDeduplicator:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.key = "sentinel:seen:social"

    async def is_seen(self, item_id: str) -> bool:
        if not self.redis:
            return False
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            score = await raw_redis.zscore(self.key, item_id)
            return score is not None
        except Exception:
            return False

    async def mark_seen(self, item_id: str):
        if not self.redis:
            return
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            now = time.time()
            pipe = raw_redis.pipeline()
            pipe.zadd(self.key, {item_id: now})
            # Prune entries older than DEDUP_TTL_DAYS
            cutoff = now - (DEDUP_TTL_DAYS * 86400)
            pipe.zremrangebyscore(self.key, 0, cutoff)
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Social dedup mark warning: {e}")


# ── TELEGRAM SCRAPER ─────────────────────────────────────────────────────────

RE_TG_TEXT = re.compile(r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', re.DOTALL)
RE_CLEAN_HTML = re.compile(r'<[^<]+?>')


async def poll_telegram_channel(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: SocialDeduplicator,
    channel: str,
    category: str,
    reliability: float,
) -> int:
    """Scrapes recent posts from a public Telegram channel via web preview."""
    url = f"https://t.me/s/{channel}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    new_count = 0

    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0
            html = await resp.text()

        matches = RE_TG_TEXT.findall(html)
        for raw_text in matches[-10:]:  # Evaluate last 10 posts
            clean_text = RE_CLEAN_HTML.sub("", raw_text).strip()
            if not clean_text or len(clean_text) < 15:
                continue

            post_hash = hashlib.sha256(f"tg:{channel}:{clean_text[:120]}".encode("utf-8")).hexdigest()[:16]
            if await dedup.is_seen(post_hash):
                continue

            await dedup.mark_seen(post_hash)

            event = RawEvent(
                source="telegram",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "title": clean_text[:120] + ("..." if len(clean_text) > 120 else ""),
                    "summary": clean_text[:800],
                    "url": f"https://t.me/{channel}",
                    "author": f"@{channel}",
                    "channel": channel,
                    "platform": "telegram",
                    "source_type": "primary_social",
                    "category": category,
                    "reliability": reliability,
                    "epistemic_confidence": reliability,
                    "tags": ["social", "telegram", f"channel:{channel}", category],
                }
            )

            await producer.send(Topics.RAW_SOCIAL, event.model_dump(), key=channel)
            new_count += 1

    except Exception as e:
        logger.debug(f"Telegram poll notice for @{channel}: {e}")

    return new_count


# ── REDDIT SUBREDDIT POLLER ──────────────────────────────────────────────────

async def poll_reddit_subreddit(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: SocialDeduplicator,
    subreddit: str,
    category: str,
    reliability: float,
) -> int:
    """Polls recent submissions from a subreddit via public JSON API."""
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=15"
    headers = {"User-Agent": "Sentinel-Intelligence-Collector/1.0"}
    new_count = 0

    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return 0
            data = await resp.json()

        posts = data.get("data", {}).get("children", [])
        for p in posts:
            p_data = p.get("data", {})
            post_id = p_data.get("id")
            title = p_data.get("title", "").strip()
            selftext = p_data.get("selftext", "").strip()
            permalink = p_data.get("permalink", "")
            author = p_data.get("author", "anonymous")
            created_utc = p_data.get("created_utc", time.time())

            if not post_id or not title:
                continue

            item_key = f"reddit:{subreddit}:{post_id}"
            if await dedup.is_seen(item_key):
                continue

            await dedup.mark_seen(item_key)

            combined_text = f"{title} {selftext[:500]}".strip()
            event = RawEvent(
                source="reddit",
                occurred_at=datetime.fromtimestamp(created_utc, tz=timezone.utc),
                raw_payload={
                    "title": title,
                    "summary": selftext[:800] if selftext else title,
                    "url": f"https://reddit.com{permalink}",
                    "author": f"u/{author}",
                    "subreddit": subreddit,
                    "platform": "reddit",
                    "source_type": "primary_social",
                    "category": category,
                    "reliability": reliability,
                    "epistemic_confidence": reliability,
                    "tags": ["social", "reddit", f"r/{subreddit}", category],
                }
            )

            await producer.send(Topics.RAW_SOCIAL, event.model_dump(), key=subreddit)
            new_count += 1

    except Exception as e:
        logger.debug(f"Reddit poll notice for r/{subreddit}: {e}")

    return new_count


# ── 3. CURATED X/TWITTER SYNDICATED POLLER ───────────────────────────────────

async def poll_twitter_handle(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: SocialDeduplicator,
    handle: str,
    category: str,
    reliability: float,
) -> int:
    """
    Polls curated high-signal X/Twitter handles via public syndication/Nitter bridges.
    Gracefully defers if upstream endpoints are unreachable or unconfigured.
    """
    new_count = 0
    bridge_base = os.getenv("TWITTER_RSS_BRIDGE_URL", "https://nitter.net")
    url = f"{bridge_base}/{handle}/rss"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return 0
            xml_text = await resp.text()

        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_text)
        items = root.findall(".//item")

        for item in items[:10]:
            title = (item.find("title").text if item.find("title") is not None else "").strip()
            desc = (item.find("description").text if item.find("description") is not None else "").strip()
            link = (item.find("link").text if item.find("link") is not None else f"https://x.com/{handle}").strip()
            guid = (item.find("guid").text if item.find("guid") is not None else link).strip()

            if not title and not desc:
                continue

            item_key = f"twitter:{handle}:{guid}"
            if await dedup.is_seen(item_key):
                continue

            await dedup.mark_seen(item_key)

            clean_text = re.sub(r"<[^>]+>", "", desc or title).strip()
            event = RawEvent(
                source="twitter",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "title": title or clean_text[:80],
                    "summary": clean_text[:800],
                    "url": link,
                    "author": f"@{handle}",
                    "platform": "twitter",
                    "source_type": "primary_social",
                    "category": category,
                    "reliability": reliability,
                    "epistemic_confidence": reliability,
                    "tags": ["social", "twitter", f"@{handle}", category],
                }
            )
            await producer.send(Topics.RAW_SOCIAL, event.model_dump(), key=handle)
            new_count += 1

    except Exception as e:
        logger.debug(f"X/Twitter poll notice for @{handle}: {e}")

    return new_count


# ── MAIN ORCHESTRATION ───────────────────────────────────────────────────────

async def collect_social_cycle(
    session: aiohttp.ClientSession,
    producer: SentinelProducer,
    dedup: SocialDeduplicator,
) -> int:
    """Executes a full social polling cycle across Telegram, Reddit, and curated X feeds."""
    tasks = []

    for channel, cat, rel in TELEGRAM_CHANNELS:
        tasks.append(poll_telegram_channel(session, producer, dedup, channel, cat, rel))

    for sub, cat, rel in REDDIT_SUBREDDITS:
        tasks.append(poll_reddit_subreddit(session, producer, dedup, sub, cat, rel))

    for handle, cat, rel in TWITTER_HANDLES:
        tasks.append(poll_twitter_handle(session, producer, dedup, handle, cat, rel))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_new = sum(r for r in results if isinstance(r, int))
    return total_new


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL Social & Primary-Source OSINT Collector")
    logger.info(f"Telegram Channels: {len(TELEGRAM_CHANNELS)} | Subreddits: {len(REDDIT_SUBREDDITS)} | Curated X Feeds: {len(TWITTER_HANDLES)}")
    logger.info("=" * 60)

    producer = SentinelProducer()
    await producer.start()

    redis_client = await get_redis()
    dedup = SocialDeduplicator(redis_client)

    # §1.1 Universal heartbeat
    hb_task = asyncio.create_task(start_heartbeat_task(redis_client, "collector-social"))

    connector = aiohttp.TCPConnector(limit=20)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            cycle = 0
            while True:
                cycle += 1
                t0 = time.time()
                try:
                    new_events = await collect_social_cycle(session, producer, dedup)
                    elapsed = time.time() - t0
                    logger.info(f"Social Poll Cycle #{cycle}: Ingested {new_events} primary-source posts in {elapsed:.1f}s")
                except Exception as e:
                    logger.error(f"Social collection cycle error: {e}", exc_info=True)

                await asyncio.sleep(POLL_INTERVAL_SEC)
    finally:
        hb_task.cancel()
        await producer.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
