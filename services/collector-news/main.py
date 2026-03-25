"""
services/collector-news/main.py
 
NEWS & RSS COLLECTOR
====================
Polls RSS feeds from 20+ sources.
Deduplicates by URL hash.
Pushes RawEvents to Kafka topic: raw.news
 
FIX (code review): URL deduplication now uses a Redis sorted set with
  timestamps as scores instead of a plain set with no TTL. The old set
  grew forever — a URL first seen two years ago kept its hash in Redis
  permanently, wasting memory and preventing re-ingestion if a source
  deletes and re-publishes content (common for corrections).
  New approach: hash → zadd with current Unix timestamp as score.
  Lookup: zscore (O(1)). Cleanup: zremrangebyscore removes entries older
  than DEDUP_WINDOW_DAYS on each cycle (runs once per full poll round).
"""
 
import asyncio
import hashlib
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
 
import aiohttp
import feedparser
from dotenv import load_dotenv
 
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
 
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
)
logger = logging.getLogger("collector.news")
 
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
 
POLL_INTERVAL      = 120   # seconds between full feed cycles
DEDUP_WINDOW_DAYS  = 30    # URLs older than this are forgotten and re-ingestible


FEEDS = [
    # Wire services
    ("reuters_world",   "https://feeds.reuters.com/reuters/worldnews",        "wire",       0.95),
    ("reuters_markets", "https://feeds.reuters.com/reuters/businessnews",     "wire",       0.95),
    ("ap_world",        "https://apnews.com/rss",                             "wire",       0.95),
    ("aljazeera",       "https://www.aljazeera.com/xml/rss/all.xml",          "wire",       0.95),
    # Maritime
    ("maritime_exec",   "https://maritime-executive.com/rss-feed",            "maritime",   0.90),
    ("tanker_trackers", "https://www.tankertracker.com/feed",                 "maritime",   0.88),
    ("seatrade",        "https://www.seatrade-maritime.com/rss/all",          "maritime",   0.85),
    ("lloyds_list",     "https://lloydslist.com/rss",                         "maritime",   0.92),
    ("gcaptain",        "https://gcaptain.com/feed",                          "maritime",   0.85),
    # Geopolitical / OSINT
    ("bellingcat",      "https://www.bellingcat.com/feed",                    "osint",      0.88),
    ("rferl",           "https://www.rferl.org/api/zpqopmjpoe",               "geopolitic", 0.82),
    ("al_monitor",      "https://www.al-monitor.com/rss",                     "geopolitic", 0.82),
    ("war_on_rocks",    "https://warontherocks.com/feed",                     "defense",    0.85),
    ("defense_news",    "https://www.defensenews.com/arc/outboundfeeds/rss/", "defense",    0.88),
    # Financial
    ("ft_markets",      "https://www.ft.com/rss/home/uk",                     "financial",  0.92),
    ("cnbc_world",      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "financial", 0.85),
    ("zerohedge",       "https://feeds.feedburner.com/zerohedge/feed",        "financial",  0.60),
    ("unusual_whales",  "https://unusualwhales.com/feed/",                   "financial",  0.80),
    # Official
    ("us_state_dept",   "https://www.state.gov/rss-feeds/press-releases/",    "official",   0.98),
    ("un_news",         "https://news.un.org/feed/subscribe/en/news/all/rss.xml", "official", 0.95),
    ("ofac_news",       "https://home.treasury.gov/system/files/126/ofac.xml","official",   0.99),
]

# ── DEDUPLICATION ─────────────────────────────────────────────────────────────
class URLDeduplicator:
    """
    Deduplicates URLs using a Redis sorted set.
    Score = Unix timestamp of first ingestion.
    Entries older than DEDUP_WINDOW_DAYS are pruned each cycle.
 
    CONCEPTUAL OVERVIEW:
      Why Sorted Sets (ZSET)?
      Standard Redis Sets (SADD) are O(1) for checking existence, but they don't support
      efficient "range" deletion. If we used a standard Set, we'd have to scan all keys
      to find old ones (O(N)), which is slow.
      
      A Sorted Set stores pairs of (Member, Score). Here: (URL_Hash, Timestamp).
      This allows us to query "Does Hash exist?" in O(1), AND "Delete all items where
      Score < Cutoff" in O(log(N)). This creates a highly efficient "Sliding Window".
    """
    REDIS_KEY = "news:seen_urls"
    def __init__(self):
        self._redis = None
        self._local_cache = {}
        self._last_prune = 0.0
    
    def _get_redis(self):
        if self._redis is None:
            try:
                self._redis = get_redis()
            except Exception:
                # FAIL-SAFE:
                # If Redis is down, we fallback to in-memory caching.
                # This ensures the collector keeps running, though deduplication
                # will reset if the service restarts.
                logger.warning("Could not connect to Redis - using local dedup cache.")
        return self._redis
    
    @staticmethod
    def _hash(url:str) -> str:
        # Optimization: We hash URLs to fixed-length strings (16 chars) to save memory/bandwidth.
        # A 16-char hex digest is sufficiently collision-resistant for news feed urls.
        return hashlib.sha256(url.encode()).hexdigest()[:16]
    
    def is_seen(self, url:str) -> bool:
        h = self._hash(url)
        r = self._get_redis()
        if r:
            # ZSCORE returns the score (timestamp) if exists, or None.
            # This acts as our existence check.
            return r.raw.zscore(self.REDIS_KEY, h) is not None
        cutoff = time.time() - DEDUP_WINDOW_DAYS * 86400
        return self._local_cache.get(h, 0) > cutoff
    
    def mark_seen(self, url:str) -> None:
        h = self._hash(url)
        r = self._get_redis()
        now = time.time()
        if r:
            r.raw.zadd(self.REDIS_KEY, {h: now})
        else:
            self._local_cache[h] = now

    def prune(self) -> None:
        """Remove entries older than DEDUP_WINDOW_DAYS. Called once per cycle."""
        now = time.time()
        # Throttling: Pruning is expensive (O(log N + M)), so we only run it once per hour.
        if now - self._last_prune < 3600:   # at most once per hour
            return
        self._last_prune = now
        cutoff = now - DEDUP_WINDOW_DAYS * 86400
        r = self._get_redis()
        if r:
            # ZREMRANGEBYSCORE: The magic command. Instantly drops all hashes
            # where the score (timestamp) is between 0 and cutoff.
            removed = r.raw.zremrangebyscore(self.REDIS_KEY, 0, cutoff)
            if removed:
                logger.debug(f"Dedup pruned {removed} stale URL hashes")
        else:
            stale = [k for k, ts in self._local_cache.items() if ts < cutoff]
            for k in stale:
                del self._local_cache[k]

# ── FEED POLLING ──────────────────────────────────────────────────────────────



def _parse_pub_date(entry) -> datetime:
    # Normalization: RSS feeds use widely varying date formats (RFC 822, ISO 8601, etc).
    # feedparser.parsed returns a `struct_time` tuple. We convert this immediately
    # to a UTC-aware datetime so downstream services don't have to guess the timezone.
    if entry.get("published_parsed"):
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc)

async def poll_feed(
    session:     aiohttp.ClientSession,
    producer:    SentinelProducer,
    dedup:       URLDeduplicator,
    feed_name:   str,
    feed_url:    str,
    category:    str,
    reliability: float,
) -> int:
    new_count = 0
    try:
        async with session.get(
            feed_url,
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "SENTINEL Intelligence Platform/1.0"},
        ) as resp:
            # Non-200 responses are expected occasionally (maintenance, blocks).
            # We log debug only to avoid spamming logs with transient errors.
            if resp.status != 200:
                logger.debug(f"{feed_name}: HTTP {resp.status}")
                return 0

            content = await resp.read()

        # ── KEY CONCEPT: Blocking vs Async ────────────────────────────────────
        # feedparser is a "blocking" (CPU-bound) library. It does extensive regex
        # and string parsing. If we called `feedparser.parse(content)` directly here,
        # it would freeze the Python Event Loop, pausing ALL other downloads.
        #
        # Solution: run_in_executor.
        # This offloads the heavy parsing work to a separate thread, allowing the
        # main event loop to continue managing other network requests.
        loop = asyncio.get_event_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, content)
        
        for entry in feed.entries[:50]:
            url = entry.get("link", "")
            # Filter: If URL has been seen in the last 30 days, skip entirely.
            if not url or dedup.is_seen(url):
                continue

            title = entry.get("title", "").strip()
            if not title:
                continue

            summary = entry.get("summary", "").strip()
            pub_date = _parse_pub_date(entry)
            tags = [t.term for t in entry.get("tags", []) if hasattr(t, "term")]

            event = RawEvent(
                source = feed_name,
                occurred_at = pub_date,
                raw_payload = {
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "category": category,
                    "reliability": reliability,
                    "tags": tags,
                    "author": entry.get("author", ""),
                },
            )
            # Kafka Producer: The 'key' determines the partition.
            # Using 'feed_name' ensures all news from 'Reuters' lands in the same
            # Kafka partition, preserving partial ordering if needed.
            producer.send(Topics.RAW_NEWS, event.dict(), key=feed_name)
            dedup.mark_seen(url)
            new_count += 1

    except asyncio.TimeoutError:
        logger.warning(f"{feed_name}: Request timed out")
    except Exception as e:
        # Broad Exception Catch:
        # In a long-running collector loop, we never want one malformed feed to crash
        # the entire process. We catch, log, and return 0 so other feeds continue.
        logger.error(f"{feed_name}: {e}", exc_info=True)
    return new_count

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

async def collect(producer: SentinelProducer):
    dedup = URLDeduplicator()
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        cycle = 0
        while True:
            cycle += 1
            t0 = time.time()
            total_new = 0

            # ── CONCURRENCY ───────────────────────────────────────────────────
            # Instead of awaiting poll_feed() one by one (Sequential), we create
            # a list of coroutines and fire them all at once using `gather`.
            # This allows us to poll 20 feeds in roughly the time it takes to poll 1.
            tasks = [
                poll_feed(session, producer, dedup, name, url, cat, rel)
                for name, url, cat, rel in FEEDS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"{FEEDS[i][0]}: task error — {result}")
                else:
                    total_new += result
                
            dedup.prune()
            elapsed = time.time() - t0

            logger.info(
                f"Cycle {cycle}: {total_new} new articles from {len(FEEDS)} feeds "
                f"in {elapsed:.1f}s — next poll in {POLL_INTERVAL}s"
            )
            await asyncio.sleep(POLL_INTERVAL)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  News Collector")
    logger.info(f"Feeds: {len(FEEDS)}  |  Poll interval: {POLL_INTERVAL}s")
    logger.info("=" * 60)
    producer = SentinelProducer()

    try:
        await collect(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.close()
        
if __name__ == "__main__":
    asyncio.run(main())