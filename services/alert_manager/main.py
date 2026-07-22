"""
services/alert-manager/main.py

ALERT MANAGER
=============
Consumes correlations, generated scenarios, and intelligence briefs.
Delivers Tier 2+ alerts via Telegram and optional webhook.
Deduplication: same entity won't alert twice for the same rule/brief within 6 hours.
Rate limiting: per-rule rate limit of 10 Telegram messages per hour.
"""

import asyncio
import logging
import os
import sys
import time
import json
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("alert-manager")

from shared.kafka import SentinelConsumer, Topics
from shared.models import CorrelationCluster, AlertTier, Scenario
from shared.db import get_timescale, get_redis

from services.alert_manager.formatters.telegram import format_correlation, format_scenario, format_intel_brief
from services.alert_manager.formatters.webhook import format_generic

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API       = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL        = os.getenv("ALERT_WEBHOOK_URL")

DEDUP_TTL         = 21600   # 6 hours
RATE_LIMIT_WINDOW = 3600    # 1 hour
RATE_LIMIT_MAX    = 10      # max alerts per rule per hour

class AlertManager:
    def __init__(self, session: aiohttp.ClientSession, db_client, redis_client):
        self._session = session
        self._db      = db_client
        self._redis   = redis_client

    async def _check_rate_limit(self, rule_name: str) -> bool:
        """Returns True if rate limit is NOT exceeded for this rule."""
        now = time.time()
        cutoff = now - RATE_LIMIT_WINDOW
        key = f"sentinel:alert:rate_limit:{rule_name}"
        
        pipe = self._redis.raw.pipeline()
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.zcard(key)
        results = await pipe.execute()
        count = results[1]
        
        return count < RATE_LIMIT_MAX

    async def _record_alert_sent(self, rule_name: str):
        """Records an alert in the Redis sorted set for rate limiting."""
        now = time.time()
        key = f"sentinel:alert:rate_limit:{rule_name}"
        
        pipe = self._redis.raw.pipeline()
        pipe.zadd(key, {str(now): now})
        pipe.expire(key, RATE_LIMIT_WINDOW)
        await pipe.execute()

    async def handle_correlation(self, cluster: CorrelationCluster):
        if cluster.alert_tier == AlertTier.WATCH:
            logger.info(f"WATCH (no alert): {cluster.rule_name}")
            return
        
        entity_key = cluster.entity_ids[0] if cluster.entity_ids else "system"
        dedup_key = f"alert:sent:{cluster.rule_name}:{entity_key}"
        if await self._redis.raw.exists(dedup_key):
            logger.debug(f"deduplication skip: {cluster.rule_name} on {entity_key}")
            return
        
        rule_name = cluster.rule_name
        if not await self._check_rate_limit(rule_name):
            logger.warning(f"Rate limit reached for {rule_name} — sleeping 60s")
            await asyncio.sleep(60)
            if not await self._check_rate_limit(rule_name):
                logger.warning(f"Rate limit still exceeded for {rule_name} after backoff. Skipping alert.")
                return

        tg_text = format_correlation(cluster)
        success = await self._send_telegram(tg_text)
        if success:
            await self._record_alert_sent(rule_name)
            await self._redis.raw.set(dedup_key, "1", ex=DEDUP_TTL)
            if WEBHOOK_URL:
                await self._send_webhook(format_generic(cluster))
            logger.info(f"!! [{cluster.alert_tier.name}] {cluster.rule_name} id:{cluster.correlation_id[:8]}")

    async def handle_scenario(self, scenario: Scenario):
        """Sends a follow-up intelligence briefing when the reasoning service finishes."""
        rule_name = "scenario"
        if not await self._check_rate_limit(rule_name):
            logger.warning(f"Rate limit reached for {rule_name} — sleeping 60s")
            await asyncio.sleep(60)
            if not await self._check_rate_limit(rule_name):
                logger.warning(f"Rate limit still exceeded for {rule_name} after backoff. Skipping alert.")
                return

        tg_text = format_scenario(scenario)
        success = await self._send_telegram(tg_text)
        if success:
            await self._record_alert_sent(rule_name)
            logger.info(f"!! [INTELLIGENCE_BRIEFING] Sent for correlation {scenario.correlation_id[:8]}")

    async def handle_intel_brief(self, brief: dict):
        b = brief.get("brief", {})
        severity = brief.get("computed_severity", b.get("severity", 3))
        if severity < 3:
            return

        run_id = brief.get("agent_run_id", "unknown")
        dedup_key = f"alert:sent:intel_brief:{run_id}"
        if await self._redis.raw.exists(dedup_key):
            return

        rule_name = "intel_brief"
        if not await self._check_rate_limit(rule_name):
            logger.warning(f"Rate limit reached for {rule_name} — sleeping 60s")
            await asyncio.sleep(60)
            if not await self._check_rate_limit(rule_name):
                logger.warning(f"Rate limit still exceeded for {rule_name} after backoff. Skipping alert.")
                return

        tg_text = format_intel_brief(brief)
        success = await self._send_telegram(tg_text)
        if success:
            await self._record_alert_sent(rule_name)
            await self._redis.raw.set(dedup_key, "1", ex=DEDUP_TTL)
            logger.info(f"!! [INTEL_BRIEF] Sent for agent run {run_id}")

    async def _send_telegram(self, text: str) -> bool:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.debug("Telegram not configured")
            return False

        for attempt in range(3):
            try:
                async with self._session.post(
                    f"{TELEGRAM_API}/sendMessage",
                    json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text":   text[:4096],
                        "parse_mode": "MarkdownV2",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        logger.debug("Telegram message sent")
                        return True
                    elif resp.status == 400:
                        logger.warning("Telegram MarkdownV2 parse failed (400). Retrying with plain text fallback...")
                        clean_text = text.replace("\\", "").replace("*", "").replace("_", "").replace("`", "")
                        async with self._session.post(
                            f"{TELEGRAM_API}/sendMessage",
                            json={
                                "chat_id": TELEGRAM_CHAT_ID,
                                "text": clean_text[:4096],
                            },
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as retry_resp:
                            if retry_resp.status == 200:
                                logger.info("✅ Telegram message delivered via plain-text fallback")
                                return True
                            logger.error(f"Telegram fallback {retry_resp.status}: {(await retry_resp.text())[:200]}")
                            return False
                    elif resp.status == 429:
                        retry_after = int((await resp.json()).get("parameters", {}).get("retry_after", 30))
                        logger.warning(f"Telegram rate limit (attempt {attempt+1}/3) - sleeping {retry_after}s")
                        await asyncio.sleep(retry_after)
                    else:
                        logger.error(f"Telegram {resp.status}: {(await resp.text())[:200]}")
                        return False
            except Exception as e:
                logger.error(f"Telegram error on attempt {attempt+1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(2)
        return False
        
    async def _send_webhook(self, payload: dict):
        try:
            async with self._session.post(
                WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status not in (200, 201, 204):
                    logger.warning(f"Webhook {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Webhook error: {e}")

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL Alert Manager")
    logger.info(f"Telegram: {'configured' if TELEGRAM_BOT_TOKEN else 'NOT configured'}")
    logger.info(f"Webhook:  {'configured' if WEBHOOK_URL else 'not configured'}")
    logger.info("=" * 60)
 
    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS, "scenarios.generated", Topics.INTEL_BRIEFS],
        group_id="alert-manager",
    )
    await consumer.start()
    connector = aiohttp.TCPConnector(limit=5)

    db_client = await get_timescale()
    redis_client = await get_redis()
    
    async with aiohttp.ClientSession(connector=connector) as session:
        manager = AlertManager(session, db_client, redis_client)
        try:
            while True:
                batches = await consumer.get_batch(timeout_ms=1000)
                if not batches: continue
                
                for tp, messages in batches.items():
                    for msg in messages:
                        try:
                            payload = json.loads(msg.value.decode('utf-8'))
                            if tp.topic == "scenarios.generated":
                                scenario = Scenario(**payload)
                                await manager.handle_scenario(scenario)
                            elif tp.topic == Topics.INTEL_BRIEFS:
                                await manager.handle_intel_brief(payload)
                            else:
                                cluster = CorrelationCluster(**payload)
                                await manager.handle_correlation(cluster)
                        except Exception as e:
                            logger.error(f"Alert processing error: {e}", exc_info=True)
                
                await consumer.commit()
                
        except asyncio.CancelledError:
            pass
        finally:
            await consumer.close()

if __name__ == "__main__":
    asyncio.run(main())