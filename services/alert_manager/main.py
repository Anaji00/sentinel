"""
services/alert-manager/main.py
 
ALERT MANAGER
=============
Consumes correlations.detected.
Delivers Tier 2+ alerts via Telegram and optional webhook.
 
Routing:
  Tier 1 (WATCH):        Silent log only.
  Tier 2 (ALERT):        Telegram correlation summary.
  Tier 3 (INTELLIGENCE): Telegram correlation + scenario if available.
 
Deduplication: same correlation_id won't alert twice within 6 hours.
Rate limiting: max 10 Telegram messages per hour.
"""
 
# ── 1. PYTHON STANDARD LIBRARY IMPORTS ────────────────────────────────────────
# Built-in modules for async operations, logging, and system path management.
import asyncio
import logging
import os
import sys
import time
import json
from pathlib import Path
 
# ── 2. THIRD-PARTY LIBRARY IMPORTS ────────────────────────────────────────────
# External packages installed via pip.
import aiohttp
from dotenv import load_dotenv
 
# Setup the project's root directory so we can safely import our internal 'shared' 
# libraries regardless of where this script is executed from.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
 
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("alert-manager")
 
# ── 3. INTERNAL SHARED IMPORTS ────────────────────────────────────────────────
# These modules are shared across all Sentinel microservices.
from shared.kafka import SentinelConsumer, Topics
from shared.models import CorrelationCluster, AlertTier, Scenario
from shared.db import get_timescale, get_redis
 
# ── 4. LOCAL RELATIVE IMPORTS ─────────────────────────────────────────────────
# BEGINNER EXPLANATION: The dot (.) before formatters means "look in the current 
# directory". This imports formatting functions specifically built for Telegram 
# and Webhooks, keeping the main logic clean (Separation of Concerns).
from services.alert_manager.formatters.telegram import format_correlation, format_scenario
from services.alert_manager.formatters.webhook import format_generic
 
# ── CONFIGURATION & SECRETS ───────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
# The base URL for making HTTP requests to the Telegram Bot API.
TELEGRAM_API       = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL        = os.getenv("ALERT_WEBHOOK_URL")
 
# ── RATE LIMITING & DEDUPLICATION SETTINGS ────────────────────────────────────
# CRITICAL THINKING: Preventing Alert Fatigue.
# If a system detects the exact same anomaly every 5 minutes, we don't want to spam 
# the human analyst's phone. Deduplication ensures we only send one alert per 
# specific correlation every 6 hours. Furthermore, rate limiting acts as a global 
# fail-safe: we will NEVER send more than 10 alerts total per hour.
DEDUP_TTL         = 21600   # 6 hours
RATE_LIMIT_WINDOW = 3600    # 1 hour
RATE_LIMIT_MAX    = 10      # max Telegram per hour

class AlertManager:
    # THE 'INIT' METHOD (Constructor)
    # This sets up our AlertManager object with all the tools it needs to do its job.
    def __init__(self, session: aiohttp.ClientSession, db_client, redis_client):
        self._session = session
        self._db      = db_client
        self._redis   = redis_client
        self._sent = []

    async def handle_correlation(self, cluster: CorrelationCluster):
        # 1. FILTERING: We don't send external notifications for low-tier (WATCH) events.
        if cluster.alert_tier == AlertTier.WATCH:
            logger.info(f"WATCH (no alert): {cluster.rule_name}")
            return
        
        # 2. DEDUPLICATION: Check the Redis cache using semantic keys (rule + entity).
        # This prevents alert fatigue even if new correlation IDs are generated.
        entity_key = cluster.entity_ids[0] if cluster.entity_ids else "system"
        dedup_key = f"alert:sent:{cluster.rule_name}:{entity_key}"
        if await self._redis.raw.exists(dedup_key):
            logger.debug(f"deduplication skip: {cluster.rule_name} on {entity_key}")
            return
        
        now = time.time()
        cutoff = now - RATE_LIMIT_WINDOW
        
        # 3. RATE LIMITING
        self._sent = [t for t in self._sent if t > cutoff]
        if len(self._sent) >= RATE_LIMIT_MAX:
            logger.warning("Rate limit reached — sleeping 60s")
            await asyncio.sleep(60)
 
        # 4. FORMATTING & DELIVERY: Send the immediate alert.
        tg_text = format_correlation(cluster)
        await self._send_telegram(tg_text)
        if WEBHOOK_URL:
            await self._send_webhook(format_generic(cluster))

        await self._redis.raw.set(dedup_key, "1", ex=DEDUP_TTL)
        self._sent.append(now)

        logger.info(f"!! [{cluster.alert_tier.name}] {cluster.rule_name} id:{cluster.correlation_id[:8]}")

    async def handle_scenario(self, scenario: Scenario):
        """Sends a follow-up intelligence briefing when the reasoning service finishes."""
        now = time.time()
        cutoff = now - RATE_LIMIT_WINDOW
        self._sent = [t for t in self._sent if t > cutoff]
        if len(self._sent) >= RATE_LIMIT_MAX:
            await asyncio.sleep(60)

        tg_text = format_scenario(scenario)
        await self._send_telegram(tg_text)
        self._sent.append(now)
        logger.info(f"!! [INTELLIGENCE_BRIEFING] Sent for correlation {scenario.correlation_id[:8]}")


    async def _send_telegram(self, text: str):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.debug("Telegram not configured")
            return

        try:
            async with self._session.post(
                f"{TELEGRAM_API}/sendMessage",
                json = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text":   text[:4096],
                    "parse_mode": "MarkdownV2",
                
                },
                timeout = aiohttp.ClientTimeout(total = 10),
            ) as resp:
                if resp.status == 200:
                    logger.debug("Telegram message sent")
                elif resp.status == 429:
                    retry = int((await resp.json()).get("parameters", {}).get("retry_after", 30))

                    logger.warning(f"Telegram rate limit - sleeping {retry}s")
                    await asyncio.sleep(retry)
                else:
                    logger.error(f"Telegram {resp.status}: {(await resp.text())[:200]}")

        except Exception as e:
            logger.error(f"Telegram error: {e}")
        
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
    logger.info("SENTINEL  Alert Manager")
    logger.info(f"Telegram: {'configured' if TELEGRAM_BOT_TOKEN else 'NOT configured'}")
    logger.info(f"Webhook:  {'configured' if WEBHOOK_URL else 'not configured'}")
    logger.info("=" * 60)
 
    # --- CONSUMER SETUP ---
    # Here we define our Kafka Consumer. It subscribes to the 'correlations.detected' topic.
    # The 'group_id' means if you run 3 instances of this script, Kafka will share 
    # the load among them rather than sending duplicate messages to all 3.
    consumer = SentinelConsumer(
        topics=[Topics.CORRELATIONS, "scenarios.generated"],
        group_id="alert-manager",
    )
    await consumer.start()
    connector = aiohttp.TCPConnector(limit=5)

    db_client = await get_timescale()
    redis_client = await get_redis()
    
    # We use aiohttp.ClientSession so we can reuse the same network connection 
    # for all our Telegram/Webhook requests. This is much faster than opening a new 
    # connection every single time.
    async with aiohttp.ClientSession(connector=connector) as session:
        manager = AlertManager(session, db_client, redis_client)
        try:
            # --- THE OUTER LOOP ---
            # The 'while True' loop keeps the program running forever.
            while True:
                batches = await consumer.get_batch(timeout_ms=1000)
                if not batches: continue
                
                for tp, messages in batches.items():
                    for msg in messages:
                        try:
                            # FIX: Decode bytes securely
                            payload = json.loads(msg.value.decode('utf-8'))
                            if tp.topic == "scenarios.generated":
                                scenario = Scenario(**payload)
                                await manager.handle_scenario(scenario)
                            else:
                                cluster = CorrelationCluster(**payload)
                                await manager.handle_correlation(cluster)
                        except Exception as e:
                            logger.error(f"Alert processing error: {e}", exc_info=True)
                
                # FIX: Checkpoint completion
                await consumer.commit()
                
        except asyncio.CancelledError:
            pass
        finally:
            await consumer.close()

if __name__ == "__main__":
    asyncio.run(main())
        


 
        

 