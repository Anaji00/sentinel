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
from shared.models import CorrelationCluster, AlertTier
from shared.db import get_timescale, get_redis
 
# ── 4. LOCAL RELATIVE IMPORTS ─────────────────────────────────────────────────
# BEGINNER EXPLANATION: The dot (.) before formatters means "look in the current 
# directory". This imports formatting functions specifically built for Telegram 
# and Webhooks, keeping the main logic clean (Separation of Concerns).
from formatters.telegram import format_correlation, format_scenario
from formatters.webhook  import format_generic
 
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
    # 'self' means "store this tool/data on the object so other methods can use it later."
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._db      = get_timescale()
        self._redis   = get_redis()
        self._sent = []

    async def handle(self, cluster: CorrelationCluster):
        # 1. FILTERING: We don't send external notifications for low-tier (WATCH) events.
        if cluster.alert_tier == AlertTier.WATCH:
            logger.info(f"WATCH (no alert): {cluster.rule_name}")
            return
        
        # 2. DEDUPLICATION: Check the Redis cache to see if we already alerted about 
        # this exact anomaly ID in the last 6 hours. If so, skip it to prevent spam.
        dedup_key = f"alert:sent:{cluster.correlation_id}"
        if self._redis.exists(dedup_key):
            logger.debug(f"deduplication skip: {cluster.correlation_id}")
            return
        
        now = time.time()
        # FIX: Subtracted the window from 'now'. Previously this was `now = RATE_LIMIT_WINDOW`
        # which overwrote the current timestamp and broke the rate limiter.
        cutoff = now - RATE_LIMIT_WINDOW
        
        # 3. RATE LIMITING: Clean up our history array (`self._sent`), keeping 
        # only the timestamps of alerts sent within the last hour.
        self._sent = [t for t in self._sent if t > cutoff]

        if len(self._sent) >= RATE_LIMIT_MAX:
            logger.warning("Rate limit reached — sleeping 60s")
            await asyncio.sleep(60)
 
        # 4. ENRICHMENT: If this is a critical Tier 3 intelligence event, reach into 
        # the database to grab the AI-generated context/scenario to attach to the alert.
        scenario = None
        if cluster.alert_tier == AlertTier.INTELLIGENCE:
            scenario = self._fetch_scenario(cluster.correlation_id)
        
        # 5. FORMATTING: Use our functional formatters to build the message text.
        tg_text = format_correlation(cluster)
        if scenario:
            # FIX: Corrected newline character from "/n/n" to "\n\n"
            tg_text += "\n\n" + format_scenario(scenario)
        
        # 6. DELIVERY: Send the alert out to our external webhooks and APIs.
        await self._send_telegram(tg_text)
        if WEBHOOK_URL:
            await self._send_webhook(format_generic(cluster))

        self._redis.set(dedup_key, "1", ttl=DEDUP_TTL)
        self._sent.append(now)

        logger.info(
            f"!! [{cluster.alert_tier.name}] {cluster.rule_name} "
            f"id:{cluster.correlation_id[:8]}"
        )

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

    def _fetch_scenario(self, correlation_id: str):
        # BEST PRACTICE TIP: 
        # This method fetches scenario data synchronously from the PostgreSQL database.
        # Because this is called from inside an `async def handle`, it might slightly block 
        # the event loop during heavy loads. In the future, wrapping this in 
        # `asyncio.get_event_loop().run_in_executor()` would make it fully non-blocking!
        try:
            from shared.models import Scenario, ScenarioStatus
            row = self._db.query_one("""
                SELECT * FROM scenarios
                WHERE correlation_id = %s
                  AND status NOT IN ('denied','expired')
                ORDER BY created_at DESC LIMIT 1
            """, (correlation_id,))
            if not row:
                return None
            return Scenario(
                scenario_id=str(row["scenario_id"]),
                correlation_id=str(row["correlation_id"]),
                headline=row["headline"] or "",
                significance=row["significance"] or "",
                hypotheses=row["hypotheses"] or [],
                recommended_monitoring=row["recommended_monitoring"] or [],
                confidence_overall=row["confidence_overall"] or 50,
                confidence_rationale=row["confidence_rationale"] or "",
                status=ScenarioStatus(row["status"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        except Exception as e:
            logger.debug(f"fetch_scenario: {e}")
            return None
        
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
        topics=[Topics.CORRELATIONS],
        group_id="alert-manager",
        auto_offset_reset="latest",
    )
    connector = aiohttp.TCPConnector(limit=5)

    # We use aiohttp.ClientSession so we can reuse the same network connection 
    # for all our Telegram/Webhook requests. This is much faster than opening a new 
    # connection every single time.
    async with aiohttp.ClientSession(connector=connector) as session:
        manager = AlertManager(session)
        try:
            # --- THE OUTER LOOP ---
            # The 'while True' loop keeps the program running forever.
            while True:
                try:
                    # --- THE INNER LOOP (The Event Pump) ---
                    # The consumer acts as a generator. This line will PAUSE the code here 
                    # until a new message arrives from Kafka. 
                    for message in consumer:
                        try:
                            # When a message arrives, it is fed in as a raw dictionary (`message.value`).
                            # We unpack it (**) into our strongly-typed `CorrelationCluster` object.
                            cluster = CorrelationCluster(**message.value)
                            # Hand the data off to our processing pipeline (the `handle` method above).
                            await manager.handle(cluster)
                            
                        except Exception as e:
                            logger.error(f"Alert loop error: {e}", exc_info=True)
                except StopIteration:
                    continue
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        
        finally:
            consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
        


 
        

 