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
from datetime import datetime, timezone
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
from shared.utils.heartbeat import start_heartbeat_task

from services.alert_manager.formatters.telegram import format_correlation, format_scenario, format_intel_brief
from services.alert_manager.formatters.webhook import format_generic
from shared.utils.tasks import safe_create_task

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API       = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL        = os.getenv("ALERT_WEBHOOK_URL")

# Where alerts live inside the platform, independent of outbound delivery.
ALERT_LOG_KEY = "sentinel:alerts:recent"
ALERT_UNDELIVERED_KEY = "sentinel:alerts:undelivered"
ALERT_LOG_MAX = 500
ALERT_LOG_TTL_SEC = 7 * 86400

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

    async def _record_alert_sent(self, rule_name: str, confidence: float = 0.0):
        """Records an alert in the Redis sorted set for rate limiting."""
        now = time.time()
        key = f"sentinel:alert:rate_limit:{rule_name}"
        
        pipe = self._redis.raw.pipeline()
        pipe.zadd(key, {str(now): now})
        pipe.expire(key, RATE_LIMIT_WINDOW)
        # Scored by confidence rather than time, so the weakest delivery of the
        # hour is a zrange away and can be displaced by better evidence.
        delivered = self._DELIVERED_KEY.format(rule=rule_name)
        pipe.zadd(delivered, {f"{now}": float(confidence or 0.0)})
        pipe.expire(delivered, RATE_LIMIT_WINDOW)
        await pipe.execute()

    # Confidence of each alert delivered per rule this hour, so a later and
    # better cluster can take a slot from an earlier and weaker one.
    _DELIVERED_KEY = "sentinel:alert:delivered:{rule}"

    async def _displaces_weakest(self, rule_name: str, cluster) -> bool:
        """Whether this cluster beats the weakest already sent under this rule.

        Returns True only when a slot is genuinely freed, and frees it as it
        answers, so two callers cannot claim the same one.
        """
        try:
            conf = float(getattr(cluster, "confidence_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            return False
        try:
            key = self._DELIVERED_KEY.format(rule=rule_name)
            raw = self._redis.raw
            # Oldest entries first: the window is an hour, and anything older
            # has already left the rate limiter's own window.
            await raw.zremrangebyscore(key, "-inf", time.time() - RATE_LIMIT_WINDOW)
            weakest = await raw.zrange(key, 0, 0, withscores=True)
            if not weakest:
                return False
            member, score = weakest[0]
            if conf <= float(score):
                return False
            await raw.zrem(key, member)
            logger.info(
                "Cluster at confidence %.3f displaces one at %.3f under '%s': the "
                "hour's budget goes to the better evidence.",
                conf, float(score), rule_name,
            )
            return True
        except Exception as e:
            logger.debug("Displacement check failed for %s: %s", rule_name, e)
            return False

    async def _record_alert(self, rule_name: str, title: str, body: str, delivered: bool) -> None:
        """Keeps the alert inside the platform, whether or not it left it.

        Outbound delivery and having produced an alert are two different facts,
        and this service only recorded the first. With Telegram unconfigured
        _send_telegram returns False, and every alert was then dropped on the
        floor: no store, no record, nothing for the dashboard or the API to
        read. A service whose entire job is not missing things was the one
        component that could lose them silently.

        Recorded first and delivered second, so an outbound failure -- an
        unconfigured token, a network fault, Telegram rate-limiting us -- costs
        the notification and never the alert itself.
        """
        try:
            entry = json.dumps({
                "rule_name": rule_name,
                "title": title,
                "body": body[:4000],
                "delivered": bool(delivered),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            pipe = self._redis.raw.pipeline()
            pipe.lpush(ALERT_LOG_KEY, entry)
            pipe.ltrim(ALERT_LOG_KEY, 0, ALERT_LOG_MAX - 1)
            pipe.expire(ALERT_LOG_KEY, ALERT_LOG_TTL_SEC)
            if not delivered:
                # A separate queue, so "produced but not delivered" is one
                # lookup rather than a scan of everything.
                pipe.lpush(ALERT_UNDELIVERED_KEY, entry)
                pipe.ltrim(ALERT_UNDELIVERED_KEY, 0, ALERT_LOG_MAX - 1)
                pipe.expire(ALERT_UNDELIVERED_KEY, ALERT_LOG_TTL_SEC)
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Could not record alert {rule_name}: {e}")

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
            # The budget is spent on the best of the hour, not the first of it.
            #
            # Live: 50 refusals for Cross-Domain Semantic Convergence and 21 for
            # Cyber Aviation Chokepoint in twenty-five minutes, both against a
            # 10/hr ceiling. The ceiling is right -- those are the two rules that
            # fire most and mean least -- but selection was arrival order, so
            # which ten of eighty got through was arbitrary. The platform ranks
            # every correlation by confidence and then discarded the surplus
            # without consulting it.
            #
            # A cluster more confident than the weakest one already delivered
            # this hour displaces it. Nothing is sent twice and the ceiling is
            # unchanged; what changes is which ten a reader sees.
            if not await self._displaces_weakest(rule_name, cluster):
                logger.info(
                    "Rate limit reached for rule '%s' (%s/hr) and this cluster "
                    "(confidence %.3f) does not beat what has already been sent.",
                    rule_name, RATE_LIMIT_MAX, float(cluster.confidence_score or 0.0),
                )
                return

        tg_text = format_correlation(cluster)
        success = await self._send_telegram(tg_text)
        if success:
            await self._record_alert_sent(rule_name, cluster.confidence_score or 0.0)
            await self._redis.raw.set(dedup_key, "1", ex=DEDUP_TTL)
            if WEBHOOK_URL:
                await self._send_webhook(format_generic(cluster))
            logger.info(f"!! [{cluster.alert_tier.name}] {cluster.rule_name} id:{cluster.correlation_id[:8]}")
        await self._record_alert(
            rule_name, cluster.summary_headline or rule_name, tg_text, success
        )

    async def handle_scenario(self, scenario: Scenario):
        """Sends a follow-up intelligence briefing when the reasoning service finishes."""
        dedup_key = f"alert:sent:scenario:{scenario.correlation_id}"
        if await self._redis.raw.exists(dedup_key):
            logger.debug(f"Deduplication skip: Scenario {scenario.correlation_id[:8]} already alerted")
            return

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
            await self._redis.raw.set(dedup_key, "1", ex=DEDUP_TTL)
            logger.info(f"!! [INTELLIGENCE_BRIEFING] Sent for correlation {scenario.correlation_id[:8]}")
        if WEBHOOK_URL:
            await self._send_webhook(scenario.model_dump(mode="json"))

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
        if WEBHOOK_URL:
            await self._send_webhook(brief)
        await self._record_alert(rule_name, str(run_id), tg_text, success)

    async def _send_telegram(self, text: str) -> bool:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            # Once per process, not once per alert. This is a deployment state,
            # not an event, and at alert volume it buried the log.
            if not getattr(self, "_telegram_unconfigured_said", False):
                self._telegram_unconfigured_said = True
                logger.warning(
                    "Telegram is not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID unset). "
                    "Alerts are still evaluated and recorded to %s; they are not sent outward.",
                    ALERT_LOG_KEY,
                )
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
                        raw_error = await resp.text()
                        logger.warning(f"Telegram API 400 error: {raw_error}. Retrying with plain-text fallback...")
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
                            logger.error(f"Telegram plain-text fallback failed ({retry_resp.status}): {(await retry_resp.text())[:200]}")
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
        topics=[Topics.CORRELATIONS, Topics.SCENARIOS_GENERATED, Topics.INTEL_BRIEFS],
        group_id="alert-manager",
    )
    await consumer.start()
    connector = aiohttp.TCPConnector(limit=5)

    db_client = await get_timescale()
    redis_client = await get_redis()
    
    async with aiohttp.ClientSession(connector=connector) as session:
        manager = AlertManager(session, db_client, redis_client)

        # §1.1 Universal heartbeat — silent alert pipeline death is catastrophic
        hb_task = safe_create_task(start_heartbeat_task(redis_client, "alert_manager"))
        try:
            while True:
                batches = await consumer.get_batch(timeout_ms=1000)
                if not batches: continue
                
                for tp, messages in batches.items():
                    for msg in messages:
                        try:
                            payload = json.loads(msg.value.decode('utf-8'))
                            if tp.topic == Topics.SCENARIOS_GENERATED:
                                scenario = Scenario(**payload)
                                await manager.handle_scenario(scenario)
                            elif tp.topic == Topics.INTEL_BRIEFS:
                                await manager.handle_intel_brief(payload)
                            elif tp.topic == Topics.CORRELATIONS:
                                # Matched, not assumed. This was an `else`, so
                                # every payload that was not a scenario or a
                                # brief was constructed as a CorrelationCluster
                                # regardless of what it actually was.
                                cluster = CorrelationCluster(**payload)
                                await manager.handle_correlation(cluster)
                            else:
                                logger.debug(
                                    "No handler for topic %s; message ignored.", tp.topic
                                )
                        except Exception as e:
                            logger.error(f"Alert processing error: {e}", exc_info=True)
                
                await consumer.commit()
                
        except asyncio.CancelledError:
            pass
        finally:
            hb_task.cancel()
            await consumer.close()

if __name__ == "__main__":
    asyncio.run(main())