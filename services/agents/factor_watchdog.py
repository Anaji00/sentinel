"""
services/agents/factor_watchdog.py

Watches the factor engine from outside it (shared/utils/engine_liveness.py).

Every CHECK_SEC it reads the 'factor-engine' heartbeat. Silent for more than 180 seconds, or gone: it raises one
STALE_FACTOR_ENGINE correlation per outage (ELEVATED during US market hours, ALERT otherwise; never INTELLIGENCE, which
would start a model scenario for an infrastructure fact) and keeps a transient halt flag set that the paper-account
trader checks before any order. A fresh beat clears the flag. No model call.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from services.correlation.event_store import EventStore
from shared.kafka import SentinelProducer, Topics
from shared.models.events import AlertTier, CorrelationCluster
from shared.utils import engine_liveness as el
from shared.utils.quiet_failures import dropped, swallowed

logger = logging.getLogger("agent.factor_watchdog")

CHECK_SEC = 30


class FactorWatchdog:
    def __init__(self, db_client: Any, redis_client: Any) -> None:
        self.db, self.redis = db_client, redis_client
        self._producer: Optional[SentinelProducer] = None

    async def run(self) -> None:
        self._producer = SentinelProducer(service_name="factor-watchdog")
        await self._producer.start()
        await asyncio.sleep(60)
        try:
            while True:
                try:
                    await self.check(datetime.now(timezone.utc))
                except Exception as exc:
                    swallowed("agents.factor_watchdog.check", exc, logger)
                await asyncio.sleep(CHECK_SEC)
        finally:
            await self._producer.close()

    async def check(self, now: datetime) -> str:
        """One look. Returns 'ok', 'stale' or 'missing'."""
        raw = getattr(self.redis, "raw", self.redis)
        age = el.heartbeat_age(await raw.get(f"sentinel:heartbeat:{el.COMPONENT}"), now)
        state, why = el.verdict(age)
        if state == "ok":
            await raw.delete(el.HALT_KEY)
            await raw.set(el.SEEN_KEY, "1")
            return state
        if state == "missing" and not await raw.get(el.SEEN_KEY):
            return "unseen"                  # the engine has not sent this heartbeat since it was added: not an outage
        await raw.set(el.HALT_KEY, why, ex=el.HALT_TTL_SEC)               # renewed while the outage lasts
        if await raw.set(el.ALERT_ONCE_KEY, "1", nx=True, ex=el.ALERT_ONCE_TTL_SEC):
            await self._alert(why, state, now)
        return state

    async def _alert(self, why: str, state: str, now: datetime) -> bool:
        market = el.in_market_hours(now)
        cluster = CorrelationCluster(
            rule_id="STALE_FACTOR_ENGINE",
            rule_name="Factor Engine Silent",
            alert_tier=AlertTier.ELEVATED if market else AlertTier.ALERT,
            primary_domain="financial",
            confidence_score=0.99,
            summary_headline=f"Factor engine {state}: {why}",
            supporting_headlines=[],
            metrics_summary={"state": state, "market_hours": market, "limit_seconds": el.STALE_AFTER_SEC,
                             "trader_halted_until_beat_resumes": True},
            trigger_event_id=str(uuid.uuid4()),
            supporting_event_ids=[],
            primary_entity_id="FACTOR-ENGINE",
            primary_entity_name="Factor engine",
            entity_ids=["FACTOR-ENGINE"],
            entity_names=["Factor engine"],
            description=(f"{why}. The nightly model, the paper-account trader and the jobs that share its loop may be "
                         "running on stale data. New orders are skipped until the heartbeat resumes; nothing held is sold."),
            tags=["infrastructure", "factor_engine", "stale"],
        )
        try:
            await EventStore(self.redis, self.db).save_correlation(cluster)
            assert self._producer is not None
            await self._producer.send(Topics.CORRELATIONS, cluster.model_dump(), key="FACTOR-ENGINE")
        except Exception as exc:
            dropped("agents.factor_watchdog.publish", f"{type(exc).__name__}: {exc}", logger)
            return False
        logger.error("STALE_FACTOR_ENGINE raised: %s", why)
        return True


__all__ = ["FactorWatchdog"]
