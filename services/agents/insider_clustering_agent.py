"""
services/agents/insider_clustering_agent.py

SEC FORM 4 EXECUTIVE INSIDER CLUSTERING AGENT
==============================================
Aggregates Form 4 insider transactions per company over a 48-hour window.
Clusters co-occurring buying/selling by C-suite executives (CEO, CFO, Directors, 10% Owners)
and computes an aggregate Insider Conviction Index (C_insider).

Emits cluster conviction alerts to `agents.insider.clusters`.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError, InferenceError

logger = logging.getLogger("agent.insider_clustering")


class InsiderClusterBrief(BaseModel):
    ticker: str
    executive_count: int
    net_direction: str  # "NET_BUYING", "NET_SELLING", "MIXED"
    total_notional_usd: float
    conviction_index: float  # 0.0 to 100.0
    strategic_signal: str
    key_executives_involved: List[str] = Field(default_factory=list)
    analytical_summary: str


class InsiderClusteringAgent(SentinelAgent):
    """
    SEC Form 4 executive trade clustering and institutional conviction agent.
    """

    @property
    def output_topic(self) -> str:
        return "agents.insider.clusters"

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw = message.get("raw_payload", message)
        source = message.get("source", "")
        event_type = message.get("type", "")

        if not (source == "sec_form4" or event_type == "INSIDER_TRADE" or "insider_trade" in message.get("tags", [])):
            return None

        fin_data = message.get("financial_data", {})
        ticker = str(fin_data.get("ticker") or message.get("primary_entity", {}).get("id") or "").upper()
        if not ticker:
            return None

        headline = str(message.get("headline", ""))
        premium = float(fin_data.get("premium_usd") or 500000.0)

        # Buffer recent Form 4 filings in Redis per ticker over 48 hours (172800s)
        redis_key = f"sentinel:insider:trades:{ticker}"
        trade_record = json.dumps({
            "headline": headline,
            "premium": premium,
            "ts": time.time()
        })

        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.rpush(redis_key, trade_record)
            pipe.ltrim(redis_key, -50, -1)
            pipe.expire(redis_key, 172800)
            await pipe.execute()

        raw_trades = await self.redis.raw.lrange(redis_key, 0, -1)
        if not raw_trades or len(raw_trades) < 1:
            return None

        trades = [json.loads(t.decode("utf-8") if isinstance(t, bytes) else t) for t in raw_trades]
        exec_count = len(trades)
        total_notional = sum(t.get("premium", 0.0) for t in trades)

        dedup_key = f"insider_cluster_eval:{ticker}:{int(time.time() // 3600)}"
        if await self.is_recently_processed(dedup_key, window_seconds=3600):
            return None
        await self.mark_processed(dedup_key, window_seconds=3600)

        logger.info(f"👔 Form 4 Insider Cluster | Ticker: {ticker} | Filings: {exec_count} | Total Notional: ${total_notional/1e6:.2f}M")

        global_context = await self.fetch_global_context()

        user_prompt = f"""
        As an institutional equity analyst, evaluate SEC Form 4 insider trade cluster:
        - Symbol: {ticker}
        - 48-Hour Filing Count: {exec_count}
        - Total Aggregate Notional Value: ${total_notional:,.2f}
        - Recent Filings:
        {json.dumps([t.get('headline') for t in trades[:5]], indent=2)}

        GLOBAL SWARM CONTEXT:
        {global_context}

        Evaluate C-suite conviction, open-market buys vs routine option exercises, and corporate governance signals.
        Generate structured insider cluster brief JSON.
        """

        try:
            brief: InsiderClusterBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are a senior institutional corporate governance & insider trading analyst.",
                user_prompt=user_prompt,
                schema=InsiderClusterBrief,
                temperature=0.1
            )

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"insider_cluster_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "metrics": {
                    "ticker": ticker,
                    "executive_count": exec_count,
                    "total_notional_usd": total_notional,
                    "conviction_index": brief.conviction_index
                }
            }

            # Cache latest insider cluster brief to Redis
            try:
                await self.redis.raw.set(f"sentinel:insider:cluster:{ticker}", json.dumps(res_payload["brief"]))
            except Exception as rx:
                self.logger.warning(f"Failed to cache insider cluster to Redis: {rx}")

            return res_payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Insider cluster LLM error for {ticker}: {e}")
            return None
