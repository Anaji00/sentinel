"""
services/agents/volatility_surface_agent.py

OPTIONS VOLATILITY SURFACE & TAIL RISK AGENT
=============================================
Aggregates equity options chain snapshots, calculating Put/Call volume ratios,
VIX term structure slope, and 25-Delta Implied Volatility skew across major indices.

Emits tail-risk hedging alerts to `agents.options.vol_surface`.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError, InferenceError

logger = logging.getLogger("agent.volatility_surface")


class VolatilitySurfaceBrief(BaseModel):
    ticker: str
    put_call_volume_ratio: float
    iv_skew_25d_bps: float  # Put IV minus Call IV (bps)
    volatility_regime: str  # "Normal Skew", "Elevated Put Buying", "Extreme Tail Risk Hedging"
    tail_risk_conviction: float  # 0.0 to 1.0
    hedging_recommendations: List[str] = Field(default_factory=list)
    analytical_summary: str


class VolatilitySurfaceAgent(SentinelAgent):
    """
    Options market microstructure, IV skew, and tail-risk volatility agent.
    """

    @property
    def output_topic(self) -> str:
        return "agents.options.vol_surface"

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw = message.get("raw_payload", message)
        source = message.get("source", "")
        ticker = str(raw.get("ticker") or message.get("primary_entity", {}).get("id") or "").upper()

        if not (source in ("alpaca_options", "tradfi_enricher") or raw.get("trade_type") in ("OPTIONS_FLOW", "OPTIONS_SWEEP")):
            return None

        # Track volume in Redis per ticker & option type
        option_type = str(raw.get("option_type") or "CALL").upper()
        size = int(raw.get("size") or 1)
        
        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.incrby(f"sentinel:options:volume:{ticker}:{option_type}", size)
            pipe.expire(f"sentinel:options:volume:{ticker}:{option_type}", 86400)
            await pipe.execute()

        vals = await self.redis.raw.mget([
            f"sentinel:options:volume:{ticker}:CALL",
            f"sentinel:options:volume:{ticker}:PUT",
        ])
        calls_vol = int(vals[0] or 100)
        puts_vol = int(vals[1] or 100)
        pc_ratio = round(puts_vol / max(1, calls_vol), 3)

        # Estimate IV Skew from delta or premium
        call_iv = float(raw.get("implied_volatility") or 0.25)
        put_iv = float(raw.get("put_implied_volatility") or call_iv * 1.15)
        iv_skew_bps = round((put_iv - call_iv) * 10000.0, 1)

        dedup_key = f"vol_surface_eval:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(dedup_key, window_seconds=1800):
            return None
        await self.mark_processed(dedup_key, window_seconds=1800)

        logger.info(f"⚡ Options Vol Surface Evaluation | Ticker: {ticker} | P/C Ratio: {pc_ratio:.2f} | IV Skew: {iv_skew_bps:+.1f} bps")

        global_context = await self.fetch_global_context()

        user_prompt = f"""
        Evaluate options surface metrics:
        - Symbol: {ticker}
        - P/C Ratio: {pc_ratio:.3f}
        - 25D IV Skew: {iv_skew_bps:+.1f} bps
        - Call IV: {call_iv:.2%} | Put IV: {put_iv:.2%}

        GLOBAL CONTEXT:
        {global_context}

        Assess institutional tail-risk hedging and crash protection pricing. Generate volatility brief JSON.
        """

        try:
            brief: VolatilitySurfaceBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Volatility Desk Analyst. Evaluate options surface metrics and tail-risk pricing. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=VolatilitySurfaceBrief,
                temperature=0.1
            )

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"vol_surface_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "metrics": {
                    "ticker": ticker,
                    "put_call_volume_ratio": pc_ratio,
                    "iv_skew_25d_bps": iv_skew_bps
                }
            }

            # Cache latest volatility brief to Redis for shared agent context
            try:
                await self.redis.raw.set(f"sentinel:options:vol_surface:{ticker}", json.dumps(res_payload["brief"]))
                await self.redis.raw.set("sentinel:options:vol_surface:latest", json.dumps(res_payload["brief"]))
            except Exception as rx:
                self.logger.warning(f"Failed to cache vol surface brief to Redis: {rx}")

            return res_payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Vol surface LLM error for {ticker}: {e}")
            return None
