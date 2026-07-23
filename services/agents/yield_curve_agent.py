"""
services/agents/yield_curve_agent.py

YIELD CURVE & MACRO RATES AGENT
===============================
Tracks US Treasury yield curve dynamics (2Y, 10Y, 30Y yields), SOFR,
and High-Yield Credit ETF spreads (HYG vs LQD).

Detects yield curve inversion/disinversion and credit spread widening
to emit macro regime shift alerts to agents.macro.rates_regime.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field
from .base import SentinelAgent, SchemaViolationError, InferenceError

logger = logging.getLogger("agent.yield_curve")


class RatesRegimeBrief(BaseModel):
    curve_state: str  # "Inverted", "Disinverted", "Normal Steepening", "Flat"
    yield_spread_2y10y_bps: float
    breakeven_inflation_bps: float
    tips_yield: float
    credit_spread_widening_signal: str  # "Stable", "Moderate Widening", "Severe Stress"
    regime_summary: str
    macro_risk_level: str  # "LOW", "ELEVATED", "CRITICAL"
    recommended_hedging: List[str] = Field(default_factory=list)


class YieldCurveMacroRatesAgent(SentinelAgent):
    """
    Fixed income, Treasury rates, TIPS real yields, and credit spread intelligence agent.
    """

    @property
    def output_topic(self) -> str:
        return "agents.macro.rates_regime"

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        raw = message.get("raw_payload", message)
        source = message.get("source", "")
        ticker = str(raw.get("ticker") or message.get("primary_entity", {}).get("id") or "").upper()

        # Gate to fixed income and macro commodity/rate events
        if not (ticker in ("US2Y", "US10Y", "US30Y", "HYG", "LQD", "SOFR", "TIP", "^TNX") or "treasury" in source or "macro" in source):
            return None

        y2 = float(await self.redis.raw.get("sentinel:quotes:latest:US2Y") or 4.25)
        y10 = float(await self.redis.raw.get("sentinel:quotes:latest:US10Y") or 4.15)
        
        # Redis 'TIP' key may store ETF share price (e.g. $107.89). Ensure tips_yield is a real yield percentage.
        raw_tips = await self.redis.raw.get("sentinel:quotes:latest:TIPS_YIELD") or await self.redis.raw.get("sentinel:quotes:latest:TIP")
        tips_val = float(raw_tips) if raw_tips else 1.85
        tips_yield = tips_val if tips_val < 15.0 else 1.85

        hyg = float(await self.redis.raw.get("sentinel:quotes:latest:HYG") or 77.5)
        lqd = float(await self.redis.raw.get("sentinel:quotes:latest:LQD") or 108.2)

        spread_2y10y_bps = (y10 - y2) * 100.0
        breakeven_inflation_bps = (y10 - tips_yield) * 100.0
        credit_ratio = hyg / max(1.0, lqd)

        # Track historical spread in Redis
        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.rpush("sentinel:macro:spread_2y10y", spread_2y10y_bps)
            pipe.ltrim("sentinel:macro:spread_2y10y", -100, -1)
            await pipe.execute()

        dedup_key = f"yield_curve_regime:{int(time.time() // 3600)}"
        if await self.is_recently_processed(dedup_key, window_seconds=3600):
            return None
        await self.mark_processed(dedup_key, window_seconds=3600)

        logger.info(f"📊 Yield Curve & TIPS Evaluation | 2Y: {y2:.2f}% | 10Y: {y10:.2f}% | 2Y-10Y: {spread_2y10y_bps:+.1f} bps | Breakeven: {breakeven_inflation_bps:.1f} bps")

        user_prompt = f"""
        As a fixed income strategist, analyze current Treasury yield curve and inflation metrics:
        - US 2-Year Yield: {y2:.3f}%
        - US 10-Year Yield: {y10:.3f}%
        - 2Y-10Y Yield Spread: {spread_2y10y_bps:+.1f} bps
        - 10-Year TIPS Real Yield: {tips_yield:.3f}%
        - 10-Year Breakeven Inflation Rate: {breakeven_inflation_bps:.1f} bps
        - HYG / LQD Credit ETF Ratio: {credit_ratio:.4f}

        Evaluate yield curve inversion / steepening dynamics, TIPS breakeven inflation expectations, and credit spread risk.
        Generate structured rates regime brief JSON.
        """

        try:
            brief: RatesRegimeBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You analyze fixed-income rates, yield curves, TIPS real yields, and credit spreads.",
                user_prompt=user_prompt,
                schema=RatesRegimeBrief,
                temperature=0.1
            )

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"rates_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "metrics": {
                    "yield_2y": y2,
                    "yield_10y": y10,
                    "tips_yield": tips_yield,
                    "spread_2y10y_bps": spread_2y10y_bps,
                    "breakeven_inflation_bps": breakeven_inflation_bps,
                    "credit_ratio": credit_ratio
                }
            }

            # Cache latest rates regime for shared swarm context ingestion
            try:
                await self.redis.raw.set("sentinel:macro:rates_regime:latest", json.dumps(res_payload["brief"]))
            except Exception as rx:
                self.logger.warning(f"Failed to cache rates regime to Redis: {rx}")

            return res_payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Rates regime LLM error: {e}")
            return None
