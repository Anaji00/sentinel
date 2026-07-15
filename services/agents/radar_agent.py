import json
import time
from typing import Any, Dict, Optional
from pydantic import BaseModel
from services.agents.base import SentinelAgent
from shared.kafka import Topics

class RadarDecision(BaseModel):
    investigate: bool
    rationale: str

class RadarAgent(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cooldown_seconds = 86400
    
    @property
    def output_topic(self) -> str:
        return Topics.RADAR_DECISIONS
    
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = message.get("raw_payload", {})
        ticker = payload.get("ticker")
        z_score = payload.get("z_score")
        notional_usd = payload.get("notional_usd", 0.0)
        
        if not ticker or notional_usd < 50_000:
            return None
        
        # Idempotency: Do not re-evaluate a ticker we already escalated today
        if await self.is_recently_processed(ticker, self.cooldown_seconds):
            return None
        
        # ─── AGENTIC REASONING ───
        entity_context = await self.fetch_entity_context(ticker)
        
        prompt = f"""
        You are a quantitative trading systems engineer.
        A background radar has detected an institutional volume anomaly for ticker: {ticker}.
        
        Metrics:
        - Z-Score: {z_score:.2f} (standard deviations above the EMA)
        - Notional 1-Minute Flow: ${notional_usd / 1_000_000:.2f} Million
        
        {entity_context}
        
        Determine if this ${notional_usd / 1_000_000:.2f}M anomaly warrants active high-frequency tracking. 
        Focus on identifying 'smart money' sweeps.
        Return ONLY valid JSON.
        Schema: {{"investigate": boolean, "rationale": "string"}}
        """

        try:
            decision = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are a quantitative trading systems engineer.",
                user_prompt=prompt,
                schema=RadarDecision,
                temperature=0.1
            )

            if decision.investigate:
                self.logger.info(f"🧠 AGENT ESCALATION: {ticker} -> Primary Surveillance. Rationale: {decision.rationale}")

                # ─── DYNAMIC INFRASTRUCTURE INJECTION ───
                # This explicitly commands the services/collector-tradfi/main.py WebSocket 
                # to subscribe to this ticker on its next sync loop.
                await self.redis.raw.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                
                await self.mark_processed(ticker, self.cooldown_seconds)

                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.rationale,
                    "z_score_trigger": z_score
                }
        except Exception as e:
            self.logger.error(f"Agent reasoning failed for {ticker}: {e}")
        
        return None
    
