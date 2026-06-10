import json
from typing import Any, Dict, Optional
from services.agents.base import SentinelAgent
from shared.kafka import Topics

class RadarAgent(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(agent_name="radar_agent", input_topics=[Topics.RAW_RADAR], *args, **kwargs)
        self.cooldown_seconds = 86400
    
    @property
    def output_topic(self) -> str:
        return Topics.ENRICHED_EVENTS
    
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = message.get("raw_payload", {})
        ticker = payload.get("ticker")
        z_score = payload.get("z_score")
        
        if not ticker:
            return None
        
        # Idempotency: Do not re-evaluate a ticker we already escalated today
        if self.is_recently_processed(ticker, self.cooldown_seconds):
            return None
        
        # ─── AGENTIC REASONING ───
        prompt = f"""
        You are a quantitative trading systems engineer.
        A background radar has detected a mathematical volume anomaly for ticker: {ticker}.
        Z-Score: {z_score:.2f} (This means volume is {z_score:.2f} standard deviations above the EMA).
        
        Determine if this ticker warrants active high-frequency tracking by the primary surveillance system.
        Return ONLY valid JSON.
        Schema: {{"investigate": boolean, "rationale": "string"}}
        """

        try:
            response = await self._llm.infer(
                prompt = prompt, 
                schema = {"type": "object", "properties": {"investigate": {"type": "boolean"}, "rationale": {"type": "string"}}}
            )
            decision = json.loads(response)

            if decision.get("investigate"):
                self.logger.info(f"🧠 AGENT ESCALATION: {ticker} -> Primary Surveillance. Rationale: {decision.get('rationale')}")

                # ─── DYNAMIC INFRASTRUCTURE INJECTION ───
                # This explicitly commands the services/collector-tradfi/main.py WebSocket 
                # to subscribe to this ticker on its next sync loop.
                await self.redis.raw.sadd("sentinel:watched:equities", ticker)
                
                await self.mark_processed(ticker, self.cooldown_seconds)

                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.get("rationale"),
                    "z_score_trigger": z_score
                }
        except Exception as e:
            self.logger.error(f"Agent reasoning failed for {ticker}: {e}")
        
        return None
    
