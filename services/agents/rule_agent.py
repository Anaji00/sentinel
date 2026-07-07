import asyncio
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from services.agents.base import SentinelAgent

logger = logging.getLogger("agent.rule_synthesizer")

class CorrelationDef(BaseModel):
    event_types: List[str]
    hours: int
    min_anomaly: float
    tags: Optional[List[str]] = None
    region: Optional[str] = None

class DynamicRule(BaseModel):
    rule_id: str
    rule_name: str
    trigger_event_type: str
    conditions: Dict[str, Any] = Field(default_factory=dict)
    correlations: List[CorrelationDef]
    alert_tier: str
    tags: List[str]
    version: int = 1
    expires_at: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp()) + 7 * 86400)

class RuleList(BaseModel):
    rules: List[DynamicRule]

class RuleSynthesizerAgent(SentinelAgent):
    """
    Subscribes to macro intelligence briefs and synthesizes/updates
    JSON DSL rules in Redis based on geopolitical and market conditions.
    """
    
    @property
    def output_topic(self) -> str:
        # Not used because handle() returns None, but required by ABC
        return "agents.rules.synthesized"

    async def handle(self, message: dict) -> None:
        """
        Triggered when the Macro Strategist publishes a new brief OR when a rule fails.
        """
        # Feedback Loop: Handle Rule Failure
        if message.get("type") == "rule_failure":
            rule_id = message.get("rule_id")
            if rule_id:
                self.logger.warning(f"Deprecating failed rule: {rule_id}")
                # Remove from ZSET
                await self.redis.raw.zrem("sentinel:correlation:dynamic_rules", rule_id)
                # Publish tombstone for hot-reloading
                tombstone = json.dumps({"rule_id": rule_id, "deprecated": True})
                await self.redis.raw.publish("sentinel:correlation:rule_updates", tombstone)
                # Emit Telemetry
                if self._producer:
                    await self._producer.send("agents.telemetry", {
                        "agent": "rule_synthesizer",
                        "event": "rule_deprecated",
                        "rule_id": rule_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
            return

        brief = message.get("brief", {})
        summary = brief.get("headline_summary", "")
        entities = brief.get("entities", [])
        
        if not summary:
            return

        self.logger.info(f"Synthesizing rules based on macro shift: {summary}")
        
        prompt = f"""
        You are the Sentinel Rule Engine Architect.
        A new macro intelligence brief has been issued:
        SUMMARY: {summary}
        ENTITIES: {entities}
        
        Synthesize up to 3 JSON correlation rules that the correlation engine should actively look for.
        For example, if tensions are rising in the Red Sea, create a rule that triggers on "vessel_dark" 
        with a condition region "Red Sea" and correlates with "options_flow" tagged "energy" in the last 96 hours.
        
        Return a RuleList containing DynamicRule objects.
        Valid event_types: vessel_dark, options_flow, futures_cot, headline, bgp_anomaly, prediction_market_trade
        """
        
        try:
            response = await self._execute_with_telemetry(
                message=message,
                system_prompt="You write JSON DSL rules for a generic evaluation engine.",
                user_prompt=prompt,
                schema=RuleList,
                temperature=0.3
            )
            
            if hasattr(response, "rules") and response.rules:
                for rule in response.rules:
                    rule_json = json.dumps(rule.model_dump())
                    
                    # Store in ZSET with score = expires_at
                    # Redis-py async client ZADD syntax: zadd(name, mapping={value: score})
                    await self.redis.raw.zadd("sentinel:correlation:dynamic_rules", mapping={rule_json: rule.expires_at})
                    
                    # Publish for hot-reloading
                    await self.redis.raw.publish("sentinel:correlation:rule_updates", rule_json)
                    
                    self.logger.info(f"Deployed new synthetic rule: {rule.rule_id} - {rule.rule_name}")
                    
                    # Emit Telemetry
                    if self._producer:
                        await self._producer.send("agents.telemetry", {
                            "agent": "rule_synthesizer",
                            "event": "rule_created",
                            "rule_id": rule.rule_id,
                            "rule_name": rule.rule_name,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                    
        except Exception as e:
            self.logger.error(f"Failed to synthesize rules: {e}")
