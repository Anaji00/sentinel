import asyncio
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from services.agents.base import SentinelAgent
from shared.kafka import Topics

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
        return Topics.RULES_SYNTHESIZED

    async def handle(self, message: dict) -> None:
        """
        Triggered when the Macro Strategist publishes a new brief OR when a rule fails.
        """
        # Feedback Loop: Handle Rule Failure
        if message.get("type") == "rule_failure":
            rule_id = message.get("rule_id")
            if rule_id:
                self.logger.warning(f"Deprecating failed rule: {rule_id}")
                # Remove from HASH
                await self.redis.raw.hdel("sentinel:correlation:dynamic_rules", rule_id)
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

        summary = ""
        entities = []
        prompt_context = ""
        
        # Branch based on message structure
        if "scenario_id" in message:
            summary = message.get("headline", "")
            hypotheses = message.get("hypotheses", [])
            sig = message.get("significance", "")
            self.logger.debug(f"Synthesizing rules based on Reasoning Scenario: {summary}")
            prompt_context = f"A new AI-generated reasoning scenario has been generated:\nSUMMARY: {summary}\nSIGNIFICANCE: {sig}\nHYPOTHESES: {hypotheses}"
        elif message.get("type") == "quant_discovery":
            summary = message.get("description", "")
            entities = message.get("correlated_assets", [])
            self.logger.debug(f"Synthesizing rules based on Quant Discovery: {summary}")
            prompt_context = f"A new quantitative peer relationship has been discovered:\nSUMMARY: {summary}\nASSETS: {entities}"
        else:
            brief = message.get("brief", {})
            summary = brief.get("headline_summary", "")
            entities = brief.get("entities", [])
            if not summary:
                return
            self.logger.debug(f"Synthesizing rules based on macro shift: {summary}")
            prompt_context = f"A new macro intelligence brief has been issued:\nSUMMARY: {summary}\nENTITIES: {entities}"

        prompt = f"""=== SYNTHETIC RULE GENERATION ===
{prompt_context}

DIRECTIVES:
Synthesize up to 3 JSON correlation rules for the SENTINEL evaluation engine.
- Valid trigger_event_type: vessel_dark, options_flow, futures_cot, headline, bgp_anomaly, prediction_market_trade, market_anomaly, equity_block, market_candle.
- Alert tiers: WATCH | ALERT | ELEVATED | INTELLIGENCE | CRITICAL.
- Return raw JSON matching RuleList schema with "rules": [{{"rule_id": "syn_1", "rule_name": "...", "trigger_event_type": "...", "conditions": {{}}, "correlations": [], "alert_tier": "ALERT", "tags": []}}]:"""
        
        try:
            response = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Rule Architect. Synthesize precise JSON DSL correlation rules for real-time anomaly detection. Return ONLY raw JSON.",
                user_prompt=prompt,
                schema=RuleList,
                temperature=0.2
            )
            
            if hasattr(response, "rules") and response.rules:
                for rule in response.rules:
                    rule_json = json.dumps(rule.model_dump())
                    
                    # Store in HASH mapping rule_id -> rule_json (7-day auto-expiry timestamp included)
                    await self.redis.raw.hset("sentinel:correlation:dynamic_rules", rule.rule_id, rule_json)
                    
                    # Publish for hot-reloading in correlation engine
                    await self.redis.raw.publish("sentinel:correlation:rule_updates", rule_json)
                    
                    self.logger.info(f"Deployed new synthetic rule: {rule.rule_id} - {rule.rule_name}")
                    
                    if self._producer:
                        # 1. Publish to RULES_SYNTHESIZED
                        await self._producer.send(Topics.RULES_SYNTHESIZED, rule.model_dump(), key=rule.rule_id)

                        # 2. Emit Telemetry
                        await self._producer.send("agents.telemetry", {
                            "agent": "rule_synthesizer",
                            "event": "rule_created",
                            "rule_id": rule.rule_id,
                            "rule_name": rule.rule_name,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })

                        # 3. Route discovered entity correlations through governed ONTOLOGY_PROPOSALS (§3.7)
                        rule_tags = [t.upper() for t in (rule.tags or []) if len(t) <= 10]
                        if len(rule_tags) >= 2:
                            for i in range(len(rule_tags) - 1):
                                prop = {
                                    "entity_id": rule_tags[i],
                                    "action": "LINK_ENTITY",
                                    "data": {
                                        "target_id": rule_tags[i+1],
                                        "source_label": "Company",
                                        "target_label": "Company",
                                        "relation_type": "MACRO_CORRELATED",
                                        "weight": 0.85,
                                        "confidence": 0.80,
                                        "properties": {
                                            "rule_id": rule.rule_id,
                                            "rule_name": rule.rule_name,
                                            "alert_tier": rule.alert_tier,
                                        }
                                    }
                                }
                                await self._producer.send(Topics.ONTOLOGY_PROPOSALS, prop, key=rule_tags[i])
        except Exception as e:
            self.logger.error(f"Failed to synthesize rules: {e}")

    async def _evaluate_and_prune_rules(self, active_rules: Dict[str, str], current_context: str) -> None:
        """
        LLM Reasoning Engine for Rule Pruning:
        Evaluates existing active correlation rules in Redis against current market context
        and hit rates, using LLM reasoning to determine which rules are obsolete, duplicate, or stale.
        """
        if not active_rules:
            return

        rule_summaries = []
        for r_id, r_json in active_rules.items():
            try:
                r_obj = json.loads(r_json)
                rule_summaries.append({
                    "rule_id": r_id,
                    "rule_name": r_obj.get("rule_name"),
                    "trigger_event": r_obj.get("trigger_event_type"),
                    "expires_at": r_obj.get("expires_at")
                })
            except Exception:
                continue

        if not rule_summaries:
            return

        prune_prompt = f"""
        You are the Sentinel Rule Engine Pruning Curator.
        CURRENT MARKET CONTEXT:
        {current_context}

        ACTIVE DYNAMIC CORRELATION RULES:
        {json.dumps(rule_summaries, separators=(',', ':'))}

        Analyze each rule's relevance to current market conditions. Identify any rules that are obsolete, contradictory, or duplicate.
        Return a JSON list of rule_ids that should be PRUNED and REMOVED from active correlation evaluation.
        """

        try:
            class PruneDecision(BaseModel):
                prune_rule_ids: List[str] = Field(default_factory=list)
                reasoning: str = ""

            decision: PruneDecision = await self._execute_with_telemetry(
                message={"type": "prune_eval"},
                system_prompt="You evaluate and prune obsolete correlation rules.",
                user_prompt=prune_prompt,
                schema=PruneDecision,
                temperature=0.1
            )

            for target_id in decision.prune_rule_ids:
                if target_id in active_rules:
                    self.logger.info(f"🧠 LLM Pruned obsolete rule: {target_id} | Rationale: {decision.reasoning[:100]}")
                    await self.redis.raw.hdel("sentinel:correlation:dynamic_rules", target_id)
                    tombstone = json.dumps({"rule_id": target_id, "deprecated": True})
                    await self.redis.raw.publish("sentinel:correlation:rule_updates", tombstone)
        except Exception as e:
            self.logger.warning(f"Failed to execute LLM rule pruning: {e}")
