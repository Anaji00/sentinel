"""
services/agents/adversarial_wargamer.py

ADVERSARIAL WARGAMER AGENT
==========================
Simulates multi-agent game-theoretic counter-maneuvers across 3 adversarial personas:
  - State_Saboteur (Aggressive geopolitical state saboteur)
  - Financial_Short_Seller (Predatory hedge-fund operator exploiting chaos)
  - Asymmetric_Defender (Advanced intelligence defense grid)

Inherits from SentinelAgent for full telemetry, scorecard prediction tracking,
and cross-agent bulletin integration.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError, InferenceShed
from shared.kafka import Topics
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("agent.adversarial_wargamer")


class SimulationMove(BaseModel):
    persona_name: str
    proposed_counter_action: str
    target_entity_id: str
    disruption_potential_percent: int = Field(default=10, ge=0, le=100)
    strategic_rationale: str


class SimulationBoard(BaseModel):
    """Every persona's move from a single turn of the simulation.

    The personas were three separate inference calls until each one was an
    independent claim on a budget that could rarely satisfy even one.
    """
    moves: List[SimulationMove] = Field(default_factory=list)


class WargameSimulationOutput(BaseModel):
    simulation_run_id: str = Field(default_factory=lambda: f"sim_{int(datetime.now(timezone.utc).timestamp())}")
    primary_vulnerability_isolated: str
    cascade_failure_probability: int
    predicted_next_target_entity_id: str
    remediation_recommendation: str


# Tiers that justify four model calls. WATCH and ALERT are the routine end of the
# scale and make up the bulk of the stream; simulating them would mean the
# genuinely serious clusters wait behind them.
_SIMULATION_WORTHY_TIERS = frozenset({"ELEVATED", "INTELLIGENCE", "CRITICAL"})

# Floor for anything arriving without a tier (news, briefs, scenarios).
_MIN_CONFIDENCE_TO_SIMULATE = 0.70


# Position fixes and their kin. These arrive in the tens of thousands per hour,
# describe nothing to simulate, and are already capped at 0.15 anomaly by the
# enricher that produced them.
_ROUTINE_TELEMETRY_PREFIXES = ("vessel", "flight", "aircraft", "maritime", "aviation", "radar")


def _is_routine_telemetry(message: Dict[str, Any]) -> bool:
    """True for high-volume positional telemetry carrying no situation."""
    for key in ("type", "event_type", "primary_domain", "domain", "source"):
        value = message.get(key)
        if not value:
            continue
        token = str(value).strip().lower()
        if token.startswith(_ROUTINE_TELEMETRY_PREFIXES):
            return True
    return False


def _is_worth_simulating(message: Dict[str, Any]) -> bool:
    """Whether this message earns an adversarial simulation.

    Deliberately permissive about *shape* and strict about *significance*: the
    agent consumes correlations, briefs, scenarios and raw news, which carry
    their severity under different names.
    """
    tier = str(message.get("alert_tier") or "").upper()
    if tier:
        return tier in _SIMULATION_WORTHY_TIERS

    for key in ("confidence_score", "anomaly_score", "severity_score"):
        value = message.get(key)
        if value is not None:
            try:
                return float(value) >= _MIN_CONFIDENCE_TO_SIMULATE
            except (TypeError, ValueError):
                continue

    # Severity as an integer scale (intel briefs use 1-5).
    severity = message.get("severity")
    if severity is not None:
        try:
            return float(severity) >= 4
        except (TypeError, ValueError):
            pass

    # Nothing stated a severity. Rejecting outright was too blunt: a news
    # headline about export controls on a named company carries no tier and is
    # exactly what this agent exists for. What the expensive path must never be
    # spent on is routine telemetry, so that is what gets excluded by name.
    return not _is_routine_telemetry(message)


class AdversarialWargamerAgent(SentinelAgent):
    """
    Agentic game-theory simulation engine.
    Consumes correlation clusters, plays 3 adversarial personas against each
    other, synthesizes cascade failure probabilities, and emits predictive
    wargame reports.
    """

    @property
    def output_topic(self) -> str:
        return Topics.AGENTS_PREDICTIONS

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Robust multi-key entity extraction across CorrelationCluster, Scenario, IntelBrief, and trade events
        raw_eids = message.get("entity_ids") or []
        if isinstance(raw_eids, str):
            raw_eids = [raw_eids]
        
        extracted = set(raw_eids)
        for key in ("primary_entity_id", "primary_entity", "target_entity_id", "ticker", "asset", "entity_id"):
            val = message.get(key)
            if val:
                if isinstance(val, dict):
                    if val.get("id"): extracted.add(str(val["id"]))
                    elif val.get("name"): extracted.add(str(val["name"]))
                elif isinstance(val, str):
                    extracted.add(val)

        for name in message.get("entity_names") or []:
            if name: extracted.add(str(name))

        entity_ids = [e for e in extracted if e]

        description = (
            message.get("description")
            or message.get("headline")
            or message.get("summary")
            or message.get("hypothesis")
            or "Generic threat cluster."
        )

        if not entity_ids:
            if description and description != "Generic threat cluster.":
                entity_ids = ["GLOBAL_SYS"]
            else:
                return None

        # A wargame costs two model calls -- one persona board and an
        # arbitration -- and the slot is shared with every other agent. Running one per inbound message was never possible:
        # each paid a Neo4j subgraph query and a Redis fetch before being shed,
        # then threw the work away. That is what held this consumer at 384
        # messages an hour.
        #
        # Two gates, cheapest first.

        # (a) Is it worth simulating? A wargame is an expensive opinion about a
        #     serious situation; running it on routine chatter spends the swarm's
        #     scarcest resource on noise.
        if not _is_worth_simulating(message):
            return None

        # (b) Is there capacity at all? Peeking does not claim the slot -- the
        #     atomic claim still happens at the inference call -- it just avoids
        #     building context for work that cannot run.
        if not await self._inference_budget.is_available():
            return None

        self.logger.info(f"⚔️ WARGAME SIMULATION | Targets: {entity_ids} | Description: {description[:70]}...")

        # 1. Fetch Neo4j Subgraph Context
        subgraph = await self._fetch_subgraph_context(entity_ids)

        # 2. Fetch Cross-Agent Intelligence
        cross_context = await self.get_cross_agent_context(limit=3)
        cross_block = f"\nCROSS-AGENT INTELLIGENCE:\n{cross_context}\n" if cross_context else ""

        # 3. Persona Maneuver Simulation -- one call, three personas.
        #
        # This was three concurrent calls, and the wargame completed zero times
        # in ninety minutes of live traffic: every attempt logged "All persona
        # turns returned empty". The cause is not the personas. InferenceShed is
        # a BaseException, so the `except Exception` inside a persona turn never
        # sees it and its fallback move is never produced; gather() collects
        # three sheds, `moves` is empty, and the run is abandoned having spent a
        # Neo4j subgraph query for nothing. errors stayed 0 throughout, which is
        # why this looked like a quiet agent rather than a broken one.
        #
        # A wargame needed four slots from a budget shared with radar, the graph
        # engine and quant. Asking for them as four independent races loses all
        # four about as often as it wins any, and a partial win is worth nothing
        # here -- arbitration still needs its own.
        # Collapsing the personas into a single structured call makes it two
        # slots instead of four and, more importantly, makes the expensive step
        # atomic: one claim, which either succeeds or sheds before any context
        # is built.
        #
        # The personas stay adversarial to each other inside the prompt; what is
        # given up is three independent samplings of the model, which is a real
        # cost and a smaller one than never running at all.
        personas = {
            "State_Saboteur": "an aggressive geopolitical state saboteur proposing high-disruption counter-maneuvers",
            "Financial_Short_Seller": "a predatory hedge-fund operator exploiting market chaos with short/squeeze moves",
            "Asymmetric_Defender": "an advanced intelligence defense grid proposing hardening & remediation counter-measures",
        }

        board = await self._execute_persona_board(personas, description, subgraph, cross_block)
        moves = board.moves if board else []

        if not moves:
            self.logger.warning(f"⚔️ WARGAME SKIPPED | No persona moves returned for {entity_ids}")
            return None

        # 4. Game-Theoretic Arbitration & Synthesis
        arbitration_prompt = f"""
        Analyze adversarial maneuvers:
        {json.dumps([m.model_dump() for m in moves], default=str)}
        {cross_block}

        Synthesize the primary vulnerability, cascade failure probability, next target entity, and remediation.
        Return raw JSON matching WargameSimulationOutput schema.
        """

        try:
            synthesis: WargameSimulationOutput = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are Principal Game Theory Analyst & Strategic Wargamer. Isolate failure points and predict cascade targets.",
                user_prompt=arbitration_prompt,
                schema=WargameSimulationOutput,
                temperature=0.05,
            )

            output = synthesis.model_dump()
            output["agent"] = self.name
            output["agent_run_id"] = f"wargame_{int(datetime.now(timezone.utc).timestamp())}"
            output["source_correlation_id"] = message.get("correlation_id", "unknown")

            self.logger.info(
                f"⚔️ WARGAME COMPLETED | Target: {synthesis.predicted_next_target_entity_id} | "
                f"Cascade Risk: {synthesis.cascade_failure_probability}% | "
                f"Vuln: {synthesis.primary_vulnerability_isolated[:60]}"
            )

            # Record prediction on agent scorecard
            if synthesis.predicted_next_target_entity_id and synthesis.predicted_next_target_entity_id != "NONE":
                await self.record_prediction(
                    ticker=synthesis.predicted_next_target_entity_id,
                    direction="bearish" if synthesis.cascade_failure_probability >= 50 else "neutral",
                    conviction=min(1.0, synthesis.cascade_failure_probability / 100.0),
                    entry_price=0.0,
                    time_horizon_hours=24
                )

            # Publish structured AgentBulletin for Consensus Engine & UI
            safe_create_task(
                self.publish_bulletin(
                    bulletin_type="alert" if synthesis.cascade_failure_probability >= 70 else "thesis",
                    summary=f"Wargame Target {synthesis.predicted_next_target_entity_id}: Cascade Risk {synthesis.cascade_failure_probability}%",
                    ticker=synthesis.predicted_next_target_entity_id,
                    conviction=min(1.0, synthesis.cascade_failure_probability / 100.0),
                    expected_direction="down" if synthesis.cascade_failure_probability >= 50 else "neutral",
                    payload=output,
                    ttl_seconds=7200,
                ),
                name=f"wargamer-bulletin-{synthesis.predicted_next_target_entity_id}"
            )

            return output

        except Exception as e:
            self.logger.error(f"Wargame synthesis failed: {e}")
            return None

    async def _fetch_subgraph_context(self, primary_entity_ids: List[str]) -> List[str]:
        extracted_edges = []
        if not self.neo4j:
            return []
        for entity_id in primary_entity_ids:
            try:
                rows = await self.neo4j.query("""
                    MATCH (a:Entity {id: $id})-[r*1..3]-(b:Entity)
                    WHERE ALL(rel in r WHERE coalesce(rel.weight, 1.0) >= 0.60)
                    RETURN a.id as src, type(r[-1]) as rel, b.id as tgt LIMIT 15
                """, {"id": str(entity_id).upper()})
                for r in rows:
                    extracted_edges.append(f"({r['src']})-[:{r['rel']}]->({r['tgt']})")
            except Exception as e:
                self.logger.debug(f"Graph context extraction failed for {entity_id}: {e}")
        return list(set(extracted_edges))

    async def _execute_persona_board(
        self,
        personas: Dict[str, str],
        scenario: str,
        subgraph: List[str],
        cross_block: str,
    ) -> Optional[SimulationBoard]:
        """Every persona's move, in one inference call.

        Returns None when the budget declines the work, which is the honest
        answer: no fallback board is fabricated. A wargame assembled from
        placeholder moves would still reach arbitration, still be published, and
        still record a prediction -- an invented opinion carrying the same
        weight as a reasoned one. Skipping is visible; fabricating is not.
        """
        roster = "\n".join(f"- {name}: {brief}" for name, brief in personas.items())
        user_prompt = (
            f"SCENARIO:\n{scenario}\n\n"
            f"GRAPH CONSTRAINTS:\n{json.dumps(subgraph)}\n{cross_block}\n"
            f"ADVERSARIAL PERSONAS:\n{roster}\n\n"
            "Play every persona above against this scenario. Each proposes one "
            "counter-maneuver, in character and in opposition to the others -- "
            "the saboteur and the defender must not converge on the same move.\n"
            "Return raw JSON: {\"moves\": [{\"persona_name\": ..., "
            "\"proposed_counter_action\": ..., \"target_entity_id\": ..., "
            "\"disruption_potential_percent\": ..., \"strategic_rationale\": ...}]}\n"
            f"Exactly {len(personas)} moves, one per persona. target_entity_id must "
            "name an entity from the scenario or graph constraints, never a new one."
        )
        try:
            return await self._execute_with_telemetry(
                message={"system": "persona_board"},
                system_prompt=(
                    "You are a red-team simulation engine voicing several adversaries "
                    "at once. Keep each persona's reasoning distinct."
                ),
                user_prompt=user_prompt,
                schema=SimulationBoard,
                temperature=0.35,
            )
        except InferenceShed:
            # Not an error, and not this agent's to absorb: the budget declined.
            raise
        except Exception as e:
            self.logger.warning(f"Persona board simulation failed: {e}")
            return None