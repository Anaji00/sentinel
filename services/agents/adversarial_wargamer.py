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
from shared.utils.text import clip
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("agent.adversarial_wargamer")


class SimulationMove(BaseModel):
    persona_name: str
    proposed_counter_action: str
    target_entity_id: str
    disruption_potential_percent: int = Field(default=10, ge=0, le=100)
    strategic_rationale: str


class WargameSimulationOutput(BaseModel):
    """The personas' moves and the conclusion drawn from them, in one answer.

    These were two calls -- a persona board, then an arbitration over it -- and
    the second was not guaranteed. `InferenceShed` is a BaseException, so the
    `except Exception` around the arbitration never caught it; the shed
    propagated and the board that had already been paid for was discarded.

    Measured 2026-09-20 over the agent's whole history:

        persona boards completed   685
        arbitrations completed      30   (4.4%)
        predictions recorded         0

    655 inferences bought a set of moves that nothing ever read. That is the
    same defect this file already fixed once, when three concurrent persona
    calls became one: a multi-call sequence on a contended budget loses the
    whole sequence about as often as it loses any single call, and a partial
    result is worth nothing here. One call is atomic -- it either runs or sheds
    before any context is built -- and it pays one prompt evaluation instead of
    two, which on this host is 44-67% of the cost of a call.
    """
    simulation_run_id: str = Field(default_factory=lambda: f"sim_{int(datetime.now(timezone.utc).timestamp())}")
    # Required, because the prompt requires it.
    #
    # This carried `default_factory=list`, and Ollama builds its decoding
    # grammar from the schema -- so a field with a default is a field the model
    # may legally omit. It did, twice running: the required scalars were filled
    # and `moves` came back `[]`, which made `if not synthesis.moves` discard a
    # synthesis that had already been paid for. Raising num_predict to 1024
    # changed nothing, because the budget was never the constraint.
    #
    # This audit already has the same defect under its own entry -- the prompt
    # says a field is required and the schema says it is optional -- and this
    # is that entry, reintroduced by me when the two calls were merged.
    #
    # Not a licence to fabricate. The model is being asked to play the personas;
    # answering with at least one move is the task, not an invention. What stays
    # forbidden is *code* minting a placeholder move after a failure, which is
    # what the old "PASS" fallback did and why it was removed.
    moves: List[SimulationMove] = Field(..., min_length=1)
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

# The ceiling on a conviction derived from a model-authored probability.
#
# `cascade_failure_probability` is a 0-100 integer the model writes, and it was
# divided by 100 and passed through. The first prediction this agent ever
# recorded -- live, 2026-09-20 -- came back at conviction 1.0, because the model
# said 100.
#
# Nothing in this platform should publish certainty; the same rule already has
# FALLBACK_MAX_SCORE = 0.995 in the streaming detectors and RULE_CONF_CEILING =
# 0.95 in correlation, and neither applied to an agent bulletin. The harm here
# is specific rather than aesthetic: conviction reaches the consensus engine's
# Subjective Logic fusion, where an opinion at exactly 1.0 drives uncertainty to
# zero, so one model saying "100%" outweighs every measured opinion beside it.
_MAX_MODEL_CONVICTION = 0.95


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
        except (TypeError, ValueError) as _exc:
            swallowed("agents.adversarial_wargamer._is_worth_simulating", _exc, logger)

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
    FOCUS_DOMAIN = "geopolitical"
    FOCUS_DOMAINS = ("geopolitical", "maritime", "aviation", "cyber")

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

        # A wargame costs one model call -- the personas and the arbitration
        # arrive together -- and the slot is shared with every other agent.
        # Running one per inbound message was never possible: each paid a Neo4j
        # subgraph query and a Redis fetch before being shed, then threw the
        # work away. That is what held this consumer at 384 messages an hour.
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
        if not await self.capacity_or_defer(message):
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

        try:
            synthesis: WargameSimulationOutput = await self._execute_wargame(
                message, personas, description, subgraph, cross_block
            )
        except InferenceShed:
            # The budget declined. Nothing was built, nothing is lost.
            raise
        except Exception as e:
            # Counted, because a wargame that fails to parse is the same
            # outcome to a reader as one that never ran, and only the count
            # separates a one-off from a schema the model can no longer meet.
            swallowed("agents.adversarial_wargamer.simulation", e, self.logger)
            return None

        if not synthesis.moves:
            self.logger.warning(f"⚔️ WARGAME SKIPPED | No persona moves returned for {entity_ids}")
            return None

        # The predicted target has to be an entity, not a description of one.
        #
        # First combined run, live: predicted_next_target_entity_id came back as
        # "50 events across 2 domains (aviation, maritime)" -- the cluster's own
        # summary echoed into an identity field. The prompt already forbids
        # inventing a target and the model did it anyway, which is the case a
        # prompt instruction cannot cover.
        #
        # Recording that would put a sentence where every consumer reads a
        # ticker, and this audit already has that defect under its own entry:
        # a bulletin whose ticker was "CPB ($21.53)". A wargame whose target is
        # unusable is still worth publishing for its reasoning; what it must not
        # do is enter the scorecard as a forecast about a named thing.
        known = {str(e).strip().upper() for e in entity_ids}
        target = str(synthesis.predicted_next_target_entity_id or "").strip()
        target_is_named = bool(target) and target.upper() in known

        try:
            output = synthesis.model_dump()
            output["agent"] = self.name
            output["agent_run_id"] = f"wargame_{int(datetime.now(timezone.utc).timestamp())}"
            output["source_correlation_id"] = message.get("correlation_id", "unknown")

            self.logger.info(
                f"⚔️ WARGAME COMPLETED | Target: {synthesis.predicted_next_target_entity_id} | "
                f"Cascade Risk: {synthesis.cascade_failure_probability}% | "
                f"Vuln: {clip(synthesis.primary_vulnerability_isolated, 60)}"
            )

            # Record prediction on agent scorecard
            if target_is_named and target.upper() != "NONE":
                # An entity claim, not a price one.
                #
                # This recorded against the directional scorer, which wants a
                # price for a "ticker" like "airlines, airports" -- and rejects
                # the record outright anyway, because entry_price=0.0 is falsy
                # and its first guard tests exactly that. Every wargame
                # prediction ever made was stored and left permanently
                # unresolved, so the agent's Brier score never moved off its
                # 0.5 starting value no matter how well or badly it predicted.
                await self.record_prediction(
                    ticker=target,
                    direction="bearish" if synthesis.cascade_failure_probability >= 50 else "neutral",
                    conviction=min(_MAX_MODEL_CONVICTION, synthesis.cascade_failure_probability / 100.0),
                    entry_price=0.0,
                    time_horizon_hours=24,
                    prediction_kind="entity_appearance",
                )

            # Publish structured AgentBulletin for Consensus Engine & UI
            safe_create_task(
                self.publish_bulletin(
                    bulletin_type="alert" if synthesis.cascade_failure_probability >= 70 else "thesis",
                    summary=f"Wargame Target {target}: Cascade Risk {synthesis.cascade_failure_probability}%",
                    # Only when it names something. The consensus engine fuses
                    # bulletins *by ticker*, so a prose ticker does not merely
                    # look wrong -- it opens a group of one that nothing can
                    # ever corroborate or contradict, which is the single-
                    # contributor problem this audit spent a pass closing.
                    ticker=target if target_is_named else None,
                    conviction=min(_MAX_MODEL_CONVICTION, synthesis.cascade_failure_probability / 100.0),
                    expected_direction="down" if synthesis.cascade_failure_probability >= 50 else "neutral",
                    payload=output,
                    ttl_seconds=7200,
                ),
                name=f"wargamer-bulletin-{target[:40] or 'unnamed'}"
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

    async def _execute_wargame(
        self,
        message: Dict[str, Any],
        personas: Dict[str, str],
        scenario: str,
        subgraph: List[str],
        cross_block: str,
    ) -> "WargameSimulationOutput":
        """The whole wargame in one inference: play the personas, then arbitrate.

        No fallback board is fabricated on failure. A wargame assembled from
        placeholder moves would still reach a conclusion, still be published,
        and still record a prediction -- an invented opinion carrying the same
        weight as a reasoned one. Skipping is visible; fabricating is not.
        """
        roster = "\n".join(f"- {name}: {brief}" for name, brief in personas.items())
        user_prompt = (
            f"SCENARIO:\n{scenario}\n\n"
            f"GRAPH CONSTRAINTS:\n{json.dumps(subgraph)}\n{cross_block}\n"
            f"ADVERSARIAL PERSONAS:\n{roster}\n\n"
            "Step 1. Play every persona above against this scenario. Each proposes "
            "one counter-maneuver, in character and in opposition to the others -- "
            "the saboteur and the defender must not converge on the same move. "
            f"Exactly {len(personas)} moves, one per persona.\n"
            "Step 2. Arbitrate across those moves: isolate the primary "
            "vulnerability, give a cascade failure probability (0-100), name the "
            "next target entity, and recommend a remediation.\n\n"
            "Every target_entity_id and predicted_next_target_entity_id must name "
            "an entity from the scenario or the graph constraints, never a new one.\n"
            "Return raw JSON matching the WargameSimulationOutput schema."
        )
        return await self._execute_with_telemetry(
            message=message,
            system_prompt=(
                "You are a red-team simulation engine and Principal Game Theory "
                "Analyst. Voice several adversaries at once, keeping each one's "
                "reasoning distinct, then isolate the failure point they expose."
            ),
            user_prompt=user_prompt,
            schema=WargameSimulationOutput,
            temperature=0.2,
            # Sized for both halves, because it is now producing both.
            #
            # The default is SMALL_MODEL_DEFAULT_TOKENS (640), which covered an
            # arbitration alone. The first combined run returned five populated
            # scalars and `moves: []` -- the model spent its budget on the
            # fields the grammar demanded and had nothing left for the array.
            # Three moves at five fields each is roughly 300 tokens before the
            # synthesis is written at all.
            num_predict=1024,
        )
