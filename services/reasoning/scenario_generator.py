"""
services/reasoning/scenario_generator.py

SENTINEL SCENARIO GENERATOR — OLLAMA EDITION
=============================================
Replaces the Gemini-based implementation entirely.
All inference now runs against local Llama3 via Ollama.

Why this works without Gemini:
  Gemini's primary advantage here was native JSON schema enforcement via
  types.Schema. Our OllamaClient provides equivalent enforcement through a
  Pydantic retry loop. The output quality difference on a well-prompted
  scenario synthesis task is minimal — Llama3 70B is competitive with
  Gemini Flash for structured analytical text generation.

  Additional benefits of going full-Ollama:
    - No API cost or rate limits
    - No external network dependency in the critical synthesis path
    - Consistent inference behavior (same model as agents)
    - Single LLM stack to monitor and debug

Architecture:
  ScenarioGenerator.generate() is called by reasoning/main.py.
  Interface is identical to the Gemini version — callers don't change.
  Internal implementation uses OllamaClient from shared/utils/ollama.py,
  which is the same client the agent swarm uses.

Prompt design for Llama3:
  Scenario synthesis requires broader narrative reasoning than the agent
  tasks (peer discovery, entity classification). The system prompt is
  longer and more structured to compensate for the fact that we're asking
  for a multi-hypothesis intelligence brief, not a simple classification.
  Temperature is set higher than agents (0.25 vs 0.1) to get more diverse
  hypotheses while still maintaining JSON structure.
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import aiohttp
from pydantic import BaseModel, Field

from shared.models import CorrelationCluster, Scenario, ScenarioStatus
from shared.utils.ollama import OllamaClient, SchemaViolationError

logger = logging.getLogger("reasoning.generator")

# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────
# More detailed than agent prompts because scenario synthesis requires broader
# narrative reasoning across multiple domains simultaneously.

SCENARIO_SYSTEM_PROMPT = """You are SENTINEL, an elite multi-domain intelligence analyst with expertise in geopolitics, maritime security, financial markets, cybersecurity, and military affairs.

You have been presented with a CONFIRMED ANOMALY CLUSTER — multiple correlated signals from different intelligence domains that have been automatically detected by the SENTINEL correlation engine. Your task is to synthesize these signals into a structured intelligence assessment.

ANALYTICAL FRAMEWORK:
- Treat each signal as a potential indicator, not a conclusion
- Consider multiple hypotheses of roughly equal plausibility
- Weight recent signals (last 4h) more heavily than older context
- Flag intelligence gaps explicitly — absence of evidence matters
- Cross-domain correlations (maritime + financial + news) carry higher weight than single-domain signals

OUTPUT RULES (CRITICAL — schema enforcement is strict):
1. Respond with ONLY a raw JSON object. No markdown. No explanations. No preamble.
2. Your entire response must start with { and end with }
3. Every field in the schema is REQUIRED. Never omit a field.
4. "hypotheses" must be a JSON array of exactly 3 objects, each with the exact keys shown.
5. "confidence_overall" must be an integer between 0 and 100.
6. "recommended_monitoring" must be a JSON array of strings (not a single string).
7. All string values must be in English, professional analytical tone.

OUTPUT SCHEMA (copy this structure exactly):
{
  "headline": "One sentence maximum 150 chars — the core intelligence judgment",
  "significance": "2-3 sentences on strategic/financial impact for decision makers",
  "hypotheses": [
    {
      "label": "Short hypothesis name (e.g. Iranian Sanctions Evasion)",
      "probability": 45,
      "mechanism": "How and why this hypothesis explains the observed signals",
      "beneficiaries": ["actor1", "actor2"],
      "watch_signals": ["observable event that would confirm this", "another confirmation signal"],
      "deny_signals": ["observable event that would refute this hypothesis"],
      "time_horizon": "immediate | 24h | 72h | 1week | 1month"
    },
    {
      "label": "Second hypothesis",
      "probability": 35,
      "mechanism": "Alternative explanation for the same signals",
      "beneficiaries": ["actor"],
      "watch_signals": ["confirmation signal"],
      "deny_signals": ["refutation signal"],
      "time_horizon": "24h"
    },
    {
      "label": "Third hypothesis",
      "probability": 20,
      "mechanism": "Lower-probability but high-impact alternative",
      "beneficiaries": ["actor"],
      "watch_signals": ["confirmation signal"],
      "deny_signals": ["refutation signal"],
      "time_horizon": "72h"
    }
  ],
  "recommended_monitoring": [
    "Specific AIS track to watch",
    "Specific financial instrument or options flow to monitor",
    "News source or keyword alert to set up",
    "Entity to add to watchlist"
  ],
  "confidence_overall": 62,
  "confidence_rationale": "Explanation of why confidence is at this level — what is known vs unknown"
}

CRITICAL: The three hypothesis probabilities must sum to 100. Do not include any text outside the JSON object."""


# ── OUTPUT SCHEMA ─────────────────────────────────────────────────────────────

class HypothesisOutput(BaseModel):
    label:          str
    probability:    int
    mechanism:      str
    beneficiaries:  List[str]           = Field(default_factory=list)
    watch_signals:  List[str]           = Field(default_factory=list)
    deny_signals:   List[str]           = Field(default_factory=list)
    time_horizon:   str                 = "unknown"


class ScenarioOutput(BaseModel):
    """
    What we ask Llama3 to produce.
    Separate from the DB Scenario model — maps to it after validation.
    """
    headline:               str
    significance:           str
    hypotheses:             List[HypothesisOutput]
    recommended_monitoring: List[str]
    confidence_overall:     int
    confidence_rationale:   str


# ── GENERATOR ─────────────────────────────────────────────────────────────────

class ScenarioGenerator:
    """
    Synthesizes correlation clusters into intelligence scenarios using Llama3.
    Drop-in replacement for the Gemini-based generator — same public interface.
    """

    def __init__(self, db_client):
        # Store the database connection so we can query raw events later
        self.db    = db_client
        self.model = os.getenv("AGENT_MODEL", "llama3")
        
        # Concurrency limit: one synthesis at a time per process.
        # The OllamaClient also acquires the global semaphore, but we keep this
        # as a generator-level guard for clarity and to prevent scenario tasks
        # from stacking up if the reasoning loop is processing fast bursts.
        # If 5 anomaly clusters arrive at the same time,
        # only 1 gets to enter the `generate()` method. The other 4 wait their turn.
        # This prevents our local Llama3 instance from running out of memory.
        self._limiter = asyncio.Semaphore(1)
        
        # HTTP session created lazily on first use
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=3, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def generate(
        self,
        cluster:  CorrelationCluster,
        context:  dict,
        patterns: list,
    ) -> Optional[Scenario]:
        """
        Main entry point. Called by reasoning/main.py.
        Returns a Scenario object ready for DB insertion, or None on failure.
        """
        # Enter the semaphore lock. No other task can pass this line until 
        # `_synthesize` is completely finished.
        async with self._limiter:
            return await self._synthesize(cluster, context, patterns)

    async def _synthesize(
        self,
        cluster:  CorrelationCluster,
        context:  dict,
        patterns: list,
    ) -> Optional[Scenario]:
        """
        The core pipeline: 
        1. Fetch details -> 2. Build Prompt -> 3. Call AI -> 4. Fix Math -> 5. Save Data.
        We separate this from `generate()` so the semaphore only wraps the execution logic,
        keeping the code clean.
        """

        # ── 1. HYDRATE RAW EVENTS FROM DB ─────────────────────────────────────
        # The `cluster` only has event IDs. We need the actual text and data 
        # to feed the LLM, so we pull them from the database.
        event_ids = list(filter(None, [
            cluster.trigger_event_id, *cluster.supporting_event_ids
        ]))
        raw_events = await self._fetch_events(event_ids)

        # ── 2. BUILD USER PROMPT ───────────────────────────────────────────────
        user_prompt = self._build_user_prompt(cluster, context, patterns, raw_events)

        # ── 3. CALL LLAMA3 ─────────────────────────────────────────────────────
        logger.info(
            "🧠 Synthesizing [%s] %s via Llama3...",
            cluster.alert_tier.name,
            cluster.rule_name,
        )

        client = OllamaClient(self._get_session(), self.model)
        max_retries = 2
        retry_delay = 5.0

        for attempt in range(max_retries):
            try:
                # Send the prompt to the AI and force it to match our Pydantic schema
                output: ScenarioOutput = await client.infer(
                    system_prompt=SCENARIO_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    schema=ScenarioOutput,
                    temperature=0.25,   # Slightly higher than agents for narrative diversity
                    max_retries=3,      # Inner retry loop inside OllamaClient
                    num_predict=1024,
                )
                break
            except SchemaViolationError as e:
                logger.error(
                    "Schema enforcement failed for %s (attempt %d/%d): %s",
                    cluster.correlation_id[:8], attempt + 1, max_retries, e,
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return None
            except Exception as e:
                logger.error(
                    "Llama3 inference error for %s: %s",
                    cluster.correlation_id[:8], e, exc_info=True,
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return None

        # ── 4. VALIDATE HYPOTHESIS PROBABILITIES ──────────────────────────────
        output = self._normalize_probabilities(output)

        # ── 5. MAP TO DB SCENARIO MODEL ────────────────────────────────────────
        # Take the raw Python dictionary/Pydantic object returned by the AI
        # and convert it into the official `Scenario` database model.
        scenario = Scenario(
            scenario_id=f"scn_{uuid.uuid4().hex[:8]}",
            correlation_id=cluster.correlation_id,
            status=ScenarioStatus.HYPOTHESIS,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            headline=output.headline,
            significance=output.significance,
            hypotheses=[h.dict() for h in output.hypotheses],
            recommended_monitoring=output.recommended_monitoring,
            confidence_overall=max(0, min(100, output.confidence_overall)),
            confidence_rationale=output.confidence_rationale,
        )

        logger.info(
            "✅ Scenario synthesized: %s (confidence=%d%%)",
            scenario.headline[:80], scenario.confidence_overall,
        )
        return scenario

    def _build_user_prompt(
        self,
        cluster:    CorrelationCluster,
        context:    dict,
        patterns:   list,
        raw_events: list,
    ) -> str:
        """
        Assembles the intelligence package for Llama3.

        Context window budget (~4096 tokens for Llama3 8B):
          - Correlation header:    ~200 tokens
          - Raw events:            ~800 tokens (capped at 5 events)
          - Graph context:         ~600 tokens (capped at 10 relationships)
          - Historical patterns:   ~400 tokens (capped at 3 patterns)
          - Recent headlines:      ~300 tokens (capped at 5)
          - Instruction:           ~200 tokens
          Total:                   ~2500 tokens — leaves room for hypothesis generation
        """
        # Cap each section to stay within context window
        # JSON.dumps converts Python objects into a string format the AI can read.
        events_section = json.dumps(raw_events[:5], indent=2, default=str)

        graph_section = json.dumps(
            context.get("entity_graph", [])[:10], indent=2, default=str
        )

        patterns_section = json.dumps(patterns[:3], indent=2, default=str)

        headlines = context.get("recent_headlines", [])[:5]
        headlines_section = "\n".join(f"• {h}" for h in headlines) if headlines else "None available"

        agent_intel = context.get("agent_intel_briefs", [])
        agent_section = ""
        if agent_intel:
            agent_section = f"""
=== AGENT INTELLIGENCE BRIEFS ===
Pre-analyzed intelligence from the SENTINEL Intel Agent:
{json.dumps(agent_intel[:2], indent=2, default=str)}
"""

        return f"""=== ANOMALY CLUSTER ===
Rule Fired: {cluster.rule_name}
Alert Tier: {cluster.alert_tier.name}
Description: {cluster.description}
Tags: {', '.join(cluster.tags)}
Detected At: {cluster.detected_at.isoformat()}

=== RAW SIGNAL DATA ===
{events_section}

=== ENTITY GRAPH CONTEXT ===
Known relationships for involved entities:
{graph_section}

=== HISTORICAL PRECEDENTS ===
Similar confirmed/denied scenarios from the past 90 days:
{patterns_section}

=== RECENT GEOPOLITICAL HEADLINES ===
{headlines_section}
{agent_section}
=== TASK ===
Synthesize all signals above into a structured intelligence assessment.
Produce exactly 3 hypotheses that together explain the observed anomaly cluster.
The hypothesis probabilities must sum to 100.
Return the JSON assessment now:"""

    async def _fetch_events(self, event_ids: List[str]) -> List[Dict]:
        """Fetch raw event details from TimescaleDB for the prompt."""
        if not event_ids:
            return []
        try:
            rows = await self.db.query(
                """
                SELECT type, source, tags, anomaly_score, occurred_at,
                       financial_data, vessel_data, flight_data,
                       crypto_data, cyber_data, headline
                FROM events
                WHERE event_id = ANY($1::uuid[])
                ORDER BY anomaly_score DESC
                """,
                event_ids
            )
            
            # Datetime objects break `json.dumps()` later on.
            # We loop through the results and convert any datetimes to ISO 8601 strings (e.g. "2023-10-24T12:00:00Z").
            cleaned = []
            for row in rows:
                cleaned_row = {}
                for k, v in row.items():
                    if isinstance(v, datetime):
                        cleaned_row[k] = v.isoformat()
                    elif v is not None:
                        cleaned_row[k] = v
                cleaned.append(cleaned_row)
            return cleaned
        except Exception as e:
            logger.error("Failed to hydrate events: %s", e)
            return []

    @staticmethod
    def _normalize_probabilities(output: ScenarioOutput) -> ScenarioOutput:
        """
        Ensure hypothesis probabilities sum to 100.
        Llama3 occasionally produces [45, 35, 25] or [40, 40, 40].
        We rescale proportionally rather than rejecting valid scenarios.
        
        CONCEPT: LLM Math Limitations
        LLMs do not "calculate" numbers; they predict the next likely word.
        Therefore, they are notoriously bad at ensuring numbers sum to exactly 100.
        Instead of failing the pipeline, we fix the AI's math programmatically.
        """
        if not output.hypotheses:
            return output

        total = sum(h.probability for h in output.hypotheses)
        if total == 0 or total == 100:
            return output

        # Proportional rescaling: If the AI output 40, 40, 40 (sum 120),
        # (40/120) * 100 = ~33.
        for h in output.hypotheses:
            h.probability = round((h.probability / total) * 100)

        # Fix rounding error on the first hypothesis to guarantee sum=100
        # Sometimes rounding makes the sum 99 or 101. We dump the remainder onto the first hypothesis.
        diff = 100 - sum(h.probability for h in output.hypotheses)
        output.hypotheses[0].probability += diff

        logger.debug(
            "Normalized hypothesis probabilities from sum=%d to 100", total
        )
        return output

    async def close(self):
        """
        Clean up HTTP session on shutdown.
        Always close aiohttp sessions to prevent "Unclosed client session" memory leak warnings.
        """
        if self._session and not self._session.closed:
            await self._session.close()