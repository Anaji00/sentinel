"""
services/agents/consensus_engine.py

AGENT CONSENSUS & CONTRADICTION ENGINE
=======================================
Reads all active AgentBulletins across the swarm, detects contradictions,
computes weighted consensus scores, and emits ConsensusReports.

Architectural Design Rationale (§3.1 — Agentic Harmonization):
    This engine uses INDEPENDENT agent estimates with WEIGHTED AGGREGATION,
    NOT multi-agent debate/consensus loops. This is deliberate:

    - MAST (2025, 1,600+ annotated traces) identifies inter-agent misalignment
      as a distinct failure category from single-model error.
    - "The Deliberative Illusion" (2025), "The Inverse-Wisdom Law" (2025),
      and "Hallucination Amplification in Multi-Agent Debate" (2025) document
      that debate/consensus architectures amplify hallucination via stance
      homogenization and informational cascade — agents converge on a shared
      wrong answer (the LLM analog of Bikhchandani/Hirshleifer herding).
    - Independent estimates + weighted aggregation is the pattern that the
      wisdom-of-crowds and prediction-market literature actually favors.

    DO NOT add a debate/chat loop between agents — you'd likely trade a
    real structural advantage for a fashionable but empirically weaker one.

Fusion Math:
    Uses Jøsang's Subjective Logic (SL) instead of raw weighted averaging.
    Each agent's bulletin is mapped to an SL opinion (belief, disbelief,
    uncertainty, base_rate) with evidence counts derived from the agent's
    consensus_weight. SL's Cumulative Fusion explicitly carries conflict
    and uncertainty forward rather than averaging it away.

    When fused uncertainty exceeds a threshold, we emit an Analysis of
    Competing Hypotheses (ACH) report instead of forcing disagreement into
    a single blended score.

Contradictions are detected when:
  - Multiple agents have bulletins for the same ticker with divergent directions
  - Conviction divergence exceeds 0.4 between agreeing/disagreeing agents
  - A "thesis" and a "regime_change" bulletin conflict
"""

import asyncio
import json
import logging
import time
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
from shared.utils.entity_resolution import resolve_entity

from pydantic import BaseModel, Field

from .base import SentinelAgent, AgentBulletin, AgentScorecard
from shared.kafka import Topics

logger = logging.getLogger("agent.consensus")

# Threshold for fused uncertainty above which we emit ACH instead of consensus
ACH_UNCERTAINTY_THRESHOLD = 0.40


# ── SUBJECTIVE LOGIC ─────────────────────────────────────────────────────────

# The prior used when there is not enough history to measure one.
#
# 0.5 is the honest answer to "how often is a claim of this kind true" when you
# have never checked: it is the non-informative prior, and Subjective Logic's
# projected probability degrades to belief-plus-half-the-uncertainty, which is
# exactly what this system did everywhere until the outcomes existed.
DEFAULT_BASE_RATE = 0.5

# Resolved predictions required before a measured rate replaces the default.
# Below this the rate is an artefact of a handful of outcomes; a base rate
# computed from four resolutions is noisier than admitting ignorance.
MIN_OUTCOMES_FOR_BASE_RATE = 25

# How long a computed rate is reused before it is recomputed. The corpus grows
# slowly and this query touches two tables.
BASE_RATE_CACHE_TTL_SEC = 900
BASE_RATE_CACHE_KEY = "sentinel:consensus:base_rate"

# Bounds. A measured rate of 0.0 or 1.0 says a class of claim is never or always
# right, which no finite sample establishes, and either extreme collapses the
# projected probability onto belief or onto certainty.
BASE_RATE_FLOOR = 0.05
BASE_RATE_CEILING = 0.95


async def measured_base_rate(db_client, redis_client, direction: str = "any") -> float:
    """How often claims of this kind have actually turned out right.

    Computed from the platform's own resolved history rather than assumed:
    directional predictions from agent_predictions, which carry the direction
    that was claimed and whether it was borne out.

    Returns DEFAULT_BASE_RATE when there is too little history, which is the
    correct answer rather than a failure -- an unmeasured prior should be
    non-informative, not confidently wrong.
    """
    cache_field = str(direction or "any").lower()

    if redis_client:
        try:
            raw_redis = getattr(redis_client, "raw", redis_client)
            cached = await raw_redis.hget(BASE_RATE_CACHE_KEY, cache_field)
            if cached:
                return float(cached.decode() if isinstance(cached, bytes) else cached)
        except Exception:
            pass

    if not db_client:
        return DEFAULT_BASE_RATE

    try:
        if cache_field in ("up", "down"):
            rows = await db_client.query(
                """
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE outcome_correct) AS correct
                FROM agent_predictions
                WHERE resolved_at IS NOT NULL
                  AND outcome_correct IS NOT NULL
                  AND lower(direction) = $1
                  AND occurred_at > NOW() - INTERVAL '90 days'
                """,
                cache_field,
            )
        else:
            rows = await db_client.query(
                """
                SELECT count(*) AS n,
                       count(*) FILTER (WHERE outcome_correct) AS correct
                FROM agent_predictions
                WHERE resolved_at IS NOT NULL
                  AND outcome_correct IS NOT NULL
                  AND occurred_at > NOW() - INTERVAL '90 days'
                """
            )
        if not rows:
            return DEFAULT_BASE_RATE
        n = int(rows[0].get("n") or 0)
        correct = int(rows[0].get("correct") or 0)
        if n < MIN_OUTCOMES_FOR_BASE_RATE:
            return DEFAULT_BASE_RATE

        rate = correct / float(n)
        rate = max(BASE_RATE_FLOOR, min(BASE_RATE_CEILING, rate))

        if redis_client:
            try:
                raw_redis = getattr(redis_client, "raw", redis_client)
                await raw_redis.hset(BASE_RATE_CACHE_KEY, cache_field, rate)
                await raw_redis.expire(BASE_RATE_CACHE_KEY, BASE_RATE_CACHE_TTL_SEC)
            except Exception:
                pass

        logger.info(
            "Base rate for %s claims measured at %.3f over %d resolved predictions.",
            cache_field, rate, n,
        )
        return rate
    except Exception as e:
        logger.debug("Base rate query failed (%s); using the non-informative prior: %s",
                     cache_field, e)
        return DEFAULT_BASE_RATE


class SubjectiveOpinion(BaseModel):
    """
    Jøsang Subjective Logic binomial opinion: ω = (b, d, u, a)

    b = belief      (evidence FOR the proposition)
    d = disbelief   (evidence AGAINST the proposition)
    u = uncertainty  (lack of evidence either way)
    a = base rate    (prior probability in absence of evidence)

    Constraint: b + d + u = 1.0
    Projected probability: P = b + a * u
    """
    belief: float = 0.0
    disbelief: float = 0.0
    uncertainty: float = 1.0
    base_rate: float = 0.5

    @property
    def projected_probability(self) -> float:
        return self.belief + self.base_rate * self.uncertainty

    @property
    def conflict_level(self) -> float:
        """Measure of internal conflict: high belief AND high disbelief."""
        return min(self.belief, self.disbelief) * 2.0

    @classmethod
    def from_bulletin(
        cls,
        bulletin: AgentBulletin,
        weight: float = 1.0,
        base_rate: float = DEFAULT_BASE_RATE,
    ) -> "SubjectiveOpinion":
        """
        Map an AgentBulletin to a Subjective Logic opinion.

        The agent's conviction becomes the evidence strength.
        The agent's consensus_weight scales the evidence count,
        controlling how much this opinion moves the fusion.
        """
        conviction = bulletin.conviction
        direction = bulletin.expected_direction or "neutral"

        # Evidence count: weight * 10 gives reasonable scaling
        # Higher weight = more evidence = lower uncertainty
        evidence_count = max(0.1, weight * 10.0)

        if direction in ("up", "bullish", "long"):
            r = conviction * evidence_count  # positive evidence
            s = (1.0 - conviction) * evidence_count * 0.3  # weak counter-evidence
        elif direction in ("down", "bearish", "short"):
            r = (1.0 - conviction) * evidence_count * 0.3
            s = conviction * evidence_count
        else:
            r = 0.1
            s = 0.1

        W = 2.0  # Non-informative prior weight
        b = r / (r + s + W)
        d = s / (r + s + W)
        u = W / (r + s + W)

        # The prior, measured where there is history to measure it.
        #
        # This returned base_rate=0.5 unconditionally, and projected probability
        # is `belief + base_rate * uncertainty` -- so a hardcoded coin flip was
        # contributing a share of every answer proportional to how uncertain
        # that answer was. With most signals carrying a single contributor,
        # uncertainty is high and the constant dominated.
        #
        # The base rate is the one parameter Subjective Logic exists to let you
        # inform, and the platform has been recording the outcomes needed to
        # compute it: agent_predictions.outcome_correct and scenarios.status.
        # Passing 0.5 while holding that history is choosing not to use it.
        return cls(belief=round(b, 6), disbelief=round(d, 6),
                   uncertainty=round(u, 6),
                   base_rate=round(max(0.0, min(1.0, float(base_rate))), 6))

    @classmethod
    def cumulative_fuse(cls, opinions: List["SubjectiveOpinion"]) -> "SubjectiveOpinion":
        """
        Jøsang Cumulative Fusion (CCF) of multiple opinions.

        Unlike raw Dempster-Shafer, CCF doesn't suffer Zadeh's paradox
        under highly conflicting evidence — it degrades gracefully to
        higher uncertainty instead of collapsing.
        """
        if not opinions:
            return cls()
        if len(opinions) == 1:
            return opinions[0].model_copy()

        result = opinions[0].model_copy()
        for op in opinions[1:]:
            result = cls._fuse_two(result, op)
        return result

    @classmethod
    def _fuse_two(cls, w1: "SubjectiveOpinion", w2: "SubjectiveOpinion") -> "SubjectiveOpinion":
        """Cumulative fusion of two opinions (Jøsang §12.3)."""
        u1, u2 = w1.uncertainty, w2.uncertainty

        # Handle edge cases: if either opinion is dogmatic (u=0)
        if u1 < 1e-10 and u2 < 1e-10:
            # Both dogmatic: weighted average by base rates
            total = 1.0
            b = (w1.belief + w2.belief) / 2.0
            d = (w1.disbelief + w2.disbelief) / 2.0
            u = 0.0
        elif u1 < 1e-10:
            return w1.model_copy()
        elif u2 < 1e-10:
            return w2.model_copy()
        else:
            # Standard cumulative fusion formula
            # κ = u1 + u2 - u1*u2 (always > 0 for non-dogmatic opinions)
            kappa = u1 + u2 - u1 * u2
            if kappa < 1e-10:
                kappa = 1e-10

            b = (w1.belief * u2 + w2.belief * u1) / kappa
            d = (w1.disbelief * u2 + w2.disbelief * u1) / kappa
            u = (u1 * u2) / kappa

        # Fused base rate (evidence-weighted average)
        a = (w1.base_rate + w2.base_rate) / 2.0

        # Normalize to ensure b + d + u = 1.0
        total = b + d + u
        if total > 1e-10:
            b /= total
            d /= total
            u /= total

        return cls(
            belief=round(b, 6),
            disbelief=round(d, 6),
            uncertainty=round(u, 6),
            base_rate=round(a, 4),
        )


# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

class CompetingHypothesis(BaseModel):
    """A single hypothesis in an ACH matrix."""
    hypothesis: str  # e.g., "bullish on AAPL"
    supporting_agents: List[str] = Field(default_factory=list)
    refuting_agents: List[str] = Field(default_factory=list)
    opinion: Optional[SubjectiveOpinion] = None
    evidence_summary: str = ""


class ACHReport(BaseModel):
    """
    Analysis of Competing Hypotheses (ACH) report.

    Emitted when Subjective Logic fusion produces high uncertainty
    or conflict — instead of forcing disagreement into a single
    consensus score that hides it, we surface the competing
    hypotheses explicitly, matching IC tradecraft.
    """
    ticker: str
    fused_opinion: SubjectiveOpinion
    competing_hypotheses: List[CompetingHypothesis] = Field(default_factory=list)
    conflict_level: float = 0.0
    fused_uncertainty: float = 0.0
    reason: str = ""  # Why ACH was triggered instead of consensus
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ContradictionReport(BaseModel):
    """Detected contradiction between agents."""
    ticker: str
    bulletin_type: str
    agents_bullish: List[str] = Field(default_factory=list)
    agents_bearish: List[str] = Field(default_factory=list)
    conviction_spread: float = 0.0  # Max conviction difference
    summary: str = ""


class EvidenceContributor(BaseModel):
    """Details of an individual agent/detector contribution to the consensus evidence trail."""
    agent_name: str
    direction: str = "neutral"
    conviction: float = 0.0
    weight: float = 1.0
    opinion: Optional[SubjectiveOpinion] = None


class ConsensusSignal(BaseModel):
    """Weighted consensus for a ticker."""
    ticker: str
    direction: str  # "bullish", "bearish", "mixed"
    consensus_score: float = 0.0  # -1.0 (strong bearish) to 1.0 (strong bullish)
    contributing_agents: int = 0
    weighted_conviction: float = 0.0
    agreement_ratio: float = 0.0  # 0.0 = total disagreement, 1.0 = total agreement
    fused_opinion: Optional[SubjectiveOpinion] = None
    bulletins: List[AgentBulletin] = Field(default_factory=list)
    evidence_trail: List[EvidenceContributor] = Field(default_factory=list)


class ConsensusReport(BaseModel):
    """Full consensus analysis across the agent swarm."""
    report_id: str = ""
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    contradictions: List[ContradictionReport] = Field(default_factory=list)
    consensus_signals: List[ConsensusSignal] = Field(default_factory=list)
    ach_reports: List[ACHReport] = Field(default_factory=list)
    stale_agents: List[str] = Field(default_factory=list)
    total_active_bulletins: int = 0
    agents_reporting: List[str] = Field(default_factory=list)
    # Every agent reads this back through get_swarm_context() as
    # "Swarm Consensus: ...". The field did not exist, so `cons.get("summary")`
    # returned None on every read and the line was never added to any prompt:
    # the engine computed fusion, contradictions and ACH matrices, persisted
    # them, and no agent ever saw a word of it.
    summary: str = ""


# ── CONSENSUS ENGINE ─────────────────────────────────────────────────────────

class ConsensusEngine(SentinelAgent):
    """
    Reads all active AgentBulletins across the swarm, detects contradictions,
    computes weighted consensus signals, and emits ConsensusReports.
    """

    def __init__(self, *args, redis_client=None, producer=None, **kwargs):
        # If redis_client is passed positionally as the 1st argument
        if args and (redis_client is None):
            redis_client = args[0]
            args = args[1:]

        kwargs.setdefault("agent_name", "consensus_engine")
        kwargs.setdefault("input_topics", [Topics.INTEL_BRIEFS, Topics.QUANT_DISCOVERIES, Topics.FINANCIAL_ADVICE, Topics.RULES_FEEDBACK])
        kwargs.setdefault("db_client", None)
        kwargs.setdefault("neo4j_client", None)
        kwargs.setdefault("producer", producer)
        kwargs.setdefault("consumer", None)
        kwargs.setdefault("dlq", None)
        kwargs["redis_client"] = redis_client

        super().__init__(*args, **kwargs)

    @property
    def output_topic(self) -> str:
        return Topics.CONSENSUS_REPORTS

    # Consensus is a property of the whole swarm's current opinions, not of any
    # one message. analyze() re-reads every bulletin and every scorecard, so
    # running it per message meant repeating an identical global computation
    # thousands of times an hour and re-deriving the same answer -- which is
    # what held this consumer at a few hundred messages an hour against 61,000
    # of backlog.
    #
    # Messages still trigger it, so a burst of activity is reflected promptly;
    # they just cannot trigger it more often than the picture can meaningfully
    # change.
    _REVIEW_INTERVAL_SEC = 120

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        now = time.monotonic()
        last = getattr(self, "_last_review_at", 0.0)
        if now - last < self._REVIEW_INTERVAL_SEC:
            return None
        self._last_review_at = now

        report = await self.analyze()
        if report.contradictions or report.consensus_signals:
            return report.model_dump()
        return None

    async def run_scheduled_review(self) -> Optional[Dict[str, Any]]:
        report = await self.analyze()
        if report.contradictions or report.consensus_signals:
            return report.model_dump()
        return None

    async def analyze(self) -> ConsensusReport:
        """
        Full consensus analysis using Subjective Logic fusion.
        Produces consensus signals when agent opinions converge, and
        ACH reports when they diverge (high fused uncertainty/conflict).
        """
        # 1. Read all active bulletins
        bulletins = await self._read_all_bulletins()
        if not bulletins:
            return ConsensusReport()

        # 2. Read all agent scorecards for weighting
        scorecards = await self._read_all_scorecards()
        score_map = {s.agent_name: s for s in scorecards}

        # 3. Detect stale agents via state digests (§3.3 context drift)
        stale_agents = await self._detect_stale_agents()

        # 4. Group bulletins by the *subject*, not by the spelling
        #
        # `b.ticker.upper()` groups two agents looking at the same company only
        # when they happened to spell it the same way. They frequently do not:
        # one publishes "AAPL", another "Apple Inc.", a third "CPB ($21.53)"
        # before that was fixed -- three groups, each with one contributor,
        # each unable to corroborate or contradict the others.
        #
        # Fusion is the entire purpose of this engine and it cannot fuse what it
        # cannot group. Sixteen of seventeen live signals carried
        # contributing_agents: 1, and the single-contributor case skips fusion
        # altogether a few lines below.
        # One measurement per pass, not one per opinion: the rate moves on the
        # timescale of resolved predictions, not of bulletins.
        prior = await measured_base_rate(
            getattr(self, "db", None) or getattr(self, "_db", None), self.redis
        )

        ticker_groups: Dict[str, List[AgentBulletin]] = defaultdict(list)
        canonical_display: Dict[str, str] = {}
        for b in bulletins:
            if b.ticker:
                subject = await resolve_entity(self.redis, b.ticker)
                if not subject:
                    continue
                ticker_groups[subject].append(b)
                # The readable spelling, for the report a person reads. The
                # canonical key is for grouping; it is not a display name.
                canonical_display.setdefault(subject, str(b.ticker))

        # 5. Analyze each ticker group
        contradictions = []
        consensus_signals = []
        ach_reports = []
        agents_reporting = set()

        for ticker, group in ticker_groups.items():
            # Compute weights (downweight stale agents)
            def _get_weight(agent_name: str) -> float:
                base = score_map.get(agent_name, AgentScorecard(agent_name=agent_name)).consensus_weight
                if agent_name in stale_agents:
                    return base * 0.3  # Heavily downweight stale/drifted agents
                return base

            def _build_evidence_trail(bulletin_group: List[AgentBulletin]) -> List[EvidenceContributor]:
                trail = []
                for b in bulletin_group:
                    w = _get_weight(b.agent_name)
                    op = SubjectiveOpinion.from_bulletin(b, w, base_rate=prior)
                    trail.append(EvidenceContributor(
                        agent_name=b.agent_name,
                        direction=b.expected_direction or "neutral",
                        conviction=b.conviction,
                        weight=round(w, 4),
                        opinion=op,
                    ))
                return trail

            if len(group) < 2:
                # Single-agent ticker: emit consensus directly
                b = group[0]
                agents_reporting.add(b.agent_name)
                weight = _get_weight(b.agent_name)
                direction = b.expected_direction or "neutral"
                opinion = SubjectiveOpinion.from_bulletin(b, weight, base_rate=prior)
                consensus_signals.append(ConsensusSignal(
                    ticker=ticker,
                    direction=self._map_direction(direction),
                    consensus_score=self._direction_to_score(direction) * b.conviction * weight,
                    contributing_agents=1,
                    weighted_conviction=b.conviction * weight,
                    # Not 1.0. One agent agreeing with itself is not unanimity,
                    # and this figure is read as "the swarm concurs". Agreement
                    # is undefined with a single opinion; 0.0 says so without
                    # inventing corroboration that nobody supplied.
                    agreement_ratio=0.0,
                    fused_opinion=opinion,
                    bulletins=group,
                    evidence_trail=_build_evidence_trail(group),
                ))
                continue

            # Multiple agents have opinions on this ticker
            bullish_agents = []
            bearish_agents = []
            neutral_agents = []
            weighted_scores = []
            total_weight = 0.0
            opinions = []

            for b in group:
                agents_reporting.add(b.agent_name)
                weight = _get_weight(b.agent_name)
                direction = b.expected_direction or "neutral"

                # Build Subjective Logic opinion from this bulletin
                opinion = SubjectiveOpinion.from_bulletin(b, weight, base_rate=prior)
                opinions.append((b.agent_name, opinion, direction))

                if direction in ("up", "bullish", "long"):
                    bullish_agents.append(b.agent_name)
                    weighted_scores.append(b.conviction * weight)
                elif direction in ("down", "bearish", "short"):
                    bearish_agents.append(b.agent_name)
                    weighted_scores.append(-b.conviction * weight)
                else:
                    neutral_agents.append(b.agent_name)
                    weighted_scores.append(0.0)

                total_weight += weight

            # Detect contradictions (unchanged logic)
            if bullish_agents and bearish_agents:
                convictions = [b.conviction for b in group]
                spread = max(convictions) - min(convictions) if convictions else 0.0
                contradiction = ContradictionReport(
                    ticker=ticker,
                    bulletin_type="signal",
                    agents_bullish=bullish_agents,
                    agents_bearish=bearish_agents,
                    conviction_spread=round(spread, 4),
                    summary=(
                        f"{', '.join(bullish_agents)} bullish vs "
                        f"{', '.join(bearish_agents)} bearish on {ticker}. "
                        f"Conviction spread: {spread:.0%}"
                    ),
                )
                contradictions.append(contradiction)

            # Subjective Logic fusion
            all_opinions = [op for _, op, _ in opinions]
            fused = SubjectiveOpinion.cumulative_fuse(all_opinions)

            # Decision: ACH report vs consensus signal
            if fused.uncertainty > ACH_UNCERTAINTY_THRESHOLD or fused.conflict_level > 0.3:
                # High uncertainty/conflict: emit ACH report instead of consensus
                bullish_hyp = CompetingHypothesis(
                    hypothesis=f"Bullish on {ticker}",
                    supporting_agents=bullish_agents,
                    refuting_agents=bearish_agents,
                    opinion=SubjectiveOpinion.cumulative_fuse(
                        [op for name, op, d in opinions if d in ("up", "bullish", "long")]
                    ) if bullish_agents else None,
                    evidence_summary=f"{len(bullish_agents)} agent(s) bullish with avg conviction {sum(b.conviction for b in group if b.expected_direction in ('up','bullish','long')) / max(1, len(bullish_agents)):.0%}"
                )
                bearish_hyp = CompetingHypothesis(
                    hypothesis=f"Bearish on {ticker}",
                    supporting_agents=bearish_agents,
                    refuting_agents=bullish_agents,
                    opinion=SubjectiveOpinion.cumulative_fuse(
                        [op for name, op, d in opinions if d in ("down", "bearish", "short")]
                    ) if bearish_agents else None,
                    evidence_summary=f"{len(bearish_agents)} agent(s) bearish with avg conviction {sum(b.conviction for b in group if b.expected_direction in ('down','bearish','short')) / max(1, len(bearish_agents)):.0%}"
                )

                ach = ACHReport(
                    ticker=ticker,
                    fused_opinion=fused,
                    competing_hypotheses=[bullish_hyp, bearish_hyp],
                    conflict_level=round(fused.conflict_level, 4),
                    fused_uncertainty=round(fused.uncertainty, 4),
                    reason=(
                        f"Fused uncertainty ({fused.uncertainty:.2f}) exceeds ACH threshold ({ACH_UNCERTAINTY_THRESHOLD}). "
                        f"Emitting competing hypotheses instead of forcing a consensus score."
                    ),
                )
                ach_reports.append(ach)
                logger.info(f"📊 ACH Report for {ticker}: uncertainty={fused.uncertainty:.2f}, conflict={fused.conflict_level:.2f}")
            else:
                # Low uncertainty: emit consensus signal with SL-derived scores
                # Map fused opinion to consensus direction and score
                projected = fused.projected_probability
                if projected > 0.6:
                    consensus_direction = "bullish"
                    consensus_score = (projected - 0.5) * 2.0  # Map [0.5, 1.0] → [0, 1.0]
                elif projected < 0.4:
                    consensus_direction = "bearish"
                    consensus_score = (projected - 0.5) * 2.0  # Map [0.0, 0.5] → [-1.0, 0]
                else:
                    consensus_direction = "mixed"
                    consensus_score = (projected - 0.5) * 2.0

                total_agents = len(bullish_agents) + len(bearish_agents) + len(neutral_agents)
                majority = max(len(bullish_agents), len(bearish_agents), len(neutral_agents))
                agreement = majority / total_agents if total_agents > 0 else 0.0

                weighted_conviction = abs(sum(weighted_scores)) / max(1.0, total_weight)

                consensus_signals.append(ConsensusSignal(
                    ticker=ticker,
                    direction=consensus_direction,
                    consensus_score=round(consensus_score, 4),
                    contributing_agents=total_agents,
                    weighted_conviction=round(weighted_conviction, 4),
                    agreement_ratio=round(agreement, 4),
                    fused_opinion=fused,
                    bulletins=group,
                    evidence_trail=_build_evidence_trail(group),
                ))

        # Sort: highest conviction first
        consensus_signals.sort(key=lambda s: abs(s.consensus_score), reverse=True)

        report = ConsensusReport(
            report_id=f"consensus-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            contradictions=contradictions,
            consensus_signals=consensus_signals,
            ach_reports=ach_reports,
            stale_agents=stale_agents,
            total_active_bulletins=len(bulletins),
            agents_reporting=sorted(agents_reporting),
            summary=self._build_summary(consensus_signals, contradictions, stale_agents),
        )

        # Persist the report
        await self._persist_report(report)

        # Publish to Kafka if available
        producer = getattr(self, "producer", None)
        if producer and (contradictions or consensus_signals or ach_reports):
            try:
                from shared.kafka import Topics
                await producer.send(
                    "agents.consensus.reports",
                    report.model_dump(mode="json"),
                )
            except Exception as e:
                logger.warning(f"Failed to emit consensus report to Kafka: {e}")

        return report

    async def get_consensus_for_ticker(self, ticker: str) -> Optional[ConsensusSignal]:
        """Quick lookup for a specific ticker's consensus."""
        report = await self.analyze()
        for signal in report.consensus_signals:
            if signal.ticker == ticker.upper():
                return signal
        return None

    async def get_contradictions(self) -> List[ContradictionReport]:
        """Quick lookup for all active contradictions."""
        report = await self.analyze()
        return report.contradictions

    # ── INTERNAL ──────────────────────────────────────────────────────────────

    async def _read_all_bulletins(self) -> List[AgentBulletin]:
        """Reads all active bulletins from Redis."""
        bulletins = []
        try:
            cursor = 0
            while True:
                cursor, keys = await self.redis.raw.scan(
                    cursor=cursor, match="sentinel:bulletins:*", count=100
                )
                if keys:
                    values = await self.redis.raw.mget(keys)
                    for val in values:
                        if val:
                            try:
                                raw = val if isinstance(val, str) else val.decode("utf-8")
                                bulletins.append(AgentBulletin(**json.loads(raw)))
                            except Exception:
                                pass
                if cursor == 0:
                    break
        except Exception as e:
            logger.error(f"Failed to read all bulletins: {e}")
        return bulletins

    async def _read_all_scorecards(self) -> List[AgentScorecard]:
        """Reads all agent scorecards from Redis."""
        scorecards = []
        try:
            cursor = 0
            while True:
                cursor, keys = await self.redis.raw.scan(
                    cursor=cursor, match="sentinel:agents:scorecard:*", count=50
                )
                if keys:
                    values = await self.redis.raw.mget(keys)
                    for val in values:
                        if val:
                            try:
                                raw = val if isinstance(val, str) else val.decode("utf-8")
                                scorecards.append(AgentScorecard(**json.loads(raw)))
                            except Exception:
                                pass
                if cursor == 0:
                    break
        except Exception as e:
            logger.debug(f"Failed to read scorecards: {e}")
        return scorecards

    @staticmethod
    def _build_summary(
        signals: List[ConsensusSignal],
        contradictions: List["ContradictionReport"],
        stale_agents: List[str],
    ) -> str:
        """One line of swarm state, written for another agent's prompt.

        Deliberately states how many agents stand behind each call. The reader
        is a model that will treat "Swarm Consensus" as corroboration, so a
        signal carried by one agent has to say so -- otherwise an agent's own
        opinion returns to it a cycle later wearing the swarm's authority, and
        the swarm converges on nothing but its own echo.
        """
        parts: List[str] = []

        corroborated = [s for s in signals if s.contributing_agents > 1]
        solo = [s for s in signals if s.contributing_agents <= 1]

        for sig in sorted(corroborated, key=lambda x: abs(x.consensus_score), reverse=True)[:3]:
            parts.append(
                f"{sig.ticker} {sig.direction} "
                f"({sig.contributing_agents} agents, {sig.agreement_ratio:.0%} agreement)"
            )
        if solo:
            names = ", ".join(sorted({s.ticker for s in solo})[:3])
            parts.append(f"single-agent only: {names}")
        if contradictions:
            parts.append(f"{len(contradictions)} contradiction(s) open")
        if stale_agents:
            parts.append(f"stale: {', '.join(sorted(stale_agents)[:3])}")

        if not parts:
            return "No corroborated swarm signals."
        return " | ".join(parts)

    async def _persist_report(self, report: ConsensusReport) -> None:
        """Stores the consensus report in Redis for dashboards and other consumers."""
        try:
            key = "sentinel:consensus:latest"
            await self.redis.raw.set(key, report.model_dump_json(), ex=3600)

            # Store contradiction count for metrics
            await self.redis.raw.set(
                "sentinel:consensus:contradiction_count",
                str(len(report.contradictions)),
                ex=3600,
            )
            # Store ACH count for metrics
            if report.ach_reports:
                await self.redis.raw.set(
                    "sentinel:consensus:ach_count",
                    str(len(report.ach_reports)),
                    ex=3600,
                )
        except Exception as e:
            logger.debug(f"Failed to persist consensus report: {e}")

    async def _detect_stale_agents(self) -> List[str]:
        """
        Read agent state digests from Redis (§3.3) and detect agents
        with stale/divergent context.

        Each agent publishes its digest to sentinel:agent:digest:{name}
        containing recently processed event IDs and current thresholds.
        If a digest is missing or too old, the agent is stale.
        """
        stale = []
        try:
            cursor = 0
            digests = {}
            while True:
                cursor, keys = await self.redis.raw.scan(
                    cursor=cursor, match="sentinel:agent:digest:*", count=50
                )
                if keys:
                    values = await self.redis.raw.mget(keys)
                    for key, val in zip(keys, values):
                        if val:
                            try:
                                key_str = key if isinstance(key, str) else key.decode("utf-8")
                                agent_name = key_str.split(":")[-1]
                                digest = json.loads(val if isinstance(val, str) else val.decode("utf-8"))
                                digests[agent_name] = digest
                            except Exception:
                                pass
                if cursor == 0:
                    break

            if not digests:
                return []  # No digests published yet, don't penalize

            # Check for staleness: if digest timestamp is > 5 minutes old
            now = datetime.now(timezone.utc)
            for agent_name, digest in digests.items():
                ts_str = digest.get("timestamp")
                if ts_str:
                    try:
                        digest_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        age_seconds = (now - digest_ts).total_seconds()
                        if age_seconds > 300:  # 5 minutes
                            stale.append(agent_name)
                            logger.info(f"🕐 Agent '{agent_name}' digest is {age_seconds:.0f}s stale — downweighting")
                    except (ValueError, TypeError):
                        pass

            # Cross-check: detect context drift by comparing entity and event ID overlap for agents sharing topics
            all_event_sets = {}
            for agent_name, digest in digests.items():
                entities = set(digest.get("recent_entities", []))
                event_ids = set(digest.get("recent_event_ids", []))
                topics = set(digest.get("input_topics", []))
                overlap_set = entities if entities else event_ids
                if overlap_set:
                    all_event_sets[agent_name] = (overlap_set, topics)

            if len(all_event_sets) >= 2:
                agents = list(all_event_sets.keys())
                for i in range(len(agents)):
                    for j in range(i + 1, len(agents)):
                        set_i, topics_i = all_event_sets[agents[i]]
                        set_j, topics_j = all_event_sets[agents[j]]

                        # Only compare drift if agents share input topics or share entity focus space
                        shared_topics = topics_i & topics_j if (topics_i and topics_j) else True
                        if not shared_topics:
                            continue

                        overlap = set_i & set_j
                        union = set_i | set_j
                        jaccard = len(overlap) / max(1, len(union))
                        if jaccard < 0.1 and agents[i] not in stale and agents[j] not in stale:
                            # Low overlap on shared topic stream: these agents have divergent focus
                            logger.info(
                                f"ℹ️ Context drift check: {agents[i]} ↔ {agents[j]} "
                                f"(Jaccard overlap: {jaccard:.2f})"
                            )

        except Exception as e:
            logger.debug(f"State digest check failed: {e}")
        return stale

    @staticmethod
    def _map_direction(direction: str) -> str:
        if direction in ("up", "bullish", "long"):
            return "bullish"
        elif direction in ("down", "bearish", "short"):
            return "bearish"
        return "mixed"

    @staticmethod
    def _direction_to_score(direction: str) -> float:
        if direction in ("up", "bullish", "long"):
            return 1.0
        elif direction in ("down", "bearish", "short"):
            return -1.0
        return 0.0
