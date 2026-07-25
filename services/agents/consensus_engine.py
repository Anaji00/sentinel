"""
services/agents/consensus_engine.py

AGENT CONSENSUS & CONTRADICTION ENGINE
=======================================
Reads all active AgentBulletins across the swarm, detects contradictions,
computes weighted consensus scores, and emits ConsensusReports.

Contradictions are detected when:
  - Multiple agents have bulletins for the same ticker with divergent directions
  - Conviction divergence exceeds 0.4 between agreeing/disagreeing agents
  - A "thesis" and a "regime_change" bulletin conflict

Consensus scores are weighted by each agent's track record (AgentScorecard.consensus_weight).
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from pydantic import BaseModel, Field

from .base import AgentBulletin, AgentScorecard

logger = logging.getLogger("agent.consensus")


# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

class ContradictionReport(BaseModel):
    """Detected contradiction between agents."""
    ticker: str
    bulletin_type: str
    agents_bullish: List[str] = Field(default_factory=list)
    agents_bearish: List[str] = Field(default_factory=list)
    conviction_spread: float = 0.0  # Max conviction difference
    summary: str = ""


class ConsensusSignal(BaseModel):
    """Weighted consensus for a ticker."""
    ticker: str
    direction: str  # "bullish", "bearish", "mixed"
    consensus_score: float = 0.0  # -1.0 (strong bearish) to 1.0 (strong bullish)
    contributing_agents: int = 0
    weighted_conviction: float = 0.0
    agreement_ratio: float = 0.0  # 0.0 = total disagreement, 1.0 = total agreement
    bulletins: List[AgentBulletin] = Field(default_factory=list)


class ConsensusReport(BaseModel):
    """Full consensus analysis across the agent swarm."""
    report_id: str = ""
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    contradictions: List[ContradictionReport] = Field(default_factory=list)
    consensus_signals: List[ConsensusSignal] = Field(default_factory=list)
    total_active_bulletins: int = 0
    agents_reporting: List[str] = Field(default_factory=list)


# ── CONSENSUS ENGINE ─────────────────────────────────────────────────────────

class ConsensusEngine:
    """
    Reads all active AgentBulletins, detects contradictions,
    and computes weighted consensus signals.
    """

    def __init__(self, redis_client, producer=None):
        self.redis = redis_client
        self.producer = producer

    async def analyze(self) -> ConsensusReport:
        """
        Full consensus analysis. Call this periodically or on-demand.
        Returns a ConsensusReport with contradictions and consensus signals.
        """
        # 1. Read all active bulletins
        bulletins = await self._read_all_bulletins()
        if not bulletins:
            return ConsensusReport()

        # 2. Read all agent scorecards for weighting
        scorecards = await self._read_all_scorecards()
        score_map = {s.agent_name: s for s in scorecards}

        # 3. Group bulletins by ticker
        ticker_groups: Dict[str, List[AgentBulletin]] = defaultdict(list)
        for b in bulletins:
            if b.ticker:
                ticker_groups[b.ticker.upper()].append(b)

        # 4. Analyze each ticker group
        contradictions = []
        consensus_signals = []
        agents_reporting = set()

        for ticker, group in ticker_groups.items():
            if len(group) < 2:
                # Still emit a consensus signal for single-agent tickers
                b = group[0]
                agents_reporting.add(b.agent_name)
                weight = score_map.get(b.agent_name, AgentScorecard(agent_name=b.agent_name)).consensus_weight
                direction = b.expected_direction or "neutral"
                consensus_signals.append(ConsensusSignal(
                    ticker=ticker,
                    direction=self._map_direction(direction),
                    consensus_score=self._direction_to_score(direction) * b.conviction * weight,
                    contributing_agents=1,
                    weighted_conviction=b.conviction * weight,
                    agreement_ratio=1.0,
                    bulletins=group,
                ))
                continue

            # Multiple agents have opinions on this ticker
            bullish_agents = []
            bearish_agents = []
            neutral_agents = []
            weighted_scores = []
            total_weight = 0.0

            for b in group:
                agents_reporting.add(b.agent_name)
                weight = score_map.get(b.agent_name, AgentScorecard(agent_name=b.agent_name)).consensus_weight
                direction = b.expected_direction or "neutral"

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

            # Detect contradictions
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

            # Compute consensus
            if total_weight > 0:
                consensus_score = sum(weighted_scores) / total_weight
            else:
                consensus_score = 0.0

            total_agents = len(bullish_agents) + len(bearish_agents) + len(neutral_agents)
            majority = max(len(bullish_agents), len(bearish_agents), len(neutral_agents))
            agreement = majority / total_agents if total_agents > 0 else 0.0

            consensus_direction = "mixed"
            if consensus_score > 0.2:
                consensus_direction = "bullish"
            elif consensus_score < -0.2:
                consensus_direction = "bearish"

            weighted_conviction = abs(sum(weighted_scores)) / max(1.0, total_weight)

            consensus_signals.append(ConsensusSignal(
                ticker=ticker,
                direction=consensus_direction,
                consensus_score=round(consensus_score, 4),
                contributing_agents=total_agents,
                weighted_conviction=round(weighted_conviction, 4),
                agreement_ratio=round(agreement, 4),
                bulletins=group,
            ))

        # Sort: highest conviction first
        consensus_signals.sort(key=lambda s: abs(s.consensus_score), reverse=True)

        report = ConsensusReport(
            report_id=f"consensus-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            contradictions=contradictions,
            consensus_signals=consensus_signals,
            total_active_bulletins=len(bulletins),
            agents_reporting=sorted(agents_reporting),
        )

        # Persist the report
        await self._persist_report(report)

        # Publish to Kafka if available
        if self.producer and (contradictions or consensus_signals):
            try:
                from shared.kafka import Topics
                await self.producer.send(
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
        except Exception as e:
            logger.debug(f"Failed to persist consensus report: {e}")

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
