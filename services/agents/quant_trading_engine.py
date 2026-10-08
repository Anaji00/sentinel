"""
services/agents/quant_trading_engine.py

MASTER QUANT & SWE CONSOLIDATED QUANT TRADING ENGINE
===================================================
Consolidates 3 quantitative agents into a single high-performance engine:
  - QuantResearcherAgent (Peer discovery, catalyst analysis, Granger causality)
  - FinancialAdvisorAgent (TA indicators, Fib levels, VaR/CVaR, Half-Kelly signals)
  - InsiderClusteringAgent (SEC Form 4 C-suite accumulation & insider flow)

Preserves 100% of existing Kafka topics, Redis keys, and output schemas.
"""

import asyncio
import json
from functools import partial
import logging
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError, DEDUP_WINDOW_MEDIUM_SEC
from shared.kafka import Topics
from shared.utils import quant_calc
from shared.utils.form4 import code_value_usd as form4_code_value_usd, dominant_code as form4_dominant_code
from shared.utils.ollama import DEFAULT_MODEL
from shared.utils.tasks import safe_create_task
from shared.utils.focus import prioritise
from shared.utils.regime import current_regime
import numpy as np
from shared.models.events import entity_cache_key, UNRATED_EDGE_CONFIDENCE
from shared.models.events import AlertTier, CorrelationCluster
from shared.models import NormalizedEvent, RawEvent
from shared.models.events import EventType
from shared.utils.equities import is_collected_crypto, is_valid_primary_equity_async
from shared.db import get_neo4j
from shared.utils.feature_flags import FeatureFlagManager
from shared.utils.candles import candle_cache_key, normalize_candle
from shared.utils.market_session import instrument_closed_at
from shared.utils.inference_budget import InferenceShed
from shared.utils.signal_families import observations
from shared.utils.signal_store import FACTOR_REPORT_KEY, FACTOR_SCORES_KEY, factor_scores_key, STUDY_KEY
from shared.utils.bayes import posterior_hit_rate
from shared.utils.evidence_fusion import (
    EVIDENCE_EVENT_TYPES, EVIDENCE_EVENTS_SQL, build_dossiers, family_record,
)
from shared.utils.options_positioning import positioning_key
from shared.utils.trade_plan import (
    deterministic_play, hedging_note, market_regime_label, thesis_warranted,
)
from shared.utils.market_cap import cached_market_cap

logger = logging.getLogger("agent.quant_trading")

# ── POSITION SIZING GUARDRAILS ────────────────────────────────────────────────
# Kelly is a ceiling derived from two estimated quantities, not a target. These
# bounds keep a short lucky streak or a thin backtest from producing a
# concentration that no risk desk would sign off on.
# Which horizon's cross-sectional ranks vote on a play. Plays resolve in a day.
# "primary": the horizon the nightly run found strongest out of sample. On
# 8 October that moved from monthly to weekly once funds were excluded -- the
# monthly edge had come from leveraged ETFs -- so the engine follows the
# report rather than a pinned horizon.
QUANT_MODEL_HORIZON = os.getenv("QUANT_MODEL_HORIZON", "primary")
# A model-backed call is graded over the model's own horizon, not a day.
MODEL_HORIZON_HOURS = {"daily": 24, "weekly": 7 * 24, "monthly": 30 * 24}
# No play from moving averages and unstudied triggers alone: 92 such calls on
# 7 October carried a 46% record. A proven family or a validated or supported
# model must agree. QUANT_REQUIRE_EVIDENCE=0 restores the old behaviour.
QUANT_REQUIRE_EVIDENCE = os.getenv("QUANT_REQUIRE_EVIDENCE", "1").strip().lower() not in ("0", "false", "no")
MIN_KELLY_SAMPLES = 30          # Predictions required before a win rate is trusted
MIN_TRADES_FOR_PAYOFF = 20      # Closed trades required before a payoff ratio is trusted
MAX_SINGLE_POSITION_PCT = 0.10  # Hard cap on any single position, independent of Kelly
UNPROVEN_POSITION_PCT = 0.02    # Cap while the win rate is still below the sample floor
DEFAULT_PAYOFF_RATIO = 1.0      # Assume no edge in payoff until measured
MAX_PAYOFF_RATIO = 3.0          # Clamp outlier backtest payoffs from tiny samples

# The strategy_type values the backtester recognises. It composes its
# strategy_id as f"{strategy_type}_{ticker.lower()}", so anything not spelled
# exactly as it spells it is a key that will never exist.
BACKTEST_STRATEGIES = ("momentum_trend", "covered_call", "mean_reversion")

# What a Granger test is worth in either direction. The penalty is smaller than
# the lift: failing to establish causality on a short hourly series is weaker
# evidence against a relationship than establishing it is evidence for one.
PEER_VERIFIED_LIFT = 0.15
PEER_REFUTED_PENALTY = 0.10

# Bars required before a Sharpe ratio or a drawdown is a measurement rather than
# an artefact of a short series.
MIN_BARS_FOR_QUALITY_METRICS = 10

# How long a cached backtest is treated as current. The backtester writes with a
# seven-day expiry; refreshing daily keeps the payoff ratio responsive to a
# regime change without re-running a 500-bar replay on every advisory.
BACKTEST_REFRESH_SEC = 86400

# Bars per replay during the scheduled refresh. The API route uses 500 for an
# interactive request; this runs unattended alongside the review sweep, so it
# takes the smaller window that still clears MIN_TRADES_FOR_PAYOFF.
BACKTEST_REFRESH_BARS = 300


# ── FORM 4 INSIDER MODELS ─────────────────────────────────────────────────────

class InsiderClusterBrief(BaseModel):
    ticker: str
    insider_sentiment: str  # "Bullish Accumulation", "Bearish Distribution", "Neutral"
    total_net_notional_usd: float
    c_suite_involvement: bool
    summary: str


# ── PEER DISCOVERY & RESEARCH MODELS ──────────────────────────────────────────

# Bars the advisory needs before it will compute anything.
MIN_ADVISORY_BARS = 5

# How much durable history to pull when the cache is short. Enough for the
# indicators the advisory computes without dragging a year of bars per message.
DURABLE_BAR_LIMIT = 200
# Fewer than this many hourly returns and a percentile VaR is a single sample.
MIN_VAR_RETURNS = 20
# An annualized Sharpe from 1h bars scales the mean by ~1,638/yr. From ten
# returns -- the old floor -- one good afternoon read as a Sharpe of 6.
# About eighteen sessions of hourly returns before it is published.
MIN_SHARPE_RETURNS = 120


def _positive_or_none(*candidates):
    """The first candidate that is a positive number, else None.

    Financial quantities are positive by construction: a share count of zero is
    not a trade and a price of zero is not a price. Returning None keeps an
    unstated quantity distinguishable from a stated one, which a 0.0 default
    does not.
    """
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _uuid_or_new(value: Any) -> str:
    """value if it is a UUID, else a fresh one."""
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return str(uuid.uuid4())


class PeerTicker(BaseModel):
    ticker: str
    relation: str
    discovery_confidence: float
    # Whether the statistics were able to support this peer, and how far they
    # got. Defaults to untested so a peer that never reaches the Granger step
    # is never mistaken for one that passed it. The model does not fill this --
    # it is set below from the test's own outcome.
    verification: str = "untested"

    @property
    def rationale(self) -> str:
        """Why this peer was proposed: the graph edge it came from, and its weight."""
        return f"graph edge {self.relation} (weight {self.discovery_confidence:.2f}), {self.verification}"

class MacroInstrument(BaseModel):
    symbol: str
    instrument_type: str  # "treasury", "forex", "commodity", "volatility"
    correlation_reasoning: str

class PeerDiscovery(BaseModel):
    primary_ticker: str
    peer_tickers: List[PeerTicker] = Field(default_factory=list)
    macro_instruments: List[MacroInstrument] = Field(default_factory=list)
    catalyst_category: str
    structural_decoupling: bool


# ── FINANCIAL ADVISORY & RISK MODELS ──────────────────────────────────────────

class BlackLittermanAllocation(BaseModel):
    ticker: str
    target_weight_pct: float
    expected_return_pct: float
    equilibrium_weight_pct: float

class GarchVolatilityCone(BaseModel):
    cond_volatility_pct: float
    tp1_sigma_1_0: float
    tp2_sigma_2_0: float
    tp3_sigma_3_0: float
    sl_sigma_1_5: float

class SmartMoneyConvergence(BaseModel):
    is_aligned: bool = False
    insider_buyer_role: Optional[str] = None
    insider_notional_usd: Optional[float] = None
    option_sweep_premium_usd: Optional[float] = None

class PortfolioMetrics(BaseModel):
    var_95_pct: Optional[float] = None
    cvar_99_pct: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    recommended_cash_pct: Optional[float] = None
    hawkes_risk_factor: Optional[float] = None
    metrics_source: str = "insufficient_history"
    # A VaR figure without its horizon is unreadable — 1-hour and 1-day VaR on
    # the same series differ by ~2.5x. Carried alongside the numbers so the UI
    # can never present them bare.
    risk_horizon: Optional[str] = None
    annualization_basis: Optional[str] = None
    # Whose price series these figures describe. They are the triggering
    # ticker's, not a portfolio's, and the brief is overwritten per ticker.
    subject_ticker: Optional[str] = None

# Upper bound on plays turned into predictions and bulletins from one brief.
#
# Not a quality filter -- highest_conviction_plays is already the model's own
# ranking. This only stops a malformed response producing an unbounded number
# of tracked claims.
MAX_RECORDED_PLAYS = int(os.getenv("QUANT_MAX_RECORDED_PLAYS", "8"))

# Black-Litterman universe and its guards.
#
# The allocation inverts a sample covariance estimated from daily bars. With the
# ~37 daily observations this deployment holds per ticker, twelve assets is
# comfortable and forty is not: a covariance with more assets than observations
# is singular, and `pinv` would return a pseudo-inverse of noise rather than
# refusing. BL_MIN_OBSERVATIONS_PER_ASSET keeps the universe below what the
# history can actually support, shrinking the universe rather than the honesty
# of the estimate.
BL_UNIVERSE_MAX = int(os.getenv("QUANT_BL_UNIVERSE_MAX", "12"))
BL_MIN_OBSERVATIONS_PER_ASSET = 3
BL_MIN_OBSERVATIONS = 20
BL_LOOKBACK_DAYS = int(os.getenv("QUANT_BL_LOOKBACK_DAYS", "120"))

# Trading days, for turning a daily expected return into the annual figure a
# brief is read in.
BL_TRADING_DAYS = 252.0

# View uncertainty, as the variance of the error on a view.
#
# Black-Litterman weights a view against the equilibrium prior by its Omega. A
# conviction of 1.0 must not produce zero uncertainty -- that is an infinitely
# confident view and it would drive the posterior on its own -- so the floor is
# real and the scale is what an unconvinced view costs.
BL_VIEW_UNCERTAINTY_FLOOR = 0.0025
BL_VIEW_UNCERTAINTY_SCALE = 0.02


class TradingSignal(BaseModel):
    ticker: str
    action: str  # "BUY", "SELL", "HOLD"
    trade_type: str = Field(default="Long/Buy", description="Type of trade recommendation e.g. 'Long/Buy', 'Short/Sell', 'Scalp/Buy', 'Swing/Long'")
    entry_level: float
    target_price: float
    stop_loss: float
    risk_reward_ratio: float
    kelly_allocation_pct: float
    # Bounded so the constraint reaches the model: these bounds appear in the
    # JSON schema Ollama now decodes against, which is the only thing that
    # stops a percentage being written into a probability field.
    conviction_score: float = Field(ge=0.0, le=1.0)
    sigma_shock: Optional[float] = None
    expected_move_pct: Optional[float] = None
    order_type: Optional[str] = "Limit"
    slippage_est_bps: Optional[float] = 3.5
    microstructure_stop_multiplier: float = 1.5
    volatility_cone: Optional[GarchVolatilityCone] = None
    smart_money: Optional[SmartMoneyConvergence] = None
    technical_indicators: Dict[str, Any] = Field(default_factory=dict)
    fib_levels: Dict[str, float] = Field(default_factory=dict)
    quantitative_rationale: str
    # "proven" only when the trigger's family passed the reaction study.
    evidence_status: Optional[str] = None


class TradeThesis(BaseModel):
    """The only thing the model writes on an advisory: prose about a play
    whose direction, size and levels were already computed."""
    thesis: str
    key_risks: List[str] = Field(default_factory=list)

class FinancialAdviceBrief(BaseModel):
    market_regime: str
    portfolio_metrics: PortfolioMetrics = Field(default_factory=PortfolioMetrics)
    black_litterman_allocations: List[BlackLittermanAllocation] = Field(default_factory=list)
    highest_conviction_plays: List[TradingSignal] = Field(default_factory=list)
    covered_call_overlays: List[Dict[str, Any]] = Field(default_factory=list)
    general_hedging_strategy: str


from shared.utils.quant_calc import compute_ta_indicators  # noqa: F401  (moved to the shared quant module; re-exported for existing callers)
from shared.utils.watchlists import WATCHED_EQUITIES_KEY
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.rates import risk_free_rate


# ── CONSOLIDATED QUANT TRADING ENGINE ──────────────────────────────────────────

class QuantTradingEngine(SentinelAgent):
    """
    Unified Quant & Trading Engine.
    Combines peer discovery, SEC Form 4 insider flow tracking, quantitative risk modeling
    (VaR/CVaR, Half-Kelly), and technical trading signal generation in a single pass.
    """
    # Which focus-set domains this agent can act on. Its candidates are
    # equity and crypto tickers, so a maritime subject is a slot it can do
    # nothing with.
    FOCUS_DOMAIN = "tradfi"
    FOCUS_DOMAINS = ("tradfi", "crypto", "market", "equity")

    def __init__(
        self,
        agent_name: str = "quant_trading_engine",
        input_topics: Optional[List[str]] = None,
        redis_client=None,
        db_client=None,
        neo4j_client=None,
        producer=None,
        consumer=None,
        dlq=None,
        model: str = DEFAULT_MODEL,
        fallback_model: Optional[str] = None,
    ):
        super().__init__(
            agent_name=agent_name,
            input_topics=input_topics or [Topics.ENRICHED_EVENTS, Topics.CORRELATIONS, Topics.RAW_TRADFI],
            redis_client=redis_client,
            db_client=db_client,
            neo4j_client=neo4j_client,
            producer=producer,
            consumer=consumer,
            dlq=dlq,
            model=model,
            fallback_model=fallback_model,
        )
        self.flags = FeatureFlagManager(self.redis)

    @property
    def output_topic(self) -> str:
        return Topics.FINANCIAL_ADVICE

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = message.get("source", "")
        event_type = message.get("type", "")
        raw = message.get("raw_payload", message)

        # ── 1. SEC FORM 4 INSIDER TRADE HANDLER ────────────────────────────────
        if source == "sec_form4" or "insider" in event_type:
            # Each filing arrives twice: raw from RAW_TRADFI, carrying the parsed
            # document, and enriched from ENRICHED_EVENTS, which keeps none of
            # it. Recording both put every filing in the 14-day window twice,
            # the second time as an unnamed "Corporate Insider" -- only the raw
            # copy can say who traded and how much.
            if not isinstance(message.get("raw_payload"), dict):
                dropped(
                    "agents.quant_trading_engine.form4_enriched_copy",
                    "the enriched copy of a Form 4 already counted from its raw message",
                    logger,
                )
                return None
            return await self._process_insider_form4(message, raw)

        trig = message.get("trigger") or {}
        pe = message.get("primary_entity") or {}
        raw_sec = message.get("security_data") or raw.get("security_data") or {}

        # Extract entity ID array if CorrelationCluster
        ent_ids = message.get("entity_ids") or raw.get("entity_ids") or []
        first_ent_id = ent_ids[0] if isinstance(ent_ids, list) and ent_ids else None

        ticker = str(
            raw.get("ticker") or
            trig.get("ticker") or
            pe.get("id") or
            first_ent_id or
            ""
        ).upper().strip()

        if not ticker or ticker == "UNKNOWN":
            return None

        # ── CVE TO EQUITY MAPPER ───────────────────────────────────────────────
        if ticker.startswith("CVE-"):
            vendor_name = (
                raw.get("vendor") or
                raw.get("vendor_name") or
                raw.get("vendorProject") or
                raw.get("affected_org") or
                trig.get("vendor") or
                trig.get("vendor_name") or
                raw_sec.get("affected_org") or
                (pe.get("name") if pe and pe.get("id") != ticker else None) or
                None
            )
            description = (
                raw.get("description") or
                raw.get("headline") or
                raw.get("summary") or
                message.get("headline") or
                ""
            )
            from shared.utils.cyber_mapper import map_cve_to_equity
            mapped_ticker, _ = map_cve_to_equity(ticker, vendor_name=vendor_name, description=description)
            if mapped_ticker:
                ticker = mapped_ticker

        # ── SUPPORTED ASSET GATE ───────────────────────────────────────────────
        # Equities *or* the crypto majors this deployment collects a perpetual
        # surface for. The gate was equity-only, so every ETH, SOL, DOGE and
        # AAVE event was dropped here -- in an engine that carries
        # _fetch_funding_context() written for those very assets, reading
        # funding rate, basis, mark and index. The data was collected, enriched,
        # stored, and then refused one step before anything could use it.
        if not (
            await is_valid_primary_equity_async(ticker, redis_client=self.redis)
            or is_collected_crypto(ticker)
        ):
            return None

        anomaly_score = float(raw.get("anomaly_score") or trig.get("anomaly_score") or message.get("anomaly_score", 0.5))
        if anomaly_score < 0.60:
            return None

        # Run Peer Discovery & Trading Advisory concurrently via asyncio.gather
        discovery_task = self._process_peer_discovery(message, ticker, anomaly_score)
        advisory_task = self._process_trading_advisory(message, ticker, anomaly_score)

        discovery_res, advisory_res = await asyncio.gather(
            discovery_task, advisory_task, return_exceptions=True
        )

        if isinstance(discovery_res, dict):
            await self._producer.send(Topics.QUANT_DISCOVERIES, discovery_res, key=ticker)

        if isinstance(advisory_res, dict):
            await self._producer.send(Topics.FINANCIAL_ADVICE, advisory_res, key=ticker)

        return advisory_res if isinstance(advisory_res, dict) else (discovery_res if isinstance(discovery_res, dict) else None)

    # ── SUB-ENGINE 1: SEC FORM 4 INSIDER CLUSTERING ───────────────────────────

    # ── SUB-ENGINE 2: QUANT PEER DISCOVERY ───────────────────────────────────

    async def _process_peer_discovery(self, message: Dict[str, Any], ticker: str, anomaly_score: float) -> Optional[Dict[str, Any]]:
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        # Not gated on inference capacity: candidates now come from the graph.
        if not await self.flags.is_enabled("peer_discovery", ticker=ticker):
            return None

        dedup_key = f"quant_discovery:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(dedup_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC):
            return None
        await self.mark_processed(dedup_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC)

        # Candidates come from the graph, not the model.
        #
        # This asked a model for "correlated peers and structural catalysts",
        # and every peer it named at 0.65 or more went onto the watchlist --
        # which spends one of fifty stream subscriptions. The graph already
        # holds what it was guessing at: sector and supply-chain edges, the
        # statistical PEER_OF and GRANGER_CAUSES edges this platform measured,
        # each with a weight. Those are the candidates; the Granger test below
        # is still what can lift or refute them.
        graph_rows = await self._fetch_graph_context(ticker)
        try:
            discovery = await self._graph_peer_discovery(ticker, message, graph_rows)
            if discovery is None:
                return None

            verified_peers = []
            for peer in discovery.peer_tickers:
                # Aligned on the hour both traded. The two series were fetched
                # separately and compared index by index, so a gap in either
                # paired one ticker's 14:00 with the other's 11:00.
                x_prices, y_prices = await self._aligned_hourly_pair(ticker, peer.ticker)
                if len(x_prices) >= 20 and len(y_prices) >= 20:
                    causality = quant_calc.granger_causality(x_prices, y_prices, max_lag=3)
                    if causality.get("x_granger_causes_y"):
                        peer.discovery_confidence = min(1.0, peer.discovery_confidence + PEER_VERIFIED_LIFT)
                        peer.verification = "granger_supported"
                    else:
                        # Tested against enough history and not supported. The
                        # peer is kept -- an untested hunch is not disproof --
                        # but it may no longer clear the 0.65 gate that puts a
                        # ticker on the watchlist and into the graph.
                        peer.discovery_confidence = max(0.0, peer.discovery_confidence - PEER_REFUTED_PENALTY)
                        peer.verification = "granger_not_supported"
                else:
                    peer.verification = "untested_insufficient_history"
                verified_peers.append(peer)
            discovery.peer_tickers = verified_peers

            # Inject top verified primary equity peers into watched equities ZSET.
            # Verified means the Granger test supported it: a watchlist seat is
            # one of fifty stream subscriptions, and an edge weight alone is a
            # claim, not a measurement of this pair's lead-lag.
            for p in discovery.peer_tickers[:4]:
                if (p.verification == "granger_supported" and p.discovery_confidence >= 0.65
                        and await is_valid_primary_equity_async(p.ticker)):
                    await self.redis.raw.zadd(WATCHED_EQUITIES_KEY, mapping={p.ticker: time.time()})

            closes, _, _ = await self._fetch_prices(ticker)
            if len(closes) >= MIN_BARS_FOR_QUALITY_METRICS:
                # Annualized on the frequency the series is actually sampled at.
                # This called sharpe_ratio(returns) bare, taking the default
                # trading_days=252 -- the daily equity convention -- on a series
                # of PRICE_TIMEFRAME ("1h") bars. That understates the figure by
                # sqrt(1638/252) = 2.55x for equities and sqrt(8760/252) = 5.9x
                # for crypto, and the result is published as
                # quality_metrics.sharpe_ratio. Twenty lines further down the
                # same engine derives the factor correctly for the same series.
                returns = quant_calc.simple_returns(closes)
                bars_per_year = quant_calc.periods_per_year(
                    self.PRICE_TIMEFRAME, quant_calc.classify_asset_class(ticker)
                )
                # Against the rate the platform collects, not against zero.
                #
                # The comment above records fixing the *other* default in this
                # same call -- trading_days -- and walked past this one. A
                # Sharpe computed with risk_free_rate=0.0 overstates every
                # strategy by the whole of the short rate, in one direction.
                rf, rf_live = await risk_free_rate(self.redis)
                sr = quant_calc.sharpe_ratio(
                    returns, risk_free_rate=rf, annualize=True, trading_days=bars_per_year
                )
                mdd, _, _ = quant_calc.max_drawdown(closes)
            else:
                # Not measured, and said so.
                #
                # This published 0.0 for both, which reads as "a Sharpe ratio of
                # zero was computed" -- a real and damning finding about a
                # strategy. The truth is that the candle cache did not hold ten
                # bars for this name, which is a fact about the cache. Every
                # discovery in the deployment carried sharpe_ratio 0.0 and
                # max_drawdown 0.0 for exactly this reason.
                sr, mdd = None, None

            discovery_dict = discovery.model_dump()
            res_payload = {
                "agent": self.name,
                "agent_run_id": f"quant_{ticker}_{int(time.time())}",
                "trigger": {"ticker": ticker, "anomaly_score": anomaly_score},
                "discovery": discovery_dict,
                "quality_metrics": {
                    "sharpe_ratio": sr,
                    "max_drawdown": mdd,
                    # So a consumer can tell an unmeasured metric from a
                    # measured one without inferring it from a null.
                    "measured": sr is not None,
                    "bars_available": len(closes),
                    "bars_required": MIN_BARS_FOR_QUALITY_METRICS,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            # Register discovered high-confidence peers in ontology/graph pipelines and emit CorrelationCluster
            high_conf_peers = [p for p in discovery.peer_tickers if p.discovery_confidence >= 0.65]
            for peer in high_conf_peers:
                await self._producer.send(
                    Topics.ONTOLOGY_PROPOSALS,
                    {
                        "source_entity": ticker,
                        "target_entity": peer.ticker,
                        "relationship": "CORRELATED_PEER",
                        "confidence": peer.discovery_confidence,
                        "rationale": peer.rationale,
                    },
                    key=peer.ticker,
                )

            if high_conf_peers:
                peer_ids = [p.ticker for p in high_conf_peers]
                corr_cluster = CorrelationCluster(
                    rule_id="QUANT_PEER_DECOUPLING",
                    rule_name="Quantitative Equity Peer Cointegration & Decoupling",
                    alert_tier=AlertTier.INTELLIGENCE,
                    primary_domain="financial",
                    confidence_score=float(high_conf_peers[0].discovery_confidence),
                    summary_headline=f"Peer Decoupling Detected: {ticker} vs {', '.join(peer_ids[:3])}",
                    supporting_headlines=[f"{p.ticker}: {p.rationale}" for p in high_conf_peers[:3]],
                    metrics_summary={
                        "catalyst_category": discovery.catalyst_category,
                        "sharpe_ratio": sr,
                        "max_drawdown": mdd,
                        "peer_count": len(peer_ids),
                    },
                    # The event that triggered discovery, when it carries a
                    # UUID; correlations and scenarios store these as uuid, and
                    # "quant_{ticker}_{ts}" failed every insert downstream.
                    trigger_event_id=_uuid_or_new(message.get("event_id")),
                    supporting_event_ids=[],
                    entity_ids=[ticker] + peer_ids,
                    entity_names=[ticker] + peer_ids,
                    description=f"QuantTradingEngine discovered cointegrated peer correlation for {ticker} with {', '.join(peer_ids)}. Catalyst: {discovery.catalyst_category}",
                    tags=["quant_peer_discovery", f"ticker:{ticker}"] + [f"peer:{p}" for p in peer_ids],
                )
                await self._producer.send(Topics.CORRELATIONS, corr_cluster.model_dump(), key=ticker)

            # Publish structured AgentBulletin
            peer_names = [p.ticker for p in discovery.peer_tickers[:4]]
            safe_create_task(
                self.publish_bulletin(
                    bulletin_type="thesis",
                    summary=f"Quant Discovery {ticker}: {discovery.catalyst_category}. Peers: {peer_names}",
                    ticker=ticker,
                    conviction=min(1.0, anomaly_score * 0.9),
                    expected_direction="uncertain",
                    payload={"peers": peer_names, "catalyst": discovery.catalyst_category},
                    ttl_seconds=3600,
                ),
                name=f"quant-discovery-bulletin-{ticker}"
            )

            return res_payload

        except (SchemaViolationError, InferenceError, Exception) as e:
            logger.error(f"Quant peer discovery failed for {ticker}: {e}")
            return None

    # ── SUB-ENGINE 3: FINANCIAL ADVISORY & RISK SIGNALS ─────────────────────

    # The producer's own range, so this consumer cannot reject what the
    # measurement is capable of producing: microstructure_stop_distance()
    # returns round(max(0.50, min(2.50, mult))), built from a base_multiplier
    # whose default is 1.5 -- the same constant this advisory had hardcoded.
    # The measured value is therefore a liquidity-adjusted version of exactly
    # the number it replaces, not a different quantity.
    #
    # Note the enricher only persists a multiplier when it comes out below 1.0,
    # so an absent key means "no tightening warranted" and the default is the
    # correct answer rather than a fallback.
    MIN_STOP_MULTIPLIER = 0.50
    MAX_STOP_MULTIPLIER = 2.50
    DEFAULT_STOP_MULTIPLIER = 1.5

    async def _measured_stop_multiplier(self, ticker: str) -> float:
        """The liquidity-adjusted ATR multiplier for this ticker, or the default.

        Reads what the tradfi enricher measured from order-flow imbalance and
        Kyle's lambda. Returns DEFAULT_STOP_MULTIPLIER when nothing has been
        measured, which is the constant this replaced, so an unmeasured ticker
        behaves exactly as before.
        """
        symbol = str(ticker or "").upper().strip()
        if not symbol or not self.redis:
            return self.DEFAULT_STOP_MULTIPLIER
        try:
            raw = await self.redis.raw.get(f"sentinel:stop_loss:{symbol}")
            if not raw:
                return self.DEFAULT_STOP_MULTIPLIER
            data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            multiplier = float(data.get("multiplier"))
        except (TypeError, ValueError, AttributeError, json.JSONDecodeError) as e:
            self.logger.debug("Stop multiplier unreadable for %s: %s", symbol, e)
            return self.DEFAULT_STOP_MULTIPLIER
        except Exception as e:
            self.logger.debug("Stop multiplier lookup failed for %s: %s", symbol, e)
            return self.DEFAULT_STOP_MULTIPLIER
        if multiplier != multiplier or multiplier <= 0:      # NaN or nonsense
            return self.DEFAULT_STOP_MULTIPLIER
        return min(self.MAX_STOP_MULTIPLIER, max(self.MIN_STOP_MULTIPLIER, multiplier))

    async def _process_trading_advisory(self, message: Dict[str, Any], ticker: str, anomaly_score: float) -> Optional[Dict[str, Any]]:
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        # One advisory per ticker per half hour. The model used to be the rate
        # limit -- about four advisories an hour -- and the plays are now
        # computed, so without this every event above 0.60 would record a
        # prediction and publish a bulletin.
        advisory_key = f"quant_advisory:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(advisory_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC):
            return None
        await self.mark_processed(advisory_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC)
        closes, highs, lows = await self._fetch_prices(ticker)
        if len(closes) < MIN_ADVISORY_BARS:
            return None

        # Calculate TA indicators
        indicators = self._compute_ta(closes, highs, lows)
        current_price = closes[-1]
        atr = indicators.get("atr", current_price * 0.02)

        # Risk calculations: EWMA Volatility, VaR, CVaR, Empirical Half-Kelly.
        # The series is sampled at PRICE_TIMEFRAME, so annualization and the VaR
        # horizon are both derived from that frequency and the asset's calendar —
        # never assumed daily.
        returns = quant_calc.simple_returns(closes) if len(closes) > 1 else [0.0]
        has_history = len(returns) >= 10

        asset_class = quant_calc.classify_asset_class(ticker)
        bars_per_year = quant_calc.periods_per_year(self.PRICE_TIMEFRAME, asset_class)
        daily_periods = quant_calc.periods_per_year("1d", asset_class)

        ewma_vol = quant_calc.ewma_volatility(returns, annualize=True, trading_days=bars_per_year)
        var_95 = quant_calc.var_historical(returns, confidence=0.95, position_value=10_000)
        cvar_95 = quant_calc.cvar_historical(returns, confidence=0.95, position_value=10_000)
        # cvar_99 at a notional was computed here and never read; the figure
        # this advisory publishes is cvar_99_pct, derived below from the
        # per-unit form. Removed rather than left as a line that looks like an
        # output.

        # Express VaR/CVaR at a 1-day horizon — the convention a reader assumes
        # when no horizon is stated — rather than the raw per-bar figure.
        var_95_bar = quant_calc.var_historical(returns, confidence=0.95, position_value=1.0)
        cvar_99_bar = quant_calc.cvar_historical(returns, confidence=0.99, position_value=1.0)
        var_95_pct = round(
            quant_calc.scale_var_to_horizon(var_95_bar, bars_per_year, daily_periods) * 100.0, 2
        )
        cvar_99_pct = round(
            quant_calc.scale_var_to_horizon(cvar_99_bar, bars_per_year, daily_periods) * 100.0, 2
        )
        risk_horizon = "1-day"
        risk_free, risk_free_is_live = await risk_free_rate(self.redis)
        sharpe_ratio_val = (
            round(float(quant_calc.sharpe_ratio(
                returns, risk_free_rate=risk_free, annualize=True, trading_days=bars_per_year
            )), 2)
            if len(returns) >= MIN_SHARPE_RETURNS else None
        )

        # ── EMPIRICAL KELLY INPUTS ──────────────────────────────────────────────
        # Kelly is acutely sensitive to both the win rate and the payoff ratio.
        # An estimate from a handful of predictions against an *assumed* payoff
        # produces concentrations no risk desk would approve, so both terms are
        # sourced empirically and the output is capped independently.
        # Conditioned on the prevailing regime where enough history exists. A
        # hit rate earned under bull steepening is not evidence about the same
        # strategy under inversion, but Kelly treats whatever it is given as the
        # true win probability -- so pooling regimes biases every position size.
        # Falls back to the strategy-wide, then global, card when a partition is
        # too thin to estimate from.
        # Scoped to the same bet the payoff ratio describes.
        #
        # The win rate came from the strategy-wide "trading_advisory" scorecard
        # while win_loss_ratio is fetched per ticker just below. Kelly assumes
        # both parameters describe one bet distribution, so mixing a
        # strategy-wide p with a per-ticker b misprices every position whose
        # payoff profile differs from the strategy average -- which is the point
        # of measuring it per ticker in the first place. The per-ticker card is
        # preferred and the strategy card is the documented fallback when that
        # partition is too thin, which is the same precedence the payoff ratio
        # already uses.
        card, card_source = await self.get_conditional_scorecard(
            strategy=f"trading_advisory:{ticker.upper()}", min_samples=MIN_KELLY_SAMPLES
        )
        strategy_card, _ = await self.get_conditional_scorecard(
            strategy="trading_advisory", min_samples=MIN_KELLY_SAMPLES
        )
        # The ticker's record shrunk toward the strategy's (shared.utils.bayes):
        # a prior worth twenty trades, centred on the strategy-wide hit rate when
        # it is measured and on a coin flip when it is not. Five lucky wins move
        # it a little; a long record moves it to the record. Replaces a raw rate
        # above a sample floor and a flat 0.55 below it.
        prior_mean = (
            strategy_card.predictions_correct / strategy_card.predictions_made
            if strategy_card.predictions_made >= MIN_KELLY_SAMPLES else 0.5
        )
        if card.predictions_made < MIN_KELLY_SAMPLES:
            card, card_source = strategy_card, "trading_advisory"
        win_prob = min(0.85, max(0.35, posterior_hit_rate(
            card.predictions_correct, card.predictions_made, prior_mean=prior_mean,
        )))

        # Realized payoff ratio from the backtester, not the 2:1 aspiration.
        win_loss_ratio = await self._fetch_realized_payoff_ratio(ticker)

        kelly_pct = quant_calc.kelly_criterion(win_prob, win_loss_ratio, half_kelly=True)

        # Hard ceiling on single-position exposure, independent of Kelly. Applied
        # before the stress check so the circuit breaker can only reduce it.
        kelly_pct = min(kelly_pct, MAX_SINGLE_POSITION_PCT)
        if card.predictions_made < MIN_KELLY_SAMPLES:
            kelly_pct = min(kelly_pct, UNPROVEN_POSITION_PCT)

        # Macro risk sizing check & circuit breaker
        # The shared regime, not a substring search of a JSON blob.
        #
        # This read the raw cached brief -- a JSON document -- into a variable
        # named `rates_regime` and asked whether the string "inverted" appeared
        # anywhere in it. That matches `curve_state`, the field this codebase
        # records as declared-as-four-values and live holding a rendered
        # sentence, and it would equally match the word appearing in
        # `regime_summary` prose. It also interpolated the whole document into
        # the prompt below as "Macro Regime: {...}".
        #
        # `current_regime` derives the label from `yield_spread_2y10y_bps`,
        # which is a measurement. Four components deriving this separately and
        # disagreeing is the reason that function exists.
        rates_regime = await current_regime(self.redis)
        is_stress = rates_regime in ("inverted", "flat")

        if is_stress:
            kelly_pct = min(0.05, kelly_pct * 0.5)

        indicators_data = {
            ticker: {
                "current_price": current_price,
                "rsi": indicators["rsi"],
                "ema_12": indicators["ema_12"],
                "ema_26": indicators["ema_26"],
                "atr": atr,
                "sma_20": indicators.get("sma_20"),
                "dist_sma_20_pct": indicators.get("dist_sma_20_pct"),
                "sma_50": indicators.get("sma_50"),
                "dist_sma_50_pct": indicators.get("dist_sma_50_pct"),
                "sma_200": indicators.get("sma_200"),
                "dist_sma_200_pct": indicators.get("dist_sma_200_pct"),
                "ma_alignment": indicators.get("ma_alignment", "NEUTRAL"),
                "ewma_volatility_annualized": ewma_vol,
                "var_95_per_10k": var_95,
                "cvar_95_per_10k": cvar_95,
                "half_kelly_allocation_pct": round(kelly_pct * 100.0, 2),
                # Which performance history the win rate came from, so a reader
                # can tell a regime-conditioned estimate from a pooled one.
                "kelly_basis": card_source,
                "kelly_sample_count": card.predictions_made,
                "fib_levels": indicators["fib_levels"],
            }
        }

        # ── 1-HOP GRAPH CONTEXT PRE-CHECKS (§7.3) ──
        graph_correlations = []
        graph_context = {
            "sector": None,
            "industry": None,
            "index_membership": [],
            "supply_chain": [],
            "competitors": [],
            # Peers this platform derived rather than a vendor published, kept
            # apart from `competitors` on purpose: PEER_OF comes from the
            # statistical discovery pass, and its strongest edges are BZ=F->CL=F
            # and GC=F->SI=F -- the commodity series that stopped updating on
            # 2026-09-04. Merging the two would hand a reader one list and no way
            # to tell which half to trust.
            "statistical_peers": [],
            # Companies sharing this one's sector, and how many there are.
            #
            # The cohort size is carried because "one of 37 semiconductor names"
            # and "one of 3" are different statements about how much a peer
            # comparison is worth, and a bare list of eight cannot tell them
            # apart.
            "sector_peers": [],
            "sector_cohort_size": 0,
            "correlated_entities": [],
            # Why each correlated entity is here. An entity with no predicate is
            # a name; with one it is a claim that can be checked.
            "relationship_provenance": {},
        }
        try:
            neo4j_client = self.neo4j or await get_neo4j()
            if neo4j_client:
                # 1. 1-hop structural relationships (Sector, Index, Supply Chain, Competitors)
                struct_query = """
                MATCH (inst) WHERE toUpper(inst.name) = $ticker OR toUpper(inst.id) = $ticker
                OPTIONAL MATCH (inst)-[:OPERATES_IN]->(s:Sector)
                OPTIONAL MATCH (inst)-[:MEMBER_OF]->(idx)
                OPTIONAL MATCH (inst)-[:SUPPLIER_TO|CUSTOMER_OF|SUPPLIES|PURCHASES_FROM]-(sc)
                // The label constraints on these two are dropped, and the
                // fourth supply predicate added, for the reason 585 records:
                // a node's label is decided by whichever producer wrote it
                // first, so pinning the far end to one label reads a slice of
                // the graph rather than the graph.
                //
                // `(sc:Entity)` was the sharper of the two. The supply-chain
                // writer emits Company-to-Company edges -- because a supplier
                // *is* a company -- so with that constraint in place the
                // `supply_chain` field would have stayed empty no matter how
                // many edges were written, and the symptom would have been
                // identical to having written none.
                //
                // `(idx:Index)` had the same shape: the Index label held zero
                // nodes until this session, so anything MEMBER_OF ever reached
                // was labelled something else.
                OPTIONAL MATCH (inst)-[:COMPETES_WITH]-(comp)
                OPTIONAL MATCH (inst)-[:PEER_OF]-(peer)
                RETURN coalesce(s.name, s.id) AS sector,
                       collect(DISTINCT coalesce(idx.name, idx.id)) AS indices,
                       collect(DISTINCT coalesce(sc.name, sc.id)) AS supply_chain,
                       collect(DISTINCT coalesce(comp.name, comp.id)) AS competitors,
                       collect(DISTINCT coalesce(peer.name, peer.id)) AS statistical_peers
                """
                struct_rows = await neo4j_client.query(struct_query, {"ticker": ticker.upper()})

                # The sector cohort: one hop past the sector this already reads.
                #
                # Kept as its own query rather than another OPTIONAL MATCH in
                # the one above, because a second traversal inside the same
                # pattern multiplies its row count against every other
                # collect() in it.
                sector_query = """
                MATCH (inst)-[:OPERATES_IN]->(s:Sector)<-[:OPERATES_IN]-(sib)
                WHERE (toUpper(inst.name) = $ticker OR toUpper(inst.id) = $ticker)
                  AND sib <> inst
                WITH s, sib, size([(sib)--() | 1]) AS degree
                ORDER BY degree DESC
                RETURN coalesce(s.name, s.id) AS sector,
                       count(sib) AS cohort,
                       collect(coalesce(sib.name, sib.id))[0..8] AS peers
                """
                try:
                    sector_rows = await neo4j_client.query(
                        sector_query, {"ticker": ticker.upper()}
                    )
                    if sector_rows and sector_rows[0]:
                        row = sector_rows[0]
                        graph_context["sector_peers"] = [p for p in (row.get("peers") or []) if p]
                        graph_context["sector_cohort_size"] = int(row.get("cohort") or 0)
                        # The sector name comes free with the cohort, and this
                        # path reaches tickers the OPTIONAL MATCH above misses.
                        if not graph_context["sector"] and row.get("sector"):
                            graph_context["sector"] = row["sector"]
                except Exception as exc:
                    logger.debug("Sector cohort lookup for %s bypass: %s", ticker, exc)
                if struct_rows and struct_rows[0]:
                    sr = struct_rows[0]
                    if sr.get("sector"):
                        graph_context["sector"] = sr["sector"]
                    if sr.get("indices"):
                        graph_context["index_membership"] = [x for x in sr["indices"] if x]
                    if sr.get("supply_chain"):
                        graph_context["supply_chain"] = [x for x in sr["supply_chain"] if x]
                    if sr.get("competitors"):
                        graph_context["competitors"] = [x for x in sr["competitors"] if x]
                    if sr.get("statistical_peers"):
                        graph_context["statistical_peers"] = [
                            x for x in sr["statistical_peers"] if x
                        ]

                # 2. Exposure & statistical correlations
                graph_query = """
                # SYMPATHY_MOVER is added and two others were not.
                #
                # Sampled before including them: SYMPATHY_MOVER links real
                # tickers -- IEFA to MSFT, SI=F to NVDA -- and is 177 edges of
                # measured co-movement, the largest unread source there was.
                #
                # HAS_EXPOSURE_IN (139) and MACRO_CORRELATED (10) point at
                # `market_anomaly`, `finnhub_equities`, `Global Sentiment
                # Analysis` and `volatile_5m_candle`: event types, a source
                # name, and prose fragments. Reading them would have traded an
                # empty field for one that names a collector as a correlated
                # company.
                MATCH (e)-[r:COMMODITY_EXPOSURE|SUPPLIES|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO|STATISTICALLY_CORRELATED_WITH|GRANGER_CAUSES|SYMPATHY_MOVER]-(inst)
                WHERE toUpper(inst.name) = $ticker OR toUpper(inst.id) = $ticker
                RETURN DISTINCT coalesce(e.name, e.id) AS correlated_entity,
                       type(r) AS predicate,
                       coalesce(r.confidence, $unrated) AS confidence,
                       r.weight AS weight
                ORDER BY coalesce(r.weight, r.confidence, $unrated) DESC
                LIMIT 12
                """
                rows = await neo4j_client.query(graph_query, {"ticker": ticker.upper(), "unrated": UNRATED_EDGE_CONFIDENCE})
                if rows:
                    graph_correlations = [
                        f"{r['correlated_entity']} -[{r['predicate']}]-> {ticker} (confidence: {r['confidence']:.2f})"
                        for r in rows if r.get("correlated_entity")
                    ]
                    graph_context["correlated_entities"] = [r['correlated_entity'] for r in rows if r.get("correlated_entity")]
                    # Which predicate put each name here, so a reader can weigh
                    # a measured co-movement against a vendor's assertion.
                    graph_context["relationship_provenance"] = {
                        r["correlated_entity"]: r["predicate"]
                        for r in rows if r.get("correlated_entity") and r.get("predicate")
                    }
        except Exception as e:
            logger.debug(f"Graph context lookup for {ticker} bypass: {e}")

        # Ground prompt with 1-hop topology
        graph_topo_lines = []
        if graph_context["sector"]:
            graph_topo_lines.append(f"Sector: {graph_context['sector']}")
        if graph_context["index_membership"]:
            graph_topo_lines.append(f"Indices: {', '.join(graph_context['index_membership'])}")
        if graph_context["supply_chain"]:
            graph_topo_lines.append(f"Supply Chain Linkages: {', '.join(graph_context['supply_chain'])}")
        if graph_context["competitors"]:
            graph_topo_lines.append(f"Key Competitors: {', '.join(graph_context['competitors'])}")
        # The three lines below close a gap this block had every time it ran:
        # the fields existed on graph_context and reached the stored record at
        # the end of the method, but nothing rendered them, so the model never
        # saw them. A field computed and not read is the same as a field that
        # was never computed.
        if graph_context["statistical_peers"]:
            # Named as derived, not asserted. These come from this platform's
            # own discovery pass, and its strongest PEER_OF edges are BZ=F->CL=F
            # and GC=F->SI=F -- two names for one series, and a commodity feed
            # that stopped on 2026-09-04. A reader told where a claim came from
            # can discount it; a reader handed one merged list cannot.
            graph_topo_lines.append(
                f"Statistically Derived Peers: {', '.join(graph_context['statistical_peers'])}"
            )
        if graph_context["sector_peers"]:
            # The cohort size travels with the list because "one of 37
            # semiconductor names" and "one of 3" are different statements about
            # what a peer comparison is worth, and eight names cannot say which.
            graph_topo_lines.append(
                f"Sector Cohort ({graph_context['sector_cohort_size']} companies): "
                f"{', '.join(graph_context['sector_peers'])}"
            )

        graph_topo_block = ("\n        GRAPH TOPOLOGY & EXPOSURES:\n        - " + "\n        - ".join(graph_topo_lines) + "\n") if graph_topo_lines else ""
        graph_block = f"\n        VALIDATED GRAPH CORRELATIONS:\n        - " + "\n        - ".join(graph_correlations) + "\n" if graph_correlations else ""

        cross_context = await self.get_cross_agent_context(ticker=ticker, limit=3)
        cross_block = f"\n        CROSS-AGENT INTELLIGENCE:\n        {cross_context}\n" if cross_context else ""

        # Earnings & derivatives context injection (Phase 4)
        earnings_ctx = await self._fetch_earnings_context(ticker)
        funding_ctx = await self._fetch_funding_context(ticker)
        earnings_block = f"\n        EARNINGS CONTEXT: {earnings_ctx}\n" if earnings_ctx else ""
        funding_block = f"\n        DERIVATIVES CONTEXT: {funding_ctx}\n" if funding_ctx else ""

        user_prompt = f"""=== FINANCIAL ADVISORY & RISK EVALUATION ===
Target Instrument: {ticker} | Macro Regime: {rates_regime}
Risk Indicators: {json.dumps(indicators_data, separators=(',', ':'), default=str)}
{graph_topo_block}{graph_block}{cross_block}{earnings_block}{funding_block}
MEASURED SIZING INPUTS:
- Empirical Win Probability (W): {win_prob:.1%} | Payoff Ratio (R): {win_loss_ratio:.1f}
- Half-Kelly Allocation Cap: {kelly_pct * 100:.1f}%"""

        # An advisory without a price is not an advisory, and this is decided
        # before the inference rather than after it.
        #
        # `entry_level` was assigned unconditionally from `current_price` with
        # no guard testing it, so a missing price published
        # "BUY EQIX @ $0.00 -> $0.00 (Kelly 2.0%)" and recorded a prediction the
        # resolver rejects as unscoreable. That was fixed -- but the check sat
        # *after* `_execute_with_telemetry`, which is the model call. Measured
        # live: a 202.6-second inference for SNDK completed and was then
        # discarded for a condition known before it began, and that was one of
        # only four inferences the agent tier completed in roughly fifty
        # minutes of runtime. On a host that affords this few, spending one to
        # learn something already in a local variable is the expensive mistake.
        if not isinstance(current_price, (int, float)) or not math.isfinite(current_price) or current_price <= 0:
            logger.warning(
                "Advisory for %s abandoned before inference: no usable current "
                "price (%r). A play priced at zero cannot be executed or "
                "resolved, and the model call is the scarcest thing here.",
                ticker, current_price,
            )
            return None

        try:
            # The trade is computed (shared.utils.trade_plan); the model, when it
            # is asked at all, writes the thesis for a play that already exists.
            trigger_obs = [
                o for o in observations({
                    **message,
                    "created_at": message.get("created_at") or datetime.now(timezone.utc),
                })
                if o.instrument.upper() == ticker.upper()
            ]
            trigger_families = {o.family for o in trigger_obs}
            proven_families = await self._proven_signal_families()
            # A family measured and found empty does not vote on direction.
            noise = await self._noise_signal_families()
            voting_obs = [o for o in trigger_obs if o.family not in noise]
            raw_watch = await self.redis.raw.zrange(WATCHED_EQUITIES_KEY, 0, -1)
            watched_now = {w.decode() if isinstance(w, bytes) else str(w) for w in (raw_watch or [])}

            model_entry = await self._model_rank(ticker)
            play_fields = deterministic_play(
                ticker.upper(),
                indicators_data[ticker],
                [o.direction for o in voting_obs],
                trigger_families,
                proven_families,
                kelly_pct * 100.0,
                model=model_entry,
                require_evidence=QUANT_REQUIRE_EVIDENCE,
            )
            brief = FinancialAdviceBrief(
                market_regime=market_regime_label(rates_regime),
                highest_conviction_plays=[TradingSignal(**play_fields)] if play_fields else [],
                general_hedging_strategy=hedging_note(is_stress),
            )

            if play_fields and thesis_warranted(ticker, anomaly_score, watched_now, trigger_families):
                thesis = await self._write_thesis(message, play_fields, user_prompt)
                if thesis is not None:
                    play = brief.highest_conviction_plays[0]
                    risks = "; ".join(r for r in thesis.key_risks[:3] if r)
                    play.quantitative_rationale = (
                        f"{play.quantitative_rationale} Thesis: {thesis.thesis.strip()}"
                        + (f" Risks: {risks}." if risks else "")
                    )

            # Post-hoc deterministic trade signal construction and risk limits enforcement
            max_kelly_pct = round(kelly_pct * 100.0, 2)

            # The stop multiplier the enrichment layer already measured for this
            # ticker, in place of a flat 1.5.
            #
            # `sentinel:stop_loss:{ticker}` carries a multiplier derived from
            # order-flow imbalance and Kyle's lambda -- how far price moves per
            # unit of volume, which is precisely what should widen a stop in a
            # thin book and tighten it in a deep one. It was written on every
            # tradfi enrichment and read by nothing, while this line used a
            # constant 1.5 for every name regardless of liquidity. Falling back
            # to 1.5 keeps the previous behaviour whenever no measurement
            # exists for the ticker.
            stop_multiplier = await self._measured_stop_multiplier(ticker)

            for play in brief.highest_conviction_plays:
                play.entry_level = round(current_price, 2)
                stop_distance = atr * stop_multiplier
                
                # Conviction-tiered Risk-Reward ratio selection
                if play.conviction_score < 0.6:
                    rr = 1.5
                elif play.conviction_score < 0.8:
                    rr = 2.0
                else:
                    rr = 3.0
                play.risk_reward_ratio = rr

                if play.action == "BUY":
                    play.stop_loss = round(play.entry_level - stop_distance, 2)
                    play.target_price = round(play.entry_level + (stop_distance * rr), 2)
                elif play.action == "SELL":
                    play.stop_loss = round(play.entry_level + stop_distance, 2)
                    play.target_price = round(play.entry_level - (stop_distance * rr), 2)
                else:
                    play.stop_loss = round(play.entry_level - stop_distance, 2)
                    play.target_price = play.entry_level

                # Hard clamp Kelly allocation to server-calculated half-Kelly limit
                play.kelly_allocation_pct = round(min(max(0.0, float(play.kelly_allocation_pct)), max_kelly_pct), 2)

            # Evaluate closed-form Covered Call recommendation if CAGG Z-score >= +2.5 (§2.3, §2.6)
            z_score = 0.0
            try:
                from shared.db import get_timescale
                db = self.db or await get_timescale()
                if db:
                    row = await db.query_one(
                        """
                        SELECT z_score FROM tradfi_bars_5m_zscore 
                        WHERE ticker = $1 
                        ORDER BY bucket_time DESC 
                        LIMIT 1;
                        """,
                        ticker.upper()
                    )
                    if row and row.get("z_score") is not None:
                        z_score = float(row["z_score"])
            except Exception as z_err:
                logger.debug(f"CAGG Z-score view lookup for {ticker} fallback: {z_err}")
                # Fallback to in-memory preceding window during cold start or offline db
                if len(returns) >= 20:
                    mean_ret = float(np.mean(returns[-21:-1])) if len(returns) > 20 else float(np.mean(returns[:-1]))
                    std_ret = float(np.std(returns[-21:-1])) if len(returns) > 20 else float(np.std(returns[:-1]))
                    if std_ret > 1e-6:
                        z_score = (returns[-1] - mean_ret) / std_ret

            live_iv = None
            raw_iv = await self.redis.raw.get(f"sentinel:options:iv:{ticker}")
            if raw_iv:
                try:
                    live_iv = float(raw_iv)
                except Exception as _exc:
                    swallowed("agents.quant_trading_engine._process_trading_advisory", _exc, logger)

            # sentinel:watched:equities, not :watchlist:.
            #
            # The same file writes sentinel:watched:equities and reads it
            # correctly elsewhere; this one site had the wrong name, so the
            # zrange always came back empty. watched_set then resolved to None,
            # and generate_covered_call_recommendation skips its scoping check
            # entirely when that argument is None -- so the overlay was
            # evaluated for every ticker rather than the 44 on the watchlist.
            # It failed open, which is why nothing ever looked wrong.
            raw_watchlist = await self.redis.raw.zrange(WATCHED_EQUITIES_KEY, 0, -1)
            watched_set = {s.decode() if isinstance(s, bytes) else str(s) for s in raw_watchlist} if raw_watchlist else None

            # Evaluate closed-form Covered Call recommendation if flag enabled and CAGG Z-score >= +2.5 (§2.3, §2.6, §B.3)
            if await self.flags.is_enabled("covered_calls", ticker=ticker):
                cc_rec = quant_calc.generate_covered_call_recommendation(
                    ticker=ticker,
                    current_price=current_price,
                    z_score=z_score,
                    target_delta=0.30,
                    dte_days=30,
                    live_iv=live_iv,
                    realized_volatility=ewma_vol,
                    watched_equities=watched_set
                )
                if cc_rec:
                    brief.covered_call_overlays.append(cc_rec)

            # Gate Black-Litterman allocations on feature flag (§B.3)
            #
            # The gate is checked first so a kill switch costs nothing: it used
            # to be the only line that touched this field, clearing a list that
            # nothing had ever filled.
            if not await self.flags.is_enabled("black_litterman", ticker=ticker):
                brief.black_litterman_allocations = []
            else:
                brief.black_litterman_allocations = await self._black_litterman_allocations(
                    brief, watched_set
                )

            # Force-write PortfolioMetrics from real computed values (§1.6, §1.7, §1.8)
            if not brief.portfolio_metrics:
                brief.portfolio_metrics = PortfolioMetrics()

            if has_history and len(returns) >= MIN_VAR_RETURNS:
                brief.portfolio_metrics.subject_ticker = ticker.upper()
                brief.portfolio_metrics.var_95_pct = var_95_pct
                brief.portfolio_metrics.cvar_99_pct = cvar_99_pct
                brief.portfolio_metrics.sharpe_ratio = sharpe_ratio_val
                hawkes_factor = quant_calc.hawkes_risk_multiplier(hawkes_intensity=1.0)
                brief.portfolio_metrics.hawkes_risk_factor = round(hawkes_factor, 3)
                brief.portfolio_metrics.metrics_source = "computed"
                brief.portfolio_metrics.risk_horizon = risk_horizon
                brief.portfolio_metrics.annualization_basis = (
                    f"{self.PRICE_TIMEFRAME} bars, {asset_class} calendar "
                    f"({bars_per_year:.0f}/yr)"
                )
            else:
                brief.portfolio_metrics.var_95_pct = None
                brief.portfolio_metrics.cvar_99_pct = None
                brief.portfolio_metrics.sharpe_ratio = None
                brief.portfolio_metrics.hawkes_risk_factor = None
                brief.portfolio_metrics.metrics_source = "insufficient_history"
                brief.portfolio_metrics.subject_ticker = None
                brief.portfolio_metrics.risk_horizon = None
                brief.portfolio_metrics.annualization_basis = None

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"fin_{ticker}_{int(time.time())}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "graph_context": graph_context,
            }

            await self.redis.raw.set("sentinel:financial:advice:latest", json.dumps(res_payload), ex=86400)

            # Record every play the brief actually produced, not the first two.
            #
            # The inference is the scarce resource here -- this host affords
            # roughly twenty an hour -- and by this point it has already been
            # spent. Each play above has been through full deterministic risk
            # construction: entry level, stop distance, conviction-tiered
            # risk-reward, Kelly sizing. Taking [:2] of a list already named
            # `highest_conviction_plays` was a second arbitrary cut on top of
            # the model's own ranking, and it discarded finished directional
            # claims that cost nothing more to keep.
            #
            # This is the cheapest available optimisation of a capacity-bound
            # tier: it does not create slots, it stops wasting the ones already
            # paid for. The cap that remains exists to bound a runaway model,
            # not to throttle value.
            for play in brief.highest_conviction_plays[:MAX_RECORDED_PLAYS]:
                direction = "up" if play.action == "BUY" else "down" if play.action == "SELL" else "neutral"

                safe_create_task(
                    self.record_prediction(
                        ticker=play.ticker,
                        direction=direction,
                        conviction=play.conviction_score,
                        entry_price=play.entry_level,
                        target_price=play.target_price,
                        time_horizon_hours=(
                            MODEL_HORIZON_HOURS.get(str((model_entry or {}).get("horizon")), 24)
                            if play.evidence_status == "model_supported" else 24
                        ),
                        # The partition this claim is made under, and the one
                        # `get_conditional_scorecard` asks for when sizing the
                        # next position in the same name. Without it the
                        # per-ticker and strategy cards stayed empty forever and
                        # every Kelly fraction used the 0.55 prior.
                        strategy=f"trading_advisory:{play.ticker.upper()}",
                        features={
                            "conviction": play.conviction_score,
                            "evidence_status": play.evidence_status,
                            "trigger_families": sorted(trigger_families),
                            "anomaly_score": anomaly_score,
                            "rsi": indicators.get("rsi"),
                            "atr_pct": round(atr / current_price * 100.0, 4) if current_price else None,
                            "ewma_vol": ewma_vol,
                            "ma_alignment": indicators.get("ma_alignment"),
                            "regime": rates_regime,
                            "kelly_cap_pct": round(kelly_pct * 100.0, 3),
                            "win_prob": round(win_prob, 4),
                        },
                    ),
                    name=f"record-prediction-{play.ticker}"
                )

                safe_create_task(
                    self.publish_bulletin(
                        bulletin_type="signal",
                        summary=(
                            f"{play.action} {play.ticker} @ ${play.entry_level:.2f} -> ${play.target_price:.2f} "
                            f"(Kelly {play.kelly_allocation_pct:.1f}%, {(play.evidence_status or 'unproven').upper()})"
                        ),
                        ticker=play.ticker,
                        conviction=play.conviction_score,
                        expected_direction=direction,
                        payload={"action": play.action, "entry": play.entry_level, "target": play.target_price, "var_95": var_95},
                        ttl_seconds=3600,
                    ),
                    name=f"quant-advisory-bulletin-{play.ticker}"
                )

            return res_payload

        except (SchemaViolationError, InferenceError, Exception) as e:
            logger.error(f"Trading advisory failed for {ticker}: {e}")
            return None

    # Graph edges that make a peer, and those that make a macro instrument.
    _PEER_EDGES = frozenset({
        "PEER_OF", "GRANGER_CAUSES", "STATISTICALLY_CORRELATED_WITH", "SYMPATHY_MOVER",
        "COMPETES_WITH", "SUPPLIER_TO", "CUSTOMER_OF", "SUPPLIES", "PURCHASES_FROM",
    })
    _CATALYSTS = {
        "options_flow": "Options Flow", "equity_block": "Block Trade",
        "market_anomaly": "Volume Anomaly", "insider_trade": "Insider Trade",
        "insider_cluster": "Insider Cluster", "filing": "SEC Filing", "headline": "News",
        "earnings": "Earnings", "thirteen_f": "Institutional Flow",
    }

    async def _graph_peer_discovery(
        self, ticker: str, message: Dict[str, Any], rows: List[Dict]
    ) -> Optional["PeerDiscovery"]:
        """Peers and macro links from measured graph edges; None when there are none."""
        peers: List[PeerTicker] = []
        macros: List[MacroInstrument] = []
        seen: set = set()
        for r in rows or []:
            try:
                rid = str(r["related_id"]).upper().strip()
                rel = str(r["rel_type"])
                conf = float(r.get("confidence") if hasattr(r, "get") else r["confidence"])
            except (TypeError, ValueError, KeyError):
                continue
            if not rid or rid == ticker.upper() or rid in seen:
                continue
            seen.add(rid)
            if rid.endswith("=F") or rid in ("US2Y", "US10Y", "US30Y", "DXY", "VIX"):
                macros.append(MacroInstrument(
                    symbol=rid,
                    instrument_type="commodity" if rid.endswith("=F") else "treasury" if rid.startswith("US") else "volatility" if rid == "VIX" else "forex",
                    correlation_reasoning=f"graph edge {rel} (weight {conf:.2f})",
                ))
            elif rel in self._PEER_EDGES and await is_valid_primary_equity_async(rid, redis_client=self.redis):
                peers.append(PeerTicker(
                    ticker=rid, relation=rel, discovery_confidence=max(0.0, min(1.0, conf)),
                ))
        if not peers and not macros:
            return None
        etype = str(message.get("type") or "").lower()
        return PeerDiscovery(
            primary_ticker=ticker.upper(),
            peer_tickers=peers[:6],
            macro_instruments=macros[:4],
            catalyst_category=self._CATALYSTS.get(etype, etype.replace("_", " ").title() or "Market Event"),
            # Not measured here; the macro engine's cointegration tracker is
            # what detects decoupling.
            structural_decoupling=False,
        )

    async def _aligned_hourly_pair(self, a: str, b: str) -> Tuple[List[float], List[float]]:
        """Hourly closes for two tickers on the hours both traded, market open."""
        if not self.db:
            return [], []
        try:
            rows = await self.db.query(
                """
                SELECT ticker, bucket_time, close FROM tradfi_bars_1h
                WHERE ticker = ANY($1::text[]) AND close IS NOT NULL AND close > 0
                  AND bucket_time > NOW() - INTERVAL '30 days'
                """,
                [a.upper(), b.upper()],
            )
        except Exception as e:
            self.logger.debug("Aligned pair lookup failed for %s/%s: %s", a, b, e)
            return [], []
        by: Dict[str, Dict[Any, float]] = {a.upper(): {}, b.upper(): {}}
        for row in rows or []:
            try:
                t = str(row["ticker"]).upper()
                ts = row["bucket_time"]
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if instrument_closed_at(t, ts):
                    continue
                by.setdefault(t, {})[ts] = float(row["close"])
            except (TypeError, ValueError, KeyError):
                continue
        shared = sorted(set(by[a.upper()]) & set(by[b.upper()]))
        return [by[a.upper()][t] for t in shared], [by[b.upper()][t] for t in shared]

    async def _model_rank(self, ticker: str) -> Optional[Dict[str, Any]]:
        """This ticker's entry in the cross-sectional ranking at QUANT_MODEL_HORIZON,
        with that horizon's chosen model and its out-of-sample record, so the
        play can weigh it (shared.utils.trade_plan.model_support); None when
        the ticker is not ranked."""
        try:
            raw_rep = await self.redis.raw.get(FACTOR_REPORT_KEY)
            if not raw_rep:
                return None
            rep = json.loads(raw_rep if isinstance(raw_rep, str) else raw_rep.decode("utf-8"))
            horizons = rep.get("horizons") or {}
            want = rep.get("primary_horizon") if QUANT_MODEL_HORIZON == "primary" else QUANT_MODEL_HORIZON
            meta = horizons.get(want)
            key = factor_scores_key(want) if meta else FACTOR_SCORES_KEY
            meta = meta or rep
            raw_scores = await self.redis.raw.get(key)
            if not raw_scores:
                return None
            scores = json.loads(raw_scores if isinstance(raw_scores, str) else raw_scores.decode("utf-8"))
            entry = scores.get(ticker.upper())
            if not entry:
                return None
            chosen = next((m for m in meta.get("models", []) if m.get("model") == meta.get("chosen")), {})
            return {**entry, "validated": bool(meta.get("validated")), "model": meta.get("chosen"),
                    "horizon": want if horizons.get(want) else None,
                    "ic_t": chosen.get("ic_t"), "net_sharpe": chosen.get("net_sharpe"),
                    "positive_fold_share": chosen.get("positive_fold_share")}
        except Exception as e:
            swallowed("agents.quant_trading_engine.model_rank", e, logger)
            return None

    async def _proven_signal_families(self) -> set:
        """Families with at least one horizon the reaction study marked proven."""
        try:
            blob = await self.redis.raw.get(STUDY_KEY)
            if not blob:
                return set()
            cells = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8")).get("cells", [])
            return {c.get("family") for c in cells if c.get("verdict") == "proven" and c.get("family")}
        except Exception as e:
            swallowed("agents.quant_trading_engine.proven_families", e, logger)
            return set()

    async def _noise_signal_families(self) -> set:
        """Families the reaction study measured and found nothing in at every
        horizon it could measure: their direction is not a vote."""
        try:
            blob = await self.redis.raw.get(STUDY_KEY)
            if not blob:
                return set()
            cells = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8")).get("cells", [])
            verdicts: Dict[str, set] = {}
            for c in cells:
                if c.get("family"):
                    verdicts.setdefault(c["family"], set()).add(c.get("verdict"))
            return {f for f, v in verdicts.items()
                    if "no_edge" in v and not v & {"proven", "unconfirmed"}}
        except Exception as e:
            swallowed("agents.quant_trading_engine.noise_families", e, logger)
            return set()

    async def _thesis_evidence_block(self, ticker: str) -> str:
        """What the measured layers say about this name, compactly, for the thesis.

        Every factor the evidence layer sees for the ticker with its study
        record, the day's options positioning, and the newest mapped headlines.
        Numbers only; the model is told to cite them and nothing else."""
        t = ticker.upper()
        lines: List[str] = []
        try:
            now = datetime.now(timezone.utc)
            rows = await self.db.query(
                EVIDENCE_EVENTS_SQL.replace(
                    "AND type = ANY($3::text[])",
                    "AND type = ANY($3::text[]) AND (primary_entity_id = $4 "
                    "OR financial_data->>'ticker' = $4 OR $5 = ANY(tags))",
                ),
                now - timedelta(hours=72), now, EVIDENCE_EVENT_TYPES, t, f"exposes:{t}",
            )
            cells: List[Dict[str, Any]] = []
            blob = await self.redis.raw.get(STUDY_KEY)
            if blob:
                cells = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8")).get("cells", [])
            obs = [o for r in rows or [] for o in observations(dict(r)) if o.instrument.upper() == t]
            dossier = build_dossiers(obs, family_record(cells)).get(t)
            if dossier:
                lines.append(f"EVIDENCE (72h): {dossier.summary}")
                for f in dossier.factors[:6]:
                    rec = f.study
                    rec_txt = (
                        f"{rec.get('verdict')}, {rec.get('units')} units, hit {rec.get('hit_rate')}, "
                        f"{rec.get('mean_bps')} bps"
                        if rec else "not yet studied"
                    )
                    lines.append(f"- {f.family} [{f.source}] dir {f.direction:+d} x{f.events}: {rec_txt}")
        except Exception as e:
            swallowed("agents.quant_trading_engine.thesis_evidence", e, logger)
        try:
            raw_pos = await self.redis.raw.get(positioning_key(t))
            if raw_pos:
                pos = json.loads(raw_pos if isinstance(raw_pos, str) else raw_pos.decode("utf-8"))
                lines.append(
                    f"OPTIONS TODAY: calls {pos.get('call_volume')}, puts {pos.get('put_volume')}, "
                    f"put/call {pos.get('put_call_ratio')} ({pos.get('measure')})"
                )
                if pos.get("iv_skew") is not None or pos.get("cp_iv_spread") is not None:
                    lines.append(
                        f"OPTION IV: skew (OTM put minus ATM call, {pos.get('skew_expiry_days')}d) "
                        f"{pos.get('iv_skew')}, ATM IV {pos.get('atm_iv')}, call-minus-put IV spread "
                        f"{pos.get('cp_iv_spread')} over {pos.get('cp_pairs')} pairs. Published record: steep "
                        "skew preceded underperformance (Xing-Zhang-Zhao 2010); a positive call-put spread "
                        "preceded outperformance (Cremers-Weinbaum 2010). Not yet measured on this system."
                    )
        except Exception as e:
            swallowed("agents.quant_trading_engine.thesis_positioning", e, logger)
        for n in (await self._fetch_news_context(t))[:3]:
            lines.append(f"HEADLINE: {str(n.get('headline'))[:160]}")
        return "\n".join(lines)

    async def _write_thesis(self, message: Dict[str, Any], play: Dict[str, Any], context_prompt: str) -> Optional["TradeThesis"]:
        """Prose for a computed play, or None. Never changes the play.

        A shed or failed call leaves the rule's own rationale standing: the
        trade does not wait on, or depend on, the model."""
        evidence = await self._thesis_evidence_block(str(play.get("ticker") or ""))
        prompt = (
            f"{context_prompt}\n\n{evidence}\n\nTHE PLAY IS ALREADY DECIDED -- do not change it:\n"
            f"{play['action']} {play['ticker']} | conviction {play['conviction_score']} | "
            f"{play['evidence_status']} | basis: {play['quantitative_rationale']}\n\n"
            "Write a two-sentence thesis explaining why the measured evidence above points this way, "
            "and up to three concrete risks that would invalidate it, each naming the observable that would show it. "
            "Use ONLY facts and numbers present above; if the evidence is thin or unproven, say so plainly. "
            "Factors marked no_edge were measured and found no edge -- do not lean on them. "
            "Do not propose prices, sizes or a different direction. Return ONLY raw JSON."
        )
        try:
            return await self._execute_with_telemetry(
                message=message,
                system_prompt=(
                    "You are SENTINEL's equity analyst. You explain trades a rule already computed; you do not make "
                    "them. You cite only the numbers you are given, distinguish proven from unproven evidence, and "
                    "never invent catalysts, prices or events."
                ),
                user_prompt=prompt,
                schema=TradeThesis,
                temperature=0.2,
                num_predict=320,
            )
        except InferenceShed:
            return None
        except (SchemaViolationError, InferenceError) as e:
            logger.info("Thesis unavailable for %s: %s", play.get("ticker"), e)
            return None

    async def run_scheduled_review(self) -> Optional[Dict[str, Any]]:
        """
        Scheduled review sweep across watched equities.
        Brings scheduled execution path to 100% parity with live trigger path.
        """
        try:
            raw_watched = await self.redis.raw.zrange(WATCHED_EQUITIES_KEY, 0, -1)
            tickers = [t.decode("utf-8") if isinstance(t, bytes) else str(t) for t in raw_watched]
            if not tickers:
                tickers = ["AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD"]

            # Refresh the backtest store before the sweep reads from it.
            #
            # Nothing in the deployment had ever written a backtest: the store
            # was populated only as a side effect of somebody calling the REST
            # route by hand, and `sentinel:backtest:*` held zero keys. So
            # _fetch_realized_payoff_ratio returned its conservative fallback
            # every time, and Kelly -- which scales inversely with the payoff
            # term -- sized every position as though no strategy had any
            # measured edge. The backtester, its validation gate and its
            # calibration curve were all built and none of them ran.
            await self._refresh_backtests(tickers[:5])

            # Subjects another agent is already examining come first.
            #
            # The sweep took the top five of its own watchlist, which is why the
            # swarm's opinions never overlapped. Additive: every ticker the
            # engine chose is still here, in its original order behind any that
            # are already under examination elsewhere.
            tickers = await prioritise(self.redis, tickers, domains=self.FOCUS_DOMAINS)

            results = []
            for ticker in tickers[:5]:  # Sweep top 5 watched tickers
                res = await self._process_trading_advisory({"ticker": ticker, "anomaly_score": 0.75}, ticker, 0.75)
                if res:
                    results.append(res)
            return {"scheduled_review": results} if results else None
        except Exception as e:
            logger.error(f"Error during quant scheduled review: {e}")
            return None

    # ── HELPER METHODS ────────────────────────────────────────────────────────

    # Bar timeframe backing _fetch_prices. Every annualized or horizon-bearing
    # statistic derived from those bars must be scaled with this, not assumed daily.
    PRICE_TIMEFRAME = "1h"

    async def _refresh_backtests(self, tickers: List[str]) -> None:
        """Replay each watched ticker through the strategies and cache the result.

        Idempotent and cheap to call: a strategy whose cached report is still
        within BACKTEST_REFRESH_SEC is skipped, so the sweep only pays for what
        has actually gone stale.

        The replay itself is CPU-bound and synchronous -- it walks several
        hundred bars building trades -- so it is offloaded rather than run on the
        event loop, which is servicing every other agent in this process.
        """
        if not self.redis or not tickers:
            return
        try:
            from services.reasoning.strategy_backtester import StrategyBacktester
        except Exception as e:
            logger.debug(f"Backtester unavailable, payoff ratios stay at default: {e}")
            return

        backtester = StrategyBacktester(db_client=self.db, redis_client=self.redis)
        loop = asyncio.get_running_loop()

        for ticker in tickers:
            for strategy in BACKTEST_STRATEGIES:
                key = f"sentinel:backtest:results:{strategy}_{ticker.lower()}"
                try:
                    # TTL tells us the age without deserialising the report:
                    # written at 7 days, so anything above (7d - refresh) is
                    # still fresh.
                    ttl = await self.redis.raw.ttl(key)
                    if ttl and ttl > (86400 * 7 - BACKTEST_REFRESH_SEC):
                        continue

                    bars = await backtester.fetch_historical_bars(
                        ticker=ticker, timeframe="5m", limit=BACKTEST_REFRESH_BARS
                    )
                    if not bars:
                        continue

                    # backtest_strategy is synchronous and CPU-bound.
                    report = await loop.run_in_executor(
                        None,
                        partial(
                            backtester.backtest_strategy,
                            ticker=ticker,
                            bars=bars,
                            strategy_type=strategy,
                        ),
                    )

                    # Stored here, on the event loop that owns the connection.
                    # The backtester used to do this itself and could not: it
                    # runs on a worker thread, where creating a task raises.
                    if report and self.redis:
                        await self.redis.raw.set(
                            f"sentinel:backtest:results:{report['strategy_id']}",
                            json.dumps(report, default=str),
                            ex=86400 * 7,
                        )
                except Exception as e:
                    logger.debug(f"Backtest refresh failed for {strategy}_{ticker}: {e}")

    async def _fetch_realized_payoff_ratio(self, ticker: str) -> float:
        """Returns the realized average-win / average-loss ratio for *ticker*.

        Prefers the backtester's measured payoff over an assumed target: Kelly
        scales inversely with this term, so assuming 2.0 where realized is 1.0
        roughly doubles the recommended stake. Falls back to a deliberately
        conservative 1.0 when no validated backtest exists, which sizes down
        rather than up in the absence of evidence.
        """
        if not self.redis:
            return DEFAULT_PAYOFF_RATIO
        try:
            # These must be the backtester's own strategy_type values, which is
            # what it builds `strategy_id` from. This read "momentum", and the
            # backtester writes "momentum_trend", so the momentum key could
            # never have been found even once the store was populated.
            for strategy in BACKTEST_STRATEGIES:
                raw = await self.redis.raw.get(
                    f"sentinel:backtest:results:{strategy}_{ticker.lower()}"
                )
                if not raw:
                    continue
                report = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))

                # Only trust a payoff ratio from a backtest built on real data
                # with enough closed trades to be meaningful.
                if report.get("data_provenance") != "authentic_market_data":
                    continue
                perf = report.get("performance_metrics") or {}
                if int(perf.get("total_trades") or 0) < MIN_TRADES_FOR_PAYOFF:
                    continue

                payoff = (report.get("risk_metrics") or {}).get("payoff_ratio")
                if payoff is None:
                    continue
                payoff_f = float(payoff)
                if payoff_f > 0:
                    return min(MAX_PAYOFF_RATIO, payoff_f)
        except Exception as e:
            logger.debug(f"Realized payoff lookup failed for {ticker}: {e}")
        return DEFAULT_PAYOFF_RATIO

    async def _fetch_prices(self, ticker: str) -> Tuple[List[float], List[float], List[float]]:
        """Bars for a ticker, from the cache when it has them and the database
        when it does not.

        This read the Redis candle cache alone, and the advisory below refuses
        to run on fewer than five bars. Measured across 572 tickers with an
        hourly list: not one had five. The deepest was four, because the candle
        lists carry a TTL, which made them evictable, and Redis was evicting
        about forty-seven keys a second to make room for a structure that had no
        TTL and so could not be evicted at all.

        With the eviction stopped the cache does accumulate -- but it still
        needs five hours of uninterrupted uptime to reach five hourly bars, and
        a restart puts it back to zero. tradfi_bars_1h already held 42 to 46
        bars for the same tickers throughout, durably, and nothing was reading
        it. The whole financial advisory path had therefore never produced an
        output, on a cache that could not hold enough history to let it.
        """
        closes, highs, lows = [], [], []
        try:
            key = candle_cache_key(ticker, self.PRICE_TIMEFRAME)
            raw = await self.redis.raw.lrange(key, 0, -1)
            # The aggregator LPUSHes, so the list is newest first. Read in that
            # order, closes[-1] -- the "current price" every entry, target and
            # stop is built from -- was the oldest bar, and every return was
            # reversed: a falling stock carried a positive Sharpe. Sorted by
            # the bar's own timestamp, oldest first, like the durable path.
            # Bars stamped while the market was shut are replays and dropped.
            parsed = []
            for item in raw:
                try:
                    bar = normalize_candle(
                        json.loads(item if isinstance(item, str) else item.decode("utf-8")), ticker
                    )
                    ts = None
                    if bar.get("ts"):
                        ts = datetime.fromisoformat(str(bar["ts"]).replace("Z", "+00:00"))
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=timezone.utc)
                    if ts is not None and instrument_closed_at(ticker, ts):
                        continue
                    close = float(bar.get("close") or 0)
                    if close <= 0:
                        continue
                    parsed.append((
                        ts, close,
                        float(bar.get("high") or close),
                        float(bar.get("low") or close),
                    ))
                except Exception as _exc:
                    swallowed("agents.quant_trading_engine._fetch_prices", _exc, logger)
            if parsed and all(b[0] is not None for b in parsed):
                parsed.sort(key=lambda b: b[0])
            else:
                parsed.reverse()   # undated: the list's own newest-first order
            closes = [b[1] for b in parsed]
            highs = [b[2] for b in parsed]
            lows = [b[3] for b in parsed]
        except Exception as e:
            self.logger.debug("Candle cache read failed for %s: %s", ticker, e)

        if len(closes) >= MIN_ADVISORY_BARS:
            return closes, highs, lows

        # The cache is short. The database is the same series, kept.
        durable = await self._fetch_prices_durable(ticker)
        if durable and len(durable[0]) > len(closes):
            return durable
        return closes, highs, lows

    async def _fetch_prices_durable(self, ticker: str) -> Optional[Tuple[List[float], List[float], List[float]]]:
        """Hourly bars from TimescaleDB, oldest first to match the cache."""
        if not self.db:
            return None
        try:
            rows = await self.db.query(
                """
                SELECT close, high, low
                FROM tradfi_bars_1h
                WHERE ticker = $1 AND close IS NOT NULL
                ORDER BY bucket_time DESC
                LIMIT $2
                """,
                ticker.upper(), DURABLE_BAR_LIMIT,
            )
        except Exception as e:
            self.logger.debug("Durable bar lookup failed for %s: %s", ticker, e)
            return None

        closes, highs, lows = [], [], []
        for row in reversed(rows or []):
            try:
                close = float(row["close"])
            except (TypeError, ValueError, KeyError):
                continue
            closes.append(close)
            highs.append(float(row.get("high") or close))
            lows.append(float(row.get("low") or close))
        return (closes, highs, lows) if closes else None

    async def _aligned_daily_closes(
        self, tickers: List[str]
    ) -> Tuple[List[str], List[List[float]]]:
        """Daily closes for several tickers on the dates they all traded.

        A covariance across assets is only defined on shared observations. Bars
        are pulled for the whole universe in one query and then intersected on
        bucket_time, because two tickers with forty bars each and no dates in
        common carry no covariance information at all -- and reading them
        separately would hide that behind two healthy-looking series.
        """
        if not self.db or not tickers:
            return [], []
        try:
            rows = await self.db.query(
                """
                SELECT ticker, bucket_time, close
                FROM tradfi_bars_1d
                WHERE ticker = ANY($1::text[])
                  AND close IS NOT NULL AND close > 0
                  AND bucket_time > NOW() - ($2 || ' days')::INTERVAL
                ORDER BY bucket_time
                """,
                tickers, str(BL_LOOKBACK_DAYS),
            )
        except Exception as e:
            self.logger.debug("Black-Litterman bar lookup failed: %s", e)
            return [], []

        by_ticker: Dict[str, Dict[Any, float]] = {}
        for row in rows or []:
            try:
                symbol = str(row["ticker"]).upper()
                by_ticker.setdefault(symbol, {})[row["bucket_time"]] = float(row["close"])
            except (TypeError, ValueError, KeyError) as _exc:
                swallowed("agents.quant_trading_engine._aligned_daily_closes", _exc, logger)

        present = [t for t in tickers if len(by_ticker.get(t, {})) >= BL_MIN_OBSERVATIONS]
        if len(present) < 2:
            return [], []

        # The universe is chosen FOR its shared support, not in spite of it.
        #
        # Taking the largest N by market cap and then intersecting lets one
        # late-joining ticker decide the estimate for all of them. Measured
        # here: six names carried 43-49 daily bars back to 13 July, TT and ALNY
        # joined the watchlist a month later with 21, and the intersection of
        # all eight was 15 dates -- fewer observations than twice the number of
        # assets, which is a rank-deficient covariance dressed up as a portfolio.
        #
        # So candidates are walked in market-cap order and each is admitted only
        # if the universe still clears BL_MIN_OBSERVATIONS_PER_ASSET shared days
        # per asset with it included. A name that would cost more history than
        # it adds breadth is skipped, not allowed to shrink everyone's window.
        chosen: List[str] = []
        shared_dates: set = set()
        for symbol in present:
            candidate_dates = set(by_ticker[symbol])
            merged = candidate_dates if not chosen else (shared_dates & candidate_dates)
            width = len(merged)
            if width < BL_MIN_OBSERVATIONS:
                continue
            if width < BL_MIN_OBSERVATIONS_PER_ASSET * (len(chosen) + 1):
                continue
            chosen.append(symbol)
            shared_dates = merged
            if len(chosen) >= BL_UNIVERSE_MAX:
                break

        if len(chosen) < 2 or len(shared_dates) < BL_MIN_OBSERVATIONS:
            return [], []

        ordered = sorted(shared_dates)
        return chosen, [[by_ticker[t][d] for d in ordered] for t in chosen]

    async def _black_litterman_allocations(
        self, brief: "FinancialAdviceBrief", watched: Optional[set]
    ) -> List[BlackLittermanAllocation]:
        """The portfolio allocation this brief has always declared and never made.

        `black_litterman_allocations` is in the brief schema, the frontend
        renders it, the feature flag gates it and `quant_calc` implements the
        maths with a passing test. Nothing ever called it: the only line that
        touched the field cleared it when the flag was off, so the list was
        empty in all 25 briefs and would have stayed empty whatever the flag
        said. This is the caller.

        The equilibrium prior is market-capitalisation weighted, from the caps
        the movers gate already resolves and caches. The views are this brief's
        own conviction plays -- which is the whole point of the model: it is the
        mechanism for blending a house view into a market prior, and the house
        view is the thing this engine spends its inference budget producing.
        """
        if not self.db or not self.redis:
            return []

        candidates = sorted({str(t).upper().strip() for t in (watched or set()) if t})
        if len(candidates) < 2:
            return []

        # Size first: the prior is capitalisation-weighted, so a ticker with no
        # resolved cap has no weight and cannot be in the universe. `Unknown is
        # not small` -- the same rule the movers board gates on.
        caps: Dict[str, float] = {}
        for symbol in candidates:
            cap = await cached_market_cap(self.redis, symbol)
            if cap and cap > 0:
                caps[symbol] = cap
        if len(caps) < 2:
            return []

        ranked = sorted(caps, key=lambda s: caps[s], reverse=True)[: BL_UNIVERSE_MAX * 3]
        universe, closes = await self._aligned_daily_closes(ranked)
        if len(universe) < 2:
            return []

        returns = [
            [
                (series[i] - series[i - 1]) / series[i - 1]
                for i in range(1, len(series))
                if series[i - 1] > 0
            ]
            for series in closes
        ]
        width = min(len(r) for r in returns)
        if width < BL_MIN_OBSERVATIONS - 1:
            return []

        try:
            cov = np.cov(np.array([r[-width:] for r in returns], dtype=np.float64))
            cov_matrix = np.atleast_2d(cov).tolist()
        except Exception as _exc:
            swallowed("agents.quant_trading_engine._bl_covariance", _exc, logger)
            return []

        # The views: this brief's own directional calls, on the names that are
        # actually in the universe. A play on a ticker with no market cap or no
        # shared history is not a view the model can express, and is dropped
        # rather than quietly applied to the wrong row.
        index = {symbol: i for i, symbol in enumerate(universe)}
        views_matrix: List[List[float]] = []
        view_returns: List[float] = []
        view_uncertainties: List[float] = []
        for play in brief.highest_conviction_plays:
            symbol = str(getattr(play, "ticker", "") or "").upper()
            position = index.get(symbol)
            if position is None:
                continue
            move = getattr(play, "expected_move_pct", None)
            if move is None:
                continue
            try:
                magnitude = abs(float(move)) / 100.0
            except (TypeError, ValueError):
                continue
            if magnitude <= 0:
                continue
            action = str(getattr(play, "action", "") or "").upper()
            if action == "HOLD":
                continue
            row = [0.0] * len(universe)
            row[position] = 1.0
            views_matrix.append(row)
            view_returns.append(-magnitude if action == "SELL" else magnitude)
            conviction = float(getattr(play, "conviction_score", 0.0) or 0.0)
            view_uncertainties.append(
                BL_VIEW_UNCERTAINTY_FLOOR
                + BL_VIEW_UNCERTAINTY_SCALE * (1.0 - max(0.0, min(1.0, conviction)))
            )

        # numpy on a 12x12 is microseconds, but it is still synchronous work on
        # the loop that serves every other agent, so it goes to the executor.
        try:
            result = await asyncio.get_running_loop().run_in_executor(
                None,
                partial(
                    quant_calc.black_litterman_optimization,
                    {symbol: caps[symbol] for symbol in universe},
                    cov_matrix,
                    views_matrix,
                    view_returns,
                    view_uncertainties,
                ),
            )
        except Exception as _exc:
            swallowed("agents.quant_trading_engine._bl_optimize", _exc, logger)
            return []

        weights = (result or {}).get("optimal_weights") or {}
        expected = (result or {}).get("expected_returns") or {}
        if not weights:
            return []

        total_cap = sum(caps[symbol] for symbol in universe) or 1.0
        allocations: List[BlackLittermanAllocation] = []
        for symbol in universe:
            try:
                allocations.append(
                    BlackLittermanAllocation(
                        ticker=symbol,
                        target_weight_pct=round(float(weights.get(symbol, 0.0)), 2),
                        # Daily returns, reported annualised, because a weight
                        # is read next to it and a per-day figure beside a
                        # portfolio weight reads as an annual one.
                        expected_return_pct=round(
                            float(expected.get(symbol, 0.0)) * BL_TRADING_DAYS * 100.0, 2
                        ),
                        equilibrium_weight_pct=round(caps[symbol] / total_cap * 100.0, 2),
                    )
                )
            except Exception as _exc:
                swallowed("agents.quant_trading_engine._bl_allocation", _exc, logger)

        if allocations:
            self.logger.info(
                "Black-Litterman: %d assets, %d views, %d shared observations",
                len(universe), len(views_matrix), width,
            )
        return allocations

    def _compute_ta(self, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
        return compute_ta_indicators(closes, highs, lows)

    async def _fetch_news_context(self, ticker: str) -> List[Dict]:
        try:
            rows = await self.db.query("""
                SELECT headline, anomaly_score, occurred_at, named_entities, tags
                FROM events
                WHERE type = 'headline'
                  AND occurred_at > NOW() - INTERVAL '4 hours'
                  AND anomaly_score >= 0.3
                  AND (
                    LOWER(headline) LIKE $1
                    OR $2 = ANY(tags)
                  )
                ORDER BY anomaly_score DESC
                LIMIT 8
            """, f"%{ticker.lower()}%", f"exposes:{ticker.upper()}")
            # Mapped headlines are tagged exposes:<TICKER>; matching the bare
            # lower-case ticker against tags found nothing, so a headline the
            # enricher had tied to this name reached the prompt only if it
            # happened to spell the ticker out.
            return [{"headline": r["headline"], "score": r["anomaly_score"]} for r in rows]
        except Exception as e:
            logger.debug(f"News context fetch error for {ticker}: {e}")
            return []

    async def _fetch_graph_context(self, ticker: str) -> List[Dict]:
        try:
            neo4j_client = self.neo4j or await get_neo4j()
            if not neo4j_client:
                return []
            # Match on either key, under any label, because one ticker is two
            # nodes.
            #
            # This asked for `(e:Entity)-[r]-(t:Entity {id: $ticker})`, which
            # requires the subject to carry an `id` AND the Entity label AND the
            # other end to carry it too. The supervisor merges on (label, name),
            # so `upsert_equity` (label "Company") and `link_entities` (default
            # "Entity") build two nodes per ticker. 50 tickers are split. On
            # NVDA the halves hold:
            #
            #   Company  GRANGER_CAUSES 16, PEER_OF 6, STATISTICALLY_* 5,
            #            SYMPATHY_MOVER 4, OPERATES_IN 1          = 32
            #   Entity   RELATED_TO 2, HAS_EXPOSURE_IN 1, SYMPATHY_MOVER 1 = 4
            #
            # and this query read the Entity half. The Entity Graph line of the
            # prompt was near-empty while the semiconductor cohort sat one label
            # away.
            #
            # `OR` rather than `coalesce(t.name, t.id)`: coalesce takes `name`
            # when it exists, so a node named "NVIDIA Corp" with id "NVDA" would
            # stop matching the ticker entirely.
            #
            # Left unlabelled and open on relationship type after checking what
            # that admits: across every company-named node the graph holds no
            # TRANSACTED_WITH edge at all, so the 38,556 crypto-wallet edges
            # cannot reach here. HAS_EXPOSURE_IN and MACRO_CORRELATED are
            # excluded because their targets are event types and source names
            # (`market_anomaly`, `finnhub_equities`), not entities.
            #
            # ORDER BY is not decoration: the caller slices [:3], so without one
            # the three facts shown to the model were whichever three Neo4j
            # happened to return.
            # Ranked by effect size, because the confidence is saturated.
            #
            # Measured over all 116 GRANGER_CAUSES edges: `confidence` is
            # 1 - p_value, and the discovery pass only writes an edge once the
            # p-value has cleared a significance filter, so 102 of the 116 sit
            # at exactly 1.0. Ordering by it produced a 9-way tie at the top and
            # the caller's [:3] then picked three of the nine arbitrarily.
            #
            # `f_stat` on the same edges has 116 distinct values across 116
            # edges, spanning 4.64 to 217.5 -- the filter never touched it. For
            # NVDA it reorders the top three from an arbitrary slice of the tie
            # to QQQ (36.7), AVGO (31.2), TLT (21.7).
            #
            # This is the fifth score found saturated by the same mechanism: a
            # number derived from a quantity some earlier filter had already
            # guaranteed. Kept as a tie-break rather than the primary key so
            # predicates that carry no f_stat still order sensibly against each
            # other.
            #
            # max(f_stat) grouped by (entity, predicate) because Granger edges
            # repeat per lag -- AVGO appears at lag 1 and lag 2 -- and two rows
            # for one relationship would eat two of the three slots the prompt
            # has.
            # Labels listed, and `id` dropped, because both are measurable.
            #
            # Every label carries a RANGE index on `name` and none carries one
            # on `id`, so `t.id = $ticker` forces a label scan. Checked whether
            # it buys anything: across Company, Entity and CryptoAsset there are
            # zero nodes whose `id` differs from their `name`. It is a redundant
            # predicate that was costing the index.
            #
            # Timed warm, same 36 edges returned:
            #   name only, labelled            2 ms
            #   name or id, labelled         600 ms
            #   name or id, unlabelled      1016 ms
            #   the original                 358 ms -- and it returned 3 edges
            #
            # Company|Entity|CryptoAsset and not "any label" because Flag holds
            # 124 ticker-shaped names: a two-letter vessel flag code would
            # otherwise answer a lookup for a two-letter ticker and attach a
            # ship registry to an equity brief.
            #
            # Three nodes in the graph carry no label at all -- AS, CL=F and VXX,
            # 37 edges between them -- and no label-scoped query can reach them.
            # That is a writer defect, recorded separately rather than papered
            # over here by dropping back to a scan of all 253,000 nodes.
            q = """
                MATCH (t:Company|Entity|CryptoAsset)
                WHERE t.name = $ticker
                MATCH (t)-[r]-(e)
                WHERE NOT type(r) IN ['HAS_EXPOSURE_IN', 'MACRO_CORRELATED']
                  AND coalesce(e.name, e.id) IS NOT NULL
                WITH coalesce(e.name, e.id) AS related_id,
                     type(r) AS rel_type,
                     max(coalesce(r.weight, r.confidence, $unrated)) AS confidence,
                     max(r.f_stat) AS strength,
                     min(r.lag) AS lag
                RETURN related_id, rel_type, confidence, strength, lag
                ORDER BY confidence DESC, coalesce(strength, 0.0) DESC
                LIMIT 10
            """
            rows = await neo4j_client.query(q, {"ticker": ticker.upper(), "unrated": UNRATED_EDGE_CONFIDENCE})
            return rows or []
        except Exception as e:
            logger.debug(f"Graph context fetch error for {ticker}: {e}")
            return []

    async def _fetch_earnings_context(self, ticker: str) -> str:
        """Fetch cached earnings context from Redis (populated by Finnhub earnings poller)."""
        try:
            raw = await self.redis.raw.get(entity_cache_key("sentinel:earnings", ticker))
            if raw:
                data = json.loads(raw)
                report_date = data.get("report_date", "")
                session = data.get("session", "")
                eps_est = data.get("eps_estimate")
                eps_act = data.get("eps_actual")
                surprise = data.get("eps_surprise_pct")
                rev_est = data.get("revenue_estimate")
                rev_act = data.get("revenue_actual")
                trade_type = data.get("trade_type", "")

                if trade_type == "EARNINGS_SURPRISE" and eps_act is not None:
                    return (
                        f"📊 EARNINGS REPORTED: {ticker} | Date: {report_date} ({session}) | "
                        f"EPS: {eps_act} vs Est {eps_est} (Surprise: {surprise:+.1f}%) | "
                        f"Revenue: {rev_act} vs Est {rev_est}"
                    )
                else:
                    session_label = {"bmo": "Pre-Market", "amc": "After-Close", "dmh": "During Hours"}.get(
                        session, session.upper() if session else "TBD"
                    )
                    return (
                        f"📅 EARNINGS UPCOMING: {ticker} | Date: {report_date} ({session_label}) | "
                        f"Est EPS: {eps_est} | Est Revenue: {rev_est}"
                    )
        except Exception as _exc:
            swallowed("agents.quant_trading_engine._fetch_earnings_context", _exc, logger)
        return ""

    async def _fetch_funding_context(self, symbol: str) -> str:
        """Fetch cached crypto funding rate context from Redis (populated by markPrice stream)."""
        try:
            for candidate in [symbol.upper(), symbol.upper() + "USDT"]:
                raw = await self.redis.raw.get(f"sentinel:crypto:funding:{candidate}")
                if raw:
                    data = json.loads(raw)
                    fr = data.get("funding_rate", 0)
                    basis = data.get("basis_bps", 0)
                    mark = data.get("mark_price", 0)
                    idx = data.get("index_price", 0)
                    annualized = abs(fr) * 3 * 365 * 100
                    return (
                        f"⚡ PERP FUNDING: {candidate} | Rate: {fr:.6f} ({annualized:.1f}% annualized) | "
                        f"Basis: {basis:.2f}bps | Mark: {mark:.2f} | Index: {idx:.2f}"
                    )
        except Exception as _exc:
            swallowed("agents.quant_trading_engine._fetch_funding_context", _exc, logger)
        return ""

    # ── 7. STATISTICAL INSIDER CLUSTER ENGINE (§1.4) ──────────────────────────

    async def _publish_cluster_event(self, cluster: Dict[str, Any]) -> None:
        """An insider cluster as a stored `insider_cluster` event."""
        ticker = cluster["ticker"]
        event_type = EventType.INSIDER_CLUSTER
        event = NormalizedEvent.model_validate({
            "type": event_type,
            "occurred_at": cluster["detected_at"],
            "source": "insider_cluster",
            "primary_entity": {"id": ticker, "type": "company", "name": ticker},
            "headline": (
                f"Insider buying cluster: {cluster['insider_count']} insiders net bought "
                f"${cluster['net_buy_usd'] / 1e6:.2f}M of {ticker} within {cluster['window_days']} days"
            ),
            "summary": "Open-market purchases (Form 4 code P) by distinct insiders: " + ", ".join(cluster["unique_buyers"]),
            "financial_data": {"ticker": ticker, "side": "BUY"},
            "tags": ["insider_cluster", f"exposes:{ticker}"],
            "anomaly_score": cluster["anomaly_score"],
        })
        try:
            await self.producer.send(
                Topics.RAW_TRADFI,
                RawEvent(source="insider_cluster", occurred_at=event.occurred_at,
                         raw_payload={"normalized_event": event.model_dump(mode="json")}).model_dump(mode="json"),
                key=ticker,
            )
        except Exception as exc:
            swallowed("agents.quant_trading_engine.publish_cluster_event", exc, logger)

    async def _process_insider_form4(self, message: Dict[str, Any], raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Processes SEC Form 4 insider transactions, records them in a rolling 14-day
        window, and evaluates multi-executive buying cluster significance.
        """
        trig = message.get("trigger") or {}
        pe = message.get("primary_entity") or {}
        sec_data = message.get("security_data") or raw.get("security_data") or {}

        ticker = str(
            raw.get("ticker") or
            raw.get("symbol") or
            trig.get("ticker") or
            pe.get("id") or
            sec_data.get("ticker") or
            ""
        ).upper().strip()

        if not ticker or ticker == "UNKNOWN":
            return None

        # Signal governance gate. is_enabled() also evaluates the platform-wide
        # master kill switch, so this call is what makes Emergency Halt reach this
        # signal class — without it the switch silently does nothing here.
        if not await self.flags.is_enabled("insider_clustering", ticker=ticker):
            return None

        # Extract transaction parameters
        insider_name = str(
            raw.get("insider_name") or
            raw.get("filer_name") or
            raw.get("reporting_owner") or
            trig.get("insider_name") or
            "Corporate Insider"
        ).strip()

        title = str(
            raw.get("title") or
            raw.get("officer_title") or
            raw.get("relationship") or
            "Executive/Director"
        ).strip()

        # An unrecognised transaction code is not a sale.
        #
        # This defaulted a missing code to "P" and then classified anything not
        # in the buy list as a SELL, so every Form 4 code outside four strings
        # became insider selling. Form 4 defines more than a dozen -- A (award),
        # G (gift), M (option exercise), F (tax withholding), J (other) -- and
        # the enricher's own fallback for an unparseable code is "J", which
        # would have been counted as a sale against the buying side of the
        # cluster gate.
        #
        # OTHER trades are recorded and excluded from the buy/sell arithmetic:
        # the filing happened, and what it was is unknown.
        # The parsed filing states the code; nothing wrote `transaction_code`,
        # so every trade was OTHER and no cluster could ever have a buyer.
        tx_code = str(
            raw.get("transaction_code") or form4_dominant_code(raw) or raw.get("trade_type") or ""
        ).upper()
        # Open-market purchases only. "ACQUISITION" was here and would count a
        # stock award (Form 4 code A) as buying if anything ever emitted it.
        if tx_code in ("P", "PURCHASE", "BUY"):
            tx_type = "BUY"
        elif tx_code in ("S", "SALE", "SELL", "D", "DISPOSITION"):
            tx_type = "SELL"
        else:
            tx_type = "OTHER"
        # A trade made under a Rule 10b5-1 plan was scheduled in advance; it is
        # routine, not a decision on current information, and the literature
        # finds no return in routine insider trades (Cohen, Malloy & Pomorski
        # 2012). Recorded, but kept out of the cluster's buy/sell arithmetic.
        if raw.get("planned_10b5_1") and tx_type in ("BUY", "SELL"):
            tx_type = "OTHER"

        # A filing that does not state a quantity is not a thousand shares.
        #
        # shares defaulted to 1000.0 and price to 0.0, and total_usd was
        # shares * price -- so a Form 4 missing its share count was sized at a
        # thousand shares times whatever price it did carry. At a $150 print
        # that invents $150,000 of insider buying, and the cluster gate below
        # fires on two distinct buyers and $250,000 net: two such filings
        # publish an insider accumulation cluster that never happened.
        #
        # The dollar value drives the gate and the z-score, so an unsizeable
        # trade contributes None rather than a number. The trade itself is
        # still real and still counts toward the distinct-buyer requirement --
        # what is unknown is how large it was.
        shares = _positive_or_none(
            raw.get("shares"), raw.get("qty"), raw.get("shares_transacted")
        )
        price = _positive_or_none(
            raw.get("price"), raw.get("price_per_share"), raw.get("transaction_price")
        )
        stated_total = _positive_or_none(
            form4_code_value_usd(raw, tx_code if tx_code in ("P", "S") else None),
            raw.get("total_value_usd"), raw.get("notional_usd"),
        )
        total_usd = stated_total if stated_total is not None else (
            shares * price if (shares is not None and price is not None) else None
        )

        now_ts = time.time()
        tx_record = {
            "ticker": ticker,
            "insider_name": insider_name,
            "title": title,
            "tx_type": tx_type,
            "shares": shares,
            "price": price,
            "total_usd": total_usd,
            "timestamp": now_ts,
        }

        # 1. Append to rolling 14-day sorted set in Redis
        rolling_key = f"sentinel:insider:trades:{ticker}"
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            pipe = raw_redis.pipeline()
            pipe.zadd(rolling_key, {json.dumps(tx_record): now_ts})
            # Prune transactions older than 14 days
            cutoff = now_ts - (14 * 86400)
            pipe.zremrangebyscore(rolling_key, 0, cutoff)
            await pipe.execute()

            # 2. Fetch all active transactions within the rolling window
            raw_items = await raw_redis.zrange(rolling_key, 0, -1)
            all_trades = []
            for item in raw_items:
                try:
                    val = item.decode("utf-8") if isinstance(item, bytes) else str(item)
                    all_trades.append(json.loads(val))
                except Exception as _exc:
                    swallowed("agents.quant_trading_engine._process_insider_form4", _exc, logger)

            # 3. Evaluate cluster conditions
            buy_trades = [t for t in all_trades if t.get("tx_type") == "BUY"]
            unique_buyers = list(set(t.get("insider_name") for t in buy_trades if t.get("insider_name")))
            
            # Unsizeable trades contribute no dollars. They were contributing
            # a fabricated 1000-share notional, which is what the gate below
            # measured.
            total_buy_usd = sum(
                t["total_usd"] for t in buy_trades if t.get("total_usd") is not None
            )
            total_sell_usd = sum(
                t["total_usd"] for t in all_trades
                if t.get("tx_type") == "SELL" and t.get("total_usd") is not None
            )
            net_buy_usd = max(0.0, total_buy_usd - total_sell_usd)

            # Statistical Significance Gate: >= 2 distinct buyers and >= $250k net bought
            if len(unique_buyers) >= 2 and net_buy_usd >= 250_000.0:
                import math
                cluster_z = min(4.5, round(math.sqrt(len(unique_buyers)) * math.log10(1.0 + (net_buy_usd / 100_000.0)), 2))
                cluster_score = min(1.0, round(0.65 + (cluster_z / 10.0), 3))

                cluster_id = f"CLUSTER-INSIDER-{ticker}-{int(now_ts)}"
                cluster_payload = {
                    "cluster_id": cluster_id,
                    "ticker": ticker,
                    "insider_count": len(unique_buyers),
                    "unique_buyers": unique_buyers,
                    "net_buy_usd": round(net_buy_usd, 2),
                    "total_buy_usd": round(total_buy_usd, 2),
                    "trades_count": len(all_trades),
                    "cluster_z_score": cluster_z,
                    "anomaly_score": cluster_score,
                    "window_days": 14,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }

                # Store active cluster in Redis
                await raw_redis.set(f"sentinel:insider:clusters:{ticker}", json.dumps(cluster_payload), ex=86400 * 7)

                # One cluster per ticker per window. Every Form 4 that arrived
                # after the gate was first met re-published the same cluster,
                # so one cluster read as several -- noise the signal study
                # would have counted as independent evidence.
                first = await raw_redis.set(
                    f"sentinel:insider:cluster_emitted:{ticker}", cluster_id,
                    ex=14 * 86400, nx=True,
                )
                if not first:
                    return None

                # Stored as an event, so the reaction study can measure what
                # insider clusters are actually followed by. A cluster that
                # only reached a Kafka topic and a bulletin left no record.
                await self._publish_cluster_event(cluster_payload)

                # Publish to Topics.INSIDER_CLUSTERS
                if hasattr(self, "producer") and self.producer:
                    await self.producer.send(Topics.INSIDER_CLUSTERS, cluster_payload, key=ticker)

                # Publish Quant Bulletin
                safe_create_task(
                    self.publish_bulletin(
                        bulletin_type="insider_cluster",
                        summary=(
                            f"👔 INSIDER CLUSTER: {len(unique_buyers)} executives net bought ${net_buy_usd/1e6:.2f}M {ticker} "
                            f"(Z={cluster_z:.2f}, Score={cluster_score:.2f})"
                        ),
                        ticker=ticker,
                        conviction=cluster_score,
                        expected_direction="up",
                        payload=cluster_payload,
                        ttl_seconds=86400,
                    ),
                    name=f"insider-cluster-bulletin-{ticker}"
                )

                logger.info(
                    f"👔 STATISTICAL INSIDER CLUSTER DETECTED | {ticker} | "
                    f"{len(unique_buyers)} Insiders | Net Buy: ${net_buy_usd/1e6:.2f}M | Z-Score: {cluster_z:.2f}"
                )

                return {
                    "agent": self.name,
                    "action": "INSIDER_CLUSTER_DETECTED",
                    "cluster": cluster_payload,
                }

        except Exception as e:
            logger.error(f"Error evaluating insider cluster for {ticker}: {e}", exc_info=True)

        return None

