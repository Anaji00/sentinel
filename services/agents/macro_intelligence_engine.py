"""
services/agents/macro_intelligence_engine.py

MASTER QUANT & SWE CONSOLIDATED MACRO ENGINE
=============================================
Consolidates 4 macro agents into a single high-performance engine:
  - YieldCurveMacroRatesAgent (Treasury 2Y/10Y/30Y, SOFR, TIPS, credit spreads)
  - VolatilitySurfaceAgent (Options Put/Call ratio, 25D IV skew, tail risk)
  - MacroAssetCointegrationEngine (Engle-Granger & Johansen cointegration, half-life)
  - MacroStrategistAgent (24h sector aggregates, ontology shifts)

Preserves 100% of existing Kafka topics, Redis keys, and output schemas.
"""

import asyncio
import json
import math
import logging
import os
import sys
import time
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

from services.agents.base import _as_probability_value, SentinelAgent, SchemaViolationError, InferenceError, DEDUP_WINDOW_SLOW_SEC, DEDUP_WINDOW_MEDIUM_SEC
from shared.kafka import Topics
from shared.utils import quant_calc
from shared.utils.tasks import safe_create_task
from shared.db import get_neo4j

logger = logging.getLogger("agent.macro_intelligence")

GRAPH_CACHE_TTL = 3600

TICKER_METADATA_REGISTRY: Dict[str, str] = {
    "ZC=F": "Corn Futures (CBOT)",
    "CL=F": "Crude Oil Futures (NYMEX)",
    "GC=F": "Gold Futures (COMEX)",
    "SI=F": "Silver Futures (COMEX)",
    "NG=F": "Natural Gas Futures (NYMEX)",
    "HG=F": "Copper Futures (COMEX)",
    "TNX": "10-Year Treasury Yield",
    "TIP": "iShares TIPS Bond ETF",
    "SPY": "SPDR S&P 500 ETF Trust",
    "QQQ": "Invesco QQQ Trust",
    "IWM": "iShares Russell 2000 ETF",
    "TLT": "iShares 20+ Year Treasury Bond ETF",
    "HYG": "iShares iBoxx $ High Yield Corporate Bond ETF",
    "LQD": "iShares iBoxx $ Investment Grade Corporate Bond ETF",
    "DX-Y.NYB": "US Dollar Index",
    "^VIX": "CBOE Volatility Index",
    "VIX": "CBOE Volatility Index",
}


# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

# How long a failed rates evaluation blocks the next attempt.
#
# Long enough that a burst of macro ticks does not all evaluate the same hour at
# once, short enough that a transient failure costs minutes rather than the
# whole hour the successful path reserves.
RATES_RETRY_COOLDOWN_SEC = 300


class RatesRegimeBrief(BaseModel):
    curve_state: str  # "Inverted", "Disinverted", "Normal Steepening", "Flat"
    yield_spread_2y10y_bps: float
    breakeven_inflation_bps: float
    tips_yield: float
    credit_spread_widening_signal: str  # "Stable", "Moderate Widening", "Severe Stress"
    regime_summary: str
    macro_risk_level: str  # "LOW", "ELEVATED", "CRITICAL"
    recommended_hedging: List[str] = Field(default_factory=list)


class VolatilitySurfaceBrief(BaseModel):
    ticker: str
    put_call_volume_ratio: float
    # Optional, and overwritten with the measured value after inference.
    #
    # The model was required to return a float here and was handed the skew in
    # its prompt, so it echoed it -- which is how a stored brief came to read
    # -375.0, the product of DEFAULT_CALL_IV and PUT_IV_SKEW, presented as an
    # observation. Requiring a number from a model that has not measured one
    # guarantees a fabricated number; the measurement belongs to the caller and
    # the analysis belongs to the model.
    iv_skew_25d_bps: Optional[float] = None
    volatility_regime: str
    tail_risk_conviction: float

    @field_validator("tail_risk_conviction", mode="before")
    @classmethod
    def _conviction_is_a_probability(cls, value):
        """The third field to carry this defect, and the third to be bounded.

        AgentPrediction.conviction was fixed after a model wrote 55.0 into it,
        and the note on that fix said the repair belonged on the shared contract
        rather than the one caller that exposed it. AgentBulletin.conviction was
        the second. This is a bare float on the brief model, filled by the same
        models, with the same 0.0-1.0 intent in its name and nothing enforcing
        it -- a live brief carried 49.47.

        It is worse here than an overweighted opinion. The model reads its own
        output back when it writes analytical_summary, and reported the number
        as an implied volatility level: "the implied volatility regime is stable
        at 49.47%", on a surface whose skew is explicitly not measured.
        """
        return _as_probability_value(value)
    hedging_recommendations: List[str] = Field(default_factory=list)
    analytical_summary: str


class UnifiedMacroAssessment(BaseModel):
    rates_regime: RatesRegimeBrief
    volatility_regime: VolatilitySurfaceBrief
    macro_risk_score: float  # 0.0 to 1.0
    strategic_summary: str
    key_catalysts: List[str] = Field(default_factory=list)


# ── CONSOLIDATED MACRO ENGINE ──────────────────────────────────────────────────

# There is no assumed implied volatility any more.
#
# DEFAULT_CALL_IV = 0.25 and PUT_IV_SKEW = 1.15 stood here, and _as_iv ended its
# fallback chain in them. Their product was published as a measured 25-delta
# skew of 375.0 bps on every surface whose IVs were not cached -- which is most
# of them, since 75.5% of options events carry no implied volatility at all.
# A surface with nothing observed now reports "not measured".


def _measured_iv(cached, measured):
    """An implied volatility that was actually observed, or None.

    Replaces the fallback chain that ended in DEFAULT_CALL_IV. Returning None
    is what lets the caller distinguish an unmeasured surface from a flat one,
    and a stated "not measured" is a fact the analyst needs where a plausible
    0.25 is a fabrication the analyst cannot see.
    """
    for candidate in (cached, measured):
        if candidate is None:
            continue
        try:
            if isinstance(candidate, bytes):
                candidate = candidate.decode("utf-8")
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


# Statistical guards on the macro/equity correlation, matching the ones the peer
# graph already applies to the same hazard.
#
# The peer graph published six pairs at |r| = 1.000 on its first live run,
# because twelve macro anchors were being polled every thirty seconds and
# republished unchanged: a series of one repeated print correlates perfectly
# with any other series of one repeated print on the few bars that did move.
# It gained a distinct-value ratio and a significance test; this path, doing the
# same arithmetic on the same feeds, did not.
MIN_CORRELATION_RETURNS = 14
INVERSE_CORRELATION_THRESHOLD = -0.55

# A series that is mostly one repeated value is not a price history. 0.40 is the
# peer graph's figure, kept identical so the two agree about what stale means.
MIN_DISTINCT_RATIO = 0.40

# Two-tailed p < 0.01, the peer graph's bar. At fourteen returns a coefficient
# of -0.55 clears the naive threshold above and lands near p = 0.04, which is
# not a finding worth publishing as "strong".
MAX_CORRELATION_P_VALUE = 0.01


def _both_series_move(x_vec, y_vec) -> bool:
    """Whether either leg is mostly a repeated print."""
    for series in (x_vec, y_vec):
        values = list(series or [])
        if not values:
            return False
        if (len(set(values)) / len(values)) < MIN_DISTINCT_RATIO:
            return False
    return True


def _is_significant(pearson_corr: float, n_returns: int) -> bool:
    """Two-tailed Student-t test on the correlation coefficient."""
    if n_returns < 3:
        return False
    r = max(-0.999999, min(0.999999, float(pearson_corr)))
    try:
        from scipy import stats
        t = r * math.sqrt((n_returns - 2) / (1.0 - r * r))
        p = 2.0 * (1.0 - stats.t.cdf(abs(t), n_returns - 2))
    except Exception:
        # Without scipy, require a coefficient strong enough that p < 0.01 holds
        # for any sample this path accepts rather than guessing at the p-value.
        return abs(r) >= 0.66
    return p <= MAX_CORRELATION_P_VALUE


def _render_metrics(rows: Optional[List[Dict[str, Any]]], label: str) -> str:
    """How a metrics query's result is stated to the model.

    `[]` was used for both "the query failed" and "the window was empty", and
    those are opposite claims. A model reading `Sector Risk Metrics: []` sees
    the strongest available statement that nothing is wrong -- produced, in the
    failure case, by an outage. Absence of evidence was being rendered as
    evidence of absence, in the one place least able to tell the difference.
    """
    if rows is None:
        return (
            f"UNAVAILABLE - the {label} query failed this cycle. "
            f"This is missing data, not an absence of risk; do not treat it as reassurance."
        )
    if not rows:
        return f"[] - queried successfully, no {label} rows in the last 24 hours."
    return json.dumps(rows, separators=(',', ':'), default=str)


class MacroIntelligenceEngine(SentinelAgent):
    """
    Unified Macro Intelligence Engine.
    Evaluates fixed income rates, options surface skew, commodity-equity cointegration,
    and systemic sector trends in a single-pass pipelined architecture.
    """

    @property
    def output_topic(self) -> str:
        return Topics.RATES_REGIME

    async def run(self):
        """Launches background scheduled macro trend reviews alongside the reactive handler."""
        review_task = safe_create_task(self._run_scheduled_macro_review())
        try:
            await super().run()
        finally:
            review_task.cancel()

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Unified event handler reacting to rates, options flow, cointegration ticks, or macro headlines.
        """
        raw = message.get("raw_payload", message)
        source = message.get("source", "")
        event_type = message.get("type", "")
        pe = message.get("primary_entity") or {}
        fd = message.get("financial_data") or {}

        ticker = str(
            raw.get("ticker") or
            pe.get("id") or
            ""
        ).upper()

        # ── 1. FIXED INCOME & RATES HANDLER ────────────────────────────────────
        if ticker in ("US2Y", "US10Y", "US30Y", "HYG", "LQD", "SOFR", "TIP", "^TNX") or "treasury" in source or "rates" in source or "rate" in str(event_type).lower():
            return await self._process_rates_and_macro_regime(message, ticker)

        # ── 2. OPTIONS VOLATILITY SURFACE HANDLER ─────────────────────────────
        if source in ("alpaca_options", "tradfi_enricher") or raw.get("trade_type") in ("OPTIONS_FLOW", "OPTIONS_SWEEP") or "options" in str(event_type).lower():
            return await self._process_volatility_surface(message, ticker, raw)

        # ── 3. COINTEGRATION SPREAD HANDLER ───────────────────────────────────
        if message.get("financial_data") or raw.get("close") or raw.get("price"):
            macro_asset = ticker or raw.get("ticker")
            price = (
                fd.get("close_price") or
                raw.get("close") or
                raw.get("price")
            )
            if macro_asset and price is not None:
                await self._process_cointegration(macro_asset, float(price))

        # ── 4. HIGH SEVERITY MACRO INTEL TRIGGER ──────────────────────────────
        severity = message.get("computed_severity") or message.get("severity") or 0
        if severity >= 4:
            await self._run_macro_review_now(trigger_event=message)

        return None

    # ── SUB-ENGINE 1: RATES & REGIME ──────────────────────────────────────────

    async def _process_rates_and_macro_regime(self, message: Dict[str, Any], ticker: str) -> Optional[Dict[str, Any]]:
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        if not await self._inference_budget.is_available():
            return None

        # Single atomic Redis mget for all macro quote dependencies
        keys = [
            "sentinel:quotes:latest:US2Y",
            "sentinel:quotes:latest:US10Y",
            "sentinel:quotes:latest:TIPS_YIELD",
            "sentinel:quotes:latest:TIP",
            "sentinel:quotes:latest:HYG",
            "sentinel:quotes:latest:LQD",
        ]
        vals = await self.redis.raw.mget(keys)

        # No defaults. These were 4.25 / 4.15 / 1.85 / 77.5 / 108.2, and the
        # quote keys have never been populated -- US2Y, US10Y, HYG and LQD are
        # all empty -- so every evaluation this engine has ever published was
        # computed from those constants:
        #
        #   📊 Macro Rates Evaluation | 2Y: 4.25% | 10Y: 4.15% | Spread: -10.0 bps
        #
        # identically, every hour. A 2s10s spread of -10bp is an inverted curve,
        # which is a recession signal, and it was arithmetic on two numbers
        # somebody typed. It then went into the model's prompt as "2Y Yield:
        # 4.250% | 10Y Yield: 4.150%", stated as measurement.
        #
        # A missing input must produce no reading, not a plausible one. The
        # same rule has_excitation_path() applies to the Hawkes model: refuse
        # rather than restate a constant as an observation.
        def _quote(raw, label):
            if raw in (None, "", b""):
                return None
            try:
                value = float(raw)
            except (TypeError, ValueError):
                return None
            return value

        y2 = _quote(vals[0], "US2Y")
        y10 = _quote(vals[1], "US10Y")

        # TIPS is a *yield* here. vals[3] is the TIP ETF's price -- 106.82 --
        # and the old `tips_val < 15.0` guard silently caught that and swapped
        # in 1.85, which is how a fabricated 230bp breakeven reached the prompt.
        # A price is not a substitute for the yield; it is a different quantity.
        tips_yield = _quote(vals[2], "TIPS_YIELD")
        if tips_yield is not None and not (-5.0 < tips_yield < 15.0):
            logger.warning(
                "Discarding TIPS_YIELD=%.3f: outside the range a real yield occupies. "
                "A price posted to a yield key is the usual cause.", tips_yield,
            )
            tips_yield = None

        hyg = _quote(vals[4], "HYG")
        lqd = _quote(vals[5], "LQD")

        if y2 is None or y10 is None:
            logger.warning(
                "Skipping macro rates evaluation: US2Y=%s US10Y=%s. Without both "
                "legs there is no curve to report, and a default would publish a "
                "recession signal nobody measured.",
                "present" if y2 is not None else "MISSING",
                "present" if y10 is not None else "MISSING",
            )
            return None

        spread_2y10y_bps = (y10 - y2) * 100.0
        breakeven_inflation_bps = (
            (y10 - tips_yield) * 100.0 if tips_yield is not None else None
        )
        credit_ratio = (hyg / lqd) if (hyg and lqd and lqd > 0) else None

        # Atomic Redis pipeline write for rolling history
        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.rpush("sentinel:macro:spread_2y10y", spread_2y10y_bps)
            pipe.ltrim("sentinel:macro:spread_2y10y", -100, -1)
            await pipe.execute()

        dedup_key = f"macro_regime:{int(time.time() // 3600)}"
        if await self.is_recently_processed(dedup_key, window_seconds=DEDUP_WINDOW_SLOW_SEC):
            return None
        # Claimed briefly here, confirmed for the full hour only on success.
        #
        # This marked the hour as done before the work was attempted, so a
        # failure still consumed the slot. Observed exactly that: the first
        # evaluation ever to get past the yield guard crashed two seconds later
        # on an unguarded format, and the engine then declined to try again for
        # an hour -- while the inputs it had been missing arrived four minutes
        # after the crash.
        #
        # A short claim still stops the same hour being evaluated concurrently
        # by every macro tick that arrives, which is what the dedup is for; it
        # just no longer treats "we tried and failed" as "we have a reading".
        await self.mark_processed(dedup_key, window_seconds=RATES_RETRY_COOLDOWN_SEC)

        logger.info(f"📊 Macro Rates Evaluation | 2Y: {y2:.2f}% | 10Y: {y10:.2f}% | Spread: {spread_2y10y_bps:+.1f} bps")

        # "not measured" is a fact the analyst needs; a number implies it was.
        tips_str = f"{tips_yield:.3f}%" if tips_yield is not None else "not measured"
        breakeven_str = (
            f"{breakeven_inflation_bps:.1f} bps" if breakeven_inflation_bps is not None
            else "not measured"
        )
        # Guarded like the two above it, which it was not.
        #
        # credit_ratio is None whenever HYG or LQD is missing, and it went into
        # the prompt as {credit_ratio:.4f} with no check, so the whole rates
        # path died on:
        #
        #   unsupported format string passed to NoneType.__format__
        #
        # It had never run. The function returns early unless both US2Y and
        # US10Y are present, and nothing wrote those keys until this session, so
        # this line was unreachable for the life of the deployment -- and the
        # first evaluation that got past the yield guard hit it two seconds
        # later, losing the brief, both Redis keys and the bulletin.
        credit_str = f"{credit_ratio:.4f}" if credit_ratio is not None else "not measured"

        cross_context = await self.get_cross_agent_context(ticker="US10Y", limit=2)
        cross_block = f"\n- Cross-Agent Intelligence:\n{cross_context}" if cross_context else ""

        user_prompt = f"""
        Analyze Treasury yield curve and inflation metrics:
        - 2Y Yield: {y2:.3f}% | 10Y Yield: {y10:.3f}%
        - 2Y-10Y Spread: {spread_2y10y_bps:+.1f} bps
        - 10Y TIPS Real Yield: {tips_str}
        - Breakeven Inflation: {breakeven_str}
        - HYG/LQD Credit Ratio: {credit_str}
        {cross_block}
        """

        try:
            brief: RatesRegimeBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Rates Analyst. Analyze fixed-income rates, yield curves, TIPS real yields, and credit spreads. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=RatesRegimeBrief,
                temperature=0.1,
            )

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"rates_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "metrics": {
                    "yield_2y": y2,
                    "yield_10y": y10,
                    "tips_yield": tips_yield,
                    "spread_2y10y_bps": spread_2y10y_bps,
                    "breakeven_inflation_bps": breakeven_inflation_bps,
                    "credit_ratio": credit_ratio,
                },
            }

            # Cache latest rates regime for shared swarm context
            await self.redis.raw.set("sentinel:macro:rates_regime:latest", json.dumps(res_payload["brief"]), ex=86400)
            await self.redis.raw.set("sentinel:macro:latest_rates_regime", json.dumps(res_payload["brief"]), ex=86400)

            # Publish structured AgentBulletin for Consensus Engine
            # The 2s10s spread is in this same object, computed from measured
            # yields. The direction was taken by searching curve_state for the
            # substring "inverted" -- a field declared as one of four enum values
            # that live holds a whole sentence ("The Treasury yield curve shows a
            # steep upward trend..."), so "no longer inverted" and "not inverted",
            # both natural ways to describe an upward curve, publish "down".
            #
            # An inverted curve is a negative spread. That is the definition, and
            # the number is right here.
            if spread_2y10y_bps is None:
                direction = "neutral"
            elif spread_2y10y_bps < 0:
                direction = "down"
            else:
                direction = "up"
            safe_create_task(
                self.publish_bulletin(
                    bulletin_type="regime_change",
                    summary=f"Rates Regime: {brief.curve_state} (2Y10Y: {spread_2y10y_bps:+.1f}bps, Risk: {brief.macro_risk_level})",
                    ticker="US10Y",
                    conviction=0.85 if brief.macro_risk_level == "CRITICAL" else 0.65,
                    expected_direction=direction,
                    payload=res_payload["metrics"],
                    ttl_seconds=3600,
                ),
                name="macro-rates-bulletin"
            )

            # The unified assessment the rest of the swarm has been subscribed to.
            #
            # agents.macro.assessment carries zero messages and has six
            # subscribers: Topics.MACRO_ASSESSMENT appears seven times in the
            # tree as a subscription and never once as a send, and
            # UnifiedMacroAssessment -- the model it would carry -- was defined
            # and never instantiated. Six agents declared a dependency on a
            # capability the system did not have, each one a consumer-group
            # assignment and a rebalance participant for a topic that has never
            # had a producer.
            #
            # Both halves of the assessment now exist: the rates brief is in
            # hand here, and the volatility surface is cached by the sub-engine
            # that produces it. Publishing what is measured -- and saying so
            # when the second half has not been -- is what makes those seven
            # subscription edges mean something.
            await self._publish_macro_assessment(brief, res_payload)

            # Confirmed only now, with a brief actually in hand.
            await self.mark_processed(dedup_key, window_seconds=DEDUP_WINDOW_SLOW_SEC)
            return res_payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Rates regime LLM error: {e}")
            return None

    async def _publish_macro_assessment(self, rates_brief, rates_payload: Dict[str, Any]) -> None:
        """Assemble and publish the unified macro assessment.

        The volatility half is read from the cache the surface sub-engine writes
        rather than being recomputed: an assessment is a view of what has been
        measured, and forcing a second inference to fill it in would spend a slot
        on a host that completes about thirty-five an hour to restate something
        already known.
        """
        try:
            vol_brief = None
            try:
                cached = await self.redis.raw.get("sentinel:options:vol_surface:latest")
                if cached:
                    vol_brief = VolatilitySurfaceBrief(
                        **json.loads(cached.decode() if isinstance(cached, bytes) else cached)
                    )
            except Exception as ve:
                logger.debug(f"No cached volatility surface for the macro assessment: {ve}")

            risk_map = {"LOW": 0.2, "MODERATE": 0.5, "ELEVATED": 0.7, "HIGH": 0.85, "CRITICAL": 0.95}
            macro_risk_score = risk_map.get(str(rates_brief.macro_risk_level).upper(), 0.5)

            catalysts = []
            spread = (rates_payload.get("metrics") or {}).get("spread_2y10y_bps")
            if spread is not None and float(spread) < 0:
                # A negative 2s10s is an inversion by definition, which is the
                # measurement the direction signal now reads instead of grepping
                # a paragraph for the word.
                catalysts.append(f"2s10s inverted at {float(spread):+.1f}bps")
            if rates_brief.credit_spread_widening_signal:
                catalysts.append(str(rates_brief.credit_spread_widening_signal))
            if vol_brief is not None and vol_brief.iv_skew_25d_bps is not None:
                catalysts.append(f"25d skew {vol_brief.iv_skew_25d_bps:+.0f}bps")

            summary = (
                f"Rates {rates_brief.curve_state}; risk {rates_brief.macro_risk_level}"
                + (f"; vol regime {vol_brief.volatility_regime}" if vol_brief else
                   "; volatility surface not measured this cycle")
            )

            payload = {
                "agent": self.name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "rates_regime": rates_brief.model_dump(),
                # Stated as absent rather than filled with a plausible default:
                # this file has already published a fabricated 375bp skew once,
                # from exactly that instinct.
                "volatility_regime": vol_brief.model_dump() if vol_brief else None,
                "macro_risk_score": macro_risk_score,
                "strategic_summary": summary,
                "key_catalysts": catalysts,
                "metrics": rates_payload.get("metrics", {}),
            }

            if self._producer:
                await self._producer.send(Topics.MACRO_ASSESSMENT, payload, key="macro")
            await self.redis.raw.set(
                "sentinel:macro:assessment:latest", json.dumps(payload), ex=86400
            )
            logger.info(
                "Published unified macro assessment (risk %.2f, %d catalyst(s), volatility %s).",
                macro_risk_score, len(catalysts), "measured" if vol_brief else "not measured",
            )
        except Exception as e:
            # An assessment that cannot be assembled must not take the rates
            # brief down with it -- that brief is the thing that took an
            # inference to produce.
            logger.warning(f"Macro assessment publish skipped: {e}")

    # ── SUB-ENGINE 2: OPTIONS VOLATILITY SURFACE ─────────────────────────────

    async def _process_volatility_surface(self, message: Dict[str, Any], ticker: str, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        if not await self._inference_budget.is_available():
            return None
        fd = message.get("financial_data") or {}
        option_type = str(raw.get("option_type") or fd.get("side") or "CALL").upper()
        size = int(raw.get("size") or raw.get("volume") or fd.get("volume") or 1)
        measured_iv = raw.get("implied_volatility") if raw.get("implied_volatility") is not None else fd.get("implied_volatility")

        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.incrby(f"sentinel:options:volume:{ticker}:{option_type}", size)
            pipe.expire(f"sentinel:options:volume:{ticker}:{option_type}", 86400)
            if measured_iv is not None:
                pipe.set(f"sentinel:options:iv:{ticker}:{option_type}", str(measured_iv), ex=86400)
            await pipe.execute()

        vals = await self.redis.raw.mget([
            f"sentinel:options:volume:{ticker}:CALL",
            f"sentinel:options:volume:{ticker}:PUT",
            f"sentinel:options:iv:{ticker}:CALL",
            f"sentinel:options:iv:{ticker}:PUT",
        ])
        calls_vol = int(vals[0] or (size if option_type == "CALL" else 100))
        puts_vol = int(vals[1] or (size if option_type == "PUT" else 100))
        pc_ratio = round(puts_vol / max(1, calls_vol), 3)

        # measured_iv is frequently absent, and this read path did not allow for
        # it while the write path four lines above already did.
        #
        # For a CALL with nothing cached, `vals[2] or (measured_iv if CALL ...)`
        # resolved to None whenever the payload carried no implied_volatility,
        # and float(None) raised. The dispatcher caught it as a transient error
        # and moved on, so the failure was invisible except as a count:
        #
        #   Transient or unhandled dispatch error: float() argument must be a
        #   string or a real number, not 'NoneType'    x516 in one day
        #
        # Every options_flow message without an IV field killed this agent's
        # dispatch, which is most of them. The engine has been producing nothing.
        call_iv = _measured_iv(vals[2], measured_iv if option_type == "CALL" else None)
        put_iv = _measured_iv(vals[3], measured_iv if option_type == "PUT" else None)

        # A skew needs both legs measured.
        #
        # This computed one regardless, from DEFAULT_CALL_IV and PUT_IV_SKEW:
        #
        #   call_iv = 0.25            (assumed)
        #   put_iv  = 0.25 * 1.15     (assumed, derived from the assumption)
        #   skew    = (0.2875 - 0.25) * 10000 = 375.0 bps, always
        #
        # and published it as a measurement. A stored brief reads
        # "iv_skew_25d_bps": -375.0 with "tail_risk_conviction": 1.0 -- maximum
        # confidence in the product of two constants. 75.5% of options events
        # carry no implied volatility at all, so this was the ordinary case
        # rather than an edge one, and the assumed 0.25 is less than half the
        # 0.5382 median of the IVs that do arrive.
        #
        # One measured leg is not enough either: a skew between a real number
        # and an assumed one measures the assumption.
        iv_skew_bps = (
            round((put_iv - call_iv) * 10000.0, 1)
            if (call_iv is not None and put_iv is not None) else None
        )
        skew_str = f"{iv_skew_bps:+.1f} bps" if iv_skew_bps is not None else "not measured"
        call_iv_str = f"{call_iv:.2%}" if call_iv is not None else "not measured"
        put_iv_str = f"{put_iv:.2%}" if put_iv is not None else "not measured"

        dedup_key = f"vol_surface_eval:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(dedup_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC):
            return None
        await self.mark_processed(dedup_key, window_seconds=DEDUP_WINDOW_MEDIUM_SEC)

        logger.info(
            "⚡ Options Vol Surface | %s | P/C Ratio: %.2f | IV Skew: %s",
            ticker, pc_ratio, skew_str,
        )

        global_context, cross_context = await asyncio.gather(
            self.fetch_global_context(),
            self.get_cross_agent_context(ticker=ticker, limit=2),
        )
        cross_block = f"\n- Cross-Agent Intelligence:\n{cross_context}" if cross_context else ""

        user_prompt = f"""
        Evaluate options surface metrics:
        - Symbol: {ticker}
        - P/C Ratio: {pc_ratio:.3f}
        - 25D IV Skew: {skew_str}
        - Call IV: {call_iv_str} | Put IV: {put_iv_str}

        GLOBAL CONTEXT:
        {global_context}
        {cross_block}
        """

        try:
            brief: VolatilitySurfaceBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Volatility Desk Analyst. Evaluate options surface metrics and tail-risk pricing. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=VolatilitySurfaceBrief,
                temperature=0.1,
            )

            # The measured value wins over whatever the model wrote, including
            # None when neither leg of the surface was observed.
            brief.iv_skew_25d_bps = iv_skew_bps

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"vol_surface_{int(time.time())}",
                "trace_id": message.get("trace_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "metrics": {
                    "ticker": ticker,
                    "put_call_volume_ratio": pc_ratio,
                    "iv_skew_25d_bps": iv_skew_bps,
                },
            }

            await self.redis.raw.set(f"sentinel:options:vol_surface:{ticker}", json.dumps(res_payload["brief"]), ex=86400)
            await self.redis.raw.set("sentinel:options:vol_surface:latest", json.dumps(res_payload["brief"]), ex=86400)

            # Publish to dedicated options topic for backwards compatibility
            await self._producer.send(Topics.VOL_SURFACE, res_payload, key=ticker)

            # Publish structured AgentBulletin
            safe_create_task(self.publish_bulletin(
                bulletin_type="alert",
                summary=f"Vol Surface {ticker}: P/C {pc_ratio:.2f}, Skew {skew_str} ({brief.volatility_regime})",
                ticker=ticker,
                conviction=brief.tail_risk_conviction,
                expected_direction="down" if pc_ratio > 1.5 else "neutral",
                payload=res_payload["metrics"],
                ttl_seconds=3600,
            ))

            # Deliberately returns nothing.
            #
            # The agent's output_topic is RATES_REGIME, and the base publishes
            # whatever a handler returns to it. This path already publishes to
            # VOL_SURFACE above, so returning the payload put a
            # VolatilitySurfaceBrief on agents.macro.rates_regime -- it was the
            # most recent message on that topic, and any consumer reading it for
            # a curve state received an options surface instead.
            return None

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Vol surface LLM error for {ticker}: {e}")
            return None

    # ── SUB-ENGINE 3: COINTEGRATION SPREAD TRACKER ───────────────────────────

    async def _process_cointegration(self, macro_asset: str, macro_price: float) -> None:
        exposed_equities = await self._get_exposed_instruments(macro_asset)
        if not exposed_equities:
            return

        keys = [f"sentinel:quotes:latest:{t}" for t in exposed_equities]
        raw_micros = await self.redis.raw.mget(keys)

        for micro_ticker, raw_micro in zip(exposed_equities, raw_micros):
            if not raw_micro:
                continue
            try:
                micro_price = float(raw_micro)
                await self._process_pair_cointegration(macro_asset, micro_ticker, macro_price, micro_price)
            except (ValueError, TypeError):
                continue

    async def _get_exposed_instruments(self, macro_asset: str) -> List[str]:
        min_confidence = float(os.getenv("MIN_EXPOSURE_EDGE_CONFIDENCE", "0.6"))
        cache_key = f"sentinel:cache:exposure:{macro_asset}:{min_confidence}"
        cached_raw = await self.redis.raw.get(cache_key)
        if cached_raw:
            return json.loads(cached_raw)

        try:
            neo4j_client = await get_neo4j()
            query = """
            MATCH (c:Entity {id: $macro_asset})-[r:COMMODITY_EXPOSURE|SUPPLIES|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO*1..2]-(e:Entity {type: 'instrument'})
            WHERE ALL(rel IN r WHERE coalesce(rel.confidence, 0.0) >= $min_confidence)
            RETURN DISTINCT e.id AS exposed_ticker
            """
            rows = await neo4j_client.query(query, {"macro_asset": macro_asset, "min_confidence": min_confidence})
            exposed = [r["exposed_ticker"] for r in rows if r.get("exposed_ticker")]

            # Fail closed: return empty list if graph has no exposure edge, rather than
            # pairing macro assets with random unconstrained quotes scanned from Redis.

            await self.redis.raw.set(cache_key, json.dumps(exposed), ex=GRAPH_CACHE_TTL)
            return exposed
        except Exception as e:
            logger.debug(f"Exposure lookup error for {macro_asset}: {e}")
            return []

    async def _process_pair_cointegration(self, macro_asset: str, micro_ticker: str, macro_price: float, micro_price: float) -> None:
        series_key = f"sentinel:cointeg:{macro_asset}:{micro_ticker}"
        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.rpush(f"{series_key}:x", macro_price)
            pipe.rpush(f"{series_key}:y", micro_price)
            pipe.ltrim(f"{series_key}:x", -300, -1)
            pipe.ltrim(f"{series_key}:y", -300, -1)
            pipe.lrange(f"{series_key}:x", 0, -1)
            pipe.lrange(f"{series_key}:y", 0, -1)
            results = await pipe.execute()

        raw_x, raw_y = results[4], results[5]
        if len(raw_x) < 30:
            return

        x_vec = [float(v) for v in raw_x]
        y_vec = [float(v) for v in raw_y]

        # Dynamic Pearson Correlation Matrix calculation (Oil vs. Equities margin compression)
        try:
            x_arr = np.array(x_vec)
            y_arr = np.array(y_vec)
            x_returns = np.diff(np.log(np.maximum(1e-4, x_arr)))
            y_returns = np.diff(np.log(np.maximum(1e-4, y_arr)))
            if len(x_returns) >= MIN_CORRELATION_RETURNS and _both_series_move(x_vec, y_vec):
                corr_matrix = np.corrcoef(x_returns, y_returns)
                pearson_corr = float(corr_matrix[0, 1])
                if (not np.isnan(pearson_corr)
                        and pearson_corr <= INVERSE_CORRELATION_THRESHOLD
                        and _is_significant(pearson_corr, len(x_returns))):
                    reasoning = f"Strong inverse correlation ({pearson_corr:.2f}) detected: Rising {macro_asset} (${x_vec[-1]:.2f}) is exerting margin pressure on {micro_ticker} (${y_vec[-1]:.2f})."
                    logger.warning(f"🔴 DYNAMIC MACRO SHOCK | {reasoning}")
                    await self.redis.raw.set(
                        f"sentinel:macro:inverse_correlation:{macro_asset}:{micro_ticker}",
                        json.dumps({
                            "macro_asset": macro_asset,
                            "equity_ticker": micro_ticker,
                            "pearson_correlation": round(pearson_corr, 4),
                            "macro_price": x_vec[-1],
                            "equity_price": y_vec[-1],
                            "context": reasoning,
                            "detected_at": datetime.now(timezone.utc).isoformat()
                        }),
                        ex=3600
                    )
                    # Publish AgentBulletin
                    safe_create_task(self.publish_bulletin(
                        bulletin_type="alert",
                        summary=f"Oil/Equity Decoupling ({macro_asset} vs {micro_ticker}): Pearson Corr {pearson_corr:.2f}",
                        conviction=abs(pearson_corr),
                        expected_direction="bearish",
                        payload={"macro_asset": macro_asset, "equity": micro_ticker, "correlation": pearson_corr, "reasoning": reasoning},
                        ttl_seconds=3600,
                    ))
        except Exception as corr_err:
            logger.debug(f"Pearson correlation calculation error for {macro_asset}/{micro_ticker}: {corr_err}")

        # Use robust quant_calc Engle-Granger module
        eg_result = quant_calc.engle_granger_cointegration(x_vec, y_vec)
        if eg_result.get("is_cointegrated"):
            beta = eg_result.get("beta", 0.0)
            alpha = eg_result.get("alpha", 0.0)
            half_life = eg_result.get("half_life", 999.0)

            curr_res = y_vec[-1] - (beta * x_vec[-1] + alpha)
            spread_std = eg_result.get("spread_std", 1.0)

            # Degenerate spread guard: skip signal emission if spread std dev is below threshold (1e-4)
            if spread_std < 1e-4:
                return

            z_score = float(np.clip(curr_res / spread_std, -5.0, 5.0))

            if abs(z_score) >= 2.5:
                payload = {
                    "macro_asset": macro_asset,
                    "micro_ticker": micro_ticker,
                    "z_score": round(z_score, 2),
                    "beta": beta,
                    "half_life_periods": half_life,
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                }
                if macro_asset in TICKER_METADATA_REGISTRY:
                    payload["macro_asset_name"] = TICKER_METADATA_REGISTRY[macro_asset]
                if micro_ticker in TICKER_METADATA_REGISTRY:
                    payload["micro_ticker_name"] = TICKER_METADATA_REGISTRY[micro_ticker]

                # Published to the topic only.
                #
                # This also wrote sentinel:macro:decoupling:latest, which
                # nothing read. Unlike the macro brief, there is no case for
                # ambient access here: MACRO_DECOUPLING has two live consumers
                # and 33,667 messages through it, and a decoupling is an event
                # about a moment rather than a standing state worth carrying
                # into every prompt. The last one is only "latest" until the
                # next tick.
                await self._producer.send(Topics.MACRO_DECOUPLING, payload, key=macro_asset)

    # ── SUB-ENGINE 4: SCHEDULED MACRO TREND REVIEWS ──────────────────────────

    async def _run_scheduled_macro_review(self):
        await asyncio.sleep(5)
        while True:
            try:
                await self._run_macro_review_now()
            except Exception as e:
                logger.error(f"Scheduled macro review failed: {e}", exc_info=True)
            await asyncio.sleep(300)

    async def _run_macro_review_now(self, trigger_event: Optional[dict] = None):
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        if not await self._inference_budget.is_available():
            return None
        logger.info("Initiating Master Macro Trend Review...")
        cooccurrence = await self.redis.raw.zrevrange("sentinel:ontology:cooccurrence", 0, 10, withscores=True)

        query = """
            SELECT type, COUNT(*) as volume, AVG(anomaly_score) as avg_anomaly
            FROM events 
            WHERE occurred_at > NOW() - INTERVAL '24 hours'
            GROUP BY type
        """
        try:
            event_type_metrics = await self.db.query(query)
        except Exception as e:
            # None, not []. An empty list is a claim -- "nothing happened in the
            # last 24 hours" -- and this branch does not know that. It knows the
            # query failed. Rendered below as an explicit unavailability so the
            # model is not told the world is calm because the database was down.
            self.logger.warning("Event-type metrics query failed: %s", e)
            event_type_metrics = None

        # Real sector-level metrics (populated once Phase 2 ref data is live)
        sector_query = """
            SELECT financial_data->>'sector' as sector, COUNT(*) as volume, 
                   AVG(anomaly_score) as avg_anomaly
            FROM events 
            WHERE occurred_at > NOW() - INTERVAL '24 hours'
              AND financial_data->>'sector' IS NOT NULL
              AND financial_data->>'sector' != ''
            GROUP BY financial_data->>'sector'
            ORDER BY avg_anomaly DESC
        """
        try:
            sector_metrics = await self.db.query(sector_query)
        except Exception as e:
            self.logger.warning("Sector risk metrics query failed: %s", e)
            sector_metrics = None

        global_context, agent_memories = await asyncio.gather(
            self.fetch_global_context(),
            self.read_agent_memories(limit=10)
        )

        user_prompt = f"""=== SYSTEMIC MACRO INTELLIGENCE REVIEW ===
Ontology Co-occurrences: {cooccurrence}
Event Type Distribution Metrics: {_render_metrics(event_type_metrics, "event-type distribution")}
Sector Risk Metrics: {_render_metrics(sector_metrics, "sector risk")}
Global Macro Context: {global_context}
{agent_memories}

DIRECTIVES:
Synthesize macro trends, rate shifts, and sector risks into an IntelBrief JSON.
Required fields: "headline", "summary", "primary_entity", "geopolitical_theater", "geographic_hotspots", "entities", "graph_triples", "severity" (1-5), "tags".
Return raw JSON matching schema:"""

        run_id = f"macro_review_{int(time.time())}"
        msg = {"event_id": run_id, "trace_id": trigger_event.get("trace_id") if trigger_event else "auto"}

        try:
            from services.agents.knowledge_graph_engine import IntelBrief
            response = await self._execute_with_telemetry(
                message=msg,
                system_prompt="You are SENTINEL Chief Macro Strategist. Synthesize systemic macroeconomic, rate regime, and geopolitical spillovers into a high-density intelligence brief. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=IntelBrief,
                temperature=0.1,
            )

            brief_payload = {
                "agent": self.name,
                "agent_run_id": run_id,
                "trace_id": msg["trace_id"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": response.model_dump() if hasattr(response, "model_dump") else response.dict(),
                "computed_severity": getattr(response, "severity", 3),
            }

            await self.redis.raw.set("sentinel:macro:latest_brief", json.dumps(brief_payload), ex=86400)
            await self._producer.send(Topics.INTEL_BRIEFS, brief_payload, key=run_id)
            logger.info("📊 Master Strategic Macro Brief published successfully.")
        except Exception as e:
            logger.error(f"Macro review failed: {e}")
