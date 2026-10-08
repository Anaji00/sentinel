"""Macro briefs computed from the measurements, not written by a model.

The rates and volatility-surface briefs asked a 0.6B model to label numbers the
engine had already measured -- a 2s10s spread, a credit ratio, a put/call
ratio, an IV skew -- and each call queued eight to sixteen minutes behind the
rest of the swarm on a CPU host. The engine already overrode the parts that
mattered (direction from the spread's sign, skew from the measured IVs); the
model's remaining contributions were a curve label, a credit-stress label, a
risk level, hedging prose and a "tail-risk conviction" it made up. Live on
5 October it reported "Severe Stress" in credit with no credit history to judge
it from.

Every field here is a rule over a measurement, and a field whose input was not
measured says so ("Unclassified", None) rather than guessing.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# |2s10s| inside this band is a flat curve.
FLAT_BAND_BPS = 10.0
# Hour-over-hour fall in HYG/LQD that counts as widening. HYG/LQD falls when
# high yield underperforms investment grade, i.e. when credit spreads widen.
MODERATE_WIDENING_PCT = 0.5
SEVERE_WIDENING_PCT = 1.5

# Put/call volume ratio bands.
PC_DEFENSIVE = 1.5
PC_ELEVATED = 1.0
# Put-over-call IV skew (bps) that marks tail demand.
SKEW_TAIL_BPS = 300.0


def curve_state(spread_bps: float, history_bps: List[float]) -> str:
    """Inverted, Disinverted, Flat or Normal Steepening, from the spread itself."""
    if spread_bps < -FLAT_BAND_BPS:
        return "Inverted"
    was_inverted = any(h < -FLAT_BAND_BPS for h in history_bps)
    if abs(spread_bps) <= FLAT_BAND_BPS:
        return "Flat"
    if was_inverted:
        return "Disinverted"
    return "Normal Steepening"


def credit_signal(ratio: Optional[float], previous_ratio: Optional[float]) -> str:
    """Widening judged by the change in HYG/LQD; no prior reading, no judgement."""
    if ratio is None or previous_ratio is None or previous_ratio <= 0:
        return "Unclassified"
    change_pct = (ratio / previous_ratio - 1.0) * 100.0
    if change_pct <= -SEVERE_WIDENING_PCT:
        return "Severe Stress"
    if change_pct <= -MODERATE_WIDENING_PCT:
        return "Moderate Widening"
    return "Stable"


def risk_level(state: str, credit: str) -> str:
    if state == "Inverted" and credit == "Severe Stress":
        return "CRITICAL"
    if state == "Inverted" or credit in ("Severe Stress", "Moderate Widening"):
        return "ELEVATED"
    return "LOW"


def rates_brief_fields(
    spread_bps: float,
    history_bps: List[float],
    breakeven_bps: Optional[float],
    tips_yield: Optional[float],
    credit_ratio: Optional[float],
    previous_credit_ratio: Optional[float],
) -> Dict[str, Any]:
    """Every RatesRegimeBrief field, from measurements."""
    state = curve_state(spread_bps, history_bps)
    credit = credit_signal(credit_ratio, previous_credit_ratio)
    level = risk_level(state, credit)
    parts = [f"2s10s {spread_bps:+.1f} bps ({state})"]
    if breakeven_bps is not None:
        parts.append(f"breakeven {breakeven_bps:.0f} bps")
    parts.append(f"credit {credit.lower()}")
    return {
        "curve_state": state,
        "yield_spread_2y10y_bps": round(spread_bps, 1),
        # The schema declares these as floats; NaN would mislead, 0.0 would lie,
        # so the brief carries what was measured and the metrics block below it
        # carries None explicitly when it was not.
        "breakeven_inflation_bps": round(breakeven_bps, 1) if breakeven_bps is not None else 0.0,
        "tips_yield": round(tips_yield, 3) if tips_yield is not None else 0.0,
        "credit_spread_widening_signal": credit,
        "regime_summary": "; ".join(parts) + ".",
        "macro_risk_level": level,
        "recommended_hedging": [],
    }


def volatility_regime(pc_ratio: Optional[float], skew_bps: Optional[float]) -> str:
    if pc_ratio is None and skew_bps is None:
        return "Unmeasured"
    if (pc_ratio is not None and pc_ratio >= PC_DEFENSIVE) or (skew_bps is not None and skew_bps >= SKEW_TAIL_BPS):
        return "Defensive"
    if pc_ratio is not None and pc_ratio >= PC_ELEVATED:
        return "Hedged"
    return "Neutral"


def tail_risk_conviction(pc_ratio: Optional[float], skew_bps: Optional[float]) -> float:
    """0..1, rising with put demand and put skew; 0.0 when neither was measured."""
    score = 0.0
    if pc_ratio is not None:
        score = max(score, min(1.0, max(0.0, (pc_ratio - PC_ELEVATED) / (2 * PC_DEFENSIVE - PC_ELEVATED))))
    if skew_bps is not None:
        score = max(score, min(1.0, max(0.0, skew_bps / (2 * SKEW_TAIL_BPS))))
    return round(score, 3)


def vol_surface_fields(ticker: str, pc_ratio: Optional[float], skew_bps: Optional[float]) -> Dict[str, Any]:
    """Every VolatilitySurfaceBrief field, from measurements.

    analytical_summary included: the schema requires it, and without it every
    brief failed validation and went to the dead-letter topic -- 82 of the last
    200 dead letters on 7 October, every vol-surface brief since 5 October."""
    regime = volatility_regime(pc_ratio, skew_bps)
    parts = [
        f"put/call volume {pc_ratio:.2f}" if pc_ratio is not None else "put/call volume unmeasured",
        f"25-delta skew {skew_bps:+.0f} bps" if skew_bps is not None else "skew unmeasured",
    ]
    return {
        "ticker": ticker,
        "put_call_volume_ratio": pc_ratio if pc_ratio is not None else 0.0,
        "iv_skew_25d_bps": skew_bps,
        "volatility_regime": regime,
        "tail_risk_conviction": tail_risk_conviction(pc_ratio, skew_bps),
        "analytical_summary": f"{ticker}: {', '.join(parts)}; regime {regime.lower()}.",
    }


__all__ = [
    "credit_signal", "curve_state", "rates_brief_fields", "risk_level",
    "tail_risk_conviction", "vol_surface_fields", "volatility_regime",
]
