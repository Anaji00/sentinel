"""One definition of what regime the market is in.

Statistics in this platform are computed over fixed windows with no notion of
whether the window spans a regime break. A GARCH estimate straddling a policy
shift describes neither side of it, a correlation learned under inversion is
not evidence about the same pair under steepening, and Kelly treats whatever
win rate it is handed as the true probability of the next bet.

The machinery to avoid that was already here and did not work.
`SentinelAgent.current_regime()` reads `regime`, `rates_regime` or `state` out
of the cached rates brief and falls back to "unknown". `RatesRegimeBrief`
defines none of those fields -- its keys are `curve_state`,
`yield_spread_2y10y_bps`, `breakeven_inflation_bps`, `tips_yield`,
`credit_spread_widening_signal`, `regime_summary`, `macro_risk_level` and
`recommended_hedging`. Verified against the live payload: the reader looked for
three keys the writer has never produced, so **it returned "unknown" on every
call since it was written**, and the regime-partitioned scorecards behind it
have never partitioned anything.

That is the same defect as the telemetry worker reading five keys the wargamer
never sent, recorded earlier in this audit, in the path that decides position
size.

The label is derived from the number rather than read from prose, for the
reason the direction signal was: `curve_state` is declared as one of four
values and live it holds "2Y Yield: 4.390% | 10Y Yield: 4.800%", while
`yield_spread_2y10y_bps` sits in the same object carrying 41.0. A model filling
a free string is not a measurement; the spread is.
"""
from __future__ import annotations

import json
import logging

from shared.utils.quiet_failures import swallowed
from typing import Any, Dict, Optional

logger = logging.getLogger("sentinel.regime")

REGIME_KEY = "sentinel:macro:rates_regime:latest"

# Curve shape, in basis points of 2s10s. Conventional boundaries: a negative
# spread is an inverted curve, a spread inside a few basis points of zero is
# flat, and the rest is a normal upward slope. The wide band is deliberate --
# an estimate that flips between two regimes on a one-basis-point move would
# repartition every scorecard daily and learn nothing from either half.
INVERTED_BPS = 0.0
FLAT_BPS = 25.0
STEEP_BPS = 150.0

UNKNOWN = "unknown"


def regime_from_spread(spread_bps: Optional[float]) -> str:
    """The curve regime implied by the 2s10s spread.

    Returns `unknown` for a missing spread rather than guessing a shape. An
    unknown regime falls back to the unpartitioned scorecard, which is the
    honest behaviour: no history for this regime is different from history
    saying the regime does not matter.
    """
    if spread_bps is None:
        return UNKNOWN
    try:
        bps = float(spread_bps)
    except (TypeError, ValueError):
        return UNKNOWN
    if bps != bps:  # NaN
        return UNKNOWN
    if bps < INVERTED_BPS:
        return "inverted"
    if bps < FLAT_BPS:
        return "flat"
    if bps < STEEP_BPS:
        return "normal_steepening"
    return "steep"


def regime_from_brief(brief: Optional[Dict[str, Any]]) -> str:
    """The regime a cached rates brief implies.

    Prefers an explicitly stamped `regime`, so a future writer that computes one
    is authoritative; otherwise derives it from the spread. Never parses
    `curve_state`: it is declared as four values and holds a rendered sentence.
    """
    if not isinstance(brief, dict):
        return UNKNOWN
    stamped = brief.get("regime")
    if isinstance(stamped, str) and stamped.strip():
        return stamped.strip().lower().replace(" ", "_")
    return regime_from_spread(brief.get("yield_spread_2y10y_bps"))


async def current_regime(redis_client) -> str:
    """The prevailing regime, from the shared cache, or `unknown`.

    One implementation, so the agent tier, the correlation layer and anything
    else that needs to gate a statistic answer the question the same way rather
    than each deriving it and disagreeing -- which is how four components came
    to hold four different opinions about an event's domain.
    """
    if redis_client is None:
        return UNKNOWN
    try:
        raw = getattr(redis_client, "raw", redis_client)
        blob = await raw.get(REGIME_KEY)
        if not blob:
            return UNKNOWN
        brief = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
    except Exception as e:
        swallowed("utils.regime.current_regime", e, logger)
        return UNKNOWN
    return regime_from_brief(brief)


def stamp(brief: Dict[str, Any]) -> Dict[str, Any]:
    """Add the derived regime to a brief before it is cached.

    Written at the producer rather than computed at each reader, so the label a
    statistic was gated on is recoverable from the stored record instead of
    being re-derived later against a curve that has since moved.
    """
    if not isinstance(brief, dict):
        return brief
    brief = dict(brief)
    brief["regime"] = regime_from_spread(brief.get("yield_spread_2y10y_bps"))
    return brief
