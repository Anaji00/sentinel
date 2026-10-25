"""
shared/utils/trading_costs.py

What a trade costs at the size it would be taken, per stock. Pure; the
recommendation engine supplies bars and the forecast.

A flat 0.2% round trip flattered small, volatile names and charged large ones
too much. Costs here have two parts, both measured per name:

  spread   the stock's own effective bid-ask spread, estimated from daily
           close, high and low (Abdi & Ranaldo 2017): the close sits half a
           spread from the day's mid-range on average, so
           S^2 = 4 E[(c_t - eta_t)(c_t - eta_t+1)], c the log close and eta the
           mean of log high and log low, averaged over the window and floored
           at zero (the paper's less biased variant). Day-to-day noise in the
           estimate is about the daily volatility, which swamps a one-basis-
           point spread, so for names trading $50M+ a day the liquidity tier the
           paper ledger uses stands; below that the estimate counts, never under
           the tier. Where bars are too few, the tier stands in.
  impact   square-root market impact, Y x daily sigma x sqrt(Q / ADV), paid on
           the way in and out. The law holds across markets and sizes (Toth et
           al. 2011; Frazzini, Israel & Moskowitz 2018 on a large manager's own
           fills); Y = 1 as in the paper ledger, so the two agree.

Size then follows from the cost. Half-Kelly chooses the weight w maximising
w (|alpha| - c(w)) - w^2 var / (2 k), with c(w) growing with w through the
impact term; solved by bisection on its derivative. Capacity is the position at
which impact alone takes the whole edge left after the spread.
"""
from __future__ import annotations

import math
from typing import Dict, Optional, Sequence

from shared.utils.paper_ledger import half_spread_bps

IMPACT_Y = 1.0
REFERENCE_ACCOUNT_USD = 1_000_000.0   # the account a stated weight is a share of
SPREAD_WINDOW = 63
MIN_SPREAD, MAX_SPREAD = 0.0001, 0.05
UNKNOWN_ONE_SIDE = 0.005               # unknown liquidity: priced as illiquid (50 bps a side)
LIQUID_ADV_USD = 5e7                   # above this the tier's spread stands (see the module note)


def effective_spread(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float],
                     window: int = SPREAD_WINDOW) -> Optional[float]:
    """Abdi-Ranaldo proportional spread over the last `window` sessions, or None."""
    n = min(len(highs), len(lows), len(closes))
    if n < 22:
        return None
    k = min(n, window + 1)                     # window pairs need window + 1 sessions
    hs, ls, cs = list(highs)[n - k:n], list(lows)[n - k:n], list(closes)[n - k:n]
    terms = []
    for i in range(len(cs) - 1):
        h0, l0, c0, h1, l1 = hs[i], ls[i], cs[i], hs[i + 1], ls[i + 1]
        if not all(x and x > 0 for x in (h0, l0, c0, h1, l1)):
            continue
        c = math.log(c0)
        eta0 = 0.5 * (math.log(h0) + math.log(l0))
        eta1 = 0.5 * (math.log(h1) + math.log(l1))
        terms.append(4.0 * (c - eta0) * (c - eta1))
    if len(terms) < 20:
        return None
    return min(MAX_SPREAD, max(MIN_SPREAD, math.sqrt(max(sum(terms) / len(terms), 0.0))))


def half_spread(adv_usd: Optional[float], spread: Optional[float]) -> float:
    """Half the spread as a fraction: the liquidity tier, or for names under
    LIQUID_ADV_USD the stock's own estimate when it is wider."""
    if not adv_usd or adv_usd <= 0:
        return UNKNOWN_ONE_SIDE if spread is None else max(spread / 2.0, UNKNOWN_ONE_SIDE)
    tier = half_spread_bps(adv_usd) / 1e4
    if spread is None or adv_usd >= LIQUID_ADV_USD:
        return tier
    return max(tier, spread / 2.0)


def round_trip(notional: float, adv_usd: Optional[float], daily_vol: Optional[float],
               spread: Optional[float] = None) -> float:
    """In and out, as a fraction of the position: the spread plus impact twice."""
    if not adv_usd or adv_usd <= 0:
        return 2 * UNKNOWN_ONE_SIDE
    sigma = daily_vol if daily_vol and daily_vol > 0 else 0.02
    return 2.0 * (half_spread(adv_usd, spread) + IMPACT_Y * sigma * math.sqrt(max(notional, 0.0) / adv_usd))


def capacity_usd(edge: float, adv_usd: Optional[float], daily_vol: Optional[float],
                 spread: Optional[float] = None, extra_cost: float = 0.0) -> Optional[float]:
    """The position at which the round trip (and `extra_cost`, e.g. borrow)
    takes the whole expected edge; 0 when the spread alone does."""
    if not adv_usd or adv_usd <= 0 or not daily_vol or daily_vol <= 0:
        return None
    room = abs(edge) - 2.0 * half_spread(adv_usd, spread) - extra_cost
    if room <= 0:
        return 0.0
    return adv_usd * (room / (2.0 * IMPACT_Y * daily_vol)) ** 2


def sized(alpha_h: Optional[float], sigma_annual: Optional[float], horizon_days: int, adv_usd: Optional[float],
          daily_vol: Optional[float], spread: Optional[float] = None, extra_cost: float = 0.0,
          account_usd: float = REFERENCE_ACCOUNT_USD, fraction: float = 0.5, cap: float = 0.05) -> Dict[str, float]:
    """Half-Kelly weight with costs that grow with it, the round-trip cost at
    that weight (with `extra_cost`, e.g. borrow over the hold), and capacity."""
    out = {"weight": 0.0, "cost": round_trip(0.0, adv_usd, daily_vol, spread) + extra_cost,
           "capacity_usd": capacity_usd(alpha_h or 0.0, adv_usd, daily_vol, spread, extra_cost) or 0.0}
    if alpha_h is None or not sigma_annual or sigma_annual <= 0:
        return out
    var = sigma_annual * sigma_annual * horizon_days / 252.0
    a = abs(alpha_h)

    def cost(w: float) -> float:
        return round_trip(w * account_usd, adv_usd, daily_vol, spread) + extra_cost

    def slope(w: float) -> float:
        # d/dw [w (a - c(w))] - w var / k; the impact part of c grows as sqrt(w),
        # so w c'(w) is half of it.
        impact = cost(w) - cost(0.0)
        return a - cost(w) - 0.5 * impact - w * var / fraction

    if slope(0.0) <= 0:
        return out
    if slope(cap) >= 0:
        w = cap
    else:
        lo, hi = 0.0, cap
        for _ in range(50):
            mid = 0.5 * (lo + hi)
            if slope(mid) > 0:
                lo = mid
            else:
                hi = mid
        w = lo
    out.update(weight=round(w, 4), cost=cost(w))
    return out


__all__ = ["LIQUID_ADV_USD", "IMPACT_Y", "REFERENCE_ACCOUNT_USD", "capacity_usd", "effective_spread", "half_spread", "round_trip",
           "sized"]
