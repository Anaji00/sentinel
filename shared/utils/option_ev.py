"""
shared/utils/option_ev.py

Which option, if any, is worth buying or selling for a forecast. Pure; the
recommendation engine supplies the live chain and the forecast.

Each candidate is priced under the model's own distribution for the stock --
lognormal, drifting at the forecast's expected return, with the stock's own
realised volatility -- and set against what it actually costs: bought at the
ask, sold at the bid. The expected value per contract, the probability it
ends in profit and the return on the capital at risk follow in closed form.

Why not fixed deltas: a strike is only worth buying when the forecast move
pays for its premium. Options are priced at implied volatility, which on
average exceeds the volatility that follows (the volatility risk premium), so
most bought options lose; this picks the ones the forecast says are cheap and
declines when none is. Where implied is far above the stock's own movement it
favours selling premium in a defined-risk spread (Goyal & Saretto 2009), and
where implied is far below it favours buying (both fall out of the same
arithmetic, not a rule). Held to expiry: early exercise and path are ignored.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

CONTRACT = 100
MIN_PROB_PROFIT = 0.25          # long shots are not ideas
MIN_EV_PER_RISK = 0.05          # 5% expected return on the capital at risk
KELLY_FRACTION = 0.5            # half Kelly: estimates are noisy, overbetting is costlier than underbetting
MAX_OPTION_BOOK_SHARE = 0.02    # no option idea above 2% of the account at risk
MAX_ABS_DELTA = 0.85            # no leg deeper in the money than this


def _ncdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True)
class Quote:
    symbol: str
    right: str          # "C" or "P"
    strike: float
    expiry: str
    bid: float
    ask: float
    delta: Optional[float] = None
    iv: Optional[float] = None


def expected_payoff(right: str, s: float, k: float, t: float, mu: float, sigma: float) -> float:
    """E[max(S_T - K, 0)] (call) or E[max(K - S_T, 0)] (put) per share, with
    S_T lognormal at annual drift `mu` and volatility `sigma` over `t` years."""
    if t <= 0 or sigma <= 0:
        st = s * math.exp(mu * max(t, 0.0))
        return max(st - k, 0.0) if right == "C" else max(k - st, 0.0)
    sd = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (mu + 0.5 * sigma * sigma) * t) / sd
    d2 = d1 - sd
    fwd = s * math.exp(mu * t)
    if right == "C":
        return fwd * _ncdf(d1) - k * _ncdf(d2)
    return k * _ncdf(-d2) - fwd * _ncdf(-d1)


def prob_above(level: float, s: float, t: float, mu: float, sigma: float) -> float:
    """P(S_T > level)."""
    if level <= 0:
        return 1.0
    if t <= 0 or sigma <= 0:
        return 1.0 if s * math.exp(mu * max(t, 0.0)) > level else 0.0
    sd = sigma * math.sqrt(t)
    return _ncdf((math.log(s / level) + (mu - 0.5 * sigma * sigma) * t) / sd)


def _structure(name: str, legs: Sequence[Tuple[str, Quote]], s: float, t: float, mu: float,
               sigma: float) -> Optional[Dict[str, Any]]:
    """EV, risk, breakeven and probability of profit for a set of (side, quote)
    legs on one expiry: bought at the ask, sold at the bid."""
    cost = 0.0             # per share, positive = debit
    ev_payoff = 0.0
    for side, q in legs:
        price = q.ask if side == "buy" else q.bid
        if price <= 0:
            return None
        sign = 1.0 if side == "buy" else -1.0
        cost += sign * price
        ev_payoff += sign * expected_payoff(q.right, s, q.strike, t, mu, sigma)
    ev = (ev_payoff - cost) * CONTRACT
    strikes = sorted(q.strike for _s, q in legs)
    if len(legs) == 1:
        q = legs[0][1]
        max_loss = cost * CONTRACT
        be = q.strike + cost if q.right == "C" else q.strike - cost
        p_win = prob_above(be, s, t, mu, sigma) if q.right == "C" else 1.0 - prob_above(be, s, t, mu, sigma)
        max_gain = None if q.right == "C" else (q.strike - cost) * CONTRACT
    else:
        width = strikes[-1] - strikes[0]
        if cost > 0:                                # debit spread
            if cost >= width:
                return None
            max_loss, max_gain = cost * CONTRACT, (width - cost) * CONTRACT
        else:                                       # credit spread
            credit = -cost
            if credit >= width:
                return None
            max_loss, max_gain = (width - credit) * CONTRACT, credit * CONTRACT
        long_leg = next(q for side, q in legs if side == "buy")
        short_leg = next(q for side, q in legs if side == "sell")
        bullish = (long_leg.right == "C" and long_leg.strike < short_leg.strike) or \
                  (long_leg.right == "P" and long_leg.strike < short_leg.strike)
        if cost > 0:
            be = (long_leg.strike + cost) if long_leg.right == "C" else (long_leg.strike - cost)
        else:
            be = (short_leg.strike - (-cost)) if short_leg.right == "P" else (short_leg.strike + (-cost))
        up = prob_above(be, s, t, mu, sigma)
        p_win = up if bullish else 1.0 - up
    if max_loss <= 0:
        return None
    return {"structure": name, "legs": [{"symbol": q.symbol, "right": q.right, "side": side, "strike": q.strike,
                                         "expiry": q.expiry, "price": q.ask if side == "buy" else q.bid,
                                         "delta": q.delta, "iv": q.iv} for side, q in legs],
            "net": round(-cost * CONTRACT, 2), "max_loss": round(max_loss, 2),
            "max_gain": round(max_gain, 2) if max_gain is not None else None, "breakeven": round(be, 2),
            "ev": round(ev, 2), "ev_per_risk": round(ev / max_loss, 4), "prob_profit": round(p_win, 3)}


def candidates(direction: int, quotes: Iterable[Quote], s: float, t: float, mu: float, sigma: float,
               max_strikes: int = 12) -> List[Dict[str, Any]]:
    """Every long option, debit spread and credit spread in the forecast's
    direction on one expiry, near the money, evaluated."""
    def live(q: Quote) -> bool:
        # Deep in-the-money quotes are thin and often stale (a 96%-certain
        # $10 call spread offered at $6.72 on MU, 17 Oct): no edge is real there,
        # and a contract that deep is the stock with a spread on it.
        # The quote's own delta, or (some snapshots carry no greeks) one at
        # the forecast volatility.
        if not 0.6 * s <= q.strike <= 1.4 * s:
            return False
        d = q.delta
        if d is None and t > 0 and sigma > 0:
            d1 = (math.log(s / q.strike) + 0.5 * sigma * sigma * t) / (sigma * math.sqrt(t))
            d = _ncdf(d1) if q.right == "C" else _ncdf(d1) - 1.0
        return d is None or abs(d) <= MAX_ABS_DELTA

    calls = sorted((q for q in quotes if q.right == "C" and live(q)), key=lambda q: q.strike)
    puts = sorted((q for q in quotes if q.right == "P" and live(q)), key=lambda q: q.strike)

    def near(qs: List[Quote]) -> List[Quote]:
        return sorted(qs, key=lambda q: abs(q.strike - s))[:max_strikes]

    calls, puts = sorted(near(calls), key=lambda q: q.strike), sorted(near(puts), key=lambda q: q.strike)
    out: List[Dict[str, Any]] = []
    if direction > 0:
        for q in calls:
            out.append(_structure("long_call", [("buy", q)], s, t, mu, sigma))
        for i, lo in enumerate(calls):
            for hi in calls[i + 1:]:
                out.append(_structure("bull_call_debit", [("buy", lo), ("sell", hi)], s, t, mu, sigma))
        for i, lo in enumerate(puts):
            for hi in puts[i + 1:]:
                out.append(_structure("bull_put_credit", [("sell", hi), ("buy", lo)], s, t, mu, sigma))
    else:
        for q in puts:
            out.append(_structure("long_put", [("buy", q)], s, t, mu, sigma))
        for i, lo in enumerate(puts):
            for hi in puts[i + 1:]:
                out.append(_structure("bear_put_debit", [("buy", hi), ("sell", lo)], s, t, mu, sigma))
        for i, lo in enumerate(calls):
            for hi in calls[i + 1:]:
                out.append(_structure("bear_call_credit", [("sell", lo), ("buy", hi)], s, t, mu, sigma))
    return [c for c in out if c is not None]


def settle(idea: Dict[str, Any], close_at_expiry: float) -> Optional[Dict[str, float]]:
    """An option idea held to expiry: profit or loss per contract (after the
    entry prices it was logged at) and that over its maximum loss."""
    legs = idea.get("legs") or []
    if not legs or close_at_expiry is None or close_at_expiry <= 0:
        return None
    pnl = float(idea.get("net") or 0.0)
    for lg in legs:
        k = float(lg["strike"])
        payoff = max(close_at_expiry - k, 0.0) if lg["right"] == "C" else max(k - close_at_expiry, 0.0)
        pnl += (1.0 if lg["side"] == "buy" else -1.0) * payoff * CONTRACT
    risk = float(idea.get("max_loss") or 0.0)
    return {"pnl": round(pnl, 2), "return_on_risk": round(pnl / risk, 4) if risk > 0 else None}


def kelly_share(c: Dict[str, Any], s: float, t: float, mu: float, sigma: float) -> float:
    """Half-Kelly share of the account to put at risk on one option idea,
    treated as a bet that wins its expected gain-when-right or loses its
    maximum: f = p - (1 - p) / b, halved and capped."""
    p = c["prob_profit"]
    risk = c["max_loss"]
    if risk <= 0 or p <= 0:
        return 0.0
    # Expected gain conditional on winning, from EV = p * G - (1 - p) * L_avg,
    # with the average loss approximated by the maximum (conservative).
    gain = (c["ev"] + (1 - p) * risk) / p
    b = gain / risk
    if b <= 0:
        return 0.0
    f = p - (1 - p) / b
    return round(max(0.0, min(MAX_OPTION_BOOK_SHARE, KELLY_FRACTION * f)), 4)


def drift_to(t: float, mu: float, edge_years: Optional[float], base_drift: float) -> float:
    """Annual drift over `t` years when the forecast's edge (`mu` above
    `base_drift`) lasts only `edge_years`: past its horizon the stock is
    expected to drift with the market, not keep earning the edge."""
    if edge_years is None or t <= edge_years or t <= 0:
        return mu
    return base_drift + (mu - base_drift) * edge_years / t


def vol_to(t: float, days: int, sigma: float, jump: Optional[Tuple[int, float]]) -> float:
    """Annual volatility over an option's `t` years (`days` calendar days):
    the diffusion `sigma`, plus the earnings move (calendar days until the
    release, typical absolute move) spread over the option's own life when
    the release falls before expiry."""
    if not jump or jump[0] < 0 or days < jump[0] or t <= 0:
        return sigma
    return math.sqrt(sigma * sigma + jump[1] * jump[1] / t)


def best(direction: int, by_expiry: Dict[str, List[Quote]], dte: Dict[str, int], s: float, mu: float,
         sigma: float, edge_years: Optional[float] = None, base_drift: float = 0.0,
         jump: Optional[Tuple[int, float]] = None) -> Optional[Dict[str, Any]]:
    """The candidate with the highest expected return on the capital at risk
    across the expiries given, among those clearing MIN_EV_PER_RISK and
    MIN_PROB_PROFIT; None when no contract is worth trading. With
    `edge_years`, expiries beyond the forecast's horizon earn its edge only
    up to the horizon (drift_to); with `jump`, expiries after the earnings
    release carry its move (vol_to)."""
    pool: List[Dict[str, Any]] = []
    for exp, quotes in by_expiry.items():
        days = max(dte.get(exp, 0), 1)
        t = days / 365.0
        for c in candidates(direction, quotes, s, t, drift_to(t, mu, edge_years, base_drift),
                            vol_to(t, days, sigma, jump)):
            c.update(expiry=exp, dte=dte.get(exp))
            pool.append(c)
    ok = [c for c in pool if c["ev_per_risk"] >= MIN_EV_PER_RISK and c["prob_profit"] >= MIN_PROB_PROFIT]
    if not ok:
        return None
    pick = max(ok, key=lambda c: c["ev_per_risk"])
    d_pick = max(pick.get("dte") or 1, 1)
    t_pick = d_pick / 365.0
    pick["kelly_share"] = kelly_share(pick, s, t_pick, drift_to(t_pick, mu, edge_years, base_drift),
                                      vol_to(t_pick, d_pick, sigma, jump))
    pick["considered"] = len(pool)
    return pick


__all__ = ["drift_to", "settle", "vol_to", "CONTRACT", "KELLY_FRACTION", "MIN_EV_PER_RISK", "MIN_PROB_PROFIT", "Quote",
           "best", "candidates", "expected_payoff", "kelly_share", "prob_above"]
