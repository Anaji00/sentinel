"""
shared/utils/option_iv.py

Option-implied measures rebuilt from history. Pure; the collector supplies the
bars (services/collector-tradfi/option_history.py).

The live poller reads implied volatility and delta off Alpaca's snapshots,
which exist only for today. Daily option bars, though, go back to February
2024 for expired contracts too, and a contract's closing price with the
stock's closing price gives its implied volatility by inverting Black-Scholes.
That rebuilds the measures options_positioning computes live -- the
Xing-Zhang-Zhao skew, the Cremers-Weinbaum call-minus-put spread, the
put/call volume ratio -- for every session since, so they can be tested
against what followed instead of waited on.

What differs from the snapshots, and why it is tolerable for ranking stocks
against each other on the same day:
- Prices are each contract's last trade, not a closing quote, so they are not
  synchronous with the stock's close. Each measure averages or matches
  several contracts and the error is not signed.
- No dividends: a dividend payer's calls price lower and puts higher than
  Black-Scholes without them, which pulls its call-put spread down by about
  its yield. One flat rate (RISK_FREE): a rate error moves every stock's
  spread together, not their order.
- Monthly expiries only (the third Friday), which always leaves one 7-60 days
  out; the volume ratio is therefore a monthly-chain ratio.
Each measure is stored apart from the live table (option_iv_history), so a
study never mixes the two definitions.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from shared.utils import options_positioning as op

RISK_FREE = 0.045
IV_LOW, IV_HIGH = 0.01, 5.0
_SQRT2 = math.sqrt(2.0)


def _ncdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / _SQRT2))


def bs_price(right: str, s: float, k: float, t: float, r: float, sigma: float) -> float:
    """European call ("C") or put ("P") price per share."""
    sd = sigma * math.sqrt(t)
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / sd
    d2 = d1 - sd
    disc = k * math.exp(-r * t)
    if right == "C":
        return s * _ncdf(d1) - disc * _ncdf(d2)
    return disc * _ncdf(-d2) - s * _ncdf(-d1)


def bs_delta(right: str, s: float, k: float, t: float, r: float, sigma: float) -> float:
    d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
    return _ncdf(d1) if right == "C" else _ncdf(d1) - 1.0


def implied_vol(price: float, right: str, s: float, k: float, t: float, r: float = RISK_FREE) -> Optional[float]:
    """The volatility at which Black-Scholes gives `price`, by bisection
    (monotone in volatility); None outside the no-arbitrage bounds or the
    IV_LOW-IV_HIGH range."""
    if price <= 0 or s <= 0 or k <= 0 or t <= 0:
        return None
    lo, hi = IV_LOW, IV_HIGH
    if not bs_price(right, s, k, t, r, lo) < price < bs_price(right, s, k, t, r, hi):
        return None
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        if bs_price(right, s, k, t, r, mid) < price:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-5:
            break
    return 0.5 * (lo + hi)


def monthly_expiries(expiries: Iterable[date]) -> List[date]:
    """The standard monthly expiry of each month present: the third Friday,
    or the Thursday before it when that Friday is a market holiday."""
    by_month: Dict[Tuple[int, int], date] = {}
    for e in expiries:
        if 15 <= e.day <= 21 and e.weekday() in (3, 4):
            cur = by_month.get((e.year, e.month))
            if cur is None or e > cur:
                by_month[(e.year, e.month)] = e
    return sorted(by_month.values())


def day_measures(day: date, spot: float, contracts: Sequence[Tuple[date, float, str, float, float]],
                 r: float = RISK_FREE) -> Optional[Dict[str, Any]]:
    """One session's measures from (expiry, strike, right, close, volume) of
    every contract that traded that day, with the stock's unadjusted close:
    the same definitions as options_positioning, on rebuilt IV and delta."""
    if not spot or spot <= 0:
        return None
    chain: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    volume: List[Tuple[str, Dict[str, Any]]] = []
    for expiry, strike, right, close, vol in contracts:
        if vol and vol > 0:
            volume.append((right, {"dailyBar": {"t": day.isoformat(), "v": vol}}))
        t = (expiry - day).days / 365.0
        iv = implied_vol(close, right, spot, strike, t, r) if t > 0 else None
        if iv is None:
            continue
        chain.append(({"strike": strike, "expiry": expiry.isoformat(), "option_type": "CALL" if right == "C" else "PUT"},
                      {"impliedVolatility": iv, "greeks": {"delta": bs_delta(right, spot, strike, t, r, iv)}}))
    out: Dict[str, Any] = dict(op.iv_measures(chain, day) or {})
    vols = op.summarize(volume, day.isoformat())
    if vols:
        out.update(call_volume=vols["call_volume"], put_volume=vols["put_volume"],
                   put_call_ratio=vols["put_call_ratio"])
    if not out:
        return None
    out["contracts"] = len(chain)
    return out


__all__ = ["RISK_FREE", "bs_delta", "bs_price", "day_measures", "implied_vol", "monthly_expiries"]
