"""
shared/utils/option_chain.py

One day's option chain for a stock, reduced to what a cross-sectional model
can rank: how much was traded and is held on each side, and what implied
volatility says about fear. From Cboe's free delayed chains (about fifteen
minutes late, taken after the close): every listed contract with its bid, ask,
implied volatility, delta, volume and open interest.

  call/put volume and open interest    the whole chain
  put/call ratios                      of volume and of open interest
  iv30                                 Cboe's own 30-day implied volatility
  atm_iv                               mean IV of the call and put nearest
                                       50 delta on the expiry nearest 30 days
  skew_25d                             25-delta put IV less 25-delta call IV,
                                       same expiry (Xing, Zhang & Zhao 2010
                                       read the put side's premium as fear)

Pure: the parsed JSON in, one row out.
"""
from __future__ import annotations

import math
import re
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

OCC = re.compile(r"^([A-Z]{1,6})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$")
TARGET_DAYS = 30
MIN_DAYS = 7


def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def parse_contract(symbol: str) -> Optional[Tuple[date, str, float]]:
    """(expiry, "C"/"P", strike) from an OCC symbol."""
    m = OCC.match(str(symbol or "").upper())
    if not m:
        return None
    try:
        expiry = date(2000 + int(m.group(2)), int(m.group(3)), int(m.group(4)))
    except ValueError:
        return None
    return expiry, m.group(5), int(m.group(6)) / 1000.0


def _nearest_delta(rows: Sequence[Mapping[str, Any]], target: float) -> Optional[float]:
    """IV of the contract whose delta is nearest `target`, among contracts with an IV."""
    best: Optional[Tuple[float, float]] = None
    for r in rows:
        d, iv = _f(r.get("delta")), _f(r.get("iv"))
        if d is None or iv is None or iv <= 0:
            continue
        gap = abs(d - target)
        if best is None or gap < best[0]:
            best = (gap, iv)
    return best[1] if best and best[0] <= 0.15 else None


def summarize(chain: Mapping[str, Any], as_of: date) -> Optional[Dict[str, Any]]:
    """The day's summary for one stock, or None for an empty chain."""
    data = chain.get("data") if isinstance(chain.get("data"), Mapping) else chain
    options: List[Mapping[str, Any]] = list((data or {}).get("options") or [])
    if not options:
        return None
    cv = pv = coi = poi = 0.0
    by_exp: Dict[date, List[Tuple[str, Mapping[str, Any]]]] = {}
    for o in options:
        parsed = parse_contract(o.get("option"))
        if parsed is None:
            continue
        expiry, right, _strike = parsed
        vol, oi = _f(o.get("volume")) or 0.0, _f(o.get("open_interest")) or 0.0
        if right == "C":
            cv, coi = cv + vol, coi + oi
        else:
            pv, poi = pv + vol, poi + oi
        if (expiry - as_of).days >= MIN_DAYS:
            by_exp.setdefault(expiry, []).append((right, o))
    atm_iv = skew = None
    expiry_days = None
    if by_exp:
        expiry = min(by_exp, key=lambda e: abs((e - as_of).days - TARGET_DAYS))
        expiry_days = (expiry - as_of).days
        calls = [o for r, o in by_exp[expiry] if r == "C"]
        puts = [o for r, o in by_exp[expiry] if r == "P"]
        c50, p50 = _nearest_delta(calls, 0.5), _nearest_delta(puts, -0.5)
        if c50 is not None and p50 is not None:
            atm_iv = (c50 + p50) / 2
        c25, p25 = _nearest_delta(calls, 0.25), _nearest_delta(puts, -0.25)
        if c25 is not None and p25 is not None:
            skew = p25 - c25
    iv30 = _f((data or {}).get("iv30"))
    return {
        "day": as_of,
        "call_volume": cv, "put_volume": pv, "call_oi": coi, "put_oi": poi,
        "pcr_volume": round(pv / cv, 4) if cv > 0 else None,
        "pcr_oi": round(poi / coi, 4) if coi > 0 else None,
        "iv30": round(iv30 / 100.0, 5) if iv30 is not None and iv30 > 0 else None,   # Cboe quotes percent
        "atm_iv": round(atm_iv, 5) if atm_iv is not None else None,
        "skew_25d": round(skew, 5) if skew is not None else None,
        "expiry_days": expiry_days,
        "contracts": len(options),
        "spot": _f((data or {}).get("current_price")),
    }


__all__ = ["parse_contract", "summarize"]
