"""A ticker's day of option volume, by calls and puts.

Pan & Poteshman (2006) found stocks with low put/call volume ratios beat those
with high ratios by more than 40 bps the next day and about 1% the next week;
Johnson & So (2012) found the option-to-stock volume ratio negatively related
to next-week returns. Both read the whole chain's volume, not single prints.

The options poller already pulls a snapshot of every contract for the watched
tickers, each carrying the contract's daily bar. Summed by right, that is the
day's call and put volume -- written here once per poll and read by the
evidence layer and the stock-tier thesis.

The volume is all trades, not the buyer-initiated opening trades Pan and
Poteshman used, which this feed cannot separate; the ratio is the coarser,
public form of their measure and is labelled so.

The same chain carries each contract's implied volatility and delta, which give
the two option-implied measures with the strongest published record for the
underlying's next returns:

- Implied-volatility skew (Xing, Zhang & Zhao 2010): an out-of-the-money put's
  IV minus an at-the-money call's. Steep skew -- someone paying up for crash
  protection -- preceded underperformance of ~10.9% a year, top quintile
  against bottom, persisting for at least six months.
- Call-minus-put IV spread (Cremers & Weinbaum 2010): put-call parity says a
  matched call and put share one IV; when calls are dearer, informed buying is
  on that side. High-spread stocks beat low-spread ones by ~50 bps a week.

Both are kept per ticker per day (option_daily) from the first day they are
read: option IV history cannot be bought back on this feed, so it is measured
going forward and studied once enough days exist.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

OPTIONS_POSITIONING_PREFIX = "sentinel:options:positioning:"
POSITIONING_TTL_SEC = 36 * 3600


def positioning_key(ticker: str) -> str:
    return f"{OPTIONS_POSITIONING_PREFIX}{ticker.upper()}"


def summarize(contracts: Iterable[Tuple[str, Dict[str, Any]]], day: Optional[str] = None
              ) -> Optional[Dict[str, Any]]:
    """(right, snapshot) pairs -> the day's call and put volume and ratio.

    `day` (YYYY-MM-DD, New York) keeps only bars from that session. A contract
    that has not traded today still carries its last session's dailyBar, so
    without it the morning's ratio was mostly yesterday's volume."""
    calls = puts = 0.0
    seen = 0
    for right, snap in contracts:
        bar = (snap or {}).get("dailyBar") or {}
        if day and str(bar.get("t") or "")[:10] != day:
            continue
        try:
            v = float(bar.get("v") or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if v <= 0:
            continue
        seen += 1
        if str(right).upper().startswith("P"):
            puts += v
        else:
            calls += v
    if seen == 0 or calls + puts <= 0:
        return None
    return {
        "call_volume": round(calls),
        "put_volume": round(puts),
        "option_volume": round(calls + puts),
        "put_call_ratio": round(puts / calls, 3) if calls > 0 else None,
        "contracts_traded": seen,
        "measure": "all-trade daily volume, near-dated chain (coarse form of Pan-Poteshman)",
    }


# Xing-Zhang-Zhao read options 10-60 days out; the nearest expiry to a month is
# used for skew, every expiry in the window for the matched-pair spread.
IV_MIN_DAYS = 7
IV_MAX_DAYS = 60
IV_TARGET_DAYS = 30
ATM_CALL_DELTA = (0.40, 0.60)
OTM_PUT_DELTA = (-0.375, -0.125)    # roughly K/S 0.85-0.95 a month out
PAIR_CALL_DELTA = (0.20, 0.80)      # near the money, where both legs are quoted
MIN_PAIRS = 3


def _f(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def iv_measures(chain: Iterable[Tuple[Dict[str, Any], Dict[str, Any]]], today: date
                ) -> Optional[Dict[str, Any]]:
    """(parsed OCC symbol, snapshot) pairs -> IV skew and call-put IV spread.

    None when the chain does not quote enough of either: a measure from one
    stray contract is noise with a number on it."""
    legs: Dict[Tuple[str, float], Dict[str, Tuple[float, float]]] = {}
    by_exp: Dict[str, List[Tuple[str, float, float]]] = {}
    days_to: Dict[str, int] = {}
    for parsed, snap in chain:
        if not parsed or not snap:
            continue
        iv = _f(snap.get("impliedVolatility"))
        delta = _f((snap.get("greeks") or {}).get("delta"))
        strike = _f(parsed.get("strike"))
        exp = parsed.get("expiry")
        right = "P" if str(parsed.get("option_type", "")).upper().startswith("P") else "C"
        if iv is None or delta is None or strike is None or not exp or not 0.01 < iv < 5.0:
            continue
        try:
            days = (date.fromisoformat(str(exp)) - today).days
        except ValueError:
            continue
        if not IV_MIN_DAYS <= days <= IV_MAX_DAYS:
            continue
        days_to[exp] = days
        by_exp.setdefault(exp, []).append((right, delta, iv))
        legs.setdefault((exp, strike), {})[right] = (iv, delta)

    out: Dict[str, Any] = {}
    for exp in sorted(by_exp, key=lambda e: abs(days_to[e] - IV_TARGET_DAYS)):
        calls = [(d, iv) for r, d, iv in by_exp[exp] if r == "C" and ATM_CALL_DELTA[0] <= d <= ATM_CALL_DELTA[1]]
        puts = [(d, iv) for r, d, iv in by_exp[exp] if r == "P" and OTM_PUT_DELTA[0] <= d <= OTM_PUT_DELTA[1]]
        if calls and puts:
            atm = min(calls, key=lambda x: abs(x[0] - 0.5))
            otm = min(puts, key=lambda x: abs(x[0] + 0.25))
            out.update(iv_skew=round(otm[1] - atm[1], 4), atm_iv=round(atm[1], 4),
                       skew_expiry_days=days_to[exp])
            break

    spreads = [
        pair["C"][0] - pair["P"][0] for pair in legs.values()
        if "C" in pair and "P" in pair and PAIR_CALL_DELTA[0] <= pair["C"][1] <= PAIR_CALL_DELTA[1]
    ]
    if len(spreads) >= MIN_PAIRS:
        out.update(cp_iv_spread=round(sum(spreads) / len(spreads), 4), cp_pairs=len(spreads))
    return out or None


__all__ = [
    "OPTIONS_POSITIONING_PREFIX", "POSITIONING_TTL_SEC", "positioning_key", "summarize", "iv_measures",
]
