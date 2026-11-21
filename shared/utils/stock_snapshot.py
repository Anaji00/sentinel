"""The fundamentals strip for one stock: market cap, 52-week range, next earnings and a trailing P/E, each with where it comes from.

P/E is market cap over trailing-twelve-month net income (SEC, USD), which is price over earnings per share without needing a per-share
count that a later split would invalidate. It is null when net income is not positive, the filings do not cover four quarters, or the cap
is unknown. Nothing is guessed: a figure that cannot be known is None, never 0. Pure; no I/O.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

# (period_start, period_end, value): latest-filed value per period, USD.
Fact = Tuple[date, date, float]


def as_day(v: Any) -> Optional[date]:
    """A date from a date, a datetime or an ISO string (the pool returns timestamps as strings); None when it is none of those."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _near(a: date, b: date, days: int) -> bool:
    return abs((a - b).days) <= days


def ttm_net_income(facts: Sequence[Fact]) -> Optional[Tuple[float, date]]:
    """(trailing-twelve-month net income, end of the newest period) from reported periods, or None when four quarters are not covered.

    Newest period ending last: an annual period is the answer; a quarter or year-to-date period is added to the last full year and the
    same stretch of the year before it is taken off. Needs both of those, else None."""
    if not facts:
        return None
    cur_s, cur_e, cur_v = max(facts, key=lambda f: (f[1], (f[1] - f[0]).days))
    length = (cur_e - cur_s).days
    if length >= 350:
        return float(cur_v), cur_e
    annual = [f for f in facts if (f[1] - f[0]).days >= 350 and _near(f[1], cur_s - timedelta(days=1), 10)]
    prior = [f for f in facts if _near(f[1], cur_e - timedelta(days=364), 10) and _near(f[0], cur_s - timedelta(days=364), 10)
             and abs((f[1] - f[0]).days - length) <= 10]
    if not annual or not prior:
        return None
    a = max(annual, key=lambda f: f[1])
    p = max(prior, key=lambda f: f[1])
    return float(a[2]) + float(cur_v) - float(p[2]), cur_e


REVENUE_CONCEPTS = ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "RevenueFromContractWithCustomerIncludingAssessedTax",
                    "SalesRevenueNet")
FLOW_CONCEPTS = ("NetIncomeLoss", "OperatingIncomeLoss", "GrossProfit", "NetCashProvidedByUsedInOperatingActivities") + REVENUE_CONCEPTS


def _newest(facts: Sequence[Fact]) -> Optional[date]:
    return max((f[1] for f in facts), default=None)


def _revenue_facts(by_concept: Mapping[str, Sequence[Fact]]) -> Sequence[Fact]:
    """The revenue line the company reports most recently (companies switched tags over the years); ties go to the earlier name."""
    best: Sequence[Fact] = []
    best_end: Optional[date] = None
    for name in REVENUE_CONCEPTS:
        facts = by_concept.get(name) or []
        end = _newest(facts)
        if end is not None and (best_end is None or end > best_end):
            best, best_end = facts, end
    return best


def _pct(num: Optional[float], den: Optional[float]) -> Optional[float]:
    return round(num / den * 100.0, 1) if num is not None and den is not None and den > 0 else None


def metrics(market_cap: Optional[float], by_concept: Mapping[str, Sequence[Fact]], equity: Optional[float]) -> Optional[Dict[str, Any]]:
    """Plain-language company figures from SEC filings, all over the trailing 12 months unless stated:
    revenue, revenue growth (against the 12 months before), profit margins, return on equity, price to sales and price to book.
    Each is None when its inputs are missing or its denominator is not positive. None overall when no revenue or income is on file."""
    revenue = _revenue_facts(by_concept)
    rev = ttm_net_income(revenue)
    income = ttm_net_income(by_concept.get("NetIncomeLoss") or [])
    if rev is None and income is None:
        return None
    op = ttm_net_income(by_concept.get("OperatingIncomeLoss") or [])
    gross = ttm_net_income(by_concept.get("GrossProfit") or [])
    cash = ttm_net_income(by_concept.get("NetCashProvidedByUsedInOperatingActivities") or [])
    growth: Optional[float] = None
    if rev is not None and rev[0] > 0:
        cutoff = rev[1] - timedelta(days=350)
        earlier = ttm_net_income([f for f in revenue if f[1] <= cutoff + timedelta(days=15)])
        if earlier is not None and abs((earlier[1] - (rev[1] - timedelta(days=364))).days) <= 20 and earlier[0] > 0:
            growth = round((rev[0] / earlier[0] - 1.0) * 100.0, 1)
    r = rev[0] if rev else None
    cap = market_cap if market_cap and market_cap > 0 else None
    ends = [x[1] for x in (rev, income) if x is not None]
    return {
        "revenue": round(r, 0) if r is not None else None,
        "revenue_growth_pct": growth,
        "gross_margin_pct": _pct(gross[0] if gross else None, r),
        "operating_margin_pct": _pct(op[0] if op else None, r),
        "net_margin_pct": _pct(income[0] if income else None, r),
        "cash_from_operations_margin_pct": _pct(cash[0] if cash else None, r),
        "return_on_equity_pct": _pct(income[0] if income else None, equity),
        "price_to_sales": round(cap / r, 2) if cap and r and r > 0 else None,
        "price_to_book": round(cap / equity, 2) if cap and equity and equity > 0 else None,
        "basis": "trailing 12 months of SEC filings; equity is the latest balance sheet",
        "as_of": max(ends).isoformat(),
    }


def pe_trailing(market_cap: Optional[float], ttm: Optional[Tuple[float, date]]) -> Optional[Dict[str, Any]]:
    if not market_cap or market_cap <= 0 or ttm is None:
        return None
    income, period_end = ttm
    if not math.isfinite(income) or income <= 0:
        return None
    return {"value": round(market_cap / income, 1), "basis": "market cap / trailing 12-month net income (SEC filings)",
            "as_of": period_end.isoformat()}


def range_52w(rows: Sequence[Tuple[date, Optional[float], Optional[float]]]) -> Optional[Dict[str, Any]]:
    """Low and high over the rows given (day, low, high); a missing low or high falls back to nothing, not zero."""
    lows = [(float(lo), d) for d, lo, hi in rows if lo is not None and float(lo) > 0]
    highs = [(float(hi), d) for d, lo, hi in rows if hi is not None and float(hi) > 0]
    if not lows or not highs:
        return None
    return {"low": round(min(lows)[0], 4), "high": round(max(highs)[0], 4),
            "from_date": min(d for _, d in lows + highs).isoformat(), "low_date": min(lows)[1].isoformat(),
            "high_date": max(highs)[1].isoformat()}


def next_earnings(confirmed: Optional[date], last_release: Optional[date], today: date) -> Optional[Dict[str, Any]]:
    """A listed report date is used as is; otherwise one quarter-cycle after the last results 8-K, flagged as an estimate."""
    from shared.utils.recommendations import next_earnings as estimate

    if confirmed is not None and confirmed >= today:
        return {"date": confirmed.isoformat(), "estimated": False}
    est = estimate(last_release, today)
    return {"date": est.isoformat(), "estimated": True} if est else None


def snapshot(ticker: str, market_cap: Optional[float], cap_as_of: Optional[str], rng: Optional[Dict[str, Any]],
             earnings: Optional[Dict[str, Any]], pe: Optional[Dict[str, Any]], company: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ticker": ticker, "market_cap": round(market_cap, 0) if market_cap else None,
            "market_cap_as_of": cap_as_of if market_cap else None,
            "range_52w": {"low": rng["low"], "high": rng["high"], "from_date": rng["from_date"]} if rng else None,
            "next_earnings": earnings, "pe_trailing": pe, "metrics": company}


__all__ = ["FLOW_CONCEPTS", "Fact", "as_day", "metrics", "next_earnings", "pe_trailing", "range_52w", "snapshot", "ttm_net_income"]
