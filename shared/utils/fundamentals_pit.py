"""
shared/utils/fundamentals_pit.py

Reported financials as they were known on each filing date.

Input is every fact a company filed (services/collector-filings/company_facts.py
stores them append-only, each with its filing date). Output, per ticker and
line item, is a series of (filed, value): the figure an investor could have
computed on that day from what had been filed by then -- trailing twelve months
for flows, the latest balance for stocks.

Why not the XBRL frames this replaces: a frame carries one value per period,
taken from the *latest* filing, with no filing date. A restatement therefore
showed up on dates when the market was still trading the original number, and
the model had to assume every figure appeared 90 days after its period ended
(Apple's 10-Qs land in about 34). Net income and cash flow came only from
annual frames, so in October 2026 the model was still reading Apple's fiscal
year to September 2025.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# Tags per line item, most preferred first. Companies move between tags
# (SalesRevenueNet gave way to RevenueFromContractWithCustomer... under ASC 606),
# so the preference is applied per period, not per company.
INSTANT_TAGS: Dict[str, Tuple[str, ...]] = {
    "assets": ("Assets",),
    "equity": ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    "shares_out": ("EntityCommonStockSharesOutstanding",),
}
FLOW_TAGS: Dict[str, Tuple[str, ...]] = {
    "net_income": ("NetIncomeLoss", "ProfitLoss", "NetIncomeLossAvailableToCommonStockholdersBasic"),
    "cfo": ("NetCashProvidedByUsedInOperatingActivities",
            "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    "revenue": ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                "RevenueFromContractWithCustomerIncludingAssessedTax"),
    "gross_profit": ("GrossProfit",),
    "op_income": ("OperatingIncomeLoss",),
}
# Weighted diluted shares cover every share class, which the cover-page count
# (one value per class) does not; a quarter's figure, not a sum.
AVERAGE_TAGS: Dict[str, Tuple[str, ...]] = {
    "shares_dil": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
}
ALL_TAGS = tuple(t for d in (INSTANT_TAGS, FLOW_TAGS, AVERAGE_TAGS) for tags in d.values() for t in tags)
FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A", "10-KT", "10-QT")

QUARTER_DAYS = (75, 105)
YEAR_DAYS = (340, 390)
NINE_MONTH_DAYS = (250, 290)
QUARTER_GAP_DAYS = (70, 110)      # between consecutive quarter ends
YEAR_AGO_DAYS = (330, 400)        # for asset growth


@dataclass(frozen=True)
class Fact:
    concept: str
    start: Optional[date]
    end: date
    value: float
    filed: date
    accn: str


def _days(f: Fact) -> int:
    return (f.end - f.start).days if f.start else 0


def _between(x: int, rng: Tuple[int, int]) -> bool:
    return rng[0] <= x <= rng[1]


def _preferred(facts: Iterable[Fact], tags: Sequence[str]) -> List[Fact]:
    """For each period, the facts of the most preferred tag that reported it."""
    rank = {t: k for k, t in enumerate(tags)}
    best: Dict[Tuple[Optional[date], date], int] = {}
    fs = [f for f in facts if f.concept in rank]
    for f in fs:
        key = (f.start, f.end)
        best[key] = min(best.get(key, 99), rank[f.concept])
    return [f for f in fs if rank[f.concept] == best[(f.start, f.end)]]


def _by_filing(facts: Sequence[Fact]) -> List[Tuple[date, List[Fact]]]:
    groups: Dict[date, List[Fact]] = defaultdict(list)
    for f in facts:
        groups[f.filed].append(f)
    return sorted(groups.items())


def _quarters(known: Dict[Tuple[Optional[date], date], float]) -> Tuple[Dict[date, float], Dict[date, Tuple[date, float]]]:
    """Quarterly values by period end, a fourth quarter derived as the year less
    its first nine months when only the annual figure was filed; and the years."""
    quarters: Dict[date, float] = {}
    years: Dict[date, Tuple[date, float]] = {}
    nine: Dict[date, float] = {}            # by start date
    for (s, e), v in known.items():
        if s is None:
            continue
        d = (e - s).days
        if _between(d, QUARTER_DAYS):
            quarters[e] = v
        elif _between(d, YEAR_DAYS):
            years[e] = (s, v)
        elif _between(d, NINE_MONTH_DAYS):
            nine[s] = v
    for e, (s, v) in years.items():
        if e not in quarters:
            for ns, nv in nine.items():
                if abs((ns - s).days) <= 5:
                    quarters[e] = v - nv
                    break
    return quarters, years


# Standardized unexpected earnings (Bernard & Thomas 1989, the seasonal random
# walk; Foster, Olsen & Shevlin 1984): the latest quarter less the same quarter
# a year earlier, over the spread of those changes in the quarters before.
SUE_MIN_HISTORY = 4
SUE_MAX_HISTORY = 8


def sue(known: Dict[Tuple[Optional[date], date], float]) -> Optional[float]:
    """The latest quarter's standardized surprise from the periods known so
    far, or None without a year-ago quarter and SUE_MIN_HISTORY earlier changes."""
    quarters, _years = _quarters(known)
    ends = sorted(quarters)

    def year_ago(e: date) -> Optional[float]:
        prior = [p for p in ends if _between((e - p).days, YEAR_AGO_DAYS)]
        return quarters[min(prior, key=lambda p: abs((e - p).days - 365))] if prior else None

    changes = []
    for e in ends:
        y = year_ago(e)
        if y is not None:
            changes.append((e, quarters[e] - y))
    if len(changes) < SUE_MIN_HISTORY + 1 or changes[-1][0] != ends[-1]:
        return None
    hist = [c for _e, c in changes[-SUE_MAX_HISTORY - 1:-1]]
    mean = sum(hist) / len(hist)
    sd = (sum((c - mean) ** 2 for c in hist) / (len(hist) - 1)) ** 0.5
    return changes[-1][1] / sd if sd > 0 else None


def ttm(known: Dict[Tuple[Optional[date], date], float]) -> Optional[Tuple[date, float]]:
    """Trailing twelve months from the periods known so far: the four latest
    consecutive quarters, a fourth quarter derived as the year less its first
    nine months when only the annual figure was filed; else the latest year."""
    quarters, years = _quarters(known)
    if quarters:
        ends = sorted(quarters)
        last4 = ends[-4:]
        if len(last4) == 4 and all(_between((b - a).days, QUARTER_GAP_DAYS) for a, b in zip(last4, last4[1:])):
            return last4[-1], sum(quarters[e] for e in last4)
    if years:
        e = max(years)
        return e, years[e][1]
    return None


def _latest(known: Dict[Tuple[Optional[date], date], float], quarterly: bool = False) -> Optional[Tuple[date, float]]:
    cands = [(e, v) for (s, e), v in known.items() if not quarterly or (s and _between((e - s).days, QUARTER_DAYS))]
    return max(cands) if cands else None


def _instant_known(facts: Sequence[Fact], summed: bool) -> List[Tuple[date, Dict[Tuple[Optional[date], date], float]]]:
    """Snapshots of known instant values after each filing date.

    `summed`: the cover-page share count is reported once per class, so
    distinct values in one filing for one date are added (Alphabet's A, B, C)."""
    out = []
    known: Dict[Tuple[Optional[date], date], float] = {}
    for filed, fs in _by_filing(facts):
        per: Dict[Tuple[str, date], set] = defaultdict(set)
        for f in fs:
            per[(f.accn, f.end)].add(f.value)
        for (_accn, end), vals in per.items():
            known[(None, end)] = sum(vals) if summed else max(vals)
        out.append((filed, dict(known)))
    return out


def series(facts: Sequence[Fact]) -> Dict[str, List[Tuple[date, float]]]:
    """One company's facts -> {item: [(filed, value)]}, point in time."""
    out: Dict[str, List[Tuple[date, float]]] = {}
    for key, tags in INSTANT_TAGS.items():
        fs = _preferred(facts, tags)
        pts = []
        for filed, known in _instant_known(fs, summed=(key == "shares_out")):
            last = _latest(known)
            if last:
                pts.append((filed, last[1]))
        if pts:
            out[key] = pts
        if key == "assets" and fs:
            growth = []
            for filed, known in _instant_known(fs, summed=False):
                last = _latest(known)
                if not last:
                    continue
                prior = sorted((abs((last[0] - e).days - 365), v) for (_s, e), v in known.items()
                               if _between((last[0] - e).days, YEAR_AGO_DAYS))
                if prior and prior[0][1] > 0:
                    growth.append((filed, last[1] / prior[0][1] - 1.0))
            if growth:
                out["asset_growth"] = growth
    surprises: List[Tuple[date, float]] = []
    for key, tags in {**FLOW_TAGS, **AVERAGE_TAGS}.items():
        fs = [f for f in _preferred(facts, tags) if f.start is not None]
        known: Dict[Tuple[Optional[date], date], float] = {}
        pts = []
        for filed, group in _by_filing(fs):
            for f in group:
                known[(f.start, f.end)] = f.value
            got = _latest(known, quarterly=True) if key in AVERAGE_TAGS else ttm(known)
            if got:
                pts.append((filed, got[1]))
            if key == "net_income":
                u = sue(known)
                if u is not None:
                    surprises.append((filed, u))
        if pts:
            out[key] = pts
    if surprises:
        out["sue"] = surprises
    return out


__all__ = ["ALL_TAGS", "FLOW_TAGS", "FORMS", "Fact", "INSTANT_TAGS", "series", "sue", "ttm"]
