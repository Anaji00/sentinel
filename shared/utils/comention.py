"""
shared/utils/comention.py

Firms linked by being named together in the news, and their momentum.

Scherbina & Schlusche ("Follow the Leader", Review of Finance 2020): stocks
mentioned in the same news stories cross-predict each other's returns, because
information diffuses slowly between them, even after customer-supplier pairs
are removed. Ali & Hirshleifer (JFE 2020): momentum in firms connected through
shared coverage earns 1.68% a month (t 9.67) and subsumes industry, customer,
supplier and technology momentum. Shared analyst coverage is not free;
co-mention in the platform's 808,000 stock headlines is, and the graph had no
supplier or customer links at all.

  links    every pair of stocks named in one headline (2 to MAX_NAMES of them),
           counted over the WINDOW_DAYS before a day -- point in time
  feature  comention_mom_21: the 21-session return of a stock's linked firms,
           weighted by co-mention count, the stock itself excluded; published
           for stocks with at least MIN_WEIGHT mentions across MIN_PEERS peers

Pure: headlines in, links out; links and returns in, feature out.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import date, timedelta
from itertools import combinations
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

FEATURE = "comention_mom_21"
WINDOW_DAYS = 365
MAX_NAMES = 5               # a roundup naming more stocks says nothing about any pair
MIN_PEERS = 2
MIN_WEIGHT = 3.0


def pair_events(headlines: Iterable[Tuple[date, Sequence[str]]]) -> List[Tuple[date, str, str]]:
    """(day, a, b) with a < b, for each pair named together in a headline."""
    out: List[Tuple[date, str, str]] = []
    for d, syms in headlines:
        names = sorted({str(s).upper() for s in syms if s})
        if 2 <= len(names) <= MAX_NAMES:
            out.extend((d, a, b) for a, b in combinations(names, 2))
    out.sort()
    return out


def links_asof(events: Sequence[Tuple[date, str, str]], day: date,
               window_days: int = WINDOW_DAYS) -> Dict[str, Dict[str, float]]:
    """{stock: {peer: co-mentions}} over (day - window_days, day), excluding the
    day itself, from events sorted by day."""
    days = [e[0] for e in events]
    lo = bisect_right(days, day - timedelta(days=window_days))
    hi = bisect_left(days, day)
    out: Dict[str, Dict[str, float]] = defaultdict(dict)
    for _, a, b in events[lo:hi]:
        out[a][b] = out[a].get(b, 0.0) + 1.0
        out[b][a] = out[b].get(a, 0.0) + 1.0
    return out


def feature(links: Mapping[str, Mapping[str, float]], returns: Mapping[str, float]) -> Dict[str, float]:
    """Co-mention-weighted mean of linked firms' returns, per stock."""
    out: Dict[str, float] = {}
    for s, peers in links.items():
        num = den = 0.0
        n = 0
        for p, w in peers.items():
            r = returns.get(p)
            if p == s or r is None:
                continue
            num += w * r
            den += w
            n += 1
        if n >= MIN_PEERS and den >= MIN_WEIGHT:
            out[s] = round(num / den, 6)
    return out


__all__ = ["FEATURE", "MAX_NAMES", "MIN_PEERS", "MIN_WEIGHT", "WINDOW_DAYS", "feature", "links_asof", "pair_events"]
