"""
shared/utils/stock_features.py

The contract between the platform's domains and the stock model: every domain
writes per-stock, per-day values into stock_features_daily, and the model reads
them back as panel columns -- only through a pre-registered trial until one is
adopted.

Each feature is valid for `ttl_days` after the day it is dated: a weekly
measure for a week, an 8-K event for a month. A value is dated the day it
became known (a Form 4 by its filing date), so reading it forward within its
window is point in time; past the window it is missing, not zero.

Builders here are pure: rows in, feature rows out.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class Feature:
    ttl_days: int
    source: str
    description: str


REGISTRY: Dict[str, Feature] = {
    "cmdty_exposure_21": Feature(4, "commodity_links", "Signed 21-session move of what the company makes or buys"),
    "cmdty_exposure_63": Feature(4, "commodity_links", "Signed 63-session move of what the company makes or buys"),
    "ins_npr_182": Feature(7, "insider", "Net insider purchase ratio, open-market buys less sells over all, 182 days "
                                         "(Lakonishok & Lee 2001)"),
    "ins_buyers_91": Feature(7, "insider", "Distinct insiders buying on the open market, 91 days"),
    "ins_opp_sells_91": Feature(7, "insider", "Opportunistic insider sales outside a 10b5-1 plan, 91 days "
                                              "(Cohen, Malloy & Pomorski 2012)"),
    "ev_8k_dilution": Feature(30, "filings", "8-K Item 3.02 unregistered equity sale in the last 30 days"),
    "ev_8k_delisting_notice": Feature(30, "filings", "8-K Item 3.01 delisting notice in the last 30 days"),
    "ev_8k_impairment": Feature(30, "filings", "8-K Item 2.06 impairment in the last 30 days"),
    "ev_8k_auditor_change": Feature(30, "filings", "8-K Item 4.01 auditor change in the last 30 days"),
    "ev_8k_nonreliance": Feature(30, "filings", "8-K Item 4.02 non-reliance on past statements in the last 30 days"),
    "ev_8k_bankruptcy": Feature(30, "filings", "8-K Item 1.03 bankruptcy in the last 30 days"),
    "cust_mom_21": Feature(4, "customer_links", "Major customers' 21-session return, weighted by their share of "
                                                "revenue in the latest 10-K (Cohen & Frazzini 2008)"),
    "comention_mom_21": Feature(4, "comention", "21-session return of the firms named in the same headlines over "
                                                "the past year, weighted by co-mentions (Scherbina & Schlusche 2020; "
                                                "Ali & Hirshleifer 2020)"),
    "ev_13d_activist": Feature(30, "activist_13d", "A Schedule 13D in the last 30 days whose Item 4 states an "
                                                 "activist purpose (Brav, Jiang, Partnoy & Thomas 2008)"),
    "llm_news_30": Feature(4, "llm_news", "Mean of a hosted language model's good/bad/unclear read of the stock's "
                                          "headlines over 30 days (Lopez-Lira & Tang); scored forward only"),
    "ev_guidance": Feature(60, "filing_extract", "An earnings release that raises (+) or cuts (-) the company's own "
                                                 "guidance: the change in the guided figure when stated, else +1 / -1"),
    "ev_comment_letter": Feature(90, "comment_letters", "A reply to an SEC comment letter: 1 agreeing to restate or "
                                                         "amend, 0.5 a substantive accounting topic, 0.25 disclosure "
                                                         "only (Ryans 2021)"),
    "cust_news_30": Feature(4, "customer_links", "Customers' llm_news_30, weighted by share of revenue (supply-chain "
                                                 "propagation, trial)"),
    "cust_fil_change": Feature(4, "customer_links", "Customers' latest fil_change, weighted by share of revenue (trial)"),
    "text_peer_mom_21": Feature(4, "sympathy", "Mean 21-session return of the company's text-based peers, from 10-K "
                                               "business descriptions (Hoberg & Phillips 2018, text-based industry momentum)"),
    "fil_change": Feature(100, "filing_change", "One minus the cosine similarity of this 10-K / 10-Q to the comparable filing "
                                                "before it (Cohen, Malloy & Nguyen 2020, Lazy Prices)"),
    "fil_change_risk": Feature(100, "filing_change", "The same for the 10-K's risk-factor section (Item 1A) alone"),
    "ev_alert_drift_5": Feature(5, "alert_drift", "Sign of the session before an alert from a family graded to "
                                                  "continue it over five sessions; forward from 30 Oct 2026"),
}

EIGHT_K_FEATURES = {"3.02": "ev_8k_dilution", "3.01": "ev_8k_delisting_notice", "2.06": "ev_8k_impairment",
                    "4.01": "ev_8k_auditor_change", "4.02": "ev_8k_nonreliance", "1.03": "ev_8k_bankruptcy"}


@dataclass(frozen=True)
class Trade:
    filed: date
    owner: str
    code: str              # "P" open-market buy, "S" open-market sale
    planned: bool          # under a 10b5-1 plan
    opportunistic: bool


def insider_rows(trades: Mapping[str, Sequence[Trade]], grid: Sequence[date]) -> List[Tuple[str, date, str, float]]:
    """(ticker, day, feature, value) on each grid day for every stock with
    open-market activity in the window; quiet stocks write nothing (missing)."""
    out: List[Tuple[str, date, str, float]] = []
    for ticker, rows in trades.items():
        rows = sorted(rows, key=lambda t: t.filed)
        days = [t.filed for t in rows]
        for d in grid:
            hi = bisect_right(days, d)
            lo182 = bisect_right(days, d - timedelta(days=182))
            window = rows[lo182:hi]
            buys = sum(1 for t in window if t.code == "P")
            sells = sum(1 for t in window if t.code == "S" and not t.planned)
            if buys + sells == 0:
                continue
            out.append((ticker, d, "ins_npr_182", round((buys - sells) / (buys + sells), 4)))
            recent = [t for t in window if t.filed > d - timedelta(days=91)]
            out.append((ticker, d, "ins_buyers_91", float(len({t.owner for t in recent if t.code == "P"}))))
            out.append((ticker, d, "ins_opp_sells_91",
                        float(sum(1 for t in recent if t.code == "S" and t.opportunistic and not t.planned))))
    return out


def eightk_rows(filings: Iterable[Tuple[str, date, Sequence[str]]]) -> List[Tuple[str, date, str, float]]:
    """(ticker, filed, feature, 1.0) for each adverse 8-K item, once per day."""
    seen = set()
    out: List[Tuple[str, date, str, float]] = []
    for ticker, filed, items in filings:
        for item in items or ():
            feat = EIGHT_K_FEATURES.get(str(item))
            if feat and (ticker, filed, feat) not in seen:
                seen.add((ticker, filed, feat))
                out.append((ticker, filed, feat, 1.0))
    return out


def to_panel(rows: Iterable[Tuple[str, date, float]], dates: Sequence[date], tickers: Sequence[str],
             ttl_days: int) -> np.ndarray:
    """[T, N] float32: each value forward from its day for `ttl_days`, NaN beyond."""
    ti = {t: j for j, t in enumerate(tickers)}
    out = np.full((len(dates), len(tickers)), np.nan, dtype=np.float32)
    by_t: Dict[int, List[Tuple[date, float]]] = defaultdict(list)
    for t, d, v in rows:
        j = ti.get(t)
        if j is not None and v is not None:
            by_t[j].append((d, float(v)))
    for j, pts in by_t.items():
        pts.sort()
        pd = [p[0] for p in pts]
        for i, day in enumerate(dates):
            k = bisect_right(pd, day) - 1
            if k >= 0 and (day - pd[k]).days <= ttl_days:
                out[i, j] = pts[k][1]
    return out


def weekly_grid(start: date, end: date) -> List[date]:
    """Every Friday from start to end."""
    d = start + timedelta(days=(4 - start.weekday()) % 7)
    out: List[date] = []
    while d <= end:
        out.append(d)
        d += timedelta(days=7)
    return out


def ttl(feature: str) -> Optional[int]:
    f = REGISTRY.get(feature)
    return f.ttl_days if f else None


__all__ = ["EIGHT_K_FEATURES", "Feature", "REGISTRY", "Trade", "eightk_rows", "insider_rows", "to_panel", "ttl",
           "weekly_grid"]
