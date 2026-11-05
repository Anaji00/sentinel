"""
shared/utils/stock_peers.py

Stocks related to a stock, as the nightly jobs measured them: the stocks whose daily
moves, net of SPY, have tracked its own (sympathy_peers), and stocks whose 10-K
business descriptions read alike (text_peers). Pure: rows in, response out.

What this deliberately does not say. Every stored pair is a positive correlation
(MIN_CORR is 0.35), so there is no "moves against it" to show. The window is fixed at
120 sessions. Nothing stores how many days two stocks moved the same way, a lag, or
how often the stock correlation agent's calls on a pair were right, so none of those
is invented. The tables are rebuilt each night and hold only pairs that passed, so
"no peers" cannot be split into "too little history" and "nothing close enough".
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Mapping, Optional, Sequence

WINDOW_SESSIONS = 120     # shared/utils/sympathy.py WINDOW, pinned by tests/test_stock_peers.py
STALE_AFTER_DAYS = 4      # rebuilt nightly: a weekend plus a missed night
MAX_MOVES_WITH = 8
MAX_SIMILAR = 5
NOTE = "Past co-movement after removing the market's own moves. Not a forecast."

_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")


def clean_ticker(raw: Optional[str]) -> Optional[str]:
    """The ticker, upper-cased, or None when it could not be one."""
    t = (raw or "").strip().upper()
    return t if _TICKER.match(t) else None


def closeness_words(correlation: float) -> str:
    """How tightly a measured peer has tracked the stock, for someone who has not met a correlation."""
    c = float(correlation)
    if c >= 0.8:
        return "moves almost in step"
    if c >= 0.6:
        return "moves closely with it"
    if c >= 0.45:
        return "often moves with it"
    return "loosely moves with it"


def build_related(
    ticker: str,
    sympathy_rows: Sequence[Mapping[str, Any]],
    text_rows: Sequence[Mapping[str, Any]],
    names: Mapping[str, str],
    latest_as_of: Optional[date],
    today: date,
) -> Dict[str, Any]:
    """The related-stocks response for `ticker`, as a plain dict."""
    def label(peer: str) -> str:
        return names.get(peer) or peer

    moves = [
        {
            "ticker": r["peer"],
            "name": label(r["peer"]),
            "correlation": round(float(r["corr"]), 3),
            "sessions": int(r["n"]),
            "closeness": closeness_words(r["corr"]),
        }
        for r in sorted(sympathy_rows, key=lambda r: -float(r["corr"]))
        if r["peer"] != ticker
    ][:MAX_MOVES_WITH]
    similar = [
        {"ticker": r["peer"], "name": label(r["peer"]), "similarity": round(float(r["sim"]), 3)}
        for r in sorted(text_rows, key=lambda r: -float(r["sim"]))
        if r["peer"] != ticker
    ][:MAX_SIMILAR]

    dates: List[date] = [r["as_of"] for r in list(sympathy_rows) + list(text_rows) if r.get("as_of")]
    as_of = max(dates) if dates else latest_as_of
    empty: Optional[str] = None
    if not moves and not similar:
        empty = "not_computed" if latest_as_of is None else "none_found"
    return {
        "ticker": ticker,
        "as_of": as_of,
        "stale": as_of is not None and (today - as_of).days > STALE_AFTER_DAYS,
        "window_sessions": WINDOW_SESSIONS,
        "moves_with": moves,
        "similar_business": similar,
        "empty_reason": empty,
        "note": NOTE,
    }


__all__ = ["MAX_MOVES_WITH", "MAX_SIMILAR", "NOTE", "STALE_AFTER_DAYS", "WINDOW_SESSIONS", "build_related",
           "clean_ticker", "closeness_words"]
