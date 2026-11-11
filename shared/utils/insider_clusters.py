"""Insider buying clusters rebuilt from stored Form 4 trades.

The rule is the live agent's own (services/agents/quant_trading_engine.py, `_process_insider_form4`), applied to the
filing dates in `insider_transactions` instead of arrival times; it is registered in reports/Event backtest scope.md
under "G3 fix 2 registered". Pure functions; no I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Sequence

WINDOW_DAYS = 14
MIN_BUYERS = 2
MIN_NET_BUY_USD = 250_000.0
SOURCE = "insider_cluster_g3_history"


@dataclass(frozen=True)
class FormFourTrade:
    """One stored open-market trade."""
    filed: date
    code: str                    # "P" purchase, "S" sale
    planned: bool                # Rule 10b5-1 box: counts neither way
    who: str                     # the insider's name (CIK where the name is missing)
    usd: Optional[float]         # shares times price when both are positive, else None


@dataclass(frozen=True)
class Cluster:
    ticker: str
    day: date                    # the filing day that met the gate
    buyers: List[str]
    net_buy_usd: float


def trade_usd(shares: Optional[float], price: Optional[float]) -> Optional[float]:
    """Dollar value, or None when either side is missing or not positive (an unsized trade is not a guess)."""
    if shares is None or price is None or shares <= 0 or price <= 0:
        return None
    return shares * price


def find_clusters(ticker: str, trades: Sequence[FormFourTrade]) -> List[Cluster]:
    """Clusters for one ticker, oldest first.

    At each filing day: the trades filed in the 14 days up to that day; BUY = unplanned P, SELL = unplanned S.
    Fires at 2+ distinct buyers and net bought dollars (floored at zero) of at least $250,000, and not again within
    14 days of the last cluster.
    """
    ordered = sorted(trades, key=lambda t: t.filed)
    days = sorted({t.filed for t in ordered})
    out: List[Cluster] = []
    last: Optional[date] = None
    lo = 0
    for day in days:
        start = day - timedelta(days=WINDOW_DAYS)
        while ordered[lo].filed < start:
            lo += 1
        buyers: Dict[str, None] = {}
        bought = sold = 0.0
        for t in ordered[lo:]:
            if t.filed > day:
                break
            if t.planned:
                continue
            if t.code == "P":
                if t.who:
                    buyers[t.who] = None
                bought += t.usd or 0.0
            elif t.code == "S":
                sold += t.usd or 0.0
        net = max(0.0, bought - sold)
        if len(buyers) >= MIN_BUYERS and net >= MIN_NET_BUY_USD:
            if last is None or (day - last).days >= WINDOW_DAYS:
                out.append(Cluster(ticker, day, list(buyers), round(net, 2)))
                last = day
    return out


def event_id(ticker: str, day: date) -> str:
    """Deterministic, so a re-run inserts nothing new."""
    return f"g3cluster:{ticker}:{day.isoformat()}"


__all__ = ["Cluster", "FormFourTrade", "SOURCE", "WINDOW_DAYS", "event_id", "find_clusters", "trade_usd"]
