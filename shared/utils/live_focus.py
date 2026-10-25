"""
shared/utils/live_focus.py

Which names get the free plan's scarce live slots: 30 IEX trade-and-quote
symbols on Alpaca, 50 trade-tick symbols on Finnhub, 200 indicative option
quotes. Spent on what the portfolio is doing rather than on whatever the radar
noticed last:

  1. names the auto-trader is trading today
  2. names entering or leaving the book at the next open
  3. held names with earnings today
  4. held names, the most extreme ranks first (largest expected moves)
  5. the radar's watched names, newest first

Pure: books, scores and lists in; one ordered list out. The collector writes it
to LIVE_FOCUS_KEY (a sorted set, higher score first) and every stream reads it.
Also the tick parsers for the IEX trade/quote channels and the option stream.
"""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

LIVE_FOCUS_KEY = "sentinel:live_focus"
ALPACA_TICK_SLOTS = 30
FINNHUB_TICK_SLOTS = 50
OPTION_QUOTE_SLOTS = 200
# Quotes kept per symbol at most this often: the top of book on a liquid name
# changes many times a second, and one state a quarter-second is the grain any
# spread or imbalance measure here needs.
QUOTE_MIN_GAP_SEC = 0.25
# OCC option symbol: underlying, YYMMDD, C/P, strike x 1000.
OCC = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


def _clean(names: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for n in names or []:
        t = str(n or "").strip().upper()
        if t and t not in out:
            out.append(t)
    return out


def rank_focus(trading: Iterable[str], changing: Iterable[str], held: Iterable[str],
               percentiles: Mapping[str, float], earnings_today: Iterable[str],
               watched: Sequence[str]) -> List[str]:
    """Every candidate once, in the order the slots should go."""
    held_l = _clean(held)
    earn = set(_clean(earnings_today))
    by_edge = sorted(held_l, key=lambda t: -abs(float(percentiles.get(t, 0.5)) - 0.5))
    tiers = [
        _clean(trading),
        _clean(changing),
        [t for t in by_edge if t in earn],
        by_edge,
        _clean(watched),
    ]
    out: List[str] = []
    for tier in tiers:
        for t in tier:
            if t not in out:
                out.append(t)
    return out


def focus_scores(ranked: Sequence[str]) -> Dict[str, float]:
    """Sorted-set scores: the first name highest."""
    n = len(ranked)
    return {t: float(n - i) for i, t in enumerate(ranked)}


def _ts(raw: Any) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def trade_row(msg: Mapping[str, Any], source: str) -> Optional[Tuple]:
    """A stream trade ({"T": "t", "S", "p", "s", "t", "i", "x", "c"}) as a tick_trades row."""
    if msg.get("T") != "t":
        return None
    sym = str(msg.get("S") or "").upper()
    ts = _ts(msg.get("t"))
    try:
        px, size = float(msg["p"]), float(msg.get("s") or 0.0)
    except (KeyError, TypeError, ValueError):
        return None
    if not sym or ts is None or not math.isfinite(px) or px <= 0 or size < 0:
        return None
    tid = msg.get("i")
    conds = msg.get("c")
    return (sym, ts, px, size, int(tid) if isinstance(tid, int) else None,
            str(msg.get("x")) if msg.get("x") is not None else None,
            [str(c) for c in conds] if isinstance(conds, list) else None, source)


def quote_row(msg: Mapping[str, Any], source: str) -> Optional[Tuple]:
    """A stream quote ({"T": "q", "S", "bp", "bs", "ap", "as", "t"}) as a tick_quotes row."""
    if msg.get("T") != "q":
        return None
    sym = str(msg.get("S") or "").upper()
    ts = _ts(msg.get("t"))
    try:
        bid, ask = float(msg.get("bp") or 0.0), float(msg.get("ap") or 0.0)
        bs, asz = float(msg.get("bs") or 0.0), float(msg.get("as") or 0.0)
    except (TypeError, ValueError):
        return None
    if not sym or ts is None or (bid <= 0 and ask <= 0):
        return None
    return (sym, ts, bid or None, bs, ask or None, asz, source)


def option_tick_row(msg: Mapping[str, Any], source: str) -> Optional[Tuple]:
    """An option stream quote or trade as an option_ticks row:
    (contract, underlying, time, kind, bid, bid_size, ask, ask_size, price, size, source).
    The stream sends MessagePack, whose timestamps decode to datetimes."""
    kind = msg.get("T")
    if kind not in ("q", "t"):
        return None
    contract = str(msg.get("S") or "").upper()
    ts = msg.get("t") if isinstance(msg.get("t"), datetime) else _ts(msg.get("t"))
    m = OCC.match(contract)
    if not contract or ts is None or m is None:
        return None
    try:
        if kind == "q":
            bid, ask = float(msg.get("bp") or 0.0), float(msg.get("ap") or 0.0)
            if bid <= 0 and ask <= 0:
                return None
            return (contract, m.group(1), ts, "q", bid or None, float(msg.get("bs") or 0.0), ask or None,
                    float(msg.get("as") or 0.0), None, None, source)
        px = float(msg["p"])
        if px <= 0:
            return None
        return (contract, m.group(1), ts, "t", None, None, None, None, px, float(msg.get("s") or 0.0), source)
    except (KeyError, TypeError, ValueError):
        return None


def pick_contracts(chain: Sequence[Tuple[str, float, str]], spot: float, per_side: int = 2) -> List[str]:
    """The `per_side` calls and puts with strikes nearest the stock's price."""
    if not spot or spot <= 0:
        return []
    out: List[str] = []
    for right in ("C", "P"):
        side = sorted((c for c in chain if c[2] == right), key=lambda c: abs(c[1] - spot))
        out.extend(c[0] for c in side[:per_side])
    return out


class QuoteThrottle:
    """Keeps a quote only when it changed and the symbol's last kept one is at
    least `gap` seconds older."""

    def __init__(self, gap: float = QUOTE_MIN_GAP_SEC) -> None:
        self.gap = gap
        self.last: Dict[str, Tuple[datetime, Tuple]] = {}

    def keep(self, row: Tuple) -> bool:
        sym, ts, state = row[0], row[1], row[2:6]
        prev = self.last.get(sym)
        if prev is not None and (prev[1] == state or (ts - prev[0]).total_seconds() < self.gap):
            return False
        self.last[sym] = (ts, state)
        return True


__all__ = ["ALPACA_TICK_SLOTS", "FINNHUB_TICK_SLOTS", "LIVE_FOCUS_KEY", "OCC", "OPTION_QUOTE_SLOTS",
           "QUOTE_MIN_GAP_SEC", "QuoteThrottle", "focus_scores", "option_tick_row", "pick_contracts", "quote_row",
           "rank_focus", "trade_row"]
