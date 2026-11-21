"""Why a signal was not raised on a stock, in plain English, from the shadow record the signal engine keeps.

The engine records a would-be signal (services/agents/signal_engine.py `_record_shadow`) when a proven signal type did not pass
the tradability test (shared/utils/tradable_cells.py) or the stock itself failed its liquidity or borrow check. Pure: entries in,
plain dictionaries out; the route reads the Redis sorted set.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

WINDOW_WORDS = {"15m": "next 15 minutes", "1h": "next hour", "1d": "next day", "5d": "next five days", "30d": "next month"}
_LIQUIDITY = re.compile(r"\$(\d+(?:\.\d+)?)M is under \$(\d+(?:\.\d+)?)M")


def explain_reason(why: str) -> Dict[str, Any]:
    """The gate and a sentence for a recorded reason; `measured` and `required` are in millions of dollars a day when known."""
    w = str(why or "")
    if w.startswith("proven cell not cleared"):
        return {"held_back_by": "tradability test",
                "explanation": "This type of signal worked in past data but is not one of the few that passed the tradability test, "
                               "so it is recorded for research and not raised as a signal."}
    m = _LIQUIDITY.search(w)
    if m:
        return {"held_back_by": "liquidity bar",
                "explanation": f"The stock trades about ${float(m.group(1)):,.0f} million a day, under the ${float(m.group(2)):,.1f} million "
                               "bar needed for a signal to be tradable.",
                "measured": float(m.group(1)), "required": float(m.group(2))}
    if w == "no dollar-volume history":
        return {"held_back_by": "liquidity bar", "explanation": "There is no recent trading-volume history for this stock, so it cannot be judged tradable."}
    if w == "no borrow data for a short":
        return {"held_back_by": "borrow check", "explanation": "This is a short signal and there is no borrowing information for the stock."}
    if w.startswith("not shortable"):
        return {"held_back_by": "borrow check", "explanation": "This is a short signal on a stock that cannot be shorted or is hard to borrow."}
    return {"held_back_by": "other", "explanation": w or "No reason was recorded."}


def entry_view(entry: Mapping[str, Any], descriptions: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """One recorded would-be signal as the dashboard shows it."""
    family = str(entry.get("family") or "")
    horizon = str(entry.get("horizon") or "")
    direction = int(entry.get("direction") or 0)
    out: Dict[str, Any] = {
        "signal": (descriptions or {}).get(family, family.replace("_", " ")),
        "expected_move": "rise" if direction > 0 else "fall" if direction < 0 else "none",
        "window": WINDOW_WORDS.get(horizon, horizon),
        "when": entry.get("at"),
    }
    out.update(explain_reason(str(entry.get("why") or "")))
    return out


def for_ticker(raw_entries: Iterable[Any], ticker: str, descriptions: Optional[Mapping[str, str]] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """The newest `limit` recorded entries for a ticker (the input is newest first); an unreadable entry is skipped."""
    t = str(ticker).strip().upper()
    out: List[Dict[str, Any]] = []
    for item in raw_entries:
        try:
            e = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
        except (ValueError, AttributeError):
            continue
        if isinstance(e, dict) and str(e.get("instrument") or "").upper() == t:
            out.append(entry_view(e, descriptions))
            if len(out) >= limit:
                break
    return out


def for_tickers(raw_entries: Iterable[Any], tickers: Iterable[str], descriptions: Optional[Mapping[str, str]] = None, limit: int = 20) -> Dict[str, List[Dict[str, Any]]]:
    """for_ticker for several tickers in one pass: each entry is parsed once (the input is newest first) and the walk stops when every ticker has `limit`."""
    wanted = {str(t).strip().upper() for t in tickers}
    out: Dict[str, List[Dict[str, Any]]] = {t: [] for t in wanted}
    full = 0
    for item in raw_entries:
        try:
            e = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
        except (ValueError, AttributeError):
            continue
        if not isinstance(e, dict):
            continue
        t = str(e.get("instrument") or "").upper()
        if t in out and len(out[t]) < limit:
            out[t].append(entry_view(e, descriptions))
            if len(out[t]) == limit:
                full += 1
                if full == len(out):
                    break
    return out


__all__ = ["WINDOW_WORDS", "entry_view", "explain_reason", "for_ticker", "for_tickers"]
