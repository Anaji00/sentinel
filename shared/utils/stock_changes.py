"""
What changed on a stock recently, from stored rows. Pure; the gateway reads the rows.

Every part carries its date and says what it is worth. A part with no rows is
"no_data" and one whose read failed is "unavailable", so a gap never reads as
"nothing changed". Buyer and seller are inferred from enrichment's tags, and flow
with no readable side stays apart rather than being shared out.
"""
from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

WINDOW_DAYS = 7
LOOKBACK_SESSIONS = 5
MAX_EVENTS = 12

MACRO_NAMES: Dict[str, str] = {
    "BZ=F": "Brent crude", "CL=F": "WTI crude", "DXY": "US dollar index", "GC=F": "Gold", "HG=F": "Copper",
    "NG=F": "Natural gas", "SB=F": "Sugar", "SI=F": "Silver", "US10Y": "10-year Treasury yield",
    "US2Y": "2-year Treasury yield", "US30Y": "30-year Treasury yield", "VIX": "VIX volatility index",
    "ZC=F": "Corn", "ZS=F": "Soybeans", "ZW=F": "Wheat",
}

FLOW_NOTES = {
    "equity_block": "Side inferred from the price against the previous trade; prices between feeds can mislead it.",
    "options_flow": "Side inferred from the quote, and most prints cannot be read. Delayed about 15 minutes, and not "
                    "every print is caught. Measured on 5 October: large prints carried no edge on direction.",
}
RANK_NOTE = "Factor model's monthly percentile, 1.0 strongest. The model has not cleared its validation gate."


def _num(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    return round((new / old - 1.0) * 100.0, 2) if new is not None and old else None


def price_change(rows: Sequence[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Rows newest first, up to LOOKBACK_SESSIONS + 1."""
    if not rows:
        return None
    last = _num(rows[0]["close"])
    return {
        "as_of": rows[0]["day"], "close": last,
        "change_1d_pct": _pct(last, _num(rows[1]["close"])) if len(rows) > 1 else None,
        "change_5d_pct": _pct(last, _num(rows[LOOKBACK_SESSIONS]["close"])) if len(rows) > LOOKBACK_SESSIONS else None,
    }


def rank_change(rows: Sequence[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Rows newest first. The earlier value is absent until enough days are stored."""
    if not rows:
        return None
    return {
        "as_of": rows[0]["as_of"], "percentile": _num(rows[0]["percentile"]),
        "percentile_before": _num(rows[LOOKBACK_SESSIONS]["percentile"]) if len(rows) > LOOKBACK_SESSIONS else None,
        "note": RANK_NOTE,
    }


def options_change(rows: Sequence[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    return {
        "as_of": rows[0]["day"], "atm_iv": _num(rows[0]["atm_iv"]),
        "atm_iv_before": _num(rows[LOOKBACK_SESSIONS]["atm_iv"]) if len(rows) > LOOKBACK_SESSIONS else None,
        "put_call_ratio": _num(rows[0]["put_call_ratio"]),
    }


def short_change(row: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "as_of": row["settlement_date"], "short_shares": _num(row["short_qty"]),
        "change_pct": _num(row["change_pct"]), "days_to_cover": _num(row["days_to_cover"]),
    }


def flow_summary(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Rows of (type, side, trades, usd), side being 'buyer', 'seller' or 'unknown'."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        kind = str(r["type"])
        side = str(r["side"])
        f = out.setdefault(kind, {
            "kind": kind, "buyer_usd": 0.0, "seller_usd": 0.0, "unknown_usd": 0.0,
            "buyer_trades": 0, "seller_trades": 0, "unknown_trades": 0, "note": FLOW_NOTES.get(kind, ""),
        })
        f[f"{side}_usd"] += _num(r["usd"]) or 0.0
        f[f"{side}_trades"] += int(r["trades"] or 0)
    return sorted(out.values(), key=lambda f: f["kind"])


def macro_links(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [{
        "symbol": r["macro"], "name": MACRO_NAMES.get(str(r["macro"]), str(r["macro"])),
        "direction": "with" if (_num(r["beta"]) or 0.0) > 0 else "against",
        "t_stat": _num(r["t"]), "sessions": r["n"], "as_of": r["as_of"],
    } for r in rows]


__all__ = ["FLOW_NOTES", "LOOKBACK_SESSIONS", "MACRO_NAMES", "MAX_EVENTS", "RANK_NOTE", "WINDOW_DAYS", "flow_summary",
           "macro_links", "options_change", "price_change", "rank_change", "short_change"]
