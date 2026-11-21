"""One row per followed stock for a 'Today' strip: price and day change, the next earnings date, the model's percentile, how many signals were held back
and the latest regime label. Every field comes from the same builders as the single-stock routes (quote, snapshot's earnings rule, evidence's regime), so a
strip row and a stock page agree. A field that cannot be known is None, never 0; a stock with no data at all still gets a row with only its ticker. Pure.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from shared.utils.regime_packet import action_fields

MAX_TICKERS = 50


def build_row(ticker: str, quote: Optional[Mapping[str, Any]], earnings: Optional[Mapping[str, Any]], model: Optional[Mapping[str, Any]],
              held_back: Optional[int], regime: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    q = quote or {}
    return {
        "ticker": ticker,
        "price": q.get("price"),
        "change_pct": q.get("change_pct"),
        "session": q.get("session"),
        "delayed": q.get("delayed"),
        "as_of": q.get("as_of"),
        "next_earnings": dict(earnings) if earnings else None,
        "model_percentile": (model or {}).get("percentile"),
        "model_as_of": (model or {}).get("as_of") if (model or {}).get("percentile") is not None else None,
        "held_back_count": held_back,
        "regime": {"label": regime.get("label"), "action": regime.get("action"), **action_fields(regime.get("action")), "alert_at": regime.get("alert_at")} if regime else None,
    }


__all__ = ["MAX_TICKERS", "build_row"]
