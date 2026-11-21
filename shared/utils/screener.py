"""Screener rows: one stock per row with its price, day change, volume against its 20-day average, the model's percentile and its next event.

Reuses the quote builder so a screener row and a hero quote agree. A figure that cannot be known is None, never 0, and a row with no value
for the sort key goes last in either direction. A minimum market cap excludes names whose cap is unknown (unknown is not large). Pure.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping, Optional, Sequence

SORT_KEYS = ("change_pct", "rel_volume", "model_pct", "next_event")
MAX_LIMIT = 500


def volume_vs_20d(volumes: Sequence[Optional[float]]) -> Optional[float]:
    """Latest completed session's volume over the mean of the 20 sessions before it; None under 15 prior sessions or a zero mean."""
    vs = [float(v) for v in volumes if v is not None and math.isfinite(float(v)) and float(v) >= 0]
    if len(vs) < 16:
        return None
    last, prior = vs[-1], vs[-21:-1]
    if len(prior) < 15:
        return None
    mean = sum(prior) / len(prior)
    return round(last / mean, 2) if mean > 0 else None


def build_row(ticker: str, quote: Mapping[str, Any], volumes: Sequence[Optional[float]], pct: Optional[float], model_as_of: Optional[str],
              next_event: Optional[Mapping[str, Any]], market_cap: Optional[float], sector: Optional[str] = None,
              industry: Optional[str] = None) -> Dict[str, Any]:
    return {
        "sector": sector,
        "industry": industry,
        "ticker": ticker,
        "price": quote.get("price"),
        "change_pct": quote.get("change_pct"),
        "volume_vs_20d": volume_vs_20d(volumes),
        "model_percentile": round(float(pct), 4) if pct is not None and math.isfinite(float(pct)) else None,
        "model_as_of": model_as_of if pct is not None else None,
        "next_event": dict(next_event) if next_event else None,
        "market_cap": round(market_cap, 0) if market_cap else None,
    }


def _key(row: Mapping[str, Any], sort: str) -> Optional[Any]:
    if sort == "change_pct":
        return row.get("change_pct")
    if sort == "rel_volume":
        return row.get("volume_vs_20d")
    if sort == "model_pct":
        return row.get("model_percentile")
    ev = row.get("next_event")
    return ev.get("date") if ev else None


def select(rows: Sequence[Mapping[str, Any]], sort: str, direction: str, limit: int, min_cap: Optional[float],
           sector: Optional[str] = None) -> List[Mapping[str, Any]]:
    """Filter by cap, order by `sort` (missing last either way) and cut to `limit`. `next_event` defaults to soonest first."""
    if sort not in SORT_KEYS:
        raise ValueError(f"sort must be one of {SORT_KEYS}")
    pool = [r for r in rows if min_cap is None or (r.get("market_cap") is not None and r["market_cap"] >= min_cap)]
    if sector:
        pool = [r for r in pool if r.get("sector") == sector]
    descending = (direction == "desc") if sort != "next_event" or direction in ("asc", "desc") else False
    have = [r for r in pool if _key(r, sort) is not None]
    lack = [r for r in pool if _key(r, sort) is None]
    have.sort(key=lambda r: (_key(r, sort), r["ticker"]), reverse=descending)
    return (have + sorted(lack, key=lambda r: r["ticker"]))[: max(1, min(int(limit), MAX_LIMIT))]


__all__ = ["MAX_LIMIT", "SORT_KEYS", "build_row", "select", "volume_vs_20d"]
