"""Implementation shortfall of the trader's fills against four references (quant report T9, measurement only): the prior close, the 09:40 New York price, the 10:30
price and the day's volume-weighted average price. Nothing here changes order timing, limits or sizes. Positive shortfall is a cost: paying above or selling below the
reference. Pure: fills and one-minute bars in, a list of rows and a summary out."""
from __future__ import annotations

import statistics
from datetime import date, datetime, time
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
Bar = Tuple[datetime, float, float]          # minute start (tz-aware), close, volume
MAX_BAR_AGE_SEC = 600


def _sign(side: object) -> int:
    return 1 if str(side).lower() == "buy" else -1 if str(side).lower() == "sell" else 0


def bps(fill: float, ref: Optional[float], side: object) -> Optional[float]:
    """Cost in basis points of a fill against `ref`: positive when the fill is worse (buy above, sell below); None without a usable price or side."""
    s = _sign(side)
    if not s or not ref or ref <= 0 or not fill or fill <= 0:
        return None
    return round(s * (fill - ref) / ref * 1e4, 2)


def price_at(bars: Sequence[Bar], day: date, at: time) -> Optional[float]:
    """Close of the last minute bar at or before `at` New York time on `day`, within 10 minutes of it; None otherwise."""
    target = datetime.combine(day, at, NY)
    best: Optional[Tuple[datetime, float]] = None
    for t, close, _ in bars:
        local = t.astimezone(NY)
        if local.date() == day and local <= target and (best is None or local > best[0]):
            best = (local, close)
    if best is None or (target - best[0]).total_seconds() > MAX_BAR_AGE_SEC:
        return None
    return best[1]


def day_vwap(bars: Sequence[Bar], day: date) -> Optional[float]:
    """Volume-weighted price of the regular session (09:30 to 16:00 New York) minute bars of `day`; None without volume."""
    pv = v = 0.0
    for t, close, vol in bars:
        local = t.astimezone(NY)
        if local.date() == day and time(9, 30) <= local.time() < time(16, 0) and vol and vol > 0:
            pv += close * vol
            v += vol
    return pv / v if v > 0 else None


def rows_for(fills: Sequence[Mapping[str, Any]], bars_by_symbol: Mapping[str, Sequence[Bar]], prior_close: Mapping[str, float], day: date) -> List[Dict[str, Any]]:
    """One row per filled order: the fill and its shortfall in bps against each reference (None where the reference is missing)."""
    out = []
    for f in fills:
        sym = str(f["symbol"])
        px = float(f.get("fill_price") or 0)
        bars = bars_by_symbol.get(sym, ())
        refs = {"prior_close": prior_close.get(sym), "ref_0940": price_at(bars, day, time(9, 40)), "ref_1030": price_at(bars, day, time(10, 30)), "vwap": day_vwap(bars, day)}
        out.append({"symbol": sym, "side": f.get("side"), "reason": f.get("reason"), "qty": float(f.get("filled_qty") or 0), "fill_price": px,
                    **{f"shortfall_{k}_bps": bps(px, v, f.get("side")) for k, v in refs.items()}, "refs": refs})
    return out


def summarize(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Count, and per reference the notional-weighted mean and the median shortfall in bps."""
    out: Dict[str, Any] = {"orders": len(rows)}
    for key in ("prior_close", "ref_0940", "ref_1030", "vwap"):
        col = f"shortfall_{key}_bps"
        vals = [(r[col], r["qty"] * r["fill_price"]) for r in rows if r.get(col) is not None]
        w = sum(n for _, n in vals)
        out[key] = {"n": len(vals), "median_bps": round(statistics.median(v for v, _ in vals), 2) if vals else None,
                    "weighted_mean_bps": round(sum(v * n for v, n in vals) / w, 2) if w > 0 else None}
    return out


__all__ = ["bps", "day_vwap", "price_at", "rows_for", "summarize"]
