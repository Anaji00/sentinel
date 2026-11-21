"""Plain sectors from SEC Standard Industrial Classification codes, and the heat-map aggregation over them.

SIC is the only industry code we hold for the whole universe (6,367 of 6,389 profiled names); it is older and coarser than GICS, so this is an
approximation, said so in the response: where the two disagree (Alphabet and Meta are SIC 7370, computer services, so they land in
Information Technology, not Communication Services) the SIC reading wins. First matching rule wins, narrow ranges before broad ones.
Pure; no I/O.
"""
from __future__ import annotations

import math
from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

SECTORS = ("Energy", "Materials", "Industrials", "Consumer Discretionary", "Consumer Staples", "Health Care", "Financials",
           "Real Estate", "Information Technology", "Communication Services", "Utilities", "Other")

# (low, high inclusive, sector), most specific first.
RULES: Tuple[Tuple[int, int, str], ...] = (
    (6770, 6770, "Other"),                                              # blank-check shells
    (6798, 6798, "Real Estate"), (6500, 6599, "Real Estate"),
    (6000, 6799, "Financials"),
    (4900, 4999, "Utilities"),
    (1200, 1399, "Energy"), (2900, 2999, "Energy"), (4610, 4619, "Energy"), (5170, 5172, "Energy"),
    (2830, 2836, "Health Care"), (3826, 3826, "Health Care"), (3841, 3845, "Health Care"), (3851, 3851, "Health Care"),
    (5047, 5047, "Health Care"), (5122, 5122, "Health Care"), (8000, 8099, "Health Care"), (8731, 8734, "Health Care"),
    (5912, 5912, "Consumer Staples"), (5331, 5331, "Consumer Staples"), (5400, 5499, "Consumer Staples"),
    (5140, 5199, "Consumer Staples"), (2000, 2199, "Consumer Staples"), (100, 999, "Consumer Staples"),
    (3570, 3579, "Information Technology"), (3661, 3679, "Information Technology"), (3670, 3699, "Information Technology"),
    (3810, 3819, "Information Technology"), (3825, 3825, "Information Technology"), (3827, 3829, "Information Technology"),
    (7370, 7379, "Information Technology"), (5045, 5045, "Information Technology"), (5065, 5065, "Information Technology"),
    (4800, 4899, "Communication Services"), (2700, 2799, "Communication Services"), (7810, 7829, "Communication Services"),
    (7840, 7841, "Communication Services"), (7310, 7319, "Communication Services"),
    (3711, 3716, "Consumer Discretionary"), (3630, 3639, "Consumer Discretionary"), (3750, 3751, "Consumer Discretionary"),
    (3900, 3999, "Consumer Discretionary"), (2200, 2399, "Consumer Discretionary"), (2500, 2599, "Consumer Discretionary"),
    (3100, 3199, "Consumer Discretionary"), (5200, 5399, "Consumer Discretionary"), (5500, 5999, "Consumer Discretionary"),
    (7000, 7099, "Consumer Discretionary"), (7830, 7839, "Consumer Discretionary"), (7842, 7999, "Consumer Discretionary"),
    (8200, 8299, "Consumer Discretionary"),
    (1000, 1199, "Materials"), (1400, 1499, "Materials"), (2400, 2499, "Materials"), (2600, 2699, "Materials"),
    (2800, 2899, "Materials"), (3200, 3399, "Materials"), (3080, 3089, "Materials"), (3000, 3079, "Materials"),
    (1500, 1799, "Industrials"), (3400, 3569, "Industrials"), (3580, 3629, "Industrials"), (3720, 3799, "Industrials"),
    (3800, 3899, "Industrials"), (4000, 4299, "Industrials"), (4400, 4599, "Industrials"), (4700, 4799, "Industrials"),
    (5000, 5099, "Industrials"), (7300, 7369, "Industrials"), (7380, 7399, "Industrials"), (7500, 7599, "Industrials"),
    (8700, 8799, "Industrials"), (4200, 4299, "Industrials"), (4300, 4399, "Industrials"),
)


def sector_of(sic: Any) -> str:
    """The plain sector for an SIC code; "Other" when the code is missing or matches no rule."""
    try:
        code = int(sic)
    except (TypeError, ValueError):
        return "Other"
    for lo, hi, name in RULES:
        if lo <= code <= hi:
            return name
    return "Other"


def _finite(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def aggregate(stocks: Sequence[Mapping[str, Any]], by: str, top_n: int = 3, min_group: int = 3) -> List[Dict[str, Any]]:
    """Groups of stocks (each {ticker, key, label, change_pct, market_cap, volume_vs_20d}) by `key`: n, cap-weighted and median day change,
    the share of stocks up, median volume against its 20-day average, and the best and worst names. Stocks with no day change are left out of
    the change statistics (counted in n_priced); a group of fewer than `min_group` priced stocks reports null statistics. Largest first."""
    groups: Dict[str, List[Mapping[str, Any]]] = {}
    for s in stocks:
        groups.setdefault(str(s["key"]), []).append(s)
    out: List[Dict[str, Any]] = []
    for key, items in groups.items():
        priced = [s for s in items if _finite(s.get("change_pct")) is not None]
        caps = [(float(s["change_pct"]), float(s["market_cap"])) for s in priced if _finite(s.get("market_cap")) and float(s["market_cap"]) > 0]
        wsum = sum(c for _, c in caps)
        ok = len(priced) >= min_group
        vols = [float(s["volume_vs_20d"]) for s in priced if _finite(s.get("volume_vs_20d")) is not None]
        ranked = sorted(priced, key=lambda s: float(s["change_pct"]))
        out.append({
            "key": key, "label": items[0].get("label") or key, "n": len(items), "n_priced": len(priced),
            "market_cap": round(sum(float(s["market_cap"]) for s in items if _finite(s.get("market_cap"))), 0),
            "change_pct": round(sum(ch * c for ch, c in caps) / wsum, 2) if ok and wsum > 0 else None,
            "change_pct_median": round(median(float(s["change_pct"]) for s in priced), 2) if ok else None,
            "breadth_up_share": round(sum(1 for s in priced if float(s["change_pct"]) > 0) / len(priced), 3) if ok else None,
            "volume_vs_20d_median": round(median(vols), 2) if ok and vols else None,
            "top": [{"ticker": s["ticker"], "change_pct": float(s["change_pct"])} for s in ranked[::-1][:top_n]] if ok else [],
            "bottom": [{"ticker": s["ticker"], "change_pct": float(s["change_pct"])} for s in ranked[:top_n]] if ok else [],
        })
    return sorted(out, key=lambda g: -g["market_cap"])


__all__ = ["RULES", "SECTORS", "aggregate", "sector_of"]
