"""
shared/utils/daily_crosscheck.py

An independent check of the stored daily bars against a second vendor. Pure;
the tradfi collector fetches the second vendor's bars and stores the report.

With a POLYGON_API_KEY: Polygon's grouped-daily bars, every US stock for one
day in one request (free tier). Without one: Yahoo's keyless chart endpoint
for a daily sample -- the most-traded names, where an error costs most, and a
rotating random draw from the rest, so the whole universe is covered over time.

Only the latest completed session is compared: adjustments are applied
backwards from each corporate action, so on the most recent day every vendor's
adjusted bar is the raw bar, whatever its adjustment policy.
"""
from __future__ import annotations

from statistics import median
import random
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

GROUPED_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{day}"
CROSSCHECK_KEY = "sentinel:quality:daily_crosscheck"   # the latest report, written by collector-tradfi
CLOSE_TOLERANCE = 0.005         # 0.5%: beyond rounding and late prints
VOLUME_TOLERANCE = 0.10         # consolidated volumes agree within a few percent
WORST_SHOWN = 20
YAHOO_DAILY_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}    # Yahoo refuses a blank client
YAHOO_SAMPLE_TOP = 40           # the most-traded names, every day
YAHOO_SAMPLE_RANDOM = 60        # plus a random draw from the rest, different each day
_NY = ZoneInfo("America/New_York")


def compare(ours: Mapping[str, Mapping[str, float]], theirs: Mapping[str, Mapping[str, float]]) -> Dict[str, Any]:
    """Stocks both vendors have for the day, and where they disagree.

    `ours` / `theirs`: {ticker: {"close": .., "volume": ..}}. Polygon writes
    share classes as BRK.B like the stored bars, so tickers join directly."""
    both = sorted(set(ours) & set(theirs))
    close_off: List[Dict[str, Any]] = []
    vol_ratios: List[float] = []
    vol_off = 0
    for t in both:
        a, b = ours[t], theirs[t]
        ac, bc = float(a.get("close") or 0), float(b.get("close") or 0)
        if ac > 0 and bc > 0:
            diff = ac / bc - 1
            if abs(diff) > CLOSE_TOLERANCE:
                close_off.append({"ticker": t, "ours": ac, "theirs": bc, "diff_pct": round(diff * 100, 2)})
        av, bv = float(a.get("volume") or 0), float(b.get("volume") or 0)
        if av > 0 and bv > 0:
            r = av / bv
            vol_ratios.append(r)
            if abs(r - 1) > VOLUME_TOLERANCE:
                vol_off += 1
    return {
        "compared": len(both),
        "only_ours": len(set(ours) - set(theirs)),
        "only_theirs": len(set(theirs) - set(ours)),
        "close_mismatches": len(close_off),
        "volume_mismatches": vol_off,
        "median_volume_ratio": round(median(vol_ratios), 4) if vol_ratios else None,
        "worst_closes": sorted(close_off, key=lambda x: -abs(x["diff_pct"]))[:WORST_SHOWN],
        "ok": bool(both) and len(close_off) <= max(5, len(both) // 200),
    }


def parse_grouped(body: Mapping[str, Any]) -> Dict[str, Dict[str, float]]:
    """Polygon grouped-daily results -> {ticker: {"close", "volume"}}."""
    out: Dict[str, Dict[str, float]] = {}
    for r in body.get("results") or []:
        t = str(r.get("T") or "").upper()
        if t and r.get("c"):
            out[t] = {"close": float(r["c"]), "volume": float(r.get("v") or 0)}
    return out


def yahoo_symbol(ticker: str) -> str:
    """Yahoo writes share classes with a dash: BRK.B -> BRK-B."""
    return ticker.upper().replace(".", "-")


def yahoo_sample(by_dollar_volume: Sequence[str], day: date, top: int = YAHOO_SAMPLE_TOP,
                 rand: int = YAHOO_SAMPLE_RANDOM) -> List[str]:
    """The day's sample: the `top` most-traded tickers and `rand` others drawn
    with the day as seed, so a rerun checks the same names."""
    head = list(by_dollar_volume[:top])
    rest = list(by_dollar_volume[top:])
    random.Random(day.toordinal()).shuffle(rest)
    return head + rest[:rand]


def parse_yahoo_daily(body: Mapping[str, Any], day: date) -> Optional[Dict[str, float]]:
    """One ticker's bar for `day` from a Yahoo chart response (interval=1d), or
    None. Bars are stamped at the session's open; the day is read in New York."""
    try:
        r = ((body.get("chart") or {}).get("result") or [])[0]
        stamps = r.get("timestamp") or []
        q = ((r.get("indicators") or {}).get("quote") or [{}])[0]
        closes, vols = q.get("close") or [], q.get("volume") or []
    except (AttributeError, IndexError, TypeError):
        return None
    for i, ts in enumerate(stamps):
        if datetime.fromtimestamp(int(ts), _NY).date() != day:
            continue
        c = closes[i] if i < len(closes) else None
        v = vols[i] if i < len(vols) else None
        if c:
            return {"close": float(c), "volume": float(v or 0)}
    return None


__all__ = ["CLOSE_TOLERANCE", "CROSSCHECK_KEY", "GROUPED_URL", "VOLUME_TOLERANCE", "YAHOO_DAILY_URL", "YAHOO_HEADERS", "compare",
           "parse_grouped", "parse_yahoo_daily", "yahoo_sample", "yahoo_symbol"]
