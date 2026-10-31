"""
shared/utils/macro_bars.py

Daily closes for macro instruments, from free sources, for macro_daily_bars.

Stock bars come from the venue's SIP feed; nothing stored the macro side. On
30 Oct 2026 the graph's exposure edges pointed at eleven futures, of which
only CL=F and GC=F had any stored bars (nine sessions each), and US10Y had
none anywhere. Without them no stock's move can be set against its macro
driver's, and no exposure to rates can be measured at all.

  - Treasury par yields: Treasury.gov's daily yield-curve XML, one request a
    year (shared/utils/treasury.py parses it). Stored under both spellings
    the platform uses (US02Y and US2Y).
  - Futures, the dollar index and VIX: Yahoo's chart API, daily interval.

Pure: responses in, (symbol, day, close) rows out.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Tuple

from shared.utils import treasury

# Our symbol -> Yahoo's. The exposure graph's targets, plus the dollar and VIX.
YAHOO_SYMBOLS: Dict[str, str] = {
    "CL=F": "CL=F", "BZ=F": "BZ=F", "NG=F": "NG=F", "HG=F": "HG=F", "GC=F": "GC=F", "SI=F": "SI=F",
    "ZC=F": "ZC=F", "ZW=F": "ZW=F", "ZS=F": "ZS=F", "KC=F": "KC=F", "SB=F": "SB=F",
    "DXY": "DX-Y.NYB", "VIX": "^VIX",
}
YAHOO_DAILY = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range}&interval=1d"
# Treasury field symbol -> every name it is stored under.
TREASURY_ALIASES: Dict[str, Tuple[str, ...]] = {"US02Y": ("US02Y", "US2Y"), "US10Y": ("US10Y",), "US30Y": ("US30Y",)}
FIRST_YEAR = 2019

Row = Tuple[str, date, float, str]


def yahoo_rows(symbol: str, chart: Mapping[str, Any]) -> List[Row]:
    """(symbol, day, close, "yahoo") per daily bar with a close. The day is the
    bar's own date on the exchange's clock (its gmtoffset), not UTC."""
    try:
        res = (chart.get("chart") or {}).get("result") or []
        r = res[0]
        stamps = r.get("timestamp") or []
        closes = ((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
        offset = int((r.get("meta") or {}).get("gmtoffset") or 0)
    except (AttributeError, IndexError, TypeError, ValueError):
        return []
    out: Dict[date, float] = {}
    for ts, c in zip(stamps, closes):
        if c is None or not isinstance(ts, (int, float)):
            continue
        day = (datetime.fromtimestamp(ts, timezone.utc) + timedelta(seconds=offset)).date()
        out[day] = float(c)                          # a repeated day keeps the later bar
    return [(symbol, d, round(c, 6), "yahoo") for d, c in sorted(out.items())]


def treasury_rows(xml_text: str) -> List[Row]:
    """(symbol, day, yield in percent, "treasury") for each stored spelling."""
    out: List[Row] = []
    for sym, points in treasury.parse_par_yields(xml_text).items():
        for p in points:
            day = date.fromisoformat(str(p["date"]))
            for name in TREASURY_ALIASES.get(sym, (sym,)):
                out.append((name, day, float(p["yield"]), "treasury"))
    return out


def years(today: date, backfill: bool) -> List[int]:
    """Treasury years to request: every year since FIRST_YEAR on a backfill,
    else this year (and last year in its first week, which the feed splits)."""
    if backfill:
        return list(range(FIRST_YEAR, today.year + 1))
    return [today.year - 1, today.year] if today.timetuple().tm_yday <= 7 else [today.year]


__all__ = ["FIRST_YEAR", "TREASURY_ALIASES", "YAHOO_DAILY", "YAHOO_SYMBOLS", "Row", "treasury_rows", "yahoo_rows",
           "years"]
