"""
shared/utils/eia.py

The EIA's weekly petroleum stocks, free and keyless: the full weekly history of
each series from its history page, and how unusual this week's change is. Pure;
collector-macro fetches, the gateway serves.

Wall Street's consensus forecast for the weekly change is not free, so the
"surprise" here is against each series' own record: the week's change as a
z-score of the last three years of weekly changes, and the level against the
five-year average for the same week of the year (inventories are seasonal --
gasoline builds into winter and draws into summer).
"""
from __future__ import annotations

import re
from datetime import date
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

HISTORY_URL = "https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s={series}&f=W"
SERIES: Dict[str, Tuple[str, str]] = {
    "crude_commercial": ("WCESTUS1", "Crude oil, commercial (excl. SPR)"),
    "gasoline": ("WGTSTUS1", "Motor gasoline"),
    "distillate": ("WDISTUS1", "Distillate fuel oil"),
    "cushing": ("W_EPC0_SAX_YCUOK_MBBL", "Crude oil at Cushing, OK"),
}
_MONTHS = {m: i for i, m in enumerate(("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
                                         "Nov", "Dec"), 1)}
_ROW = re.compile(r"<tr>(.*?)</tr>", re.S)
_YEAR_MONTH = re.compile(r"class='B6'>(?:&nbsp;)*\s*(\d{4})-([A-Z][a-z]{2})")
_PAIR = re.compile(r"class='B5'>(\d{2})/(\d{2})&nbsp;</td>\s*<td class='B3'>([\d,]+)")


def parse_history(html: str) -> List[Tuple[date, float]]:
    """(week ending, thousand barrels), oldest first."""
    out: Dict[date, float] = {}
    for row in _ROW.findall(html):
        ym = _YEAR_MONTH.search(row)
        if not ym:
            continue
        year, month = int(ym.group(1)), _MONTHS.get(ym.group(2))
        if not month:
            continue
        for mm, dd, val in _PAIR.findall(row):
            m, d = int(mm), int(dd)
            # A week listed under a month can end in the next year (a row for
            # December dated 01/02); the date's own month decides the year.
            y = year + 1 if month == 12 and m == 1 else year
            try:
                out[date(y, m, d)] = float(val.replace(",", ""))
            except ValueError:
                continue
    return sorted(out.items())


def week_report(history: Sequence[Tuple[date, float]], change_years: int = 3,
                seasonal_years: int = 5) -> Optional[Dict[str, Any]]:
    """This week's level and change, and how unusual each is."""
    if len(history) < 60:
        return None
    (d1, v1), (_, v0) = history[-1], history[-2]
    change = v1 - v0
    changes = [b - a for (_, a), (_, b) in zip(history[-(52 * change_years + 1):-1], history[-52 * change_years:-1])]
    sd = pstdev(changes) if len(changes) > 10 else 0.0
    z = (change - mean(changes)) / sd if sd > 0 else None
    same_week = [v for d, v in history[:-1]
                 if d1.year - seasonal_years <= d.year < d1.year and abs((d.replace(year=d1.year) - d1).days) <= 3]
    seasonal = mean(same_week) if same_week else None
    return {
        "week_ending": d1.isoformat(),
        "level": v1,
        "change": change,
        "change_z": round(z, 2) if z is not None else None,
        "five_year_avg": round(seasonal, 1) if seasonal is not None else None,
        "vs_five_year_pct": round((v1 / seasonal - 1) * 100, 2) if seasonal else None,
    }


__all__ = ["HISTORY_URL", "SERIES", "parse_history", "week_report"]
