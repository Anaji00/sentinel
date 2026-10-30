"""
shared/utils/gpr.py

The Caldara-Iacoviello geopolitical risk index (matteoiacoviello.com/gpr.htm):
a count of newspaper articles about geopolitical tension, daily since 1985,
split into threats and acts, with monthly country series. Free, published as
Stata files that pandas reads without another package.

Pure: frames in, rows and a summary out.
"""
from __future__ import annotations

import math
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

DAILY_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_daily_recent.dta"
MONTHLY_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_export.dta"
SOURCE = "caldara_iacoviello"
DAILY_SERIES = {"GPRD": "gpr_daily", "GPRD_ACT": "gpr_daily_acts", "GPRD_THREAT": "gpr_daily_threats"}
MONTHLY_SERIES = {"GPR": "gpr_monthly", "GPRT": "gpr_monthly_threats", "GPRA": "gpr_monthly_acts"}
COUNTRY_PREFIX = "GPRC_"           # GPRC_USA, GPRC_CHN, ... -> gpr_country_usa
LATEST_KEY = "sentinel:gpr:latest"
ELEVATED_Z = 1.5


def _day(v: Any) -> Optional[date]:
    try:
        d = v.date() if hasattr(v, "date") else v
        return d if isinstance(d, date) else None
    except (TypeError, ValueError):
        return None


def _num(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def daily_rows(records: Sequence[Dict[str, Any]]) -> List[Tuple[str, date, float, str]]:
    """The daily file's records ({"date": ..., "GPRD": ...}) as table rows."""
    out = []
    for r in records:
        d = _day(r.get("date"))
        if d is None:
            continue
        for col, series in DAILY_SERIES.items():
            v = _num(r.get(col))
            if v is not None:
                out.append((series, d, v, SOURCE))
    return out


def monthly_rows(records: Sequence[Dict[str, Any]]) -> List[Tuple[str, date, float, str]]:
    """The monthly file's headline and country series, dated the first of the month."""
    out = []
    for r in records:
        d = _day(r.get("month"))
        if d is None:
            continue
        d = d.replace(day=1)
        for col, v in r.items():
            series = MONTHLY_SERIES.get(col) or (f"gpr_country_{col[len(COUNTRY_PREFIX):].lower()}"
                                                 if col.startswith(COUNTRY_PREFIX) else None)
            x = _num(v) if series else None
            if series and x is not None:
                out.append((series, d, x, SOURCE))
    return out


def summary(days: Sequence[date], values: Sequence[float]) -> Optional[Dict[str, Any]]:
    """The latest 7-day average against the past year's: level, z, and whether
    it is elevated. None with under a year of data."""
    if len(values) < 372:
        return None
    ma = [sum(values[i - 6:i + 1]) / 7 for i in range(6, len(values))]
    last, year = ma[-1], ma[-366:-1]
    mean = sum(year) / len(year)
    sd = math.sqrt(sum((x - mean) ** 2 for x in year) / (len(year) - 1))
    z = (last - mean) / sd if sd > 0 else 0.0
    return {"as_of": days[-1].isoformat(), "ma7": round(last, 1), "year_mean": round(mean, 1), "z": round(z, 2),
            "elevated": z >= ELEVATED_Z}


def fact(s: Optional[Dict[str, Any]]) -> Optional[str]:
    """One line for an agent's facts, or None."""
    if not s:
        return None
    return (f"Geopolitical risk (Caldara-Iacoviello daily index), 7-day average {s['ma7']} on {s['as_of']} against "
            f"a past-year mean of {s['year_mean']} (z {s['z']:+.1f}{', elevated' if s['elevated'] else ''}).")


__all__ = ["DAILY_URL", "LATEST_KEY", "MONTHLY_URL", "daily_rows", "fact", "monthly_rows", "summary"]
