"""
services/collector-macro/fred_releases.py

Scheduled US macro releases, read from FRED.

The economic calendar polled Finnhub's /calendar/economic, a premium endpoint
that answers 403 on this key, so the platform had never ingested a single CPI,
payrolls, PCE, GDP or Fed funds release -- the largest scheduled source of
equity repricing there is. FRED publishes the official values themselves,
minutes after each release, with the time each series was last updated.

What FRED does not carry is a consensus forecast. A release here is therefore
measured against the prior reading, and says so: the event is tagged
`no_consensus` and its headline reads "vs prior", because calling a change from
last month a surprise would claim a market expectation nobody supplied.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("feed.macro_fred")

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE = "https://api.stlouisfed.org/fred"

# Where the latest seen observation of each series is kept, so a restart does
# not re-announce a release it already published.
FRED_STATE_KEY = "sentinel:macro:fred:last_observation"

# On a cold start nothing is known to have been announced. Releases published
# within this window are emitted; older ones are recorded silently, so a first
# boot does not announce last month's CPI as news.
FIRST_RUN_LOOKBACK = timedelta(hours=48)

# FRED allows 120 requests a minute. Two per series, eight series, every ten
# minutes is under two a minute.
FRED_POLL_INTERVAL_SEC = 600


@dataclass(frozen=True)
class FredSeries:
    indicator: str          # key into economic_calendar.INDICATOR_PROFILES
    series_id: str
    transform: str          # "yoy", "diff", "level"


FRED_SERIES: List[FredSeries] = [
    FredSeries("CPI", "CPIAUCSL", "yoy"),
    FredSeries("CORE_CPI", "CPILFESL", "yoy"),
    FredSeries("NFP", "PAYEMS", "diff"),
    FredSeries("UNEMPLOYMENT", "UNRATE", "level"),
    FredSeries("PCE", "PCEPI", "yoy"),
    FredSeries("CORE_PCE", "PCEPILFE", "yoy"),
    FredSeries("GDP", "A191RL1Q225SBEA", "level"),
    # Daily, so a new observation appears every day. It is a release only
    # when the target actually moves; see `release_from_observations`.
    FredSeries("FOMC_RATE_DECISION", "DFEDTARU", "level"),
]

# Observations fetched per series: a year-on-year comparison of the latest and
# the previous month, with room for months FRED marks missing.
OBSERVATION_LIMIT = 26


# Market conditions rather than scheduled releases: read every poll, kept as
# context for every agent prompt, and announced only when they move hard.
# Measured on 1 October: high-yield spreads 2.80% -> 3.24% in a week, +44bp, a
# credit-stress move the platform had no way to see.
@dataclass(frozen=True)
class FredContextSeries:
    indicator: str
    series_id: str
    label: str
    lookback: int           # observations back for "a week ago"
    move: float             # absolute weekly change that is announced
    zero_cross: bool = False  # announce a change of sign (curve inversion)


FRED_CONTEXT_SERIES: List[FredContextSeries] = [
    FredContextSeries("HY_OAS", "BAMLH0A0HYM2", "HY OAS", 5, 0.30),
    FredContextSeries("IG_OAS", "BAMLC0A0CM", "IG OAS", 5, 0.10),
    FredContextSeries("CURVE_10Y2Y", "T10Y2Y", "10y-2y", 5, 0.25, zero_cross=True),
    FredContextSeries("CURVE_10Y3M", "T10Y3M", "10y-3m", 5, 0.25, zero_cross=True),
    FredContextSeries("NFCI", "NFCI", "NFCI", 1, 0.10),
]

from shared.utils.rates import FRED_CONTEXT_KEY  # noqa: E402  (one definition)


def context_reading(series: FredContextSeries, observations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Latest value, the value a week back, and whether the move is news."""
    vals = _values(observations)
    if len(vals) <= series.lookback:
        return None
    (date, latest), (_, prior) = vals[0], vals[series.lookback]
    change = round(latest - prior, 4)
    crossed = series.zero_cross and (latest < 0) != (prior < 0)
    return {
        "indicator": series.indicator, "label": series.label, "date": date,
        "value": latest, "week_ago": prior, "change": change,
        "notable": abs(change) >= series.move or crossed, "crossed_zero": crossed,
    }


def context_line(readings: Dict[str, Dict[str, Any]]) -> str:
    """One line for an agent prompt, in a fixed order."""
    parts = []
    for s in FRED_CONTEXT_SERIES:
        r = readings.get(s.indicator)
        if r:
            parts.append(f"{s.label} {r['value']:.2f} ({r['change']:+.2f} 1w)")
    return " | ".join(parts)


def _values(observations: List[Dict[str, Any]]) -> List[tuple]:
    """(date, float) pairs, newest first, skipping FRED's "." for missing."""
    out = []
    for o in observations or []:
        try:
            out.append((str(o["date"]), float(o["value"])))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def release_from_observations(
    series: FredSeries, observations: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """The latest reading and the one before it, in the units the indicator uses.

    Pure. `observations` is FRED's list, newest first. Returns None when there is
    not enough history to compute the transform.
    """
    vals = _values(observations)
    if series.transform == "yoy":
        # Matched by calendar month, never by position. FRED marks a month it
        # has no value for with "." -- October 2025 CPI, never collected during
        # the shutdown -- and counting back twelve rows across that gap
        # compares August with the July of the year before.
        by_month = {d[:7]: v for d, v in vals}
        if len(vals) < 2:
            return None

        def year_ago(date: str) -> Optional[float]:
            year, month = date[:4], date[5:7]
            return by_month.get(f"{int(year) - 1:04d}-{month}")

        latest_base = year_ago(vals[0][0])
        if not latest_base:
            return None
        latest = (vals[0][1] / latest_base - 1.0) * 100.0
        prior_base = year_ago(vals[1][0])
        prior = round((vals[1][1] / prior_base - 1.0) * 100.0, 2) if prior_base else None
        mom = (vals[0][1] / vals[1][1] - 1.0) * 100.0
        return {"period": vals[0][0], "actual": round(latest, 2),
                "previous": prior, "detail": f"m/m {mom:+.2f}%"}
    if series.transform == "diff":
        if len(vals) < 3:
            return None
        return {"period": vals[0][0], "actual": round(vals[0][1] - vals[1][1], 1),
                "previous": round(vals[1][1] - vals[2][1], 1), "detail": ""}
    if series.transform == "level":
        if len(vals) < 2:
            return None
        if series.series_id == "DFEDTARU":
            # The last value that differs is the previous decision.
            prior = next((v for _, v in vals[1:] if v != vals[0][1]), vals[0][1])
            return {"period": vals[0][0], "actual": vals[0][1], "previous": prior,
                    "detail": "", "decision_value": vals[0][1]}
        return {"period": vals[0][0], "actual": vals[0][1],
                "previous": vals[1][1], "detail": ""}
    return None


def is_new_release(
    series: FredSeries,
    release: Dict[str, Any],
    last_seen: Optional[str],
    last_updated: Optional[datetime],
    now: datetime,
) -> bool:
    """Whether this reading has not been announced before.

    Keyed on the observation's period, except for the Fed funds target, whose
    period moves daily and whose release is a change of value. With no record
    at all, only a reading published inside FIRST_RUN_LOOKBACK counts.
    """
    marker = state_marker(series, release)
    if last_seen is None:
        return bool(last_updated) and (now - last_updated) <= FIRST_RUN_LOOKBACK and (
            series.series_id != "DFEDTARU" or release["actual"] != release["previous"]
        )
    return marker != last_seen


def state_marker(series: FredSeries, release: Dict[str, Any]) -> str:
    """What is stored to recognise this reading again."""
    if series.series_id == "DFEDTARU":
        return f"value:{release['actual']}"
    return f"period:{release['period']}"


def parse_last_updated(raw: Optional[str]) -> Optional[datetime]:
    """FRED's "2026-10-02 08:24:33-05" as an aware datetime."""
    if not raw:
        return None
    try:
        text = raw.strip()
        if len(text) >= 3 and text[-3] in "+-" and ":" not in text[-3:]:
            text = text + ":00"
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


async def fetch_series(session, series: FredSeries) -> tuple:
    """(observations newest first, last_updated) for one series."""
    params = {"series_id": series.series_id, "api_key": FRED_API_KEY, "file_type": "json"}
    async with session.get(f"{FRED_BASE}/series", params=params) as resp:
        resp.raise_for_status()
        meta = (await resp.json()).get("seriess") or [{}]
    obs_params = dict(params, sort_order="desc", limit=OBSERVATION_LIMIT)
    async with session.get(f"{FRED_BASE}/series/observations", params=obs_params) as resp:
        resp.raise_for_status()
        observations = (await resp.json()).get("observations") or []
    return observations, parse_last_updated(meta[0].get("last_updated"))
