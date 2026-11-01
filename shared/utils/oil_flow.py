"""
shared/utils/oil_flow.py

Pure pieces of the oil-flow indices (services/agents/oil_flow_job.py): where
the gates and ports are, what a Sentinel-1 reading says, when a tanker is
loaded, and how a daily measure relates to the oil price.

What the receivers can and cannot see decides which instrument measures which
place. A coverage test (31 Oct 2026, 30 days of positions) found:

- Singapore Strait: 731 AIS gate crossings, roughly a tenth of real traffic.
  Usable as a relative index of East Asia's seaborne crude and product flow.
- Hormuz 12 crossings, Bosphorus 28, Taiwan Strait 0, Bab-el-Mandeb no
  positions at all. Shore receivers see the ports, not the straits, so these
  are measured by radar (Sentinel-1 target density) and never by AIS counts.
- Ports: the Singapore anchorages (6,381 ships) and Jebel Ali (337) are seen;
  Fujairah barely (23 ships), Ras Tanura, Kharg and Basra not at all.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, TypedDict


@dataclass(frozen=True)
class Gate:
    """A line across a strait. A ship whose consecutive positions fall on
    either side of it, at a point inside the band, has transited."""
    name: str
    axis: str                                  # "lon": a meridian at `at`; "lat": a parallel
    at: float
    band: Tuple[float, float]                  # extent of the line along the other axis
    box: Tuple[float, float, float, float]     # lat_lo, lat_hi, lon_lo, lon_hi: positions fetched
    positive: str                              # direction name when crossing toward larger lon/lat
    negative: str


@dataclass(frozen=True)
class Port:
    name: str
    box: Tuple[float, float, float, float]


GATES: Tuple[Gate, ...] = (
    Gate("singapore_strait", "lon", 104.0, (1.10, 1.45), (0.8, 1.7, 103.6, 104.4), "east", "west"),
)
PORTS: Tuple[Port, ...] = (
    Port("singapore_anchorages", (1.15, 1.35, 103.6, 104.1)),
    Port("jebel_ali", (24.9, 25.1, 54.85, 55.15)),
)
COVERAGE_NOTES: Dict[str, str] = {
    "strait_of_hormuz": "radar only: 12 AIS gate crossings in 30 days, shore receivers see ports not the strait",
    "bab_el_mandeb": "radar only: no AIS positions received",
    "taiwan_strait": "radar only: no AIS gate crossings",
    "bosphorus": "radar not tasked; 28 AIS crossings in 30 days is too few for an index",
    "kharg_island": "radar only: no AIS coverage of the terminal",
    "ras_tanura": "radar only: no AIS coverage of the terminal",
    "fujairah": "radar; AIS sees 23 ships in 30 days, too few for a presence count",
}

MAX_STEP_H = 12.0          # positions further apart than this say nothing about a crossing
MIN_RANGE = 1.25           # max draught must exceed min by 25% before laden/ballast is called
LADEN_AT = 0.6             # upper share of the ship's own draught range that counts as loaded
BALLAST_AT = 0.4
MIN_HISTORY = 12           # same as the chokepoint baselines: fewer readings give no z
EPISODE_Z = 2.0
MIN_PAIRS = 10             # fewer aligned days than this and no correlation is reported
# The best of eleven lags is reported, so chance alone produces a large r on a
# month of data. Two-sided 5% across eleven tests: z = 2.84, and r clears
# chance only above roughly z / sqrt(n).
LAGS_BONFERRONI_Z = 2.84
# A receiver outage reads as an empty strait. A day whose AIS volume is below
# this share of the site's median is a coverage gap, not a flow of zero.
GAP_SHARE = 0.25

_SAR = re.compile(
    r"Sentinel-1 SAR over (?P<site>.+?) on (?P<day>\d{4}-\d{2}-\d{2}): (?P<targets>\d+) target pixels of "
    r"(?P<water>\d+) water pixels\.\s*(?P<threshold>.*?)\.\s*This is a target density",
)
_NO_AIS = re.compile(r"(\d+) radar contact\(s\) with no AIS")
# Readings before 17 Sep 2026 thresholded raw backscatter at 0 dB and read
# density 1.0 everywhere; they are a different instrument and not a series.
_VALID_THRESHOLD = re.compile(r"sigma0\s*>\s*(?P<lin>[\d.]+) linear")


@dataclass(frozen=True)
class SarReading:
    site: str
    day: date
    density: float
    targets: int
    water: int
    calibration: str
    no_ais: Optional[int]


def site_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def parse_sar(headline: str, summary: str) -> Optional[SarReading]:
    """One Sentinel-1 reading from its event text, or None when the text is
    not a reading or was produced by the retired raw-backscatter threshold."""
    m = _SAR.search(summary or "")
    if not m:
        return None
    cal = _VALID_THRESHOLD.search(m["threshold"])
    water = int(m["water"])
    if not cal or water <= 0:
        return None
    contacts = _NO_AIS.search(headline or "")
    return SarReading(
        site=site_key(m["site"]), day=date.fromisoformat(m["day"]), density=int(m["targets"]) / water,
        targets=int(m["targets"]), water=water, calibration=f"sigma0>{cal['lin']}",
        no_ais=int(contacts[1]) if contacts else None,
    )


def transits(track: Sequence[Tuple[datetime, float, float]], gate: Gate) -> List[Tuple[datetime, str]]:
    """Each time one ship's time-ordered (t, lat, lon) track crosses `gate`,
    with the direction. A crossing is credited to the later position."""
    out: List[Tuple[datetime, str]] = []
    for (t0, la0, lo0), (t1, la1, lo1) in zip(track, track[1:]):
        if (t1 - t0).total_seconds() > MAX_STEP_H * 3600:
            continue
        a0, a1, b = (lo0, lo1, (la0 + la1) / 2) if gate.axis == "lon" else (la0, la1, (lo0 + lo1) / 2)
        if (a0 < gate.at) == (a1 < gate.at) or not gate.band[0] <= b <= gate.band[1]:
            continue
        out.append((t1, gate.positive if a1 > a0 else gate.negative))
    return out


def draught_state(draught: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[str]:
    """'laden', 'ballast', or None while the ship's own range is unknown.

    Draught alone does not say loaded: a VLCC in ballast draws more than a
    loaded product tanker. So the call is relative to the deepest and
    shallowest this ship has been seen at, and only once those differ enough
    to have seen it both ways.
    """
    if not draught or not lo or not hi or hi < lo * MIN_RANGE:
        return None
    share = (draught - lo) / (hi - lo)
    if share >= LADEN_AT:
        return "laden"
    if share <= BALLAST_AT:
        return "ballast"
    return None


def zscore(value: float, history: Sequence[float]) -> Optional[float]:
    if len(history) < MIN_HISTORY:
        return None
    mean = sum(history) / len(history)
    sd = math.sqrt(sum((x - mean) ** 2 for x in history) / (len(history) - 1))
    return None if sd <= 0 else (value - mean) / sd


def changes(series: Mapping[date, float], log: bool = False) -> Dict[date, float]:
    """Day-over-observation changes: each value against the previous one
    present, so a weekend or a missed radar pass is one step, not a hole."""
    days = sorted(series)
    out: Dict[date, float] = {}
    for d0, d1 in zip(days, days[1:]):
        a, b = series[d0], series[d1]
        if log:
            if a > 0 and b > 0:
                out[d1] = math.log(b / a)
        else:
            out[d1] = b - a
    return out


class LagCorr(TypedDict):
    lag: int
    r: Optional[float]
    n: int


def _corr(pairs: Sequence[Tuple[float, float]]) -> Optional[float]:
    n = len(pairs)
    if n < MIN_PAIRS:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    sxy = sum((x - mx) * (y - my) for x, y in pairs)
    sxx = sum((x - mx) ** 2 for x, _ in pairs)
    syy = sum((y - my) ** 2 for _, y in pairs)
    return None if sxx <= 0 or syy <= 0 else sxy / math.sqrt(sxx * syy)


def lead_lag(x: Mapping[date, float], y: Mapping[date, float], max_lag: int = 5) -> List[LagCorr]:
    """Correlation of x on day d with y on the `lag`-th trading day of y after
    d (negative lag: before). Lags count y's own observations, so a weekend
    is not a lag. A positive lag with a strong r is x leading y."""
    ys = sorted(y)
    idx = {d: i for i, d in enumerate(ys)}
    out: List[LagCorr] = []
    for lag in range(-max_lag, max_lag + 1):
        pairs = []
        for d, xv in x.items():
            i = idx.get(d)
            if i is not None and 0 <= i + lag < len(ys):
                pairs.append((xv, y[ys[i + lag]]))
        r = _corr(pairs)
        out.append({"lag": lag, "r": None if r is None else round(r, 3), "n": len(pairs)})
    return out


def chance_r(n: int) -> float:
    """The |r| the best of the eleven lags reaches by chance on n pairs."""
    return LAGS_BONFERRONI_Z / math.sqrt(n) if n > 0 else 1.0


def coverage_gaps(volume: Mapping[date, float]) -> set[date]:
    """Days whose receiver volume fell below GAP_SHARE of the median day."""
    vals = sorted(volume.values())
    if not vals:
        return set()
    mid = len(vals) // 2
    median = vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2
    return {d for d, v in volume.items() if v < GAP_SHARE * median}


def weekly_sum(series: Mapping[date, float], week_ending: Sequence[date]) -> Dict[date, float]:
    """`series` summed into the seven days ending on each of `week_ending`
    (EIA weeks end on Friday), for weeks with at least one observation."""
    out: Dict[date, float] = {}
    for we in week_ending:
        vals = [v for d, v in series.items() if we - timedelta(days=6) <= d <= we]
        if vals:
            out[we] = sum(vals)
    return out


__all__ = [
    "COVERAGE_NOTES", "EPISODE_Z", "GATES", "chance_r", "coverage_gaps", "Gate", "LagCorr", "PORTS", "Port", "SarReading", "changes",
    "draught_state", "lead_lag", "parse_sar", "site_key", "transits", "weekly_sum", "zscore",
]
