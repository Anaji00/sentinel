"""
shared/utils/reaction_study.py

Did the market actually move after this kind of signal -- more than it moves
anyway, more than once, and in the direction the reason says?

For each family of signal and each horizon (1 hour, 1 day, 5 days), the
episodes' forward moves are measured on the instruments they point at, net of
what the instrument would have done anyway, scaled by its normal volatility,
and tested. A family is `proven`, and only then allowed to raise a trading
signal, if it clears every one of:

  * enough independent evidence: at least MIN_UNITS units, where episodes of
    one family in one window are averaged into a single unit (one escalation
    headline moves crude, Brent, gas and tankers at once -- that is one
    observation, not four) and units do not overlap within the horizon;
  * significance after correcting for every family and horizon tested at
    once (Benjamini-Hochberg false-discovery control at FDR_Q);
  * confirmation on the later VALIDATION_SHARE of the history, held out;
  * a mean move larger than the round-trip cost of trading it;
  * a hit rate above MIN_HIT_RATE;
  * the direction its reason predicts -- except for families whose direction
    comes from the tape (volume spikes, blocks), where the data may choose
    continuation or reversal.

Everything else is reported, with the reason it failed, and raises nothing.

Pure: episodes and price series in, verdicts out. No look-ahead: entry is the
first bar at or after the moment the platform knew.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from shared.utils.signal_families import PRICE_DIRECTED, Episode

HORIZONS: Dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "5d": timedelta(days=5),
}

MIN_UNITS = 20
FDR_Q = 0.10
VALIDATION_SHARE = 0.4
MIN_VALIDATION_UNITS = 8
MIN_VALIDATION_T = 1.0
MIN_HIT_RATE = 0.53
# Round-trip cost, in basis points: spread, fees and slippage on liquid names.
COST_BPS_EQUITY = 10.0
COST_BPS_FUTURE = 5.0
# How long to wait for a bar at entry and at exit (weekends, closed sessions).
MAX_BAR_WAIT = timedelta(days=4)
# A pre-event move smaller than this gives a price-directed episode no direction.
MIN_PRE_MOVE = 0.001
PRE_MOVE_WINDOW = timedelta(minutes=30)
# Volatility and drift need this many non-overlapping windows to be trusted.
MIN_VOL_SAMPLES = 20

Series = Tuple[List[float], List[float]]   # (epoch seconds ascending, close)


def is_future(symbol: str) -> bool:
    return symbol.endswith("=F")


def cost_bps(symbol: str) -> float:
    return COST_BPS_FUTURE if is_future(symbol) else COST_BPS_EQUITY


def _ts(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def price_at_or_after(series: Series, t: float, max_wait: float) -> Optional[Tuple[float, float]]:
    times, closes = series
    i = bisect.bisect_left(times, t)
    if i >= len(times) or times[i] - t > max_wait:
        return None
    return times[i], closes[i]


def price_at_or_before(series: Series, t: float, max_age: float) -> Optional[Tuple[float, float]]:
    times, closes = series
    i = bisect.bisect_right(times, t) - 1
    if i < 0 or t - times[i] > max_age:
        return None
    return times[i], closes[i]


def forward_window(series: Series, known_at: datetime, horizon: timedelta) -> Optional[Tuple[float, float, float]]:
    """(entry time, exit time, log return) from the first bar at or after `known_at`."""
    wait = MAX_BAR_WAIT.total_seconds()
    entry = price_at_or_after(series, _ts(known_at), wait)
    if entry is None or entry[1] <= 0:
        return None
    exit_ = price_at_or_after(series, entry[0] + horizon.total_seconds(), wait)
    if exit_ is None or exit_[1] <= 0:
        return None
    return entry[0], exit_[0], math.log(exit_[1] / entry[1])


def window_return(series: Series, t0: float, t1: float) -> Optional[float]:
    """Log return between the bars at or after t0 and t1 (for the benchmark)."""
    wait = MAX_BAR_WAIT.total_seconds()
    a = price_at_or_after(series, t0, wait)
    b = price_at_or_after(series, t1, wait)
    if a is None or b is None or a[1] <= 0 or b[1] <= 0:
        return None
    return math.log(b[1] / a[1])


def pre_move(series: Series, known_at: datetime) -> Optional[float]:
    """The log move over the half hour before the platform knew."""
    t = _ts(known_at)
    max_age = 2 * 3600.0
    now = price_at_or_before(series, t, max_age)
    before = price_at_or_before(series, t - PRE_MOVE_WINDOW.total_seconds(), max_age)
    if now is None or before is None or now[1] <= 0 or before[1] <= 0 or now[0] == before[0]:
        return None
    return math.log(now[1] / before[1])


def horizon_stats(
    series: Series, horizon: timedelta, exclude: Sequence[float] = (),
) -> Optional[Tuple[float, float]]:
    """(mean, standard deviation) of non-overlapping horizon log returns.

    Windows containing any time in `exclude` -- the episodes being studied --
    are left out, so the baseline is what the instrument does when nothing is
    happening. Including them lets the effect under test raise its own
    baseline and cancel itself.
    """
    times, closes = series
    if not times:
        return None
    step = horizon.total_seconds()
    marks = sorted(exclude)
    clean: List[float] = []
    every: List[float] = []
    t = times[0]
    wait = MAX_BAR_WAIT.total_seconds()
    while True:
        a = price_at_or_after(series, t, wait)
        if a is None:
            break
        b = price_at_or_after(series, a[0] + step, wait)
        if b is None:
            break
        if a[1] > 0 and b[1] > 0:
            r = math.log(b[1] / a[1])
            every.append(r)
            i = bisect.bisect_left(marks, a[0] - step)
            if not (i < len(marks) and marks[i] <= b[0]):
                clean.append(r)
        t = b[0]
    # Quiet windows when there are enough of them. An instrument signalled
    # almost every day has too few, and then every window is used: that
    # includes the effect in its own baseline, which can only shrink a real
    # edge, never manufacture one.
    rets = clean if len(clean) >= MIN_VOL_SAMPLES else every
    if len(rets) < MIN_VOL_SAMPLES:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    sd = math.sqrt(var)
    return (mean, sd) if sd > 0 else None


def scaled_horizon_stats(
    series: Series, horizon: timedelta, exclude: Sequence[float] = (),
) -> Optional[Tuple[float, float]]:
    """Horizon stats, or one day's scaled by time when the horizon is too long.

    Seventy-five days hold fifteen non-overlapping five-day windows, too few to
    estimate a volatility from. The daily estimate scaled by the square root of
    the number of days (and its mean by the number) is the standard answer.
    """
    stats = horizon_stats(series, horizon, exclude)
    day = timedelta(days=1)
    if stats is not None or horizon <= day:
        return stats
    base = horizon_stats(series, day, exclude)
    if base is None:
        return None
    k = horizon / day
    return base[0] * k, base[1] * math.sqrt(k)


@dataclass
class Measured:
    """One episode's move, net and scaled, aligned with its direction."""
    episode: Episode
    direction: int
    entry: float
    abnormal: float          # log return net of benchmark or drift
    standardized: float      # abnormal / the instrument's horizon volatility
    aligned: float           # standardized * direction


def measure(
    ep: Episode,
    horizon: timedelta,
    series: Optional[Series],
    benchmark: Optional[Series],
    stats: Optional[Tuple[float, float]],
) -> Optional[Measured]:
    """One episode on one horizon, or None when it cannot be measured honestly."""
    if series is None or stats is None:
        return None
    direction = ep.direction
    if ep.family in PRICE_DIRECTED:
        pm = pre_move(series, ep.known_at)
        if pm is None or abs(pm) < MIN_PRE_MOVE:
            return None
        direction = 1 if pm > 0 else -1
    if direction == 0:
        return None
    window = forward_window(series, ep.known_at, horizon)
    if window is None:
        return None
    entry, exit_, ret = window
    drift, sd = stats
    if is_future(ep.instrument) or benchmark is None:
        abnormal = ret - drift
    else:
        bench = window_return(benchmark, entry, exit_)
        if bench is None:
            return None
        abnormal = ret - bench
    standardized = abnormal / sd
    return Measured(ep, direction, entry, abnormal, standardized, standardized * direction)


@dataclass
class Unit:
    """Independent evidence: measured episodes of one family that overlap in time."""
    entry: float
    aligned: float               # mean aligned standardized move
    aligned_abnormal_bps: float  # mean aligned abnormal move, basis points
    cost_bps: float
    episodes: int


def units(measured: Sequence[Measured], horizon: timedelta) -> List[Unit]:
    """Group into non-overlapping blocks of `horizon`, averaging within each."""
    out: List[Unit] = []
    block: List[Measured] = []
    start: Optional[float] = None
    step = horizon.total_seconds()

    def close() -> None:
        if block:
            n = len(block)
            out.append(Unit(
                entry=block[0].entry,
                aligned=sum(m.aligned for m in block) / n,
                aligned_abnormal_bps=1e4 * sum(m.abnormal * m.direction for m in block) / n,
                cost_bps=sum(cost_bps(m.episode.instrument) for m in block) / n,
                episodes=n,
            ))

    for m in sorted(measured, key=lambda m: m.entry):
        if start is None or m.entry >= start + step:
            close()
            block = [m]
            start = m.entry
        else:
            block.append(m)
    close()
    return out


def t_stat(xs: Sequence[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    if var <= 0:
        return None
    return mean / math.sqrt(var / n)


def two_sided_p(t: float, df: int) -> float:
    from scipy.stats import t as student_t

    return float(2.0 * student_t.sf(abs(t), df))


def benjamini_hochberg(pvalues: Sequence[float]) -> List[float]:
    """Adjusted p-values (q-values), in the input order."""
    n = len(pvalues)
    if n == 0:
        return []
    order = sorted(range(n), key=lambda i: pvalues[i])
    adjusted = [0.0] * n
    running = 1.0
    for rank in range(n, 0, -1):
        i = order[rank - 1]
        running = min(running, pvalues[i] * n / rank)
        adjusted[i] = min(1.0, running)
    return adjusted


@dataclass
class CellResult:
    family: str
    horizon: str
    episodes: int
    units: int
    instruments: List[str]
    mean_aligned: Optional[float] = None
    t: Optional[float] = None
    p: Optional[float] = None
    q: Optional[float] = None
    hit_rate: Optional[float] = None
    mean_bps: Optional[float] = None
    cost_bps: Optional[float] = None
    validation_mean: Optional[float] = None
    validation_t: Optional[float] = None
    sign: int = 0                 # +1 with the stated/tape direction, -1 against
    verdict: str = "insufficient_data"
    reason: str = ""
    se: Optional[float] = None
    extra: Dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, object]:
        return {k: v for k, v in self.__dict__.items()}


def evaluate_cell(family: str, horizon: str, measured: Sequence[Measured], n_episodes: int) -> CellResult:
    """Everything except the cross-cell FDR step, which needs every cell's p."""
    h = HORIZONS[horizon]
    instruments = sorted({m.episode.instrument for m in measured})
    us = units(measured, h)
    res = CellResult(family=family, horizon=horizon, episodes=n_episodes, units=len(us), instruments=instruments)
    if not measured:
        res.verdict, res.reason = "unmeasurable", "no price history around these episodes"
        return res
    if len(us) < MIN_UNITS:
        res.verdict, res.reason = "insufficient_data", f"{len(us)} independent units of the {MIN_UNITS} needed"
        return res
    xs = [u.aligned for u in us]
    t = t_stat(xs)
    mean = sum(xs) / len(xs)
    res.mean_aligned = round(mean, 4)
    res.t = round(t, 3) if t is not None else None
    res.p = two_sided_p(t, len(xs) - 1) if t is not None else 1.0
    res.sign = 1 if mean > 0 else -1
    res.hit_rate = round(sum(1 for x in xs if (x > 0) == (res.sign > 0)) / len(xs), 3)
    res.mean_bps = round(sum(u.aligned_abnormal_bps for u in us) / len(us), 2)
    res.cost_bps = round(sum(u.cost_bps for u in us) / len(us), 2)
    sd = math.sqrt(sum((x - mean) ** 2 for x in xs) / (len(xs) - 1))
    res.se = round(sd / math.sqrt(len(xs)), 4)

    split = int(len(us) * (1 - VALIDATION_SHARE))
    val = xs[split:]
    if len(val) >= 2:
        res.validation_mean = round(sum(val) / len(val), 4)
        vt = t_stat(val)
        res.validation_t = round(vt, 3) if vt is not None else None
    res.verdict = "pending_fdr"
    return res


def finalize(cells: List[CellResult]) -> List[CellResult]:
    """Apply false-discovery control across cells, then every remaining gate."""
    tested = [c for c in cells if c.verdict == "pending_fdr"]
    qs = benjamini_hochberg([c.p if c.p is not None else 1.0 for c in tested])
    for c, q in zip(tested, qs):
        c.q = round(q, 4)
        if q > FDR_Q:
            c.verdict, c.reason = "no_edge", f"q={q:.2f} after testing {len(tested)} family-horizons (needs <= {FDR_Q})"
            continue
        if c.sign < 0 and c.family not in PRICE_DIRECTED:
            c.verdict, c.reason = "contradicts_prior", "significant, but against the direction its reason predicts"
            continue
        val_ok = (
            c.validation_mean is not None and c.validation_t is not None
            and (c.validation_mean > 0) == (c.sign > 0)
            and abs(c.validation_t) >= MIN_VALIDATION_T
            and c.units * VALIDATION_SHARE >= MIN_VALIDATION_UNITS
        )
        if not val_ok:
            c.verdict, c.reason = "unconfirmed", "did not hold on the held-out later 40% of its history"
            continue
        if c.mean_bps is None or abs(c.mean_bps) <= (c.cost_bps or 0):
            c.verdict, c.reason = "below_costs", f"mean move {c.mean_bps} bps does not clear {c.cost_bps} bps of costs"
            continue
        if c.hit_rate is None or c.hit_rate < MIN_HIT_RATE:
            c.verdict, c.reason = "low_hit_rate", f"hit rate {c.hit_rate} under {MIN_HIT_RATE}"
            continue
        c.verdict = "proven"
        c.reason = (
            ("continuation" if c.sign > 0 else "reversal") + " after the tape's direction"
            if c.family in PRICE_DIRECTED else "moves in the direction its reason predicts"
        )
    return cells


def shrunk_hit_rate(hit_rate: float, units_: int, prior_weight: int = 10) -> float:
    """A hit rate pulled toward a coin flip in proportion to how little evidence it has."""
    hits = hit_rate * units_
    return (hits + 0.5 * prior_weight) / (units_ + prior_weight)


def combine(hit_rates: Sequence[float]) -> float:
    """Independent proven families agreeing on one move, as one probability.

    Log-odds add under independence; capped, because families sharing a cause
    are not fully independent and a combined 0.97 would claim more than any
    of this evidence supports.
    """
    logit = sum(math.log(p / (1 - p)) for p in hit_rates if 0 < p < 1)
    return min(0.8, 1 / (1 + math.exp(-logit)))


__all__ = [
    "CellResult", "FDR_Q", "HORIZONS", "MIN_HIT_RATE", "MIN_UNITS", "Measured", "Series", "Unit",
    "benjamini_hochberg", "combine", "cost_bps", "evaluate_cell", "finalize", "forward_window",
    "horizon_stats", "is_future", "measure", "scaled_horizon_stats", "pre_move", "shrunk_hit_rate", "t_stat", "units",
]
