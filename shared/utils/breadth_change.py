"""Pure statistics for pre-registered trial A3, ownership breadth (reports/Upgrade pre-registrations - 12 Nov.md).

Feature: the change over three months in the number of funds reporting a position in a stock (`fund_holdings_monthly.positions`), visible only
from the date its file was public. Per rebalance date the feature is ranked within the tradable set, and the statistic is the mean 21-session
excess return of the top quintile minus the bottom quintile. The test is two-sided, date-clustered (one spread per rebalance date, 21 sessions
apart, so windows never overlap), bar |t| >= 2 in both halves of the dates with the same sign as the full sample.

Report only: nothing here feeds a score, a gate or the model; the registered route into the model is the existing walk-forward as one extra feature.
Pure; no I/O, no clock, no network. The caller supplies rows, bars and rebalance dates.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from datetime import date
from statistics import mean, stdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

LOOKBACK_MONTHS = 3          # the change is measured over this many months of report_month
MIN_BASE_POSITIONS = 10      # a base below this makes a percentage change noise (1 to 2 funds)
MAX_STALE_DAYS = 135         # the newest visible month may be at most this old at the rebalance date
MIN_NAMES = 100              # fewer ranked names than this on a date: the date is not used
QUINTILE = 0.2
MIN_ABS_T = 2.0

Entry = Tuple[date, date, int]      # report_month, public date, positions
Panel = Dict[str, List[Entry]]


def _month_index(d: date) -> int:
    return d.year * 12 + d.month


def build_panel(rows: Iterable[Mapping[str, Any]]) -> Panel:
    """Per ticker, one entry per report month, ascending: the earliest public date for that month and the positions in that file.

    Later quarterly files restate earlier months; using the first public copy keeps the feature what a reader could have known on that day."""
    best: Dict[Tuple[str, date], Tuple[date, int]] = {}
    for r in rows:
        try:
            key = (str(r["ticker"]), r["report_month"])
            pub, pos = r["first_public"], int(r["positions"])
        except (KeyError, TypeError, ValueError):
            continue
        if pos < 0 or not isinstance(pub, date) or not isinstance(key[1], date):
            continue
        cur = best.get(key)
        if cur is None or pub < cur[0]:
            best[key] = (pub, pos)
    panel: Panel = defaultdict(list)
    for (tk, month), (pub, pos) in best.items():
        panel[tk].append((month, pub, pos))
    for v in panel.values():
        v.sort()
    return dict(panel)


def breadth_change(entries: Sequence[Entry], asof: date) -> Optional[float]:
    """Newest visible report month against the month `LOOKBACK_MONTHS` earlier (also visible): positions_new / positions_base - 1, else None.

    None when either month is missing or not yet public, when the base is under MIN_BASE_POSITIONS, or when the newest month is older than MAX_STALE_DAYS."""
    visible = {_month_index(m): (m, pos) for m, pub, pos in entries if pub <= asof}
    if not visible:
        return None
    newest = max(visible)
    month, pos = visible[newest]
    if (asof - month).days > MAX_STALE_DAYS:
        return None
    base = visible.get(newest - LOOKBACK_MONTHS)
    if base is None or base[1] < MIN_BASE_POSITIONS:
        return None
    return pos / base[1] - 1.0


def quintile_spread(features: Mapping[str, float], returns: Mapping[str, float]) -> Optional[float]:
    """Mean return of the top quintile of `features` minus the bottom quintile, over names present in both; None under MIN_NAMES names.

    Ties on the feature (many names have an unchanged count) are broken by ticker so the split is deterministic, and the quintile size is floor(n * 0.2)."""
    names = [t for t in features if t in returns and math.isfinite(features[t]) and math.isfinite(returns[t])]
    if len(names) < MIN_NAMES:
        return None
    names.sort(key=lambda t: (features[t], t))
    k = max(1, int(len(names) * QUINTILE))
    low, high = names[:k], names[-k:]
    return mean(returns[t] for t in high) - mean(returns[t] for t in low)


def _t(values: Sequence[float]) -> float:
    if len(values) < 3:
        return 0.0
    sd = stdev(values)
    return mean(values) / (sd / math.sqrt(len(values))) if sd > 0 else 0.0


def clustered_bootstrap(spreads: Sequence[Tuple[date, float]], draws: int = 10000, seed: int = 0) -> Dict[str, Any]:
    """Date-clustered result for per-date spreads: mean, t over dates, a bootstrap 95% interval and two-sided p (draws resample the dates).

    The same-sign half check is made on the first and second half of the dates; `passes` is the registered bar and needs both halves at |t| >= MIN_ABS_T."""
    pts = sorted((d, s) for d, s in spreads if math.isfinite(s))
    vals = [s for _, s in pts]
    n = len(vals)
    if n < 6:
        return {"dates": n, "passes": False, "reason": "fewer than 6 rebalance dates"}
    rng = random.Random(seed)
    means = sorted(mean(rng.choices(vals, k=n)) for _ in range(max(1, draws)))
    lo, hi = means[int(0.025 * len(means))], means[min(len(means) - 1, int(0.975 * len(means)))]
    below = sum(1 for m in means if m <= 0) / len(means)
    p = min(1.0, 2.0 * min(below, 1.0 - below))
    half = n // 2
    first, second = vals[:half], vals[half:]
    t_full, t1, t2 = _t(vals), _t(first), _t(second)
    same_sign = (mean(first) > 0) == (mean(second) > 0) == (mean(vals) > 0)
    return {
        "dates": n, "mean_bps": round(mean(vals) * 1e4, 2), "t": round(t_full, 3), "ci95_bps": [round(lo * 1e4, 2), round(hi * 1e4, 2)],
        "p_boot": round(p, 4), "half1_t": round(t1, 3), "half2_t": round(t2, 3), "same_sign": bool(same_sign),
        "passes": bool(abs(t1) >= MIN_ABS_T and abs(t2) >= MIN_ABS_T and same_sign and abs(t_full) >= MIN_ABS_T),
    }


def rebalance_dates(sessions: Sequence[date], start: date, step: int = 21) -> List[date]:
    """Every `step`-th session from the first session on or after `start`; non-overlapping windows when `step` equals the horizon."""
    idx = next((i for i, d in enumerate(sessions) if d >= start), None)
    return [] if idx is None else list(sessions[idx::step])


def forward_log_excess(stock: Mapping[date, float], bench: Mapping[date, float], day: date, exit_day: date) -> Optional[float]:
    """Log return of the stock from its close on `day` to its close on `exit_day`, minus the benchmark's over the same dates; None if any close is missing."""
    a, b = stock.get(day), stock.get(exit_day)
    ba, bb = bench.get(day), bench.get(exit_day)
    if not a or not b or not ba or not bb or min(a, b, ba, bb) <= 0:
        return None
    return math.log(b / a) - math.log(bb / ba)
