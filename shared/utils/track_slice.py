"""One slice of the paper ledger, summarised for a card: how many calls, how many were right, what they made after costs, how widely the
outcomes spread, and the walk-forward back-test beside it. Nothing is invented: a figure that cannot be known is None, never 0. Pure; no I/O."""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

MIN_SPREAD_N = 5          # percentiles of fewer outcomes than this are noise, so they are withheld (n is still reported)


def _bps(x: float) -> float:
    return round(x * 1e4, 1)


def percentile(sorted_xs: Sequence[float], q: float) -> float:
    """Linear-interpolated percentile (q in 0..1) of an ascending list."""
    if len(sorted_xs) == 1:
        return float(sorted_xs[0])
    pos = q * (len(sorted_xs) - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(sorted_xs) - 1)
    return float(sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * (pos - lo))


def outcome_spread(net_excess: Iterable[Optional[float]]) -> Dict[str, Any]:
    xs = sorted(float(x) for x in net_excess if x is not None and math.isfinite(float(x)))
    if len(xs) < MIN_SPREAD_N:
        return {"p10_bps": None, "p50_bps": None, "p90_bps": None, "n": len(xs)}
    return {"p10_bps": _bps(percentile(xs, 0.10)), "p50_bps": _bps(percentile(xs, 0.50)),
            "p90_bps": _bps(percentile(xs, 0.90)), "n": len(xs)}


def backtest_of(report: Optional[Mapping[str, Any]], horizon: str) -> Dict[str, Any]:
    """The chosen model's walk-forward figures for a horizon from the factor report; all None when the report has none."""
    empty: Dict[str, Any] = {"model": None, "months": None, "periods": None, "ic": None, "net_sharpe_after_fee": None, "validated": None}
    body = ((report or {}).get("horizons") or {}).get(horizon)
    if not isinstance(body, Mapping):
        return empty
    chosen = body.get("chosen")
    row = next((m for m in body.get("models") or [] if m.get("model") == chosen), None)
    if row is None:
        return empty
    dates = row.get("dates")
    return {"model": chosen, "months": dates if horizon == "monthly" else None, "periods": dates, "ic": row.get("mean_ic"),
            "net_sharpe_after_fee": row.get("net_sharpe"), "validated": row.get("validated")}


def slice_summary(closed: Sequence[Mapping[str, Any]], open_marks: Sequence[Mapping[str, Any]], report: Optional[Mapping[str, Any]],
                  horizon: str) -> Dict[str, Any]:
    """`closed` are closed paper trades and `open_marks` open ones marked at the latest close, each with `net_excess` (a fraction) and
    `idea_day`. Both count: a horizon longer than the ledger's history closes only the trades its stops cut, so closed trades alone are
    the losers and would read as a 3% hit rate while the winners are still running."""
    def usable(rows: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        return [r for r in rows if r.get("net_excess") is not None and math.isfinite(float(r["net_excess"]))]

    done, running = usable(closed), usable(open_marks)
    graded = done + running
    nets = sorted(float(r["net_excess"]) for r in graded)
    n = len(nets)
    days: List[Any] = [r["idea_day"] for r in graded if r.get("idea_day") is not None]
    since = min(days) if days else None
    last = max(days) if days else None
    iso = lambda d: d.isoformat() if hasattr(d, "isoformat") else (str(d) if d else None)      # noqa: E731
    distinct_days = len({iso(d) for d in days})
    return {
        "n_calls": n,
        "n_closed": len(done),
        "n_open_marked": len(running),
        # Open trades with no daily bar to mark them are counted here rather than dropped from the slice (review F14).
        "n_open_unmarked": len(open_marks) - len(running),
        "stopped": sum(1 for r in done if r.get("exit_reason") == "stop"),
        "share_right": round(sum(1 for x in nets if x > 0) / n, 3) if n else None,
        "mean_net_bps_after_costs": _bps(sum(nets) / n) if n else None,
        "median_net_bps": _bps(percentile(nets, 0.5)) if n else None,
        "live": {"n": n, "since": iso(since)},
        # How many separate days the calls span: 345 calls on 6 dates are one stretch, not 345 observations.
        "n_dates": distinct_days, "first_date": iso(since), "last_date": iso(last),
        "backtest": backtest_of(report, horizon),
        "outcome_spread": outcome_spread(nets),
    }


__all__ = ["MIN_SPREAD_N", "backtest_of", "outcome_spread", "percentile", "slice_summary"]
