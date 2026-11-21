"""Registered test of the monthly calibration's top end (Upgrade pre-registrations - 12 Nov; design from reports/System improvement research - 13 Nov.md, Addendum 3). Read-only.

Question: is the top decile by raw alpha better than the middle deciles (4 to 7), and does ranking by the rank z alone do better than ranking by alpha
(alpha = IC x sigma x z, so alpha also ranks on volatility)? The unit is the date: every statistic is a per-date mean difference, then a t over dates
and a bootstrap that resamples dates, so the thousands of names on one date are never counted as independent.

Run in its own capped container, never inside factor-model, and not between 22:00 and 06:00 UTC (it builds the whole panel and re-runs the monthly walk-forward):

    docker run --rm --memory 9g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro \
        -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app \
        sentinel-factor-model python scripts/research_calibration_top.py [--model lgbm_rank]

Self-test of the pure helpers (no database, no model): python scripts/research_calibration_top.py --selftest

Bars (registered before any run):
  T1  top decile by alpha minus deciles 4 to 7, mean over dates: one-sided bootstrap p < 0.05 -> the top end is better than the middle.
  T2  top decile by rank z minus top decile by alpha, mean over dates: one-sided bootstrap p < 0.10 -> the volatility multiplier is hurting the top end;
      then the calibration map should be fitted on rank z. Both results are reported whatever they are.
Realised values are the walk-forward's log excess returns, the same series the lab's calibration uses.
"""
import argparse
import asyncio
import json
import math
import sys
from pathlib import Path
from statistics import NormalDist
from typing import Dict, List, Optional, Sequence

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MIN_NAMES = 50          # a date with fewer ranked names is skipped, as in portfolio_lab.alpha_calibration
BOOTSTRAP_DRAWS = 20000
SEED = 20261113
T1_P = 0.05
T2_P = 0.10


def decile_slices(n: int) -> List[slice]:
    """Ten consecutive slices of 0..n-1 whose sizes differ by at most one (a rank 0..n-1 sorted ascending)."""
    edges = [round(i * n / 10) for i in range(11)]
    return [slice(edges[i], edges[i + 1]) for i in range(10)]


def date_stats(score_alpha: np.ndarray, score_z: np.ndarray, realised: np.ndarray) -> Optional[Dict[str, float]]:
    """One date's means: top decile by alpha, deciles 4 to 7 by alpha, top decile by z. None if the date is too thin."""
    ok = np.isfinite(score_alpha) & np.isfinite(score_z) & np.isfinite(realised)
    if ok.sum() < MIN_NAMES:
        return None
    a, z, y = score_alpha[ok], score_z[ok], realised[ok]
    sl = decile_slices(len(y))
    by_a = y[np.argsort(a, kind="stable")]
    by_z = y[np.argsort(z, kind="stable")]
    top_a = float(by_a[sl[9]].mean())
    mid = float(by_a[sl[3].start:sl[6].stop].mean())
    top_z = float(by_z[sl[9]].mean())
    return {"top_alpha": top_a, "mid": mid, "top_z": top_z, "n": float(len(y))}


def bootstrap_one_sided(diffs: Sequence[float], draws: int = BOOTSTRAP_DRAWS, seed: int = SEED) -> Dict[str, float]:
    """Mean of per-date differences, its t over dates, and the one-sided bootstrap p that the mean is > 0
    (share of date-resamples, centred on the sample mean, whose mean reaches the observed one)."""
    d = np.asarray(list(diffs), dtype=float)
    n = len(d)
    if n < 3:
        return {"n_dates": float(n), "mean": float("nan"), "t": float("nan"), "p_one_sided": float("nan")}
    mean = float(d.mean())
    sd = float(d.std(ddof=1))
    t = mean / (sd / math.sqrt(n)) if sd > 0 else float("nan")
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n, size=(draws, n))
    centred = d - mean
    boot = centred[idx].mean(axis=1)
    p = float((boot >= mean).mean())
    return {"n_dates": float(n), "mean": mean, "t": t, "p_one_sided": p}


def evaluate(per_date: Sequence[Dict[str, float]]) -> Dict[str, object]:
    """The two registered tests from per-date stats."""
    t1 = bootstrap_one_sided([s["top_alpha"] - s["mid"] for s in per_date])
    t2 = bootstrap_one_sided([s["top_z"] - s["top_alpha"] for s in per_date])
    t3 = bootstrap_one_sided([s["top_z"] - s["mid"] for s in per_date])
    return {
        "dates": len(per_date),
        "T1_top_alpha_minus_middle": {k: round(v, 5) for k, v in t1.items()},
        "T1_passes": bool(t1["p_one_sided"] < T1_P),
        "T2_top_z_minus_top_alpha": {k: round(v, 5) for k, v in t2.items()},
        "T2_volatility_multiplier_hurts": bool(t2["p_one_sided"] < T2_P),
        "context_top_z_minus_middle": {k: round(v, 5) for k, v in t3.items()},
        "mean_top_alpha_bps": round(1e4 * float(np.mean([s["top_alpha"] for s in per_date])), 1),
        "mean_middle_bps": round(1e4 * float(np.mean([s["mid"] for s in per_date])), 1),
        "mean_top_z_bps": round(1e4 * float(np.mean([s["top_z"] for s in per_date])), 1),
    }


def selftest() -> None:
    rng = np.random.default_rng(1)
    # Skill present: realised rises with the score; the top beats the middle on every date.
    dates = []
    for _ in range(40):
        z = rng.normal(size=400)
        sig = rng.uniform(0.2, 0.9, size=400)
        y = 0.01 * z + rng.normal(scale=0.03, size=400)
        s = date_stats(z * sig, z, y)
        assert s is not None
        dates.append(s)
    out = evaluate(dates)
    assert out["T1_passes"] is True, out
    # No skill: nothing passes, and the p-value is not extreme in the passing direction.
    null_dates = []
    for _ in range(40):
        z = rng.normal(size=400)
        y = rng.normal(scale=0.03, size=400)
        null_dates.append(date_stats(z * rng.uniform(0.2, 0.9, size=400), z, y))
    assert evaluate(null_dates)["T1_passes"] is False
    # Volatility hurts the top end: high-sigma names under-realise, so ranking on z alone beats ranking on alpha.
    hurt = []
    for _ in range(40):
        z = rng.normal(size=400)
        sig = rng.uniform(0.2, 0.9, size=400)
        y = 0.01 * z - 0.03 * (sig - 0.55) + rng.normal(scale=0.02, size=400)
        hurt.append(date_stats(z * sig, z, y))
    assert evaluate(hurt)["T2_volatility_multiplier_hurts"] is True
    # Thin dates are skipped; decile slices tile the range.
    assert date_stats(np.arange(10.0), np.arange(10.0), np.arange(10.0)) is None
    for n in (50, 57, 283):
        s = decile_slices(n)
        assert s[0].start == 0 and s[-1].stop == n and all(s[i].stop == s[i + 1].start for i in range(9))
    # The bootstrap is deterministic.
    a = bootstrap_one_sided([0.1, -0.05, 0.2, 0.0, 0.07])
    b = bootstrap_one_sided([0.1, -0.05, 0.2, 0.0, 0.07])
    assert a == b
    print("selftest ok")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="lgbm_rank")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        selftest()
        return

    from shared.db import get_redis, get_timescale
    from shared.utils import expected_returns as er
    from shared.utils import factor_model as fm
    from shared.utils import portfolio_lab as pl
    from services.agents.factor_engine import FactorEngine

    horizon = fm.HORIZON_SPECS["monthly"].horizon if hasattr(fm.HORIZON_SPECS["monthly"], "horizon") else 21
    db, redis = await get_timescale(), await get_redis()
    panel = await FactorEngine(db, redis)._panel()
    if panel is None:
        print("no panel")
        return
    panel = fm.tradable_subset(panel)
    feats = fm.features(panel)
    preds: list = []
    fm.walk_forward(panel, args.model, spec=fm.HORIZON_SPECS["monthly"], feats=feats, collect_preds=preds)

    nd = NormalDist()
    with np.errstate(all="ignore"):
        d = np.diff(np.log(panel.close), axis=0, prepend=np.nan)
    ics: List[float] = []
    per_date: List[Dict[str, float]] = []
    for row, cols, pred, raw in sorted(preds, key=lambda p: p[0]):
        ok = np.isfinite(pred) & np.isfinite(raw)
        if ok.sum() < MIN_NAMES:
            continue
        c, p, y = cols[ok], pred[ok], raw[ok]
        if ics:   # the IC known before this date, exactly as portfolio_lab.alpha_calibration
            zz = np.array([nd.inv_cdf(v) for v in (np.argsort(np.argsort(p)) + 0.5) / len(p)])
            sig = np.nanstd(d[max(0, row - 62): row + 1, c], axis=0) * math.sqrt(252)
            a = np.array([er.alpha(float(np.mean(ics)), float(s_), float(z_), horizon) or np.nan for s_, z_ in zip(sig, zz)])
            st = date_stats(a, zz, y)
            if st is not None:
                per_date.append(st)
        ics.append(pl._spearman(p, y) or 0.0)
    print(json.dumps(evaluate(per_date), indent=1))


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        asyncio.run(main())
