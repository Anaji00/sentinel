"""Throwaway copy of the harness hooks for the registered trials T3 (five-seed model), T6 (label variants) and the flat-bar cell, 19 Nov 2026.
`shared/utils/factor_model.py` is NOT edited: `install()` replaces three module-level names in the imported module, in the trial process only,
and every other cell runs the live code path unchanged. The hooks are:

* model "lgbm_rank_s5": five `lgbm_rank` fits (same parameters and features, `random_state` 7 to 11, the live seed being 7), their cross-sectional
  ranks averaged with equal weight (T3 cell A).
* training_target modes (used through `walk_forward(target_mode=...)`; evaluation is always on the forward excess return, as in the live code):
    "multi_horizon"  per session, the mean of the cross-sectional ranks, among that session's eligible names, of the 10-, 21- and 42-session forward
                     excess returns (log return net of SPY, as `forward_excess`); a row missing any of the three is not a training row (T6).
    "vol_scaled"     the 21-session forward excess return divided by the name's trailing 63-session standard deviation of daily log returns (at
                     least 32 observations, at least 1e-4), known at the row (T6).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.utils import factor_model as fm  # noqa: E402

SEED_MODEL = "lgbm_rank_s5"
SEEDS = (7, 8, 9, 10, 11)
SEED_PREFIX = "lgbm_rank_seed"      # noise-floor cells (20 Nov): "lgbm_rank_seed8" is lgbm_rank with random_state 8 and nothing else changed
MULTI_HORIZONS = (10, 21, 42)
VOL_WINDOW = 63
_installed = False
_orig = {}


class SeedBag:
    """Five LGBMRanker fits that differ only in `random_state`; the prediction is the mean of their cross-sectional ranks."""

    def __init__(self) -> None:
        self.members = []
        for s in SEEDS:
            m = _orig["make"]("lgbm_rank")
            m.set_params(random_state=s)
            self.members.append(m)

    def fit(self, X: np.ndarray, y: np.ndarray, d: np.ndarray) -> None:
        for m in self.members:
            _orig["fit"]("lgbm_rank", m, X, y, d)

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.mean([fm._rank(m.predict(X)) for m in self.members], axis=0)


def _rolling_std(x: np.ndarray, w: int, min_obs: int) -> np.ndarray:
    ok = np.isfinite(x)
    z = np.where(ok, x, 0.0)
    c1, c2, cn = (np.cumsum(a, axis=0) for a in (z, z * z, ok.astype(float)))

    def win(c: np.ndarray) -> np.ndarray:
        out = c.copy()
        out[w:] = c[w:] - c[:-w]
        return out
    s1, s2, n = win(c1), win(c2), win(cn)
    with np.errstate(all="ignore"):
        var = (s2 - s1 * s1 / n) / (n - 1)
        return np.where(n >= min_obs, np.sqrt(np.maximum(var, 0.0)), np.nan)


def multi_horizon_target(panel: fm.Panel) -> np.ndarray:
    T = len(panel.dates)
    fwd = [fm.forward_excess(panel, h) for h in MULTI_HORIZONS]
    out = np.full(panel.close.shape, np.nan)
    for i in range(T):
        ok = fm._eligible(panel, i)
        if ok.sum() < fm.MIN_NAMES:
            continue
        ranks = [fm._rank(np.where(ok, f[i], np.nan)) for f in fwd]
        out[i] = np.mean(ranks, axis=0)
    return out


def vol_scaled_target(panel: fm.Panel, horizon: int) -> np.ndarray:
    with np.errstate(all="ignore"):
        lr = np.diff(np.log(panel.close.astype(np.float64)), axis=0, prepend=np.nan)
    vol = _rolling_std(lr, VOL_WINDOW, VOL_WINDOW // 2)
    vol = np.where(vol >= 1e-4, vol, np.nan)
    return fm.forward_excess(panel, horizon) / vol


def install() -> None:
    """Idempotent. Adds the model and the two target modes to the imported `factor_model` module for this process only."""
    global _installed
    if _installed:
        return
    _orig.update(make=fm._make, fit=fm._fit, target=fm.training_target)

    def _make(model: str):
        if model.startswith(SEED_PREFIX):
            m = _orig["make"]("lgbm_rank")
            m.set_params(random_state=int(model[len(SEED_PREFIX):]))
            return m
        return SeedBag() if model == SEED_MODEL else _orig["make"](model)

    def _fit(model_name: str, est, X: np.ndarray, y: np.ndarray, d: np.ndarray) -> None:
        if model_name == SEED_MODEL:
            est.fit(X, y, d)
        elif model_name.startswith(SEED_PREFIX):
            _orig["fit"]("lgbm_rank", est, X, y, d)
        else:
            _orig["fit"](model_name, est, X, y, d)

    def training_target(panel: fm.Panel, horizon: int, feats: Dict[str, np.ndarray], mode: str = "excess") -> np.ndarray:
        if mode == "multi_horizon":
            return multi_horizon_target(panel)
        if mode == "vol_scaled":
            return vol_scaled_target(panel, horizon)
        return _orig["target"](panel, horizon, feats, mode)

    fm._make, fm._fit, fm.training_target = _make, _fit, training_target
    fm.NAN_OK_MODELS = fm.NAN_OK_MODELS | {SEED_MODEL} | {f"{SEED_PREFIX}{k}" for k in range(1, 100)}
    _installed = True


def selftest() -> None:
    """No database: the seed bag fits and averages ranks, the volatility-scaled and multi-horizon targets have the panel's shape and no look-ahead
    at the last rows, and an unmodified mode still returns the live target."""
    rng = np.random.default_rng(3)
    T, N = 600, 80
    close = 50 * np.exp(np.cumsum(rng.normal(0, 0.01, (T, N)), axis=0)).astype(np.float32)
    close[:, 0] = 100 * np.exp(np.cumsum(rng.normal(0, 0.005, T)))
    panel = fm.Panel([f"d{i}" for i in range(T)], ["SPY"] + [f"T{i}" for i in range(1, N)], close, close, np.full((T, N), 1e7, dtype=np.float32), {})
    panel.extra["_tradable"] = np.ones((T, N), dtype=bool)
    install()
    feats: Dict[str, np.ndarray] = {}
    live = fm.training_target(panel, 21, feats, "excess")
    assert np.allclose(live, fm.forward_excess(panel, 21), equal_nan=True)
    vs = fm.training_target(panel, 21, feats, "vol_scaled")
    assert vs.shape == close.shape and np.isnan(vs[-21:]).all() and np.isfinite(vs[300, 5])
    mh = fm.training_target(panel, 21, feats, "multi_horizon")
    assert np.isnan(mh[-42:]).all() and np.isfinite(mh[300, 5]) and -0.5 <= np.nanmin(mh) <= np.nanmax(mh) <= 0.5
    X = rng.normal(size=(600, 6))
    y = (rng.normal(size=600) > 0).astype(float) - 0.5
    d = np.repeat(np.arange(6), 100)
    est = fm._make(SEED_MODEL)
    fm._fit(SEED_MODEL, est, X, y, d)
    p = fm._predict(SEED_MODEL, est, X)
    assert p.shape == (600,) and len(est.members) == 5 and [m.get_params()["random_state"] for m in est.members] == list(SEEDS)
    print("harness selftest ok")


if __name__ == "__main__":
    selftest()
