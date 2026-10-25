"""The overlay's size: volatility target, equity-regime gauge and beta hedge."""

import pathlib
import sys
from datetime import date, timedelta
from typing import Dict

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import equity_regime as er  # noqa: E402
from shared.utils import model_portfolio as mp  # noqa: E402


def _days(n: int):
    d0 = date(2026, 1, 2)
    return [d0 + timedelta(days=i) for i in range(n)]


def test_a_volatile_book_is_scaled_down_and_its_net_beta_hedged_in_the_core() -> None:
    rng = np.random.default_rng(1)
    days = _days(130)
    spy = {d: float(x) for d, x in zip(days, rng.normal(0, 0.01, len(days)))}
    rets: Dict[str, Dict[date, float]] = {}
    for i in range(10):          # longs: beta 1.5 and plenty of their own noise
        rets[f"L{i}"] = {d: 1.5 * spy[d] + float(rng.normal(0, 0.05)) for d in days}
    for i in range(10):          # shorts: beta 0.5
        rets[f"S{i}"] = {d: 0.5 * spy[d] + float(rng.normal(0, 0.05)) for d in days}
    out = mp.overlay_sizing(rets, spy, [f"L{i}" for i in range(10)], [f"S{i}" for i in range(10)])
    assert out["ex_ante_vol"] > mp.TARGET_LS_VOL and out["vol_scale"] < 1.0
    assert abs(out["beta_long"] - 1.5) < 0.3 and abs(out["beta_short"] - 0.5) < 0.3
    assert out["core"] < 1.0                                   # the overlay's beta is taken out of the core
    assert abs(out["core"] - (1 - out["gross"] * (out["beta_long"] - out["beta_short"]))) < 1e-3
    shrunk = mp.overlay_sizing(rets, spy, [f"L{i}" for i in range(10)], [f"S{i}" for i in range(10)], multiplier=0.6)
    assert abs(shrunk["gross"] - 0.6 * out["gross"]) < 1e-3


def test_thin_history_keeps_the_plain_design_and_sizing_applies_from_the_next_session() -> None:
    assert mp.overlay_sizing({}, {}, ["A"], ["B"]) ["core"] == 1.0
    hist = [{"decided_on": "2026-10-14", "gross": 0.2, "core": 0.9},
            {"decided_on": "2026-10-15", "gross": 0.1, "core": 0.95}]
    assert mp.sizing_for(hist, date(2026, 10, 15)) == (0.2, 0.9)
    assert mp.sizing_for(hist, date(2026, 10, 16)) == (0.1, 0.95)
    assert mp.sizing_for([], date(2026, 10, 16)) == (mp.OVERLAY_GROSS, 1.0)
    step = mp.day_step(1.0, 1.0, 0.01, 0.0, 0.0, gross=0.3, core=0.9, shorts_held=False)
    assert abs(step["ret"] - 0.009) < 1e-12


def test_the_gauge_reads_a_momentum_reversal_as_stressed() -> None:
    T, N = 400, 200
    rng = np.random.default_rng(3)
    mom = np.tile(np.arange(N, dtype=float), (T, 1))          # name i always ranks i
    r = rng.normal(0, 0.01, (T, N))
    r[-10:, -40:] -= 0.02                                      # last ten days: winners fall
    r[-10:, :40] += 0.02                                       # and losers rally
    close = 100 * np.cumprod(1 + r, axis=0)
    mask = np.ones((T, N), dtype=bool)
    mom_ls = er.momentum_factor(close, mom, mask)
    spy = 100 * np.cumprod(1 + rng.normal(0.0005, 0.005, T))
    calm = er.level_at(T - 15, mom_ls, spy)
    hit = er.level_at(T - 1, mom_ls, spy)
    assert calm["level"] == "calm" and hit["level"] == "stressed"
    assert hit["momentum_drawdown"] < er.STRESS_DRAWDOWN and hit["multiplier"] == er.MULTIPLIER["stressed"]
    assert er.level_at(10, mom_ls, spy) is None                  # under a year of history
