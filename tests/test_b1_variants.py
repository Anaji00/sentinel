"""Daily-path B1 variants (shared/utils/b1_variants.py): the simulator is checked on data where the right answer is known."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import b1_variants as bv  # noqa: E402


def _panel(T=260, N=40, seed=0, drift_front=0.0):
    rng = np.random.default_rng(seed)
    r = rng.normal(0, 0.01, (T, N))
    r[:, 0] = rng.normal(0, 0.005, T)                                   # column 0 is the market
    close = 100 * np.exp(np.cumsum(r, axis=0))
    return close, r


def _positions(rows, longs, shorts):
    return [(row, c, 1, 1.0, 0.0) for row in rows for c in longs] + [(row, c, -1, 1.0, 0.0) for row in rows for c in shorts]


def test_v0_matches_the_forward_excess_return_less_the_short_fee() -> None:
    close, _ = _panel()
    horizon, rows = 21, [100, 140, 180]
    out = bv.simulate(_positions(rows, range(1, 11), range(11, 21)), close, 0, horizon, variants=("V0",))
    de = np.diff(np.log(close), axis=0, prepend=np.nan)
    de = de - de[:, [0]]
    for row, got in zip(rows, out["V0"]):
        longs = np.mean([de[row + 1: row + 22, c].sum() for c in range(1, 11)])
        shorts = np.mean([de[row + 1: row + 22, c].sum() for c in range(11, 21)])
        assert abs(got - ((longs - shorts) / 2 - bv.SHORT_FEE_BPS / 1e4 / 2)) < 1e-9


def test_a_front_loaded_signal_gets_a_short_half_life_and_the_decay_exit_cuts_a_late_reversal() -> None:
    horizon = 21
    gain = np.concatenate([np.full(5, 0.01), np.full(16, -0.0015)])      # up fast, then gives most of it back
    paths = np.cumsum(np.tile(gain, (30, 1)), axis=1)
    hl = bv.accrual_half_life(paths, horizon)
    assert hl <= 3                                                         # half the final gain was in by session 3
    flat = np.cumsum(np.tile(np.full(horizon, 0.001), (30, 1)), axis=1)
    assert bv.accrual_half_life(flat, horizon) in (10, 11)                 # steady accrual: about half way
    assert bv.accrual_half_life(-flat, horizon) == horizon                 # no gain: no early exit


def test_an_adverse_3_sigma_move_exits_early_and_a_stop_only_arms_after_a_gain() -> None:
    sigma, horizon = 0.01, 21
    crash = np.cumsum(np.full(horizon, -0.012))                           # falls through -3 sigma within a few sessions
    day = bv._exit_day(crash, sigma, horizon, "V2", horizon)
    assert 1 < day < 10
    steady_up = np.cumsum(np.full(horizon, 0.002))
    assert bv._exit_day(steady_up, sigma, horizon, "V2", horizon) == horizon
    run_up_then_fade = np.concatenate([np.cumsum(np.full(10, 0.01)), 0.10 - np.cumsum(np.full(11, 0.004))])
    assert bv._exit_day(run_up_then_fade, sigma, horizon, "V4", horizon) < horizon
    never_armed = np.cumsum(np.full(horizon, -0.0005))
    assert bv._exit_day(never_armed, sigma, horizon, "V4", horizon) == horizon


def test_shrinkage_weights_sum_to_one_are_nonnegative_and_favour_the_quiet_name() -> None:
    rng = np.random.default_rng(3)
    ret = rng.normal(0, 0.01, (63, 6))
    ret[:, 0] *= 0.2                                                       # one very quiet name
    w = bv.shrinkage_min_variance(ret)
    assert abs(w.sum() - 1) < 1e-9 and (w >= 0).all() and w[0] == w.max()
    assert np.allclose(bv.shrinkage_min_variance(ret[:5]), np.full(6, 1 / 6))       # too little history: equal weights


def test_the_full_simulation_runs_every_variant_and_early_exits_cost_money() -> None:
    close, _ = _panel(seed=5)
    out = bv.simulate(_positions([100, 140, 180], range(1, 11), range(11, 21)), close, 0, 21)
    assert set(out) == {"V0", "V1", "V2", "V3", "V4"} and all(len(v) == 3 for v in out.values())
    assert isinstance(bv.sharpe(out["V0"], 21), float) and bv.sharpe([0.01], 21) == 0.0


def test_the_harness_verdict_applies_the_registered_bar() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import run_b1_variants as h

    wins = {o: {"V0": 0.40, "V1": 0.55, "V3": 0.30} for o in range(8)}
    assert h.verdict(wins, "V1", 8) == {"wins": 8, "of": 8, "mean_gain": 0.15, "candidate": True}
    assert h.verdict(wins, "V3", 8)["candidate"] is False
    thin = {o: {"V0": 0.40, "V1": 0.41} for o in range(8)}
    assert h.verdict(thin, "V1", 8)["candidate"] is False                      # beats V0 everywhere but not by 0.10
    mixed = {o: {"V0": 0.40, "V1": 0.60 if o < 5 else 0.30} for o in range(8)}
    assert h.verdict(mixed, "V1", 8)["wins"] == 5 and h.verdict(mixed, "V1", 8)["candidate"] is False
