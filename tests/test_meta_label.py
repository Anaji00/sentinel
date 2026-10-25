"""The sizing layer helps when something predicts which picks pay, and is harmless when nothing does."""

import pathlib
import random
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import meta_label as ml  # noqa: E402


def _picks(seed, informative):
    rng = random.Random(seed)
    T, per = 400, 40
    vol = np.full((T, 50), np.nan)
    picks = []
    for t in range(T):
        for k in range(per):
            col = k
            v = rng.uniform(0.1, 0.6)
            vol[t, col] = v
            edge = (0.004 if v < 0.3 else -0.004) if informative else 0.0
            side = 1 if k % 2 else -1
            fwd = side * (edge + rng.gauss(0, 0.03))
            picks.append(ml.Pick(t, col, side, 0.4 * side, fwd))
    feats = {"vol_63": vol}
    X = ml.design(picks, feats, np.ones(T))
    return picks, X


def test_the_layer_sizes_toward_the_picks_that_pay() -> None:
    picks, X = _picks(1, informative=True)
    out = ml.walk_forward(picks, X, cost=0.0005, horizon=5, embargo=5)
    assert out["periods"] > 300
    assert out["meta"]["sharpe"] > out["base"]["sharpe"] + 1.0


def test_with_nothing_to_learn_it_mostly_stands_aside_without_blowing_up() -> None:
    picks, X = _picks(2, informative=False)
    out = ml.walk_forward(picks, X, cost=0.0005, horizon=5, embargo=5)
    assert out["mean_size"] < 0.2
    assert abs(out["meta"]["mean_bps"]) <= abs(out["base"]["mean_bps"]) + 1.0


def test_size_is_zero_at_or_below_a_coin_flip() -> None:
    assert list(ml.size_from_probability(np.array([0.3, 0.5, 0.75, 1.0]))) == [0.0, 0.0, 0.5, 1.0]


def test_costs_are_charged_when_a_position_opens_not_every_period() -> None:
    picks = [ml.Pick(0, 1, 1, 0.4, 0.0), ml.Pick(1, 1, 1, 0.4, 0.0), ml.Pick(2, 2, 1, 0.4, 0.0),
             ml.Pick(2, 1, -1, -0.4, 0.0)]
    assert list(ml.entry_costs(picks, 0.002)) == [0.002, 0.0, 0.002, 0.002]
