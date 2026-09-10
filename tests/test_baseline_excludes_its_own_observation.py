"""A baseline must not contain the observation it judges.

`window_ret = returns[i-20:i]` includes `returns[i-1]`, which is the value the
z-score tests. The point pulls the mean toward itself and inflates the standard
deviation it is then divided by, so a genuine outlier is systematically
understated -- and it caps what the statistic can ever say, because a point
inside its own sample of n cannot exceed (n-1)/sqrt(n).

Same mistake as the earnings surprise seeded with its own observation, recorded
earlier in this audit: "No history means no z-score, not a z-score of zero."
Here there was history, and the reading was quietly compressed.
"""
import ast
import pathlib

import numpy as np
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "services" / "reasoning" / "strategy_backtester.py").read_text(encoding="utf-8")


def _z(sample, window):
    return (sample - np.mean(window)) / max(1e-4, np.std(window))


def test_including_the_point_understates_a_real_outlier():
    """The measurement that motivated the fix, as a property."""
    rng = np.random.default_rng(7)
    inc, exc = [], []
    for _ in range(4000):
        r = rng.normal(0, 0.01, 21)
        r[-1] = -0.031                     # a genuine ~3.1 sigma move
        inc.append(_z(r[-1], r[-20:]))     # window contains the tested point
        exc.append(_z(r[-1], r[-21:-1]))   # window excludes it
    assert abs(np.mean(inc)) < abs(np.mean(exc)), (
        "self-inclusion should shrink the magnitude of a real outlier"
    )
    # It is not a rounding difference: about a fifth of the signal.
    assert abs(np.mean(exc)) - abs(np.mean(inc)) > 0.5


def test_self_inclusion_caps_the_statistic():
    """A point inside its own sample cannot exceed a fixed ceiling.

    With the population standard deviation `np.std` computes, the largest
    standardised deviation attainable by a member of its own sample of n is
    sqrt(n - 1) -- 4.36 for the 20-bar window here. So conviction, which is
    0.50 + 0.10*|z| capped at 0.90, was bounded by arithmetic rather than by
    the market: every move past about four sigma read the same.
    """
    n = 20
    ceiling = np.sqrt(n - 1)
    rng = np.random.default_rng(1)
    worst = 0.0
    for _ in range(5000):
        w = rng.normal(0, 0.01, n)
        w[-1] = w[:-1].min() * 50          # make the last point extreme
        worst = max(worst, abs(_z(w[-1], w)))
    assert worst <= ceiling + 1e-6, (worst, ceiling)
    assert ceiling < 4.4

    # Excluded from its own window, the same point is unbounded.
    unbounded = 0.0
    for _ in range(5000):
        w = rng.normal(0, 0.01, n + 1)
        w[-1] = w[:-1].min() * 50
        unbounded = max(unbounded, abs(_z(w[-1], w[:-1])))
    assert unbounded > ceiling


def test_the_entry_threshold_is_reachable_after_the_fix():
    """A -3.1 sigma move must clear a -2.0 entry essentially always."""
    rng = np.random.default_rng(11)
    hits_inc = hits_exc = 0
    trials = 4000
    for _ in range(trials):
        r = rng.normal(0, 0.01, 21)
        r[-1] = -0.031
        hits_inc += _z(r[-1], r[-20:]) <= -2.0
        hits_exc += _z(r[-1], r[-21:-1]) <= -2.0
    assert hits_exc > hits_inc
    assert hits_exc / trials > 0.99


@pytest.mark.parametrize("fn_name", ["backtest_strategy"])
def test_neither_backtest_window_contains_the_tested_return(fn_name):
    """Read out of the source, so a future edit that reintroduces it fails."""
    tree = ast.parse(SRC)
    slices = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript) or not isinstance(node.slice, ast.Slice):
            continue
        val = node.value
        if not (isinstance(val, ast.Name) and val.id == "returns"):
            continue
        slices.append(ast.unparse(node))

    assert slices, "no returns[...] window found; the check has stopped checking"
    for sl in slices:
        assert "i - 20):i]" not in sl.replace(" ", " "), (
            f"{sl} ends at i, so it contains returns[i-1] -- the value the "
            "z-score tests. Use returns[max(0, i - 21):i - 1]."
        )
