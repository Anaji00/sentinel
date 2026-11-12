"""Market-neutral daily correlation with FDR control (shared/utils/residual_corr.py)."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import residual_corr as rc  # noqa: E402


def _closes(returns):
    return list(100.0 * np.cumprod(1.0 + np.asarray(returns)))


def test_benjamini_hochberg_rejects_the_small_p_values_and_keeps_input_order() -> None:
    p = [0.9, 0.001, 0.04, 0.0004, 0.5]
    assert rc.benjamini_hochberg(p, 0.05) == [False, True, False, True, False] or rc.benjamini_hochberg(p, 0.05) == [False, True, True, True, False]
    assert rc.benjamini_hochberg([0.5, 0.6], 0.05) == [False, False] and rc.benjamini_hochberg([], 0.05) == []


def test_a_shared_market_factor_is_significant_raw_but_not_after_removing_it() -> None:
    rng = np.random.default_rng(1)
    n = 300
    mkt = rng.normal(0, 0.01, n)
    names = {f"S{i}": _closes(1.0 * mkt + rng.normal(0, 0.01, n)) for i in range(8)}          # every stock = market + own noise
    out = rc.market_neutral_pairs(names, _closes(mkt))
    assert out["tested"] == 28 and out["raw_significant"] >= 20            # all pairs look related through the market
    assert out["neutral_significant"] <= 2                                 # and almost none once it is removed


def test_a_real_pair_link_survives_market_neutralisation() -> None:
    rng = np.random.default_rng(2)
    n = 300
    mkt = rng.normal(0, 0.01, n)
    shared = rng.normal(0, 0.012, n)                                       # a common non-market driver (an industry move)
    names = {"A": _closes(mkt + shared + rng.normal(0, 0.004, n)), "B": _closes(mkt + shared + rng.normal(0, 0.004, n))}
    for i in range(6):
        names[f"N{i}"] = _closes(mkt + rng.normal(0, 0.012, n))
    out = rc.market_neutral_pairs(names, _closes(mkt))
    assert out["pairs"] and out["pairs"][0][:2] == ("A", "B") and out["pairs"][0][2] > 0.5


def test_short_or_flat_series_are_skipped_not_scored() -> None:
    assert rc.corr_p(np.zeros(200), np.arange(200.0)) == (0.0, 1.0)
    assert rc.corr_p(np.arange(50.0), np.arange(50.0)) == (0.0, 1.0)
    out = rc.market_neutral_pairs({"A": [1, 2, 3], "B": [1, 2, 3]}, [1, 2, 3])
    assert out["tested"] == 0


def test_stable_pairs_keep_a_persistent_link_and_drop_one_that_appeared_in_one_half_only() -> None:
    rng = np.random.default_rng(3)
    n = 300
    mkt = rng.normal(0, 0.01, n)
    shared = rng.normal(0, 0.012, n)
    a = mkt + shared + rng.normal(0, 0.004, n)
    b = mkt + shared + rng.normal(0, 0.004, n)
    c = mkt + rng.normal(0, 0.012, n)
    d = mkt + rng.normal(0, 0.012, n)
    c[:150] += 1.0 * shared[:150]                       # linked to A and B only in the first half
    d[:150] += 1.0 * shared[:150]
    names = {"A": _closes(a), "B": _closes(b), "C": _closes(c), "D": _closes(d)}
    for i in range(4):
        names[f"N{i}"] = _closes(mkt + rng.normal(0, 0.012, n))
    out = rc.stable_pairs(names, _closes(mkt))
    got = {(x, y) for x, y, *_ in out["stable"]}
    assert ("A", "B") in got                              # holds in both halves
    assert ("C", "D") not in got and "_residuals" not in out
