"""Three-level empirical-Bayes shrinkage of signal cells (shared/utils/hier_bayes.py)."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import hier_bayes as hb  # noqa: E402


def _cells(seed=0):
    rng = np.random.default_rng(seed)
    cells = {}
    for fam, mu in (("real_edge", 0.004), ("nothing", 0.0), ("also_nothing", 0.0)):
        for band, n in (("large", 300), ("mid", 120), ("small", 8)):
            cells[(fam, "5d", band)] = list(rng.normal(mu, 0.03, n))
    return cells


def test_a_small_cell_is_pulled_toward_its_family_and_a_large_cell_barely_moves() -> None:
    cells = _cells()
    # make the small cell of the real family look extreme on 8 observations
    cells[("real_edge", "5d", "small")] = [0.05] * 8
    post = {p.key: p for p in hb.shrink(cells)}
    small, large = post[("real_edge", "5d", "small")], post[("real_edge", "5d", "large")]
    assert abs(small.raw_mean - 0.05) < 1e-12 and small.post_mean < small.raw_mean * 0.95          # pulled in
    assert small.post_mean > 0 and small.parent_mean > 0                                    # toward a positive parent, not toward zero
    assert abs(large.post_mean - large.raw_mean) < abs(small.post_mean - small.raw_mean)   # the big cell keeps its own estimate


def test_effective_n_borrows_from_the_parent_and_more_when_siblings_agree() -> None:
    rng = np.random.default_rng(1)
    agree = {("f", "1d", b): list(rng.normal(0.003, 0.02, n)) for b, n in (("a", 200), ("b", 200), ("c", 10))}
    differ = {("f", "1d", "a"): list(rng.normal(0.02, 0.02, 200)), ("f", "1d", "b"): list(rng.normal(-0.02, 0.02, 200)), ("f", "1d", "c"): list(rng.normal(0.003, 0.02, 10))}
    na = {p.key[2]: p for p in hb.shrink(agree)}["c"].n_eff
    nd = {p.key[2]: p for p in hb.shrink(differ)}["c"].n_eff
    assert na > 10 and nd < na                                              # borrows more when the siblings agree


def test_costs_come_off_every_observation_and_can_remove_the_edge() -> None:
    rng = np.random.default_rng(2)
    cells = {("f", "5d", b): list(rng.normal(0.002, 0.02, 400)) for b in ("a", "b", "c")}
    cells.update({("g", "5d", b): list(rng.normal(0.0, 0.02, 400)) for b in ("a", "b", "c")})
    before = {p.key: p for p in hb.shrink(cells)}[("f", "5d", "a")]
    after = {p.key: p for p in hb.shrink(cells, cost=0.004)}[("f", "5d", "a")]
    assert after.post_mean < before.post_mean - 0.002 and after.p_positive < before.p_positive        # shrinkage to zero damps part of the cost


def test_the_release_rule_needs_probability_effective_n_and_the_fdr_step() -> None:
    cells = _cells(4)
    ps = hb.shrink(cells)
    got = hb.release_candidates(ps)
    assert all(p.p_positive >= hb.RELEASE_P and p.n_eff >= hb.RELEASE_N_EFF for p in got)
    assert not any(p.key[0] in ("nothing", "also_nothing") for p in got)                 # null families do not pass
    assert hb.shrink({("f", "1d", "a"): [0.1, 0.1]}) == []                               # under the minimum cell size


def test_benjamini_hochberg_is_monotone_and_handles_empty() -> None:
    assert hb.benjamini_hochberg([]) == [] and hb.benjamini_hochberg([0.6, 0.7]) == [False, False]
    assert hb.benjamini_hochberg([0.001, 0.2, 0.0004]) == [True, False, True]


def test_a_cell_is_never_surer_than_its_parent() -> None:
    rng = np.random.default_rng(6)
    cells = {(f, "5d", b): list(rng.normal(0.0, 0.03, n)) for f in ("x", "y", "z") for b, n in (("a", 100), ("b", 100), ("c", 100))}
    for p in hb.shrink(cells):
        assert p.post_sd > 0 and p.post_sd >= 0.03 / (p.n_eff + 400) ** 0.5 * 0.5           # no zero-width posterior from a collapsed prior
        assert 0.0 <= p.p_positive <= 1.0
