"""The macro/equity correlation needs the guards the peer graph already has.

Both compute a Pearson coefficient over the same macro feeds. The peer graph
published six pairs at |r| = 1.000 on its first live run, because twelve macro
anchors were polled every thirty seconds and republished unchanged: a series of
one repeated print correlates perfectly with another series of one repeated
print across the few bars that did move. It gained a distinct-value ratio and a
Student-t significance test.

This path did the same arithmetic on the same feeds with neither. It required
fourteen returns and a coefficient at or below -0.55, which at that sample size
is roughly p = 0.04 -- published as "Strong inverse correlation" with the
coefficient's magnitude used directly as the bulletin's conviction.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.macro_intelligence_engine import (  # noqa: E402
    MAX_CORRELATION_P_VALUE, MIN_DISTINCT_RATIO,
    _both_series_move, _is_significant,
)


def test_a_frozen_series_is_rejected():
    """211 rows carrying one distinct close is what produced |r| = 1.000."""
    frozen = [716.76] * 20
    moving = [100.0 + i for i in range(20)]
    assert not _both_series_move(frozen, moving)
    assert not _both_series_move(moving, frozen)


def test_a_mostly_frozen_series_is_rejected():
    """The dangerous case: a few real moves inside a wall of repeats."""
    mostly = [716.76] * 18 + [717.0, 718.0]
    assert (len(set(mostly)) / len(mostly)) < MIN_DISTINCT_RATIO
    assert not _both_series_move(mostly, [100.0 + i for i in range(20)])


def test_two_live_series_pass():
    a = [100.0 + i * 0.7 for i in range(20)]
    b = [50.0 - i * 0.3 for i in range(20)]
    assert _both_series_move(a, b)


def test_an_empty_series_is_rejected_rather_than_dividing_by_zero():
    assert not _both_series_move([], [1.0, 2.0])
    assert not _both_series_move(None, [1.0, 2.0])


def test_the_marginal_coefficient_no_longer_qualifies():
    """-0.55 at fourteen returns is about p = 0.04, not a strong finding."""
    assert not _is_significant(-0.55, 14)


def test_a_genuinely_strong_coefficient_qualifies():
    assert _is_significant(-0.85, 20)


def test_more_samples_make_the_same_coefficient_admissible():
    """The point of a significance test rather than a bare threshold."""
    assert not _is_significant(-0.60, 12)
    assert _is_significant(-0.60, 60)


def test_a_degenerate_sample_is_refused():
    assert not _is_significant(-0.99, 2)
    assert not _is_significant(-0.99, 0)


def test_perfect_correlation_does_not_divide_by_zero():
    """r = 1.0 puts a zero in the t-statistic's denominator."""
    assert _is_significant(1.0, 30) in (True, False)
    assert _is_significant(-1.0, 30) in (True, False)


def test_the_threshold_matches_the_peer_graph():
    assert MAX_CORRELATION_P_VALUE == 0.01
    assert MIN_DISTINCT_RATIO == 0.40


def test_the_guards_are_wired_into_the_correlation_path():
    source = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
    assert "_both_series_move(x_vec, y_vec)" in source
    assert "_is_significant(pearson_corr, len(x_returns))" in source
