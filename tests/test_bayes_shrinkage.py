"""Small-sample hit rates are shrunk, not trusted or ignored."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.bayes import posterior_hit_rate, prob_rate_above  # noqa: E402


def test_a_lucky_streak_barely_moves_the_estimate() -> None:
    assert 0.55 < posterior_hit_rate(5, 5) < 0.62     # 5/5 raw would be 1.0


def test_a_long_record_dominates_the_prior() -> None:
    assert abs(posterior_hit_rate(600, 1000) - 0.6) < 0.01


def test_the_prior_centres_on_the_reference_rate() -> None:
    assert posterior_hit_rate(0, 0, prior_mean=0.58) == 0.58


def test_probability_above_a_coin_flip_rises_with_evidence() -> None:
    few = prob_rate_above(6, 10)
    many = prob_rate_above(600, 1000)
    assert few is not None and many is not None and many > few > 0.5
