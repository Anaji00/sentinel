"""Scores that stopped rising at a cap now keep rising, with every existing cut where it was (12 Nov 2026 audit)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.earnings_reaction import reaction_score  # noqa: E402
from shared.utils.liquidations import burst_score  # noqa: E402


def test_liquidation_bursts_are_unchanged_up_to_7x_and_spread_above_it() -> None:
    assert [burst_score(m) for m in (3, 5, 7)] == [0.5, 0.7, 0.9]
    above = [burst_score(m) for m in (7.5, 10, 15, 25, 60)]
    assert above == sorted(above) and len(set(above)) == 5 and above[0] > 0.9 and above[-1] <= 0.97
    assert burst_score(1000) <= 0.97


def test_an_earnings_reaction_clears_the_old_cut_at_the_same_move_and_stops_saturating() -> None:
    assert abs(reaction_score(0.08) - 0.8) < 1e-3 and abs(reaction_score(-0.08) - 0.8) < 1e-3
    assert reaction_score(0.0) == 0.0
    moves = [0.02, 0.05, 0.08, 0.10, 0.16, 0.24, 0.30]
    scores = [reaction_score(m) for m in moves]
    assert scores == sorted(scores) and len(set(scores)) == len(moves)       # strictly rising, no pile-up at 1.0
    assert reaction_score(0.10) < 1.0 and reaction_score(0.24) < 0.999 and reaction_score(0.40) <= 1.0
    # the cut: the same set of reactions is at or above 0.8 (|move| of 8% or more), except the 8% to 10% band that used to be 0.8 to 1.0
    assert all((reaction_score(m) >= 0.8) == (m >= 0.08 - 1e-9) for m in moves)
