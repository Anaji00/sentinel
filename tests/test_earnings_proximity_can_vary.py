"""A detector whose output never varies ranks nothing -- twice over.

`_pre_announcement_score` exists because an earlier pass found that upcoming
earnings carried a flat 0.3: "all 183 upcoming-earnings events in a 45-minute
window carry one score... a detector whose output never varies ranks nothing."
It was replaced with proximity plus issuer volatility.

Measured on the running deployment 2026-09-21, 36 hours of events:

    report_date | n  | distinct_scores | score
    2026-09-28  | 50 |               1 |   0.2

Fifty events, one date, one score -- and 0.20 is exactly
PRE_ANNOUNCEMENT_FLOOR, so both varying terms were zero on every one.

The cause is a boundary. The collector emits a report when it first enters the
lookahead window, which is the day `days_out == EARNINGS_LOOKAHEAD_DAYS`, and
`days_out / EARNINGS_LOOKAHEAD_DAYS` is exactly 1.0 there -- so proximity was
0.0 for every event the collector will ever emit. The replacement produced a
flat 0.20 for the same reason the original produced a flat 0.3.

Dividing by the window plus one keeps the ranking monotone and leaves 0.0 to
mean "outside the window", which is the only case that should contribute
nothing.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")

LOOKAHEAD = 7
FLOOR = 0.20
PROXIMITY_WEIGHT = 0.25


def _proximity(days_out: int, lookahead: int = LOOKAHEAD) -> float:
    """The shipped formula, isolated."""
    if 0 <= days_out <= lookahead:
        return 1.0 - (days_out / float(lookahead + 1))
    return 0.0


# ── the boundary that made it constant ───────────────────────────────────────


def test_the_far_edge_of_the_window_is_not_zero():
    """Where every emitted event actually lands."""
    assert _proximity(LOOKAHEAD) > 0.0


def test_the_score_at_the_far_edge_is_above_the_floor():
    assert FLOOR + PROXIMITY_WEIGHT * _proximity(LOOKAHEAD) > FLOOR


def test_outside_the_window_still_contributes_nothing():
    """0.0 must keep meaning one thing, and this is the thing."""
    assert _proximity(LOOKAHEAD + 1) == 0.0
    assert _proximity(-1) == 0.0


def test_today_is_the_maximum():
    assert _proximity(0) == 1.0


def test_proximity_falls_monotonically_across_the_window():
    values = [_proximity(d) for d in range(0, LOOKAHEAD + 1)]
    assert values == sorted(values, reverse=True)
    assert len(set(values)) == len(values), "a rank needs distinct values"


def test_every_day_in_the_window_scores_differently():
    """The property the original flat 0.3 lacked, and the 0.20 rewrite too."""
    scores = {round(FLOOR + PROXIMITY_WEIGHT * _proximity(d), 6)
              for d in range(0, LOOKAHEAD + 1)}
    assert len(scores) == LOOKAHEAD + 1


# ── the shipped code matches ─────────────────────────────────────────────────


def test_the_divisor_is_the_window_plus_one():
    assert "float(EARNINGS_LOOKAHEAD_DAYS + 1)" in SRC


def test_the_old_divisor_is_gone():
    code = "\n".join(
        line for line in SRC.splitlines() if not line.lstrip().startswith("#")
    )
    assert "max(1.0, float(EARNINGS_LOOKAHEAD_DAYS))" not in code


def test_the_floor_is_still_the_answer_for_an_unparseable_date():
    """A missing report_date must not invent proximity."""
    assert _proximity(0, lookahead=LOOKAHEAD) == 1.0  # sanity
    block = SRC[SRC.index("async def _pre_announcement_score"):]
    block = block[: block.index("\n    async def ", 10)]
    assert "except (ValueError, TypeError):" in block
    assert "proximity = 0.0" in block
