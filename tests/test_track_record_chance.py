"""The track record states the hit rate beside the rate chance alone would give, on independent periods."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.track_record import IDEA_OVERLAP_DAYS, direction_skill, summarize  # noqa: E402


def spread(calls, days):
    """Calls dealt out across `days` separate days, as (direction, signed excess, day)."""
    return [(d, x, i % days) for i, (d, x) in enumerate(calls)]


def test_leaning_the_way_the_market_went_is_not_skill() -> None:
    # 100 long calls, 70 of them right: the stocks beat the market 70% of the time, and so would any long call.
    s = direction_skill(spread([(1, 0.01) if i < 70 else (1, -0.01) for i in range(100)], 50))
    assert s["n_called"] == 100 and s["hit_called"] == 0.7 and s["chance_hit"] == 0.7
    assert s["heidke"] == 0.0 and s["skill_verdict"] == "none"


def test_picking_the_winners_scores_above_chance() -> None:
    calls = [(1, 0.01)] * 50 + [(-1, 0.01)] * 50          # longs that beat the market, shorts that trailed it
    s = direction_skill(spread(calls, 60))
    assert s["hit_called"] == 1.0 and s["heidke"] == 1.0 and s["skill_verdict"] == "skill"


def test_many_rows_on_few_days_are_too_few() -> None:
    """339 weekly ideas on 18 days read worse than chance at z -5.5 by rows; there were about four independent weeks."""
    calls = [(1, 0.01)] * 60 + [(1, -0.01)] * 279            # 18% hit rate on longs, but only 18 days
    row = direction_skill(spread(calls, 18), overlap=IDEA_OVERLAP_DAYS["weekly"])
    assert row["n_called"] == 339 and row["n_days"] == 18 and row["n_independent"] == 3.6
    assert row["skill_verdict"] == "too few" and abs(row["heidke_z"]) < 2
    assert row["hit_called"] == 0.177 and row["chance_hit"] == 0.177            # all long: chance is the hit rate


def test_a_short_record_says_too_few_and_keeps_the_numbers() -> None:
    s = direction_skill(spread([(1, 0.01)] * 5 + [(-1, 0.01)] * 5, 10))
    assert s["skill_verdict"] == "too few" and s["n_called"] == 10 and s["chance_hit"] == 0.5


def test_ties_and_missing_rows_are_left_out_and_nothing_raises() -> None:
    s = direction_skill([(1, 0.0, 1), (0, 0.01, 2), (None, 0.01, 3), (1, None, 4), (1, float("nan"), 5), (-1, 0.02, 6)])
    assert s["n_called"] == 1 and direction_skill([])["n_called"] == 0 and direction_skill([])["chance_hit"] is None
    assert direction_skill([(1, 0.01), (-1, 0.01)])["n_called"] == 2          # rows with no day still count


def test_it_sits_beside_the_existing_hit_rate_and_the_route_uses_it() -> None:
    assert "hit_rate" in summarize([0.01, -0.01]) and "chance_hit" not in summarize([0.01])
    src = (ROOT / "services/api_gateway/routes/predictions.py").read_text(encoding="utf-8")
    assert "direction_skill(called.get(h, []), IDEA_OVERLAP_DAYS.get(h, 5))" in src
    assert "direction" in src.split("_GRADED_IDEAS_SQL")[1][:200]
