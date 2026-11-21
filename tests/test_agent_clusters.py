import pathlib
from datetime import date, timedelta

from shared.utils.agent_clusters import MIN_DATES, grade_agents, signed_excess, verdict

D0 = date(2026, 10, 1)
ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_sign_follows_the_direction_called():
    assert signed_excess("up", 0.02) == 0.02
    assert signed_excess("down", 0.02) == -0.02
    assert signed_excess("flat", 0.02) is None
    assert signed_excess("up", None) is None


def test_one_busy_date_counts_once():
    calls = [("a", 24, D0, -0.05)] * 200 + [("a", 24, D0 + timedelta(days=i), 0.01) for i in range(1, 6)]
    row = [r for r in grade_agents(calls) if r["horizon_h"] is None][0]
    assert row["dates"] == 6 and row["calls"] == 205
    assert row["mean_signed_bps"] == round(1e4 * (-0.05 + 5 * 0.01) / 6, 1)
    assert row["top3_date_share"] > 0.95


def test_too_few_dates_is_not_judged_not_no_skill():
    calls = [("a", 24, D0 + timedelta(days=i), 0.01 * (i + 1)) for i in range(8)]
    assert grade_agents(calls)[0]["verdict"] == "not judged: too few dates"


def test_enough_dates_can_be_judged():
    calls = [("a", 24, D0 + timedelta(days=i), 0.01 + 0.001 * (i % 3)) for i in range(MIN_DATES)]
    assert grade_agents(calls)[0]["verdict"] == "beats the market on the date-clustered test"
    assert verdict(-3.0, -0.01, MIN_DATES) == "trails the market on the date-clustered test"
    assert verdict(0.2, 0.001, MIN_DATES) == "no difference from the market"
    assert verdict(2.03, 0.008, 24) == "no difference from the market"      # the macro engine: best of eight, under the multiple-test bar


def test_calls_without_an_excess_return_are_counted_not_graded():
    calls = [("a", 24, D0 + timedelta(days=i), 0.01) for i in range(4)] + [("a", 24, D0, None)] * 3
    row = [r for r in grade_agents(calls) if r["horizon_h"] is None][0]
    assert row["calls"] == 4 and row["skipped_no_excess"] == 3


def test_fewer_than_three_dates_is_not_shown():
    assert grade_agents([("a", 24, D0, 0.01), ("a", 24, D0 + timedelta(days=1), 0.02)]) == []


def test_the_daily_job_stores_the_agent_table_under_the_same_key():
    src = (ROOT / "services" / "agents" / "grades_v2_job.py").read_text(encoding="utf-8")
    assert '"agents": {' in src and 'ac.grade_agents, calls("excess_beta")' in src
