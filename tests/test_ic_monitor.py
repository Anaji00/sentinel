"""Live model health: each night's scores graded once their horizon has passed."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import ic_monitor as icm  # noqa: E402


def test_spearman_handles_ties_and_order() -> None:
    assert icm.spearman([1, 2, 3, 4], [10, 20, 30, 40]) == 1.0
    assert icm.spearman([1, 2, 3, 4], [4, 3, 2, 1]) == -1.0
    assert abs(icm.spearman([1, 1, 2, 3], [1, 2, 3, 4]) - 0.9487) < 1e-3
    assert icm.spearman([1, 2], [1, 2]) is None


def test_a_night_is_graded_against_spy_and_missing_prices_are_left_out() -> None:
    names = [f"T{i}" for i in range(60)]
    pct = {t: i / 60 for i, t in enumerate(names)}
    start = {t: 100.0 for t in names}
    end = {t: 100.0 * (1 + 0.001 * i) for i, t in enumerate(names)}     # better rank, better return
    end["T0"] = None
    g = icm.grade(pct, start, end, spy_start=100.0, spy_end=101.0)
    assert g["n"] == 59 and g["ic"] == 1.0
    assert icm.grade(pct, start, end, 0, 1)["ic"] is None
    assert icm.grade({"A": 0.5}, {"A": 1}, {"A": 2}, 1, 1) == {"ic": None, "n": 1}


def test_health_says_warming_ok_weak_or_broken() -> None:
    assert icm.health([0.05, 0.04], 0.06, 1)["status"] == "warming_up"
    assert icm.health([0.06, 0.07, 0.05, 0.08, 0.06], 0.06, 1)["status"] == "ok"
    assert icm.health([0.01, 0.02, 0.0, 0.01, 0.02], 0.06, 1)["status"] == "weak"
    assert icm.health([-0.05, -0.06, -0.04, -0.07, -0.05], 0.06, 1)["status"] == "broken"


def test_overlapping_weekly_grades_are_thinned_before_the_t_test() -> None:
    h = icm.health([0.03] * 9 + [0.05] * 6, 0.03, 5)
    assert h["independent"] == 3 and h["periods"] == 15
