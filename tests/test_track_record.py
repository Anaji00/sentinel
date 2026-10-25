"""Ideas and board snapshots are graded net of SPY, from the price they were given at."""

import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import track_record as tr  # noqa: E402

DAYS = [date(2026, 10, 1) + timedelta(days=k) for k in range(10)]


def test_forward_close_counts_sessions_after_the_day() -> None:
    closes = [100.0 + k for k in range(10)]
    assert tr.forward_close(DAYS, closes, DAYS[2], 1) == (DAYS[3], 103.0)
    assert tr.forward_close(DAYS, closes, DAYS[2], 5) == (DAYS[7], 107.0)
    assert tr.forward_close(DAYS, closes, DAYS[8], 5) is None          # not closed yet


def test_grades_are_signed_and_net_of_the_market() -> None:
    long = tr.grade(1, 100.0, 110.0, 400.0, 404.0)
    assert abs(long["excess"] - 0.09) < 1e-12 and long["hit"] == 1.0
    short = tr.grade(-1, 100.0, 110.0, 400.0, 404.0)
    assert abs(short["signed_excess"] + 0.09) < 1e-12 and short["hit"] == 0.0
    assert tr.grade(1, 0.0, 110.0, None, None)["excess"] is None


def test_summary_and_buckets() -> None:
    s = tr.summarize([0.01, 0.02, -0.005, None])
    assert s["n"] == 3 and s["hit_rate"] == round(2 / 3, 3) and s["mean_bps"] == round(0.025 / 3 * 1e4, 1)
    assert [tr.bucket_of(x) for x in (-4, -1, 0, 2, 6)] == [
        "strong bear (net <= -3)", "bear (-2..-1)", "none (0)", "bull (1..2)", "strong bull (net >= 3)"]


def test_the_grader_grades_once_the_close_exists() -> None:
    import asyncio
    from services.agents.grader import Grader

    closes = {"AAA": [100.0 + k for k in range(10)], "SPY": [400.0] * 10}
    updates = []

    class DB:
        async def query(self, sql, *a):
            if "FROM recommendation_log" in sql and "created_at" in sql:
                return []                                               # the paper ledger's reader
            if "FROM recommendation_log" in sql:
                return [{"day": DAYS[2], "horizon": "weekly", "ticker": "AAA", "direction": 1,
                         "entry_price": 102.0, "spy_entry_price": None},
                        {"day": DAYS[8], "horizon": "weekly", "ticker": "AAA", "direction": 1,
                         "entry_price": 108.0, "spy_entry_price": None}]
            if "FROM signal_matrix_log" in sql or "FROM paper_trades" in sql:
                return []
            return [{"ticker": t, "day": d, "close": c} for t, cs in closes.items() for d, c in zip(DAYS, cs)]

        async def execute(self, sql, *a):
            updates.append(a)

    out = asyncio.run(Grader(DB()).run_once())
    assert out["ideas"] == 1                                            # the later idea has not closed
    day, horizon, ticker, direction, exit_day, exit_price, ret, spy_ret, excess, signed, hit = updates[0]
    assert exit_day == DAYS[7] and exit_price == 107.0 and abs(excess - (107 / 102 - 1)) < 1e-12 and hit is True


def test_rows_on_one_day_count_as_one_observation() -> None:
    from shared.utils.track_record import day_clustered_t

    days = [date(2026, 10, 1) + timedelta(days=k) for k in range(10)]
    # One shared move a day, copied onto 100 stocks: row t is huge, day t is not.
    moves = [0.01, -0.008, 0.012, -0.011, 0.009, -0.007, 0.010, -0.009, 0.011, -0.006]
    rows = [(d, x) for d, x in zip(days, moves) for _ in range(100)]
    one = day_clustered_t(rows)
    assert one["days"] == 10
    assert abs(one["t_days"]) < 1.0
    assert day_clustered_t(rows[:200])["t_days"] is None          # two days: too few


def test_overlapping_windows_widen_the_error() -> None:
    from shared.utils.track_record import day_clustered_t

    days = [date(2026, 10, 1) + timedelta(days=k) for k in range(40)]
    # A five-day return logged daily: neighbours share most of their days.
    daily = [0.004 if (k // 5) % 2 == 0 else -0.001 for k in range(44)]
    rows = [(d, sum(daily[k:k + 5])) for k, d in enumerate(days)]
    plain, nw = day_clustered_t(rows, 1), day_clustered_t(rows, 5)
    assert nw["t_days"] is not None and plain["t_days"] is not None
    assert abs(nw["t_days"]) < abs(plain["t_days"])
    assert day_clustered_t(rows[:19], 5)["t_days"] is None        # under four windows of days
