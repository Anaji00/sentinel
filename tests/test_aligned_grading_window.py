"""Layer audit 5.2: session-aligned grading windows (shared/utils/excess_return.aligned_window, used by services/agents/excess_grading)."""
from datetime import date, timedelta

from shared.utils import excess_return as er

# Mon 2 Nov to Fri 20 Nov 2026, weekdays only.
SESSIONS = [date(2026, 11, 2) + timedelta(days=i) for i in range(19) if (date(2026, 11, 2) + timedelta(days=i)).weekday() < 5]


def test_a_friday_midday_24h_call_exits_on_monday_not_saturday() -> None:
    # Friday 6 Nov 15:00 UTC = 10:00 New York: entry is Thursday's close, exit one session later (Friday).
    assert er.aligned_window("2026-11-06T15:00:00+00:00", 24, SESSIONS) == (date(2026, 11, 5), date(2026, 11, 6))
    # Friday after the close (22:00 UTC = 17:00 NY): entry Friday, exit Monday: a real return, not zero.
    assert er.aligned_window("2026-11-06T22:00:00+00:00", 24, SESSIONS) == (date(2026, 11, 6), date(2026, 11, 9))


def test_a_pre_open_call_starts_from_the_last_completed_close() -> None:
    # 03:31 UTC on Tuesday 10 Nov is 22:31 New York on Monday: the Monday close has happened.
    assert er.aligned_window("2026-11-10T03:31:00+00:00", 24, SESSIONS) == (date(2026, 11, 9), date(2026, 11, 10))
    # 13:00 UTC Tuesday = 08:00 NY, before the open: Monday's close, not Tuesday's.
    assert er.aligned_window("2026-11-10T13:00:00+00:00", 24, SESSIONS)[0] == date(2026, 11, 9)


def test_weekend_calls_start_at_the_friday_close_and_multi_day_horizons_count_sessions() -> None:
    assert er.aligned_window("2026-11-08T15:00:00+00:00", 24, SESSIONS) == (date(2026, 11, 6), date(2026, 11, 9))
    assert er.aligned_window("2026-11-02T22:00:00+00:00", 72, SESSIONS) == (date(2026, 11, 2), date(2026, 11, 5))


def test_unplaceable_calls_return_none_instead_of_a_zero_window() -> None:
    assert er.aligned_window("2026-11-19T22:00:00+00:00", 72, SESSIONS) is None      # the calendar ends before the exit session
    assert er.aligned_window("2026-10-01T15:00:00+00:00", 24, SESSIONS) is None      # before the calendar
    assert er.aligned_window("2026-11-06T22:00:00+00:00", 24, []) is None


def test_window_return_over_an_aligned_window_is_not_zero_for_a_friday_call() -> None:
    closes = [100.0 + i for i in range(len(SESSIONS))]
    start, end = er.aligned_window("2026-11-06T22:00:00+00:00", 24, SESSIONS)
    assert er.window_return(SESSIONS, closes, start, end) > 0


def test_grade_pending_uses_the_aligned_window_only_when_enabled(monkeypatch) -> None:
    import asyncio
    import math

    from services.agents import excess_grading as eg

    class Db:
        def __init__(self, pending, bars):
            self.pending, self.bars, self.executed = pending, bars, []

        async def query(self, sql, *args):
            return self.pending if "agent_predictions" in sql else self.bars

        async def execute(self, sql, *args):
            self.executed.append((sql, args))

    start = date(2026, 9, 1)
    bars = []
    for i in range(12):
        d = start + timedelta(days=i)
        bars += [{"ticker": "AAA", "day": d, "close": 100.0 + 2 * i}, {"ticker": "SPY", "day": d, "close": 400.0 + i}]
    pending = [{"id": 1, "ticker": "AAA", "direction": "up", "occurred_at": "2026-09-03T12:00:00+00:00", "horizon_hours": 48, "sic": None}]

    def graded(env):
        monkeypatch.setenv("EXCESS_ALIGNED_WINDOWS", env)
        db = Db(pending, bars)
        asyncio.run(eg.grade_pending(db))
        return [a for sql, a in db.executed if "excess_graded_at = NOW()" in sql][0][1]

    assert math.isclose(graded(""), 108 / 104 - 1)         # old window: Sep 3 close to Sep 5 close
    assert math.isclose(graded("1"), 106 / 102 - 1)        # aligned: Sep 2 close (last completed at 08:00 New York) to Sep 4
