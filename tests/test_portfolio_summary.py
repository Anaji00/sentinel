from datetime import date, datetime, timezone

from shared.utils.portfolio_summary import day_change, summary

ROWS = [(date(2026, 11, 9), 99000.0), (date(2026, 11, 10), 100000.0), (date(2026, 11, 12), 101000.0)]


def test_change_is_against_newest_row_before_today():
    ch, pct, base = day_change(101500.0, ROWS, date(2026, 11, 12))
    assert (ch, pct, base) == (1500.0, 1.5, date(2026, 11, 10))


def test_no_prior_row_or_no_equity_is_null_not_zero():
    assert day_change(101500.0, [], date(2026, 11, 12)) == (None, None, None)
    assert day_change(101500.0, [(date(2026, 11, 12), 1.0)], date(2026, 11, 12)) == (None, None, None)
    assert day_change(None, ROWS, date(2026, 11, 12)) == (None, None, None)


def test_summary_shape():
    now = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
    s = summary({"portfolio_value": 101500.0, "positions_count": 20}, ROWS, date(2026, 11, 12), now)
    assert set(s) == {"equity", "day_change", "day_change_pct", "base_date", "positions_count", "as_of"}
    assert s["equity"] == 101500.0 and s["positions_count"] == 20 and s["base_date"] == "2026-11-10"
    assert summary({}, ROWS, date(2026, 11, 12), now)["equity"] is None


def test_an_unconfigured_account_reports_no_equity_not_zero():
    now = datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc)
    s = summary({"portfolio_value": 0.0, "positions_count": 0}, ROWS, date(2026, 11, 12), now)
    assert s["equity"] is None and s["day_change"] is None and s["day_change_pct"] is None
