from datetime import datetime, timezone

from shared.utils.market_quotes import build_quote, clean_list
from shared.utils.market_session import Session

DAILY = [("2026-11-09", 100.0), ("2026-11-10", 110.0), ("2026-11-11", 120.0)]


def test_live_quote_measures_from_last_completed_close_and_flags_delay():
    now = datetime(2026, 11, 12, 16, 0, tzinfo=timezone.utc)          # 11:00 ET, regular session
    fresh = build_quote("AAA", (datetime(2026, 11, 12, 15, 59, tzinfo=timezone.utc), 126.0), DAILY, now)
    assert fresh["session"] == "regular" and fresh["delayed"] is False
    assert fresh["prev_close"] == 120.0 and fresh["change_base_date"] == "2026-11-11" and fresh["change_pct"] == 5.0
    stale = build_quote("AAA", (datetime(2026, 11, 12, 15, 30, tzinfo=timezone.utc), 126.0), DAILY, now)
    assert stale["delayed"] is True and stale["age_sec"] == 1800


def test_regular_session_ignores_todays_forming_daily_bar():
    now = datetime(2026, 11, 12, 16, 0, tzinfo=timezone.utc)
    q = build_quote("AAA", (now, 132.0), DAILY + [("2026-11-12", 131.0)], now)
    assert q["prev_close"] == 120.0


def test_no_bar_falls_back_to_daily_close_and_missing_is_null_not_zero():
    now = datetime(2026, 11, 14, 15, 0, tzinfo=timezone.utc)           # Saturday
    q = build_quote("AAA", None, DAILY, now)
    assert q["source"] == "daily_close" and q["price"] == 120.0 and q["delayed"] is False
    empty = build_quote("ZZZ", None, [], now)
    assert empty["price"] is None and empty["as_of"] is None and empty["change_pct"] is None
    assert empty["age_sec"] is None and empty["delayed"] is None and empty["prev_close"] is None


def test_session_can_be_forced():
    now = datetime(2026, 11, 12, 16, 0, tzinfo=timezone.utc)
    q = build_quote("AAA", None, DAILY, now, session=Session.CLOSED)
    assert q["session"] == "closed"


def test_clean_list_dedupes_uppercases_and_drops_junk():
    assert clean_list("aapl, msft;AAPL, ,bad sym,ES=F,^VIX") == ["AAPL", "MSFT", "ES=F", "^VIX"]
    assert len(clean_list(",".join(f"T{i}" for i in range(80)))) == 50
