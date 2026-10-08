"""A sweep is dated by its fill, and a stale last-print is not a sweep."""

import importlib.util
import pathlib
from datetime import datetime, timezone

TRADFI = pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-tradfi" / "main.py"


def _module():
    spec = importlib.util.spec_from_file_location("tradfi_main_for_fill_time", TRADFI)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except SystemExit:
        pass
    return module


def test_alpaca_nanosecond_timestamps_parse() -> None:
    m = _module()
    ts = m._option_trade_time({"t": "2026-10-05T14:31:07.123456789Z"})
    assert ts == datetime(2026, 10, 5, 14, 31, 7, 123456, tzinfo=timezone.utc)


def test_missing_or_bad_timestamps_are_none() -> None:
    m = _module()
    assert m._option_trade_time({}) is None
    assert m._option_trade_time({"t": "yesterday"}) is None


def test_the_poller_dates_events_by_the_fill_and_skips_stale_prints() -> None:
    src = TRADFI.read_text(encoding="utf-8")
    assert "occurred_at=trade_at or datetime.now(timezone.utc)" in src
    assert "> OPTION_TRADE_MAX_AGE_SEC" in src


def test_the_age_limit_outlasts_the_feeds_own_delay() -> None:
    """The indicative feed is 15 minutes late; a limit at or below that rejects
    every fill, which is how options_flow fell to zero for two sessions."""
    m = _module()
    assert m.OPTION_TRADE_MAX_AGE_SEC > m.OPTION_FEED_DELAY_SEC + 5 * 60
