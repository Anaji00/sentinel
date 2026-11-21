from datetime import datetime, timedelta, timezone

from shared.utils.candle_range import plan, split

NOW = datetime(2026, 11, 12, 21, 0, tzinfo=timezone.utc)


def bars(n, step, end=NOW):
    return [{"ts": (end - step * (n - 1 - i)).isoformat(), "close": float(i)} for i in range(n)]


def test_plan_picks_bar_size_and_caps_warmup():
    assert plan("1D", 200)["timeframe"] == "5m" and plan("3M", 0)["timeframe"] == "1h"
    assert plan("5Y", 10_000)["warmup"] == 500 and plan("1Y", -3)["warmup"] == 0


def test_one_day_is_the_latest_session_with_warmup_before_it():
    rows = bars(400, timedelta(minutes=5))
    r = split(rows, "1D", 50, NOW)
    flags = [c["warmup"] for c in r["candles"]]
    assert r["warmup_count"] == 50 and flags[:50] == [True] * 50 and not any(flags[50:])
    assert r["candles"][0]["ts"] < r["candles"][-1]["ts"]


def test_short_history_is_flagged_not_hidden():
    rows = bars(60, timedelta(days=1))                    # about two months of daily bars
    r = split(rows, "1Y", 0, NOW)
    assert r["history_limited"] is True and r["warmup_count"] == 0 and len(r["candles"]) == 60


def test_full_history_is_not_flagged_and_unsorted_input_is_ordered():
    rows = list(reversed(bars(400, timedelta(days=1))))
    r = split(rows, "1Y", 20, NOW)
    assert r["history_limited"] is False and r["warmup_count"] == 20
    assert [c["ts"] for c in r["candles"]] == sorted(c["ts"] for c in r["candles"])


def test_empty_is_null():
    r = split([], "1M", 10, NOW)
    assert r["candles"] == [] and r["window_start"] is None and r["history_limited"] is None


def test_zero_warmup_returns_no_warmup_bars():
    r = split(bars(400, timedelta(days=1)), "1M", 0, NOW)
    assert r["warmup_count"] == 0 and not any(c["warmup"] for c in r["candles"])
