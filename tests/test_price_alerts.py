from datetime import datetime, timezone

import pytest

from shared.utils.price_alerts import AlertInvalid, as_dt, crossed, evaluate, validate_new

T = datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)


def test_validate_rejects_bad_input_and_trims_note():
    assert validate_new("NVDA", "above", "170.5", "  watch  ") == {"ticker": "NVDA", "direction": "above", "level": 170.5, "note": "watch"}
    assert validate_new("NVDA", "below", 1, "")["note"] is None
    for args in [(None, "above", 1, None), ("X", "up", 1, None), ("X", "above", "abc", None), ("X", "above", 0, None),
                 ("X", "above", -3, None), ("X", "above", float("inf"), None), ("X", "above", float("nan"), None)]:
        with pytest.raises(AlertInvalid):
            validate_new(*args)
    assert len(validate_new("X", "above", 1, "n" * 500)["note"]) == 140


def test_crossed_uses_high_for_above_and_low_for_below():
    assert crossed("above", 100, 100.0, 90) and not crossed("above", 100, 99.9, 90)
    assert crossed("below", 100, 120, 100.0) and not crossed("below", 100, 120, 100.1)
    assert not crossed("above", 100, None, None) and not crossed("below", 100, None, None)


def test_evaluate_fires_between_polls_and_skips_tickers_without_bars():
    alerts = [{"id": 1, "ticker": "A", "direction": "above", "level": 105.0}, {"id": 2, "ticker": "A", "direction": "below", "level": 95.0},
              {"id": 3, "ticker": "B", "direction": "above", "level": 10.0}]
    fired = evaluate(alerts, {"A": (106.0, 96.0, T)})
    assert fired == [{"id": 1, "price": 106.0, "at": T}]


def test_as_dt_reads_iso_strings_the_pool_returns():
    assert as_dt("2026-11-12T21:21:25.337304+00:00") == datetime(2026, 11, 12, 21, 21, 25, 337304, tzinfo=timezone.utc)
    assert as_dt(datetime(2026, 11, 12, 1, 0)).tzinfo is not None and as_dt(None) is None and as_dt("nope") is None
