from datetime import datetime, timedelta, timezone

from shared.utils.filing_time import occurred_at

NOW = datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)


def test_the_acceptance_time_is_used_when_it_is_usable():
    ts, used = occurred_at("2026-11-13T14:05:12.000Z", NOW)
    assert used is True and ts == datetime(2026, 11, 13, 14, 5, 12, tzinfo=timezone.utc)
    ts, used = occurred_at("2026-11-13T09:05:12-05:00", NOW)
    assert used is True and ts == datetime(2026, 11, 13, 14, 5, 12, tzinfo=timezone.utc)


def test_missing_garbage_future_and_ancient_values_fall_back_to_collection_time():
    for raw in (None, "", "yesterday", "2026-11-14T00:00:00Z", (NOW - timedelta(days=30)).isoformat()):
        assert occurred_at(raw, NOW) == (NOW, False)
