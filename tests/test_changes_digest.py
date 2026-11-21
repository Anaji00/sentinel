from datetime import datetime, timedelta, timezone

import pytest

from shared.utils.changes import SinceInvalid, failed, feed_item, merge_feed, parse_since, part, side, total_changes

NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def test_parse_since_accepts_z_and_offsets_and_clamps_old_values():
    assert parse_since("2026-11-13T09:30:00Z", NOW) == datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)
    assert parse_since("2026-11-13T04:30:00-05:00", NOW) == datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)
    assert parse_since("2026-11-13T09:30:00", NOW).tzinfo is not None
    assert parse_since("2026-10-01T00:00:00Z", NOW) == NOW - timedelta(days=7)


@pytest.mark.parametrize("bad", [None, "", "yesterday", "2026-11-14T00:00:00Z"])
def test_parse_since_refuses_missing_garbage_and_future(bad):
    with pytest.raises(SinceInvalid):
        parse_since(bad, NOW)


def test_part_caps_items_but_keeps_the_true_count_and_an_error_is_not_zero():
    p = part([{"i": n} for n in range(50)], total=50)
    assert p["count"] == 50 and len(p["items"]) == 20 and p["status"] == "ok"
    e = failed("could not read")
    assert e["status"] == "error" and e["count"] is None and e["items"] == []


def test_total_ignores_failed_sources_and_is_none_when_all_failed():
    assert total_changes({"a": part([{}, {}]), "b": failed("x"), "c": part([], total=3)}) == 5
    assert total_changes({"a": failed("x")}) is None


def test_side_maps_direction_numbers():
    assert side(1) == "long" and side(-1) == "short" and side(0) is None and side("x") is None


def test_the_feed_is_newest_first_with_stable_unique_ids_and_undated_last():
    a = feed_item("filing", "e1", "NVDA", "2026-11-12T17:59:00+00:00", "8-K")
    b = feed_item("divergence", "c9", "TSLA", "2026-11-12T18:11:57+00:00", "Moving against peers")
    c = feed_item("call", "2026-11-12:monthly:CRCL:1", "CRCL", "2026-11-12T00:00:00+00:00", "Long idea", precision="day")
    d = feed_item("price_alert", 7, "AAPL", None, "Above 400")
    out = merge_feed([a, b, c, d, dict(a)], 10)
    assert [i["id"] for i in out] == ["divergence:c9", "filing:e1", "call:2026-11-12:monthly:CRCL:1", "price_alert:7"]
    assert merge_feed([a, b, c, d], 2) == out[:2] and out[2]["at_precision"] == "day"
    assert [i["id"] for i in merge_feed([c, b, a, d], 10)] == [i["id"] for i in out]            # the order does not depend on arrival
