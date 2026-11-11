"""Commodity futures as a page reads them."""
from datetime import date, timedelta

from shared.utils.commodity_board import build_commodity_board

END = date(2026, 11, 5)


def _series(key, days, start=100.0, step=1.0):
    return [{"series": key, "day": END - timedelta(days=days - 1 - i), "value": start + step * i} for i in range(days)]


def test_changes_are_measured_back_from_the_last_price_and_absent_where_history_is_short():
    out = build_commodity_board(_series("GC=F", 400), END)
    gold = next(i for c in out["categories"] for i in c["items"] if i["key"] == "GC=F")
    assert gold["latest"] == 499.0 and gold["change_day"] == round((499 / 498 - 1) * 100, 2)
    assert gold["change_week"] == round((499 / 492 - 1) * 100, 2)
    assert gold["change_year"] is not None
    short = build_commodity_board(_series("CL=F", 10), END)
    oil = short["categories"][0]["items"][0]
    assert oil["change_week"] is not None and oil["change_month"] is None and oil["change_year"] is None


def test_series_are_grouped_into_categories_in_a_fixed_order_with_names():
    rows = _series("ZC=F", 40) + _series("CL=F", 40) + _series("GC=F", 40) + _series("NOPE=F", 40)
    out = build_commodity_board(rows, END)
    assert [c["key"] for c in out["categories"]] == ["energy", "metals", "farm"]
    assert out["categories"][0]["items"][0]["name"] == "WTI crude oil"
    assert all(i["key"] != "NOPE=F" for c in out["categories"] for i in c["items"])


def test_each_commodity_says_which_stocks_it_helps_and_hurts():
    out = build_commodity_board(_series("CL=F", 40), END)
    oil = out["categories"][0]["items"][0]
    assert "XOM" in oil["gain_when_up"] and "DAL" in oil["pay_when_up"]


def test_a_series_that_stopped_updating_is_flagged_and_old_data_is_stale():
    rows = _series("GC=F", 40) + [{"series": "SI=F", "day": END - timedelta(days=30), "value": 30.0}, {"series": "SI=F", "day": END - timedelta(days=31), "value": 29.0}]
    out = build_commodity_board(rows, END)
    silver = next(i for c in out["categories"] for i in c["items"] if i["key"] == "SI=F")
    assert silver["stale"] is True
    assert out["stale"] is False
    assert build_commodity_board(rows, END + timedelta(days=10))["stale"] is True


def test_nothing_stored_is_an_empty_answer():
    out = build_commodity_board([], END)
    assert out["categories"] == [] and out["as_of"] is None and out["stale"] is False
