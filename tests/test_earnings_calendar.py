from datetime import date

from shared.utils.earnings_calendar import build_rows, window

START, END = date(2026, 11, 13), date(2026, 11, 20)


def row(t, d, hour="amc", eps=1.0):
    return {"ticker": t, "report_date": d, "hour": hour, "eps_estimate": eps, "revenue_estimate": None}


def test_window_is_clamped():
    assert window(START, 7) == (START, date(2026, 11, 20)) and window(START, 0)[1] == date(2026, 11, 14)
    assert window(START, 999)[1] == date(2026, 12, 13)


def test_rows_sort_by_date_then_size_and_keep_the_earlier_listing():
    raw = [row("SMALL", date(2026, 11, 17)), row("BIG", date(2026, 11, 17)), row("EARLY", date(2026, 11, 14)),
           row("BIG", date(2026, 11, 19)), row("OUT", date(2026, 11, 25))]
    out = build_rows(raw, START, END, None, {"BIG": 2e12, "SMALL": 5e9, "EARLY": 1e9}, None, 50)
    assert [r["ticker"] for r in out] == ["EARLY", "BIG", "SMALL"] and out[1]["date"] == "2026-11-17"


def test_universe_and_min_cap_filters_exclude_unknown_caps():
    raw = [row("A", date(2026, 11, 16)), row("B", date(2026, 11, 16)), row("C", date(2026, 11, 16))]
    caps = {"A": 5e9, "B": 5e8}
    assert [r["ticker"] for r in build_rows(raw, START, END, {"A", "B"}, caps, None, 50)] == ["A", "B"]
    assert [r["ticker"] for r in build_rows(raw, START, END, None, caps, 1e9, 50)] == ["A"]


def test_missing_numbers_are_null_and_hours_are_normalised():
    r = build_rows([{"ticker": "x", "report_date": "2026-11-16", "hour": "DMH", "eps_estimate": "nan", "revenue_estimate": None}],
                   START, END, None, {}, None, 5)[0]
    assert r["hour"] == "during_market" and r["eps_estimate"] is None and r["revenue_estimate"] is None
    assert r["market_cap"] is None and r["estimated"] is False and r["ticker"] == "X"


def test_a_zero_revenue_estimate_is_the_vendors_placeholder_not_a_forecast():
    r = build_rows([{"ticker": "A", "report_date": "2026-11-16", "revenue_estimate": 0.0, "eps_estimate": 0.0}], START, END, None, {}, None, 5)[0]
    assert r["revenue_estimate"] is None and r["eps_estimate"] == 0.0
