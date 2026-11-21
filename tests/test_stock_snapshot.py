from datetime import date

from shared.utils.stock_snapshot import as_day, metrics, next_earnings, pe_trailing, range_52w, snapshot, ttm_net_income

D = date
ANNUAL = (D(2025, 9, 28), D(2026, 9, 26), 132.0)


def test_ttm_is_the_latest_annual_when_that_is_newest():
    assert ttm_net_income([(D(2024, 9, 29), D(2025, 9, 27), 112.0), ANNUAL]) == (132.0, D(2026, 9, 26))


def test_ttm_rolls_forward_from_year_to_date():
    facts = [(D(2024, 9, 29), D(2025, 9, 27), 100.0), (D(2024, 9, 29), D(2025, 6, 28), 70.0), (D(2025, 9, 28), D(2026, 6, 27), 90.0)]
    assert ttm_net_income(facts) == (100.0 + 90.0 - 70.0, D(2026, 6, 27))


def test_ttm_needs_the_prior_year_stretch_else_none():
    facts = [(D(2024, 9, 29), D(2025, 9, 27), 100.0), (D(2025, 9, 28), D(2026, 6, 27), 90.0)]
    assert ttm_net_income(facts) is None and ttm_net_income([]) is None


def test_pe_is_null_for_losses_or_unknown_cap():
    t = (50.0, D(2026, 9, 26))
    assert pe_trailing(1000.0, t)["value"] == 20.0 and pe_trailing(1000.0, t)["as_of"] == "2026-09-26"
    assert pe_trailing(1000.0, (-5.0, D(2026, 9, 26))) is None and pe_trailing(None, t) is None and pe_trailing(1000.0, None) is None


def test_range_uses_lows_and_highs_and_dates_the_window():
    r = range_52w([(D(2026, 1, 5), 90.0, 100.0), (D(2026, 3, 1), 80.0, 120.0), (D(2026, 5, 1), None, None)])
    assert (r["low"], r["high"], r["from_date"]) == (80.0, 120.0, "2026-01-05") and range_52w([]) is None


def test_next_earnings_prefers_a_listed_date_else_flags_an_estimate():
    today = D(2026, 11, 12)
    assert next_earnings(D(2026, 11, 20), D(2026, 8, 1), today) == {"date": "2026-11-20", "estimated": False}
    e = next_earnings(None, D(2026, 8, 1), today)
    assert e["estimated"] is True and e["date"] >= "2026-11-12"
    assert next_earnings(None, None, today) is None


def test_snapshot_nulls_when_unknown():
    s = snapshot("ZZZ", None, "x", None, None, None)
    assert s["market_cap"] is None and s["market_cap_as_of"] is None and s["range_52w"] is None and s["pe_trailing"] is None


def test_as_day_reads_dates_datetimes_and_iso_strings():
    assert as_day("2026-09-02T23:13:53.890747+00:00") == D(2026, 9, 2) and as_day(D(2026, 9, 2)) == D(2026, 9, 2)
    assert as_day(None) is None and as_day("nope") is None


def _annual(end_year, value):
    return (D(end_year - 1, 9, 28), D(end_year, 9, 26), value)


def test_metrics_computes_growth_margins_and_valuation_ratios():
    by = {"Revenues": [_annual(2025, 400.0), _annual(2026, 500.0)],
          "NetIncomeLoss": [_annual(2025, 80.0), _annual(2026, 100.0)],
          "OperatingIncomeLoss": [_annual(2026, 150.0)],
          "GrossProfit": [_annual(2026, 250.0)],
          "NetCashProvidedByUsedInOperatingActivities": [_annual(2026, 125.0)]}
    m = metrics(2000.0, by, 400.0)
    assert m["revenue"] == 500.0 and m["revenue_growth_pct"] == 25.0
    assert m["net_margin_pct"] == 20.0 and m["operating_margin_pct"] == 30.0 and m["gross_margin_pct"] == 50.0
    assert m["cash_from_operations_margin_pct"] == 25.0 and m["return_on_equity_pct"] == 25.0
    assert m["price_to_sales"] == 4.0 and m["price_to_book"] == 5.0 and m["as_of"] == "2026-09-26"


def test_metrics_nulls_what_it_cannot_know_and_never_divides_by_a_loss():
    by = {"Revenues": [_annual(2026, 500.0)], "NetIncomeLoss": [_annual(2026, -20.0)]}
    m = metrics(None, by, -50.0)
    assert m["revenue_growth_pct"] is None and m["gross_margin_pct"] is None and m["price_to_sales"] is None
    assert m["net_margin_pct"] == -4.0 and m["return_on_equity_pct"] is None and m["price_to_book"] is None
    assert metrics(1000.0, {}, 10.0) is None


def test_metrics_takes_the_revenue_tag_filed_most_recently():
    by = {"SalesRevenueNet": [_annual(2018, 300.0)], "RevenueFromContractWithCustomerExcludingAssessedTax": [_annual(2026, 500.0)],
          "NetIncomeLoss": [_annual(2026, 50.0)]}
    assert metrics(None, by, None)["revenue"] == 500.0
