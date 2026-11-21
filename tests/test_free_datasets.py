"""Parsers for the three free datasets (CFTC COT, Treasury auctions, Reg SHO list) and the event-study statistics. Fixtures are trimmed copies of real responses fetched 13 Nov 2026."""
import importlib.util
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from shared.utils import auction_parse, cot_parse, event_study, reg_sho_parse

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


COT_RECORD = {
    "market_and_exchange_names": "CRUDE OIL, LIGHT SWEET-WTI - NEW YORK MERCANTILE EXCHANGE", "report_date_as_yyyy_mm_dd": "2026-11-03T00:00:00.000",
    "contract_market_name": "CRUDE OIL, LIGHT SWEET-WTI", "cftc_contract_market_code": "067411", "cftc_market_code": "NYME", "commodity_name": "PETROLEUM",
    "open_interest_all": "1908732", "prod_merc_positions_long": "639282", "prod_merc_positions_short": "269935",
    "swap_positions_long_all": "135333", "swap__positions_short_all": "615957", "swap__positions_spread_all": "132622",
    "m_money_positions_long_all": "147382", "m_money_positions_short_all": "125654", "m_money_positions_spread": "305122",
    "other_rept_positions_long": "177141", "other_rept_positions_short": "118580", "nonrept_positions_long_all": "82416",
    "nonrept_positions_short_all": "51428", "change_in_m_money_long_all": "-10051", "change_in_m_money_short_all": "-651",
}


def test_cot_row_and_release_time():
    rows = cot_parse.parse_records([COT_RECORD])
    assert len(rows) == 1 and len(rows[0]) == len(cot_parse.COLUMNS)
    r = dict(zip(cot_parse.COLUMNS, rows[0]))
    assert r["report_date"] == date(2026, 11, 3) and r["contract_code"] == "067411"
    assert r["mm_long"] == 147382 and r["mm_short"] == 125654 and r["swap_short"] == 615957 and r["chg_mm_long"] == -10051
    # Tuesday 3 Nov positions are released Friday 6 Nov at 15:30 ET (EST, UTC-5) = 20:30 UTC.
    assert r["published_at"] == datetime(2026, 11, 6, 20, 30, tzinfo=timezone.utc)
    assert r["published_basis"] == cot_parse.PUBLISHED_BASIS


def test_cot_summer_release_uses_daylight_time():
    assert cot_parse.published_at(date(2026, 7, 7)) == datetime(2026, 7, 10, 19, 30, tzinfo=timezone.utc)


def test_cot_skips_unusable_records_and_blank_numbers():
    bad = [{"report_date_as_yyyy_mm_dd": "x", "cftc_contract_market_code": "1", "open_interest_all": "5"},
           {"report_date_as_yyyy_mm_dd": "2026-11-03T00:00:00.000", "cftc_contract_market_code": "", "open_interest_all": "5"},
           {"report_date_as_yyyy_mm_dd": "2026-11-03T00:00:00.000", "cftc_contract_market_code": "1"}]
    assert cot_parse.parse_records(bad) == []
    blank = dict(COT_RECORD, m_money_positions_long_all="")
    assert dict(zip(cot_parse.COLUMNS, cot_parse.parse_records([blank])[0]))["mm_long"] is None


def test_cot_where_clause_is_half_open():
    mod = _load("cftc_cot_collector", "services/collector-macro/cftc_cot.py")
    w = mod.where_clause(date(2024, 1, 1), date(2025, 1, 1))
    assert ">= '2024-01-01T00:00:00.000'" in w and "< '2025-01-01T00:00:00.000'" in w
    assert "<" not in mod.where_clause(date(2024, 1, 1), None)


AUCTION = {
    "cusip": "91282CQQ7", "securityType": "Note", "securityTerm": "10-Year", "competitiveAuctionCloseDate": "2026-11-12T00:00:00",
    "competitiveAuctionCloseTime": "01:00 PM", "issueDate": "2026-11-16T00:00:00", "bidToCoverRatio": "2.46", "highYield": "5.020",
    "medianYield": "4.961", "offeringAmount": "42000000000", "indirectBiddersCompetitiveParAwarded": "25566955000",
    "directBiddersCompetitiveParAwarded": "12109950200", "primaryDealersCompetitiveParAwarded": "4080513500", "reopening": "No",
    "cashManagementBill": "No",
}


def test_auction_row_shares_and_release_time():
    rows = auction_parse.parse_records([AUCTION])
    r = dict(zip(auction_parse.COLUMNS, rows[0]))
    assert r["bid_to_cover"] == 2.46 and r["high_yield"] == 5.02 and r["reopening"] is False
    assert math.isclose(r["indirect_share"] + r["direct_share"] + r["dealer_share"], 1.0, abs_tol=1e-4)
    assert 0.61 < r["indirect_share"] < 0.62
    # 13:00 ET close on 12 Nov (EST) plus ten minutes = 18:10 UTC.
    assert r["published_at"] == datetime(2026, 11, 12, 18, 10, tzinfo=timezone.utc) and r["close_time"] == "13:00"


def test_bill_uses_high_rate_and_unauctioned_rows_are_skipped():
    bill = dict(AUCTION, cusip="912797WF0", securityType="Bill", securityTerm="4-Week", highYield="", highRate="3.955",
                competitiveAuctionCloseTime="11:30 AM")
    r = dict(zip(auction_parse.COLUMNS, auction_parse.parse_records([bill])[0]))
    assert r["high_yield"] == 3.955 and r["close_time"] == "11:30"
    future = dict(AUCTION, bidToCoverRatio="")
    assert auction_parse.parse_records([future, dict(AUCTION, cusip="")]) == []


def test_auction_missing_bidder_class_leaves_shares_null():
    r = dict(zip(auction_parse.COLUMNS, auction_parse.parse_records([dict(AUCTION, primaryDealersCompetitiveParAwarded="")])[0]))
    assert r["indirect_share"] is None and r["bid_to_cover"] == 2.46


def test_auction_month_windows_cover_the_range_exactly():
    mod = _load("treasury_auctions_collector", "services/collector-macro/treasury_auctions.py")
    w = mod.month_windows(date(2025, 11, 20), date(2026, 1, 5))
    assert w == [(date(2025, 11, 1), date(2025, 11, 30)), (date(2025, 12, 1), date(2025, 12, 31)), (date(2026, 1, 1), date(2026, 1, 31))]


SHO_FILE = """Symbol|Security Name|Market Category|Reg SHO Threshold Flag|Rule 3210|Filler
AAPD|DIREXION SHS ETF TR DAILY AAPL|G|Y|N|
ACTU|ACTUATE THERAPEUTICS INC COM|G|Y|N|
nope|NOT FLAGGED|S|N|N|
ZNB|ZETA NETWORK GROUP CL A ORD SH|S|Y|N|
20261112230014
"""


def test_reg_sho_file_rows_and_creation_time():
    rows = reg_sho_parse.parse_file(SHO_FILE, date(2026, 11, 12))
    assert [r[1] for r in rows] == ["AAPD", "ACTU", "ZNB"]
    assert rows[0][3] == "G" and rows[0][4] == "NASDAQ" and rows[2][3] == "S"
    assert rows[0][5] == datetime(2026, 11, 13, 4, 0, 14, tzinfo=timezone.utc)   # 23:00:14 ET (EST) on the 12th


def test_reg_sho_non_list_text_gives_nothing():
    assert reg_sho_parse.parse_file("<html>moved</html>", date(2026, 11, 12)) == []
    assert reg_sho_parse.parse_file("", date(2026, 11, 12)) == []
    assert reg_sho_parse.url_for(date(2026, 11, 12)).endswith("nasdaqth20261112.txt")


def test_reg_sho_weekdays_skip_weekends():
    mod = _load("reg_sho_collector", "services/collector-macro/reg_sho.py")
    assert mod.weekdays(date(2026, 11, 12), date(2026, 11, 16)) == [date(2026, 11, 12), date(2026, 11, 13), date(2026, 11, 16)]


def test_benjamini_hochberg_known_values():
    q = event_study.benjamini_hochberg([0.01, 0.04, 0.03, 0.20])
    assert [round(x, 4) for x in q] == [0.04, 0.0533, 0.0533, 0.2]
    assert event_study.benjamini_hochberg([]) == []


def test_forward_excess_enters_after_publication_and_needs_horizon():
    days = [date(2026, 1, 5) + timedelta(days=i) for i in range(30) if (date(2026, 1, 5) + timedelta(days=i)).weekday() < 5]
    px = (days, [100.0 * (1.01 ** i) for i in range(len(days))])
    bench = {d: 100.0 for d in days}
    r = event_study.forward_excess(px, bench, days[2])          # entry is days[3]
    assert math.isclose(r, 5 * math.log(1.01), rel_tol=1e-9)
    assert event_study.forward_excess(px, bench, days[-3]) is None
    assert event_study.forward_excess(px, {}, days[2]) is None


def test_cell_passes_only_when_every_bar_is_met():
    day0 = date(2026, 1, 5)
    # 120 events on 120 different dates, all +50 bps with a little spread: passes.
    good = [("A", day0 + timedelta(days=i), 0.005 + 0.001 * ((i % 5) - 2)) for i in range(120)]
    # 120 events, positive first half, negative second half: fails the same-sign bar.
    flip = [("B", day0 + timedelta(days=i), (0.01 if i < 60 else -0.01) + 0.001 * (i % 3)) for i in range(120)]
    # only 40 events: fails the count bar.
    few = [("C", day0 + timedelta(days=i), 0.005 + 0.001 * (i % 4)) for i in range(40)]
    out = {r["cell"]: r for r in event_study.evaluate(good + flip + few)}
    assert out["A"]["passes"] and out["A"]["n"] == 120
    assert not out["B"]["same_sign"] and not out["B"]["passes"]
    assert out["C"]["t"] > 2 and not out["C"]["passes"]


def test_clustering_counts_one_date_once():
    d = date(2026, 3, 2)
    same_day = [("X", d, 0.01 + 0.0001 * i) for i in range(200)]
    assert event_study._cell_stats([(c[1], c[2]) for c in same_day])["clusters"] == 1
    assert event_study.evaluate(same_day)[0]["t"] == 0.0     # fewer than three clusters: no statistic claimed


def test_trailing_percentile_and_z_exclude_the_current_value():
    vals = [1.0, 2.0, 3.0, 4.0, 100.0]
    assert event_study.trailing_percentile(vals, 10, 4) == [None, None, None, None, 1.0]
    z = event_study.trailing_z(vals, 10, 4)
    assert z[:4] == [None] * 4 and z[4] > 30
    assert event_study.trailing_z([5.0] * 6, 10, 4)[5] is None
