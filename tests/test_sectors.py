import pytest

from shared.utils.sectors import RULES, SECTORS, aggregate, sector_of

KNOWN = {
    "AAPL": (3571, "Information Technology"), "MSFT": (7372, "Information Technology"), "NVDA": (3674, "Information Technology"),
    "JPM": (6022, "Financials"), "BRK": (6331, "Financials"), "XOM": (2911, "Energy"), "CVX": (2911, "Energy"), "SLB": (1389, "Energy"),
    "PFE": (2834, "Health Care"), "MDT": (3845, "Health Care"), "UNH": (6324, "Financials"), "WMT": (5331, "Consumer Staples"),
    "KO": (2080, "Consumer Staples"), "COST": (5399, "Consumer Discretionary"), "HD": (5211, "Consumer Discretionary"),
    "TSLA": (3711, "Consumer Discretionary"), "MCD": (5812, "Consumer Discretionary"), "NEE": (4911, "Utilities"),
    "AMT": (6798, "Real Estate"), "T": (4813, "Communication Services"), "NFLX": (7841, "Communication Services"),
    "CAT": (3531, "Industrials"), "BA": (3721, "Industrials"), "UPS": (4210, "Industrials"), "LIN": (2810, "Materials"),
    "NEM": (1040, "Materials"), "SPAC": (6770, "Other"), "GOOGL": (7370, "Information Technology"),
}


@pytest.mark.parametrize("ticker", sorted(KNOWN))
def test_known_names_land_in_their_sector(ticker):
    sic, want = KNOWN[ticker]
    assert sector_of(sic) == want, (ticker, sic)


def test_missing_or_unknown_codes_are_other_and_every_rule_names_a_real_sector():
    assert sector_of(None) == "Other" and sector_of("abc") == "Other" and sector_of(9999) == "Other"
    assert {name for _, _, name in RULES} <= set(SECTORS)
    assert all(lo <= hi for lo, hi, _ in RULES)


def stock(t, key, ch, cap, vol=1.0):
    return {"ticker": t, "key": key, "label": key.title(), "change_pct": ch, "market_cap": cap, "volume_vs_20d": vol}


def test_group_statistics_are_cap_weighted_with_breadth_and_extremes():
    g = aggregate([stock("A", "x", 2.0, 300.0), stock("B", "x", -1.0, 100.0), stock("C", "x", 1.0, 100.0), stock("D", "x", None, 50.0)], "sector")[0]
    assert g["n"] == 4 and g["n_priced"] == 3 and g["market_cap"] == 550.0
    assert g["change_pct"] == 1.2 and g["change_pct_median"] == 1.0 and g["breadth_up_share"] == 0.667
    assert [x["ticker"] for x in g["top"]] == ["A", "C", "B"] and [x["ticker"] for x in g["bottom"]] == ["B", "C", "A"]


def test_a_group_with_too_few_priced_stocks_reports_null_not_a_guess():
    g = aggregate([stock("A", "y", 5.0, 10.0), stock("B", "y", None, 10.0)], "sector")[0]
    assert g["change_pct"] is None and g["breadth_up_share"] is None and g["top"] == [] and g["n"] == 2


def test_groups_are_ordered_largest_first():
    out = aggregate([stock("A", "small", 1, 10), stock("B", "big", 1, 1000)], "sector", min_group=1)
    assert [g["key"] for g in out] == ["big", "small"]
