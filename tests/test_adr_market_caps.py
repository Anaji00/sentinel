"""Layer audit 18 Nov, finding 11: ADR market caps (shared/utils/market_cap.adr_adjusted via cap_rows)."""
from shared.utils import market_cap as mc


def test_tsm_computed_cap_is_divided_by_its_ordinary_shares_per_ads() -> None:
    rows = mc.cap_rows({"TSM": 9_404e9}, {"TSM": None}, ["TSM"])
    assert abs(rows["TSM"] - 1_880.8e9) < 1e6


def test_a_vendor_figure_in_another_currency_is_ignored() -> None:
    rows = mc.cap_rows({"EQNR": 90e9}, {"EQNR": 946e9}, ["EQNR"])
    assert rows["EQNR"] == 90e9
    assert mc.cap_rows({}, {"EQNR": 946e9}, ["EQNR"]) == {}          # no computed figure: unknown, not 946bn


def test_unlisted_names_and_ordinary_caps_are_unchanged() -> None:
    assert mc.cap_rows({"AAPL": 3.9e12}, {"AAPL": 3.8e12}, ["AAPL"]) == {"AAPL": 3.9e12}
    assert mc.adr_adjusted("MSFT", 5.0, 6.0) == (5.0, 6.0)
    assert mc.adr_adjusted("ngg", 427e9, None) == (85.4e9, None)


def test_shel_keeps_the_larger_of_the_adjusted_and_vendor_figures() -> None:
    assert mc.cap_rows({"SHEL": 537e9}, {"SHEL": 208e9}, ["SHEL"])["SHEL"] == 268.5e9
