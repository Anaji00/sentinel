"""13F holdings: the reporting unit, the position/security distinction, and identity.

Three defects that all produced numbers that looked plausible:

  * `<value>` was multiplied by 1000 whenever it sat below 1e11, which is every
    real holding, so every post-2023 filing -- the SEC switched the unit to
    whole dollars on 2023-01-03 -- was inflated a thousandfold.
  * Each information-table *row* was treated as a holding, but a filer files one
    row per (security, other manager, discretion), so a single position arrived
    as several and every weight was computed against a fragment.
  * Ticker resolution fell back to an unanchored substring scan over the whole
    SEC registry and returned the first dict-order hit.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "thirteen_f_under_test", ROOT / "services" / "collector-filings" / "thirteen_f.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def tf():
    mod = _load()
    mod._DYNAMIC_TITLE_TO_TICKER.clear()
    mod._STRIPPED_TITLE_TO_TICKER.clear()
    mod._DYNAMIC_TITLE_TO_TICKER.update({
        "ALPHABET INC": "GOOGL",
        "ALPHABET INC CL C": "GOOG",
        "GOOGN HOLDINGS INC": "GOOGN",
        "APPLE INC": "AAPL",
        "BERKSHIRE HATHAWAY INC": "BRK-B",
        "CHIPOTLE MEXICAN GRILL INC": "CMG",
        "GRILL CONCEPTS INC": "GRIL",
    })
    mod._rebuild_stripped_index()
    return mod


def _row(cusip, shares, value, name="APPLE INC", ticker="AAPL"):
    return {
        "ticker": ticker, "cusip": cusip, "issuer_name": name,
        "class_title": "COM", "shares": shares, "market_value_usd": value,
    }


class TestReportingUnit:
    def test_a_whole_dollar_filing_is_not_multiplied(self, tf):
        # Post-2023 convention: 1,000 shares of a $225 stock is $225,000.
        rows = [_row("037833100", 1000.0, 225_000.0)]
        assert tf._value_scale_for_table(rows) == 1.0

    def test_a_thousands_filing_is_scaled_to_dollars(self, tf):
        # Pre-2023 convention: the same position is filed as 225.
        rows = [_row("037833100", 1000.0, 225.0)]
        assert tf._value_scale_for_table(rows) == 1000.0

    def test_the_unit_is_decided_on_the_median_not_one_odd_row(self, tf):
        # One warrant line priced at a fraction of a cent must not convince the
        # parser that a whole-dollar filing is in thousands.
        rows = [_row(f"0000000{i:02d}", 1000.0, 225_000.0) for i in range(10)]
        rows.append(_row("99999999X", 1_000_000.0, 500.0, name="WARRANT"))
        assert tf._value_scale_for_table(rows) == 1.0

    def test_a_table_with_no_share_counts_is_left_as_filed(self, tf):
        # No implied price is measurable, so inventing a factor of 1000 in
        # either direction is worse than reporting what the filing said.
        rows = [_row("037833100", 0.0, 225_000.0)]
        assert tf._value_scale_for_table(rows) == 1.0


class TestPositionsVersusSecurities:
    def test_rows_for_one_security_become_one_holding(self, tf):
        rows = [
            _row("037833100", 100.0, 22_500.0),
            _row("037833100", 300.0, 67_500.0),
            _row("037833100", 600.0, 135_000.0),
        ]
        out = tf._aggregate_by_security(rows)
        assert len(out) == 1
        assert out[0]["shares"] == 1000.0
        assert out[0]["market_value_usd"] == 225_000.0

    def test_distinct_securities_stay_distinct(self, tf):
        rows = [
            _row("037833100", 100.0, 22_500.0),
            _row("02079K305", 50.0, 8_000.0, name="ALPHABET INC", ticker="GOOGL"),
        ]
        assert len(tf._aggregate_by_security(rows)) == 2

    def test_the_check_digit_is_not_part_of_identity(self, tf):
        # Some filers omit the 9th character; the same security must not split.
        rows = [_row("037833100", 100.0, 22_500.0), _row("03783310", 100.0, 22_500.0)]
        out = tf._aggregate_by_security(rows)
        assert len(out) == 1 and out[0]["shares"] == 200.0

    def test_aggregation_preserves_first_seen_order(self, tf):
        rows = [
            _row("02079K305", 50.0, 8_000.0, name="ALPHABET INC", ticker="GOOGL"),
            _row("037833100", 100.0, 22_500.0),
            _row("02079K305", 50.0, 8_000.0, name="ALPHABET INC", ticker="GOOGL"),
        ]
        assert [r["ticker"] for r in tf._aggregate_by_security(rows)] == ["GOOGL", "AAPL"]


class TestIssuerIdentity:
    def test_alphabet_does_not_resolve_to_a_registry_neighbour(self, tf):
        # The substring scan returned GOOGN here, because that title was reached
        # first and `title in name or name in title` accepted it.
        assert tf.resolve_ticker_dynamically("ALPHABET INC") == "GOOGL"
        assert tf.resolve_ticker_dynamically("ALPHABET INC", "02079K305") == "GOOGL"

    def test_share_class_selects_between_listings(self, tf):
        assert tf.resolve_ticker_dynamically("ALPHABET INC CL C") == "GOOG"
        # Class A is not separately listed in the registry under that spelling,
        # so it resolves to the primary listing rather than to class C.
        assert tf.resolve_ticker_dynamically("ALPHABET INC CL A") == "GOOGL"

    def test_corporate_suffixes_are_not_identity(self, tf):
        for spelling in ("APPLE INC", "APPLE INC COM", "APPLE INCORPORATED"):
            assert tf.resolve_ticker_dynamically(spelling) == "AAPL", spelling

    def test_a_shared_word_does_not_make_two_companies_one(self, tf):
        assert tf.resolve_ticker_dynamically("CHIPOTLE MEXICAN GRILL INC") == "CMG"
        assert tf.resolve_ticker_dynamically("GRILL CONCEPTS INC") == "GRIL"

    def test_an_unknown_issuer_resolves_to_nothing(self, tf):
        # Not to whichever registry title happened to share a fragment with it.
        assert tf.resolve_ticker_dynamically("SOME PRIVATE PARTNERS LLC") is None
