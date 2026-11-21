import pytest

from shared.utils.subjects import is_non_company_subject


@pytest.mark.parametrize("subject", ["AI", "ai", "EPS", "CEO", "IPO", "seekingalpha_currents", "benzinga_news", "reuters_markets"])
def test_terms_and_feed_names_are_not_subjects(subject):
    assert is_non_company_subject(subject) is True


@pytest.mark.parametrize("subject", ["NVDA", "AAPL", "BRK.B", "COO", "CTO", "US", "UK", "EU", "NATO", "OPEC", "FDA", "FOMC", "ECB", "USD", "US10Y", "MACRO:CPI_YOY", "EURUSD", "BTCUSDT", "ES=F", "UNKNOWN", "0xabc", "fed-cuts-march", "will-btc-hit-100k", "", None])
def test_companies_assets_pairs_and_series_pass(subject):
    assert is_non_company_subject(subject) is False
