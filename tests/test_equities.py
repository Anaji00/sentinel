"""
tests/test_equities.py

UNIT TESTS FOR PRIMARY EQUITY VALIDATION & LIGHTWEIGHT ASSET CLASSIFICATION
=============================================================================
Tests sub-millisecond fast_classify_equity and is_valid_primary_equity logic:
  - Clean primary US common equities (AAPL, NVDA, MSFT, INTC, TSLA)
  - Explicit crypto exception (BTC, BTCUSDT)
  - Exclusion of single-stock leveraged ETFs (IONZ, IONU, NVDL, TSLL)
  - Exclusion of YieldMax option income funds (NVDY, CONY, TSLY)
  - Exclusion of 2x/3x Leveraged Bull/Bear funds (TQQQ, SQQQ, SOXL, UPRO)
  - Exclusion of Volatility ETNs (UVXY, SVIX, VXX)
"""

import pytest
from shared.utils.equities import is_valid_primary_equity, fast_classify_equity, ALLOWED_CRYPTO_TOKENS


def test_clean_primary_equities():
    valid_tickers = ["AAPL", "NVDA", "MSFT", "INTC", "TSLA", "PLTR", "AMZN", "GOOGL", "META"]
    for ticker in valid_tickers:
        assert is_valid_primary_equity(ticker) is True, f"Failed for valid equity {ticker}"
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is True
        assert res["asset_class"] == "PRIMARY_COMMON_EQUITY"


def test_btc_crypto_exception():
    for token in ALLOWED_CRYPTO_TOKENS:
        assert is_valid_primary_equity(token) is True
        res = fast_classify_equity(token)
        assert res["is_primary_equity"] is True
        assert res["asset_class"] == "CRYPTO_TOKEN"


def test_single_stock_leveraged_etfs_exclusion():
    leveraged_tickers = ["IONZ", "IONU", "IOND", "NVDL", "TSLL", "TSLZ", "AAPU", "MSTZ"]
    for ticker in leveraged_tickers:
        assert is_valid_primary_equity(ticker) is False, f"Failed to reject leveraged ETF {ticker}"
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is False
        assert res["asset_class"] == "LEVERAGED_INVERSE_ETF"


def test_yieldmax_option_funds_exclusion():
    yieldmax_tickers = ["NVDY", "CONY", "TSLY", "AMZY", "MSTY", "APLY"]
    for ticker in yieldmax_tickers:
        assert is_valid_primary_equity(ticker) is False, f"Failed to reject YieldMax fund {ticker}"
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is False
        assert res["asset_class"] in ("SYNTHETIC_OPTION_YIELD", "LEVERAGED_INVERSE_ETF")


def test_broad_leveraged_etfs_exclusion():
    broad_leveraged = ["TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU", "FNGU", "BULZ"]
    for ticker in broad_leveraged:
        assert is_valid_primary_equity(ticker) is False, f"Failed to reject broad leveraged ETF {ticker}"
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is False
        assert res["asset_class"] == "LEVERAGED_INVERSE_ETF"


def test_volatility_etns_exclusion():
    vol_etns = ["UVXY", "SVIX", "UVIX", "VIXY", "VXX"]
    for ticker in vol_etns:
        assert is_valid_primary_equity(ticker) is False, f"Failed to reject volatility ETN {ticker}"
        res = fast_classify_equity(ticker)
        assert res["is_primary_equity"] is False
        assert res["asset_class"] in ("VOLATILITY_ETN", "LEVERAGED_INVERSE_ETF")
