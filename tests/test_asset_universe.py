"""Funds, leveraged products and non-common securities are told apart from stocks."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.asset_universe import classify, compact  # noqa: E402


def test_names_classify() -> None:
    cases = {
        "Direxion Shares ETF Trust Direxion Daily MU Bull 2X ETF": "fund",
        "MicroSectors Gold Miners -3x Inverse Leveraged ETN": "fund",
        "abrdn Physical Silver Shares ETF": "fund",
        "State Street SPDR S&P 500 ETF Trust": "fund",
        "Apple Inc. Common Stock": "stock",
        "Federal Realty Investment Trust Common Stock": "stock",
        "AstraZeneca PLC American Depositary Shares": "stock",
        "Nebius Group N.V. Class A Ordinary Shares": "stock",
        "Index Fund Advisors Inc Common Stock": "stock",
        "Rocket Lab Warrants": "other",
        "Sprott Physical Uranium Trust Units": "other",
        "": "other",
        # Fund families with no fund word in the name (8 Oct: 103 ProShares
        # funds and QQQ had been ranked as companies).
        "ProShares UltraShort S&P500": "fund",
        "Invesco QQQ Trust, Series 1": "fund",
        "iShares Gold Trust": "fund",
        "Invesco CurrencyShares Euro Currency Trust": "fund",
        "BlackRock Taxable Municipal Bond Trust": "fund",
        "Eaton Vance Floating-Rate Income Trust": "fund",
        "Synthetic Fixed-Income Securities STRATS 2006-2": "other",
        # ...while the managers themselves, REITs and Ultra-named companies stay stocks.
        "BlackRock, Inc. Common Stock": "stock",
        "Invesco LTD": "stock",
        "Franklin Templeton, Inc.": "stock",
        "Universal Health Realty Income Trust": "stock",
        "Ultra Clean Holdings, Inc. Common Stock": "stock",
        "Invesco Mortgage Capital Inc.": "stock",
    }
    for name, kind in cases.items():
        assert classify(name) == kind, name


def test_compact_keeps_borrow_flags() -> None:
    out = compact([{"symbol": "jagx", "name": "Jaguar Health, Inc. Common Stock",
                    "shortable": False, "easy_to_borrow": False}])
    assert out["JAGX"][:3] == ["stock", False, False]


def test_delisted_stocks_exclude_reused_tickers_and_non_stocks() -> None:
    from shared.utils.asset_universe import delisted_stocks

    inactive = [
        {"symbol": "CIVI", "exchange": "NYSE", "name": "Civitas Resources, Inc. Common Stock"},
        {"symbol": "SC", "exchange": "NYSE", "name": "Santander Consumer USA Common Stock"},
        {"symbol": "192CVR025", "exchange": "NASDAQ", "name": "Some CVR"},
        {"symbol": "OLDF", "exchange": "ARCA", "name": "Old Fund ETF"},
        {"symbol": "PINK", "exchange": "OTC", "name": "Pink Co Common Stock"},
    ]
    assert delisted_stocks(inactive, active_symbols={"SC"}) == ["CIVI"]
