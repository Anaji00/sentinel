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


def test_bankrupt_roots_recover_the_exchange_ticker() -> None:
    from shared.utils.asset_universe import bankrupt_roots
    assets = [
        {"symbol": "SIVBQ", "exchange": "OTC", "name": "SVB Financial Group Common Stock"},
        {"symbol": "NKLAQ", "exchange": "OTC", "name": "Nikola Corporation Common Stock"},
        {"symbol": "AAPLQ", "exchange": "OTC", "name": "Fake Common Stock"},            # root trades today: reuse
        {"symbol": "XYZQ", "exchange": "OTC", "name": "Short Common Stock"},             # four letters: no root
        {"symbol": "ABCDQ", "exchange": "NYSE", "name": "Listed Common Stock"},          # not OTC
        {"symbol": "EFGHQ", "exchange": "OTC", "name": "Some Fund ETF"},                 # not a stock
    ]
    assert bankrupt_roots(assets, {"AAPL"}) == ["NKLA", "SIVB"]


def test_a_lapsed_asset_list_is_fetched_not_read_as_empty(monkeypatch):
    """An empty list means no filter to every reader, so funds would be ranked and traded."""
    import asyncio
    import json as _json

    from shared.utils import asset_universe as au

    class Raw:
        def __init__(self):
            self.store = {}

        async def get(self, k):
            return self.store.get(k)

    raw = Raw()

    async def refreshed(r, session, headers):
        assert headers["APCA-API-KEY-ID"] == "k"
        r.store[au.ASSET_META_KEY] = _json.dumps({"AAPL": ["stock", True, True, "Apple"]})
        return 1

    monkeypatch.setattr(au, "refresh_asset_meta", refreshed)
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    assert asyncio.run(au.load_asset_meta(raw)) == {"AAPL": ["stock", True, True, "Apple"]}


def test_without_keys_the_asset_list_stays_empty_and_makes_no_call(monkeypatch):
    import asyncio

    from shared.utils import asset_universe as au

    class Raw:
        async def get(self, k):
            return None

    async def never(*a):
        raise AssertionError("no keys, no call")

    monkeypatch.setattr(au, "refresh_asset_meta", never)
    for name in ("ALPACA_API_KEY", "APCA_API_KEY_ID", "ALPACA_SECRET_KEY", "ALPACA_API_SECRET", "APCA_API_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    assert asyncio.run(au.load_asset_meta(Raw())) == {}
