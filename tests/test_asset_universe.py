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
    }
    for name, kind in cases.items():
        assert classify(name) == kind, name


def test_compact_keeps_borrow_flags() -> None:
    out = compact([{"symbol": "jagx", "name": "Jaguar Health, Inc. Common Stock",
                    "shortable": False, "easy_to_borrow": False}])
    assert out["JAGX"][:3] == ["stock", False, False]
