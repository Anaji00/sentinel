"""A day's option chain reduced to volume, open interest, IV level and skew."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import option_chain as oc  # noqa: E402


def _o(sym, delta, iv, vol, oi):
    return {"option": sym, "delta": delta, "iv": iv, "volume": vol, "open_interest": oi}


CHAIN = {"data": {"current_price": 100.0, "iv30": 31.5, "options": [
    # 3 days out: counted in totals, too near for the IV measures.
    _o("XYZ261026C00100000", 0.52, 0.90, 50, 100),
    # About 28 days out: the expiry nearest 30.
    _o("XYZ261120C00100000", 0.51, 0.30, 10, 200),
    _o("XYZ261120C00110000", 0.26, 0.27, 5, 50),
    _o("XYZ261120P00100000", -0.49, 0.32, 20, 300),
    _o("XYZ261120P00090000", -0.24, 0.36, 15, 80),
    # Far out.
    _o("XYZ270115C00100000", 0.55, 0.33, 1, 10),
    {"option": "garbage"},
]}}


def test_totals_ratios_and_iv_measures() -> None:
    s = oc.summarize(CHAIN, date(2026, 10, 23))
    assert s["call_volume"] == 66 and s["put_volume"] == 35
    assert s["call_oi"] == 360 and s["put_oi"] == 380
    assert s["pcr_volume"] == round(35 / 66, 4) and s["pcr_oi"] == round(380 / 360, 4)
    assert s["iv30"] == 0.315                                  # Cboe's percent as a fraction
    assert s["expiry_days"] == 28
    assert abs(s["atm_iv"] - 0.31) < 1e-9                      # (0.30 + 0.32) / 2
    assert abs(s["skew_25d"] - 0.09) < 1e-9                    # 0.36 - 0.27: puts dearer
    assert s["contracts"] == 7 and s["spot"] == 100.0


def test_an_empty_or_unusable_chain() -> None:
    assert oc.summarize({"data": {"options": []}}, date(2026, 10, 23)) is None
    only_near = {"data": {"options": [_o("XYZ261026C00100000", 0.5, 0.4, 1, 1)]}}
    s = oc.summarize(only_near, date(2026, 10, 23))
    assert s["atm_iv"] is None and s["skew_25d"] is None and s["call_volume"] == 1


def test_contract_symbols_parse() -> None:
    assert oc.parse_contract("AAPL261120P00222500") == (date(2026, 11, 20), "P", 222.5)
    assert oc.parse_contract("AAPL") is None


def test_the_poller_writes_every_summary_field_in_order() -> None:
    sys.path.insert(0, str(ROOT / "services" / "collector-tradfi"))
    import cboe_chains as cc

    s = oc.summarize(CHAIN, date(2026, 10, 23))
    r = cc.row("XYZ", s)
    assert r[0] == "XYZ" and r[1] == date(2026, 10, 23) and len(r) == cc.UPSERT_SQL.count("$")
