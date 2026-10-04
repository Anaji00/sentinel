"""Who owns what, from 13F.

Live on 4 October, BlackRock's 13F for 2026-06-30 parsed to 5,455 positions
($6.71T); kept against the platform's universe it held 8.8% of State Street,
7.8% of Bank of America and 8.1% of Exxon, while State Street held 3.9% of
BlackRock. Before this, the managers that own most of the market were not
tracked, and only hedge-fund top holdings reached the graph.
"""

import asyncio
import importlib.util
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from shared.utils import ownership as own

ROOT = Path(__file__).resolve().parents[1]

INFOTABLE = b"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable><nameOfIssuer>STATE STR CORP</nameOfIssuer><titleOfClass>COM</titleOfClass>
    <cusip>857477103</cusip><value>2500000000</value>
    <shrsOrPrnAmt><sshPrnamt>25000000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>
  <infoTable><nameOfIssuer>STATE STR CORP</nameOfIssuer><titleOfClass>COM</titleOfClass>
    <cusip>857477103</cusip><value>100000000</value>
    <shrsOrPrnAmt><sshPrnamt>1000000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>
  <infoTable><nameOfIssuer>STATE STR CORP</nameOfIssuer><titleOfClass>COM</titleOfClass>
    <cusip>857477103</cusip><value>5000000</value><putCall>Put</putCall>
    <shrsOrPrnAmt><sshPrnamt>50000</sshPrnamt><sshPrnamtType>SH</sshPrnamtType></shrsOrPrnAmt></infoTable>
  <infoTable><nameOfIssuer>SOME NOTE</nameOfIssuer><cusip>000000AA1</cusip><value>1</value>
    <shrsOrPrnAmt><sshPrnamt>1000</sshPrnamt><sshPrnamtType>PRN</sshPrnamtType></shrsOrPrnAmt></infoTable>
</informationTable>"""


def test_the_table_streams_and_aggregates_by_security():
    rows = list(own.iter_infotable(io.BytesIO(INFOTABLE)))
    assert len(rows) == 2                     # the put and the note are not stakes
    agg = own.aggregate_by_cusip(rows)
    assert agg["857477103"]["shares"] == 26_000_000 and agg["857477103"]["value"] == 2_600_000_000
    assert own.pct_outstanding(26_000_000, 295_000_000) == 8.814
    assert own.pct_outstanding(1, None) is None


def test_follow_the_money_measures():
    xom = {"BLACKROCK": 8.1, "VANGUARD": 9.6, "STATE_STREET": 5.0, "CITADEL": 0.3}
    cvx = {"BLACKROCK": 7.0, "VANGUARD": 8.9, "BERKSHIRE_HATHAWAY": 6.5}
    assert own.top_holders(xom, 2) == [("VANGUARD", 9.6), ("BLACKROCK", 8.1)]
    assert own.big_three_share(xom) == 22.7
    assert own.common_ownership(xom, cvx) == 15.9      # 7.0 + 8.9: the same two on both registers
    assert own.common_ownership(xom, {"PERSHING_SQUARE": 3.0}) == 0


def test_institutions_that_hold_each_other_are_found():
    stakes = {"BLACKROCK": {"STT": 8.8, "JPM": 7.8, "BLK": 6.2}, "STATE_STREET": {"BLK": 3.9, "JPM": 4.7},
              "JPMORGAN": {"BLK": 1.1}}
    listed = {"BLACKROCK": "BLK", "STATE_STREET": "STT", "JPMORGAN": "JPM"}
    rows = own.cross_holdings(stakes, listed)
    assert ("BLACKROCK", "STATE_STREET", 8.8, 3.9) in rows
    assert ("STATE_STREET", "BLACKROCK", 3.9, 8.8) in rows
    assert ("BLACKROCK", "JPMORGAN", 7.8, 1.1) in rows
    assert all(a != b for a, b, *_ in rows)        # BlackRock's funds holding BLK is not a cross-holding


def test_holders_reach_a_prompt_line():
    line = own.holders_line("XOM", {
        "VANGUARD": {"filer": "Vanguard Group", "pct_outstanding": 9.6},
        "BLACKROCK": {"filer": "BlackRock, Inc.", "pct_outstanding": 8.1},
        "STATE_STREET": {"filer": "State Street Corp", "pct_outstanding": 5.0},
    })
    assert line.startswith("XOM: Vanguard Group 9.6%, BlackRock, Inc. 8.1%, State Street Corp 5.0%")
    assert "22.7%" in line
    assert own.holders_line("ZZZ", {}) == ""


def _job():
    spec = importlib.util.spec_from_file_location("ownership_job_t", ROOT / "services/collector-filings/ownership.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ownership_job_t"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_figi_picks_the_us_listing_and_refuses_funds():
    job = _job()
    exxon = [{"ticker": "EXMOC", "securityType": "Common Stock"}, {"ticker": "XOMCHF", "securityType": "Common Stock"},
             {"ticker": "XOM_KZ", "securityType": "Common Stock"}, {"ticker": "XOM", "securityType": "Common Stock"},
             {"ticker": "XOM", "securityType": "Common Stock"}]
    assert job._figi_pick(exxon) == "XOM"
    assert job._figi_pick([{"ticker": "IVV", "securityType": "ETP"}]) == "-"
    assert job._figi_pick([{"ticker": "BRK/B", "securityType": "Common Stock"}]) == "BRK-B"


def test_every_filer_is_distinct_and_the_listed_ones_name_a_ticker():
    ids = [f.filer_id for f in own.OWNERSHIP_FILERS.values()]
    assert len(ids) == len(set(ids))
    assert own.OWNERSHIP_FILERS["0002012383"].listed_as == "BLK"      # post-2024 BlackRock, not BlackRock Finance
    assert "0001364742" not in own.OWNERSHIP_FILERS
    for f in own.OWNERSHIP_FILERS.values():
        assert (f.listed_as is None) == (f.listed_via == "")


def test_the_scenario_context_names_who_holds_the_exposed_companies():
    from services.reasoning.context_builder import ContextBuilder

    class Raw:
        async def hgetall(self, key):
            if key.endswith(":XOM"):
                return {b"VANGUARD": json.dumps({"filer": "Vanguard Group", "pct_outstanding": 9.6}).encode()}
            return {}

    builder = ContextBuilder.__new__(ContextBuilder)
    with patch("services.reasoning.context_builder.get_redis", AsyncMock(return_value=SimpleNamespace(raw=Raw()))):
        out = asyncio.run(builder._with_holders("• CL=F -- rise with it: XOM", ["XOM", "CVX"]))
    assert "Largest reported holders" in out and "XOM: Vanguard Group 9.6%" in out
    with patch("services.reasoning.context_builder.get_redis", AsyncMock(side_effect=ConnectionError())):
        assert asyncio.run(builder._with_holders("x", ["XOM"])) == "x"


def test_impossible_stakes_and_thousands_tables_are_caught():
    """BRK-B came out at 8,415% against another class's share count, and T.
    Rowe Price's table still reads in thousands of dollars."""
    assert own.pct_outstanding(5_000_000, 50_000) is None
    thousands = {"a": {"shares": 1_000_000.0, "value": 150_000.0}}    # $0.15 a share
    dollars = {"a": {"shares": 1_000_000.0, "value": 150_000_000.0}}  # $150 a share
    assert own.value_scale(thousands) == 1000.0 and own.value_scale(dollars) == 1.0


def test_a_multi_class_stake_falls_back_to_market_value():
    """Class B shares over a class A share count read as 38% of Berkshire."""
    # 1% of a $1T company held, but a share count from the wrong class.
    assert own.stake_pct(shares=20_000_000, value_usd=10e9, shares_out=52_000_000, market_cap_usd=1e12) == 1.0
    # When the two agree within a quarter's drift, the share count stands.
    assert own.stake_pct(shares=8_000_000, value_usd=1.1e9, shares_out=100_000_000, market_cap_usd=12e9) == 8.0
    # Intuit, live: 28.9M of 273.5M shares, filed at $662, market at $275 today.
    # The share count stands -- the value figure would say 25.5%.
    assert own.stake_pct(shares=28_918_438, value_usd=19.16e9, shares_out=273_540_000,
                         market_cap_usd=75.1e9) == 10.572
    assert own.stake_pct(shares=1, value_usd=5e9, shares_out=None, market_cap_usd=1e11) == 5.0
    assert own.stake_pct(shares=1, value_usd=0.0, shares_out=None, market_cap_usd=None) is None
    assert "LMT" in own.DEFENSE_PRIMES and "Aerospace & Defense" in own.OSINT_SECTORS


def test_a_stake_that_rounds_to_zero_does_not_fail_the_filer():
    assert own.stake_pct(shares=10, value_usd=1_000.0, shares_out=10_000_000_000, market_cap_usd=1e12) == 0.0
