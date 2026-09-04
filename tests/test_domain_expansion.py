"""
tests/test_domain_expansion.py

Comprehensive test suite verifying Part 2: Domain Expansion:
1. SEC EDGAR Corporate Filings Collector (§2.1)
2. Prominent 13F Institutional Holdings & Consensus Flow (§2.1)
3. API Gateway Filings & 13F Endpoints (§2.1)
4. Supply Chain & Freight Shipping Rate Index Poller (§2.3)
5. Regulatory & Legislative Policy Tracker (§2.4)
6. Anti-Surveillance Perimeter Enforcement (§2.5)
"""

import asyncio
import json
import pytest
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import importlib.util
from pathlib import Path

from fastapi.testclient import TestClient

from shared.kafka import Topics
from shared.models import RawEvent, NormalizedEvent, EventType, EntityType
from shared.models.events import FilingData, ThirteenFData, RegulatoryData, SupplyChainData
from shared.models.ontology import validate_data_boundary_compliance, ALLOWED_NODE_LABELS

# Dynamic imports for modules in hyphenated directories
filings_collector_path = Path(__file__).resolve().parents[1] / "services" / "collector-filings" / "main.py"
spec_f = importlib.util.spec_from_file_location("collector_filings_main", filings_collector_path)
collector_filings_main = importlib.util.module_from_spec(spec_f)
spec_f.loader.exec_module(collector_filings_main)

FilingDeduplicator = collector_filings_main.FilingDeduplicator
get_cik_for_ticker = collector_filings_main.get_cik_for_ticker
poll_company_filings = collector_filings_main.poll_company_filings

tf_path = Path(__file__).resolve().parents[1] / "services" / "collector-filings" / "thirteen_f.py"
spec_tf = importlib.util.spec_from_file_location("thirteen_f", tf_path)
thirteen_f_mod = importlib.util.module_from_spec(spec_tf)
spec_tf.loader.exec_module(thirteen_f_mod)

PROMINENT_FILERS = thirteen_f_mod.PROMINENT_FILERS
parse_13f_xml_table = thirteen_f_mod.parse_13f_xml_table
compute_portfolio_differential = thirteen_f_mod.compute_portfolio_differential
fetch_live_13f_from_edgar = thirteen_f_mod.fetch_live_13f_from_edgar
generate_curated_seed_13f = thirteen_f_mod.generate_curated_seed_13f

freight_path = Path(__file__).resolve().parents[1] / "services" / "collector-macro" / "freight.py"
spec_fr = importlib.util.spec_from_file_location("freight", freight_path)
freight_mod = importlib.util.module_from_spec(spec_fr)
spec_fr.loader.exec_module(freight_mod)

poll_freight_indices = freight_mod.poll_freight_indices
FREIGHT_BENCHMARKS = freight_mod.FREIGHT_BENCHMARKS

reg_path = Path(__file__).resolve().parents[1] / "services" / "collector-macro" / "regulatory.py"
spec_reg = importlib.util.spec_from_file_location("regulatory", reg_path)
reg_mod = importlib.util.module_from_spec(spec_reg)
spec_reg.loader.exec_module(reg_mod)

RegulatoryDeduplicator = reg_mod.RegulatoryDeduplicator
poll_federal_register = reg_mod.poll_federal_register
TARGET_KEYWORDS = reg_mod.TARGET_KEYWORDS

from services.api_gateway.routes.main import app


class MockRedis:
    def __init__(self):
        self.store = {}
        self.zsets = {}
        self.hashes = {}
        self.sets = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = value

    async def hget(self, name, key):
        return self.hashes.get(name, {}).get(key)

    async def hset(self, name, key, value):
        if name not in self.hashes:
            self.hashes[name] = {}
        self.hashes[name][key] = value

    async def zadd(self, key, mapping):
        if key not in self.zsets:
            self.zsets[key] = {}
        self.zsets[key].update(mapping)

    async def zscore(self, key, member):
        return self.zsets.get(key, {}).get(member)

    async def zrange(self, key, start, stop):
        z = self.zsets.get(key, {})
        items = sorted(z.items(), key=lambda x: x[1])
        slice_items = items[start : stop + 1 if stop != -1 else None]
        return [item[0] for item in slice_items]

    async def zremrangebyscore(self, key, min_score, max_score):
        if key in self.zsets:
            self.zsets[key] = {k: v for k, v in self.zsets[key].items() if not (min_score <= v <= max_score)}

    async def sadd(self, key, member):
        if key not in self.sets:
            self.sets[key] = set()
        self.sets[key].add(member)

    async def smembers(self, key):
        return self.sets.get(key, set())

    async def publish(self, channel, message):
        return 1

    def pipeline(self):
        return MockPipeline(self)


class MockPipeline:
    def __init__(self, redis):
        self.redis = redis
        self.ops = []

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, value, ex))
        return self

    def zadd(self, key, mapping):
        self.ops.append(("zadd", key, mapping))
        return self

    def zremrangebyscore(self, key, min_s, max_s):
        self.ops.append(("zremrangebyscore", key, min_s, max_s))
        return self

    async def execute(self):
        results = []
        for op in self.ops:
            if op[0] == "set":
                await self.redis.set(op[1], op[2], ex=op[3])
                results.append(True)
            elif op[0] == "zadd":
                await self.redis.zadd(op[1], op[2])
                results.append(1)
            elif op[0] == "zremrangebyscore":
                await self.redis.zremrangebyscore(op[1], op[2], op[3])
                results.append(1)
        self.ops = []
        return results


# ── 1. SEC EDGAR FILINGS TESTS (§2.1) ─────────────────────────────────────────

def test_sec_edgar_filings_collector():
    async def _test():
        redis = MockRedis()
        dedup = FilingDeduplicator(redis)

        # 1. CIK Resolution
        cik = await get_cik_for_ticker("NVDA", redis)
        assert cik == "0001045810"

        # 2. Dedup
        acc_num = "0001045810-26-000045"
        assert await dedup.is_seen(acc_num) is False
        await dedup.mark_seen(acc_num)
        assert await dedup.is_seen(acc_num) is True

    asyncio.run(_test())


# ── 2. 13F INSTITUTIONAL HOLDINGS & DIFFERENTIAL TESTS (§2.1) ─────────────────

def test_13f_portfolio_differential_and_consensus():
    # 1. Real SEC EDGAR XML Infotable Parsing
    sec_xml_sample = """<?xml version="1.0" encoding="UTF-8"?>
    <informationTable xmlns="http://www.sec.gov/edgar/thirteenf/informationtable">
        <infoTable>
            <nameOfIssuer>APPLE INC</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>037833100</cusip>
            <value>69000000</value>
            <shrsOrPrnAmt>
                <sshPrnamt>300000000</sshPrnamt>
                <sshPrnamtType>SH</sshPrnamtType>
            </shrsOrPrnAmt>
            <investmentDiscretion>SOLE</investmentDiscretion>
        </infoTable>
        <infoTable>
            <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
            <titleOfClass>COM</titleOfClass>
            <cusip>594918104</cusip>
            <value>15500000</value>
            <shrsOrPrnAmt>
                <sshPrnamt>35000000</sshPrnamt>
                <sshPrnamtType>SH</sshPrnamtType>
            </shrsOrPrnAmt>
            <investmentDiscretion>SOLE</investmentDiscretion>
        </infoTable>
    </informationTable>"""

    holdings = parse_13f_xml_table(sec_xml_sample)
    assert len(holdings) == 2
    assert holdings[0]["ticker"] == "AAPL"
    assert holdings[0]["shares"] == 300_000_000
    assert holdings[0]["market_value_usd"] == 69_000_000_000
    assert holdings[1]["ticker"] == "MSFT"
    assert holdings[1]["shares"] == 35_000_000

    # 2. Differential Computation Across Periods
    prev_holdings = [
        {"ticker": "AAPL", "cusip": "037833100", "issuer_name": "APPLE INC", "shares": 350_000_000, "market_value_usd": 80_500_000_000},
        {"ticker": "GOOGL", "cusip": "02079K305", "issuer_name": "ALPHABET INC", "shares": 10_000_000, "market_value_usd": 1_800_000_000},
    ]
    filer_meta = PROMINENT_FILERS["0001067983"]
    report = compute_portfolio_differential(holdings, prev_holdings, filer_meta, "2026-Q2")

    assert report.filer_id == "berkshire_hathaway"
    assert report.total_portfolio_value_usd == 84_500_000_000
    assert report.is_synthetic is False
    assert report.source_type == "LIVE_MEASUREMENT"
    assert any(p.ticker == "MSFT" and p.change_type == "NEW" for p in report.new_positions)
    assert any(p.ticker == "AAPL" and p.change_type == "DECREASED" for p in report.decreased_positions)
    assert any(p.ticker == "GOOGL" and p.change_type == "EXITED" for p in report.exited_positions)


from services.api_gateway.dependencies import create_jwt_token

def get_auth_cookies():
    token = create_jwt_token({"sub": "test-analyst-user", "role": "analyst"})
    return {"sentinel_session": token}


# ── 3. API GATEWAY FILINGS ROUTES TESTS (§2.1) ────────────────────────────────

def test_filings_api_gateway_routes():
    client = TestClient(app)
    cookies = get_auth_cookies()

    # 1. Latest corporate filings
    res_latest = client.get("/api/v1/filings/latest", cookies=cookies)
    assert res_latest.status_code == 200
    filings = res_latest.json()
    assert len(filings) > 0
    assert filings[0]["ticker"] in ("NVDA", "AAPL", "MSFT", "TSLA")

    # 2. Prominent 13F filers summary
    res_prominent = client.get("/api/v1/filings/13f/prominent", cookies=cookies)
    assert res_prominent.status_code == 200
    filers = res_prominent.json()
    assert len(filers) >= 6
    manager_names = [f["manager_name"] for f in filers]
    assert any("Buffett" in m for m in manager_names)

    # 3. 13F portfolio detail for Berkshire Hathaway
    res_berkshire = client.get("/api/v1/filings/13f/berkshire_hathaway", cookies=cookies)
    assert res_berkshire.status_code == 200
    berkshire_data = res_berkshire.json()
    assert berkshire_data["manager_name"] == "Warren Buffett"
    assert "total_portfolio_value_usd" in berkshire_data

    # 4. Consensus for ticker
    #
    # This asserted len(institutional_buyers) > 0, which held only because the
    # endpoint invented names when it had no filings: NVDA returned a hardcoded
    # "Warren Buffett, Ken Griffin, Jim Simons, Cathie Wood". The test was
    # therefore pinning the fabrication in place -- it would have failed the
    # moment the endpoint started telling the truth, which is what it did.
    #
    # With no 13F consensus data ingested in this test, the honest response is
    # an empty list, and derived_from_filings says which of the two empty cases
    # this is: no prominent filer holds it, or no filings were available to ask.
    res_consensus = client.get("/api/v1/filings/13f/consensus/NVDA", cookies=cookies)
    assert res_consensus.status_code == 200
    consensus_data = res_consensus.json()
    assert consensus_data["ticker"] == "NVDA"
    assert consensus_data["institutional_buyers"] == []
    assert consensus_data["derived_from_filings"] is False
    assert consensus_data["consensus_sentiment"] == "NEUTRAL"
    # Every name returned must be backed by an ingested filing.
    assert len(consensus_data["institutional_buyers"]) == consensus_data["total_prominent_holders"]


# ── 4. SUPPLY CHAIN & FREIGHT INDEX POLLER TESTS (§2.3) ───────────────────────

def test_freight_shipping_index_poller():
    async def _test():
        redis = MockRedis()
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()

        session = MagicMock()
        count = await poll_freight_indices(session, mock_producer, redis)
        assert count == len(FREIGHT_BENCHMARKS)
        assert mock_producer.send.call_count == len(FREIGHT_BENCHMARKS)

        # Verify emitted topic and structure
        call_args = mock_producer.send.call_args_list[0]
        assert call_args[0][0] == Topics.RAW_TRADFI
        payload = call_args[0][1]["raw_payload"]
        assert "current_rate" in payload
        assert "index_symbol" in payload

    asyncio.run(_test())


# ── 5. REGULATORY POLICY TRACKER TESTS (§2.4) ─────────────────────────────────

def test_regulatory_policy_poller():
    async def _test():
        redis = MockRedis()
        dedup = RegulatoryDeduplicator(redis)

        doc_id = "2026-14820"
        assert await dedup.is_seen(doc_id) is False
        await dedup.mark_seen(doc_id)
        assert await dedup.is_seen(doc_id) is True

    asyncio.run(_test())


# ── 6. ANTI-SURVEILLANCE BOUNDARY TESTS (§2.5) ───────────────────────────────

def test_anti_surveillance_perimeter_guard():
    # Permitted institutional and corporate types
    assert validate_data_boundary_compliance("corporate_filing") is True
    assert validate_data_boundary_compliance("maritime_ais") is True
    assert validate_data_boundary_compliance("macro_freight") is True
    assert validate_data_boundary_compliance("aviation_adsb") is True

    # Prohibited mass surveillance of private citizens
    assert validate_data_boundary_compliance("mobile_carrier_location") is False
    assert validate_data_boundary_compliance("consumer_device_telemetry") is False
    assert validate_data_boundary_compliance("facial_recognition") is False
    assert validate_data_boundary_compliance("biometric_surveillance") is False
    assert validate_data_boundary_compliance("private_citizen_communications") is False
