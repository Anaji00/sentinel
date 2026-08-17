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
compute_portfolio_differential = thirteen_f_mod.compute_portfolio_differential
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
        z = self.zsets.get(key, {})
        to_del = [k for k, v in z.items() if min_score <= v <= max_score]
        for k in to_del:
            del z[k]

    async def sadd(self, key, member):
        if key not in self.sets:
            self.sets[key] = set()
        self.sets[key].add(member)

    async def smembers(self, key):
        return self.sets.get(key, set())

    def pipeline(self):
        return MockPipeline(self)


class MockPipeline:
    def __init__(self, mock_redis):
        self.redis = mock_redis
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
    filer_meta = PROMINENT_FILERS["0001067983"]  # Berkshire Hathaway
    report = generate_curated_seed_13f("0001067983", "2026-Q2")

    assert report.filer_id == "berkshire_hathaway"
    assert report.manager_name == "Warren Buffett"
    assert report.total_portfolio_value_usd > 200_000_000_000
    assert len(report.top_holdings) > 0
    assert report.top_10_concentration_pct > 70.0

    # Verify positions have valid change_type
    for pos in report.top_holdings:
        assert pos.change_type in ("NEW", "INCREASED", "DECREASED", "EXITED", "MAINTAINED")


from services.api_gateway.dependencies import create_jwt_token

def get_auth_cookies():
    token = create_jwt_token({"sub": "test-analyst-user", "role": "analyst"})
    return {"sentinel_session": token}


# ── 3. API GATEWAY FILINGS ROUTES TESTS (§2.1) ────────────────────────────────

def test_filings_api_gateway_routes():
    client = TestClient(app)
    cookies = get_auth_cookies()
    headers = {"X-User-Role": "ANALYST"}

    # 1. Latest corporate filings
    res_latest = client.get("/api/v1/filings/latest", cookies=cookies, headers=headers)
    assert res_latest.status_code == 200
    filings = res_latest.json()
    assert len(filings) > 0
    assert filings[0]["ticker"] in ("NVDA", "AAPL", "MSFT", "TSLA")

    # 2. Prominent 13F filers summary
    res_prominent = client.get("/api/v1/filings/13f/prominent", cookies=cookies, headers=headers)
    assert res_prominent.status_code == 200
    filers = res_prominent.json()
    assert len(filers) >= 6
    manager_names = [f["manager_name"] for f in filers]
    assert any("Buffett" in m for m in manager_names)

    # 3. 13F portfolio detail for Scion (Michael Burry)
    res_scion = client.get("/api/v1/filings/13f/scion", cookies=cookies, headers=headers)
    assert res_scion.status_code == 200
    scion_data = res_scion.json()
    assert scion_data["manager_name"] == "Michael Burry"
    assert len(scion_data["top_holdings"]) > 0

    # 4. Consensus for ticker
    res_consensus = client.get("/api/v1/filings/13f/consensus/NVDA", cookies=cookies, headers=headers)
    assert res_consensus.status_code == 200
    consensus_data = res_consensus.json()
    assert consensus_data["ticker"] == "NVDA"
    assert len(consensus_data["institutional_buyers"]) > 0


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
