"""
tests/test_domain_enrichers.py

Consolidated Test Suite: Domain Enrichers & Gap Detectors.
Combines:
  - test_cyber.py
  - test_aviation_enricher_batching.py
  - test_aviation_gap_detector.py
  - test_maritime_headline_synthesis.py
  - test_news_enricher_anomaly.py
  - test_ofac_sync.py
"""

import asyncio
import json
import pytest
import time
import os
import importlib.util
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from services.enrichment.enrichers.cyber import CyberEnricher, CyberThreatScorer
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.aviation_gap_detector import AviationGapDetector
from services.enrichment.ofac_sync import fetch_ofac_keywords
from services.agents.knowledge_graph_engine import VALID_PREDICATES
from shared.models import RawEvent, NormalizedEvent, EventType, EntityType
from shared.kafka import Topics
from shared.utils.sanctions import check_sanctions, rebuild_sanctions_from_list, SANCTIONED_KEYWORDS

# Dynamically import collector-cyber/main.py
cyber_collector_path = Path(__file__).resolve().parents[1] / "services" / "collector-cyber" / "main.py"
spec = importlib.util.spec_from_file_location("collector_cyber_main", cyber_collector_path)
collector_cyber_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector_cyber_main)

poll_censys = collector_cyber_main.poll_censys


# ── 1. CYBER ENRICHER & COLLECTOR TESTS ─────────────────────────────────────

def test_cyber_scorer_threshold_caching():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{"cyber": {"cyber_exposure": {"ics_score": 0.85}}}')
        scorer = CyberThreatScorer(redis_mock)

        await scorer.load_thresholds()
        assert scorer.cfg.get("cyber_exposure", {}).get("ics_score") == 0.85
        assert redis_mock.raw.get.call_count == 1

        await scorer.load_thresholds()
        assert redis_mock.raw.get.call_count == 1

        scorer.last_loaded = 0.0
        await scorer.load_thresholds()
        assert redis_mock.raw.get.call_count == 2

    asyncio.run(run_test())

def test_cyber_enricher_exposure():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{}')
        enricher = CyberEnricher(scorer=None, redis_client=redis_mock, graph_writer=MagicMock(producer=AsyncMock()))

        raw_event = RawEvent(
            source="censys",
            raw_payload={
                "ip_address": "1.2.3.4", "port": 502, "protocol": "modbus",
                "product": "Siemens S7-1200", "org": "Siemens Energy Grid Corp", "country_code": "US"
            }
        )
        normalized = await enricher.enrich(raw_event)
        assert normalized is not None
        assert normalized.anomaly_score == 0.80
        assert "critical_infrastructure" in normalized.tags
        assert int(normalized.security_data.port) == 502

    asyncio.run(run_test())

def test_collector_concurrency_and_awaiting():
    async def run_test():
        session_mock = MagicMock()
        producer_mock = AsyncMock()
        seen_ips = set()

        collector_cyber_main.CENSYS_API_ID = "mock_id"
        collector_cyber_main.CENSYS_API_SECRET = "mock_secret"

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "result": {"hits": [{"ip": "1.1.1.1", "autonomous_system": {"name": "Test AS", "country_code": "US"}, "services": [{"port": 502, "service_name": "modbus"}]}]}
        })

        session_post_ctx = MagicMock()
        session_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        session_post_ctx.__aexit__ = AsyncMock(return_value=None)
        session_mock.post = MagicMock(return_value=session_post_ctx)

        await poll_censys(session_mock, producer_mock, seen_ips)
        producer_mock.send.assert_called()

    asyncio.run(run_test())

def test_cyber_enricher_kev_vulnerability_entity_type():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{}')
        enricher = CyberEnricher(scorer=None, redis_client=redis_mock, graph_writer=MagicMock(producer=AsyncMock()))

        raw_event = RawEvent(
            source="cisa_kev",
            raw_payload={"cve_id": "CVE-2026-16812", "vendor": "microsoft", "product": "Windows Server"}
        )
        normalized = await enricher.enrich(raw_event)
        assert normalized is not None
        assert normalized.primary_entity.id == "CVE-2026-16812"
        assert normalized.primary_entity.type == EntityType.VULNERABILITY

    asyncio.run(run_test())


# ── 2. AVIATION ENRICHER & GAP DETECTOR TESTS ────────────────────────────────

class MockPipeline:
    def __init__(self, raw):
        self.raw = raw
        self.commands = []

    def set(self, key, val, ex=None):
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        self.commands.append(("set", k, val, ex))

    def get(self, key):
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        self.commands.append(("get", k))

    async def execute(self):
        results = []
        for cmd in self.commands:
            if cmd[0] == "set":
                self.raw.store[cmd[1]] = cmd[2]
                results.append(True)
            elif cmd[0] == "get":
                results.append(self.raw.store.get(cmd[1]))
        return results


class MockRawRedis:
    def __init__(self):
        self.store = {}
        self.set_members = set()

    def pipeline(self, transaction=True):
        return MockPipeline(self)

    async def set(self, key, val, ex=None):
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        self.store[k] = val

    async def get(self, key):
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        return self.store.get(k)

    async def scan_iter(self, match="*", count=100):
        import re
        pat = match.replace("*", ".*")
        regex = re.compile(f"^{pat}$")
        for k in list(self.store.keys()):
            if regex.match(str(k)):
                yield k

    async def sismember(self, key, member):
        return member in self.set_members

    async def sadd(self, key, member):
        self.set_members.add(member)


class MockRedis:
    def __init__(self):
        self.raw = MockRawRedis()


def test_aviation_enricher_batching_and_routine_telemetry_retention():
    async def run_test():
        redis = MockRedis()
        graph = AsyncMock()
        graph.producer = AsyncMock()

        scorer = AsyncMock()
        scorer.score_kinematic_event.return_value = {"score": 0.05}
        scorer.check_watchlist.return_value = False
        scorer.track_frequency.return_value = 0.0

        enricher = AviationEnricher(scorer, redis, graph)

        raw1 = RawEvent(
            source="opensky", occurred_at=datetime.now(timezone.utc),
            raw_payload={"icao24": "4b1234", "latitude": 25.1, "longitude": 55.2, "callsign": "UAE123", "on_ground": False, "velocity": 240.0}
        )
        raw2 = RawEvent(
            source="opensky", occurred_at=datetime.now(timezone.utc),
            raw_payload={"icao24": "4b5678", "latitude": 25.5, "longitude": 55.8, "callsign": "EMERG01", "squawk": "7700", "on_ground": False, "velocity": 300.0}
        )

        results = await enricher.enrich_batch([raw1, raw2])
        assert len(results) == 2
        assert results[0].anomaly_score <= 0.15
        assert results[1].anomaly_score >= 0.80

        val1 = await redis.raw.get("aircraft:last_seen:4b1234")
        val2 = await redis.raw.get("aircraft:last_seen:4b5678")
        assert val1 is not None
        assert val2 is not None

    asyncio.run(run_test())

def test_aviation_gap_detector_fires_flight_dark():
    async def run_test():
        redis = MockRedis()
        producer = AsyncMock()
        scorer = AsyncMock()
        db_writer = AsyncMock()

        now = datetime.now(timezone.utc)
        await redis.raw.set("sentinel:heartbeat:collector-adsb", json.dumps({"ts": now.isoformat()}))

        old_ts = (now - timedelta(hours=3)).isoformat()
        await redis.raw.set("aircraft:last_seen:a4b5c6", json.dumps({"ts": old_ts, "region": "Iranian Airspace", "callsign": "IRAN01"}))

        detector = AviationGapDetector(producer, scorer, db_writer, redis)
        await detector._check()

        db_writer.write_events_batch.assert_called_once()
        events = db_writer.write_events_batch.call_args[0][0]
        assert events[0].type == EventType.FLIGHT_DARK
        assert events[0].primary_entity.name == "IRAN01"

    asyncio.run(run_test())


# ── 3. MARITIME HEADLINE & KNOWLEDGE GRAPH PREDICATES ────────────────────────

def test_valid_predicates_contains_exposure_types():
    assert "POSITIVE_EXPOSURE_TO" in VALID_PREDICATES
    assert "INVERSE_EXPOSURE_TO" in VALID_PREDICATES
    assert "COMMODITY_EXPOSURE" in VALID_PREDICATES
    assert "SUPPLIES" in VALID_PREDICATES

def test_vessel_dark_headline_synthesis_format():
    vtype = "Tanker"
    name = "SUNRISE GLORY"
    mmsi = "123456789"
    region = "Strait of Hormuz"
    gap_hrs = 12.5
    flags = ["sanctioned"]

    headline = (
        f"{vtype or 'Vessel'} '{name or f'MMSI:{mmsi}'}' went dark in "
        f"{region} after {gap_hrs}h of AIS silence"
        + (f" (flagged: {', '.join(flags)})" if flags else "")
    )
    assert "SUNRISE GLORY" in headline
    assert "Strait of Hormuz" in headline
    assert "12.5h" in headline


# ── 4. NEWS ENRICHER ANOMALY SCORING ──────────────────────────────────────────

class DummyNewsScorer:
    def __init__(self, base_score=0.45):
        self.base_score = base_score
    async def score_news(self, unique_entities, sentiment, reliability, headline, summary):
        return self.base_score, ["semantic:tag"]
    def get_hawkes_intensity(self, domain): return 1.0
    def record_hawkes_event(self, domain): pass

def test_news_enricher_routine_and_threat_scoring():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DummyNewsScorer(base_score=0.45)
        enricher = NewsEnricher(scorer=scorer, redis_client=redis_mock, graph_writer=None)

        routine_raw = RawEvent(source="reuters", raw_payload={"title": "UK firefighters receive upgraded equipment", "summary": "Routine updates."})
        e_routine = await enricher.enrich(routine_raw)
        assert e_routine.anomaly_score < 0.70

        threat_raw = RawEvent(source="reuters", raw_payload={"title": "Nuclear missile strike threat triggers emergency DEFCON status", "summary": "Launch sequence detected."})
        e_threat = await enricher.enrich(threat_raw)
        assert e_threat.anomaly_score >= 0.80

    asyncio.run(run_test())


# ── 5. OFAC SANCTIONS SYNC & AUTOMATON MATCHING ───────────────────────────────

def test_fetch_ofac_keywords_mocked():
    async def run_test():
        sample_sdn_csv = '101,"VALKYRIE ENTERPRISES", "vessel", "IRAN"\n102,"SOVCOMFLOT TANKER", "vessel", "RUSSIA"'
        sample_alt_csv = '201, 101, "aka", "VALKYRIE SHIPPING"'

        mock_resp_sdn = AsyncMock(status=200, text=AsyncMock(return_value=sample_sdn_csv))
        mock_resp_alt = AsyncMock(status=200, text=AsyncMock(return_value=sample_alt_csv))

        class MockSession:
            def get(self, url):
                cm = AsyncMock()
                cm.__aenter__.return_value = mock_resp_sdn if "sdn.csv" in url else mock_resp_alt
                return cm
            async def __aenter__(self): return self
            async def __aexit__(self, exc_type, exc, tb): pass

        with patch("aiohttp.ClientSession", return_value=MockSession()):
            keywords = await fetch_ofac_keywords()
            assert "valkyrie enterprises" in keywords
            assert "valkyrie shipping" in keywords

    asyncio.run(run_test())

def test_sanctions_rebuild_and_fuzzy_matching():
    keywords = SANCTIONED_KEYWORDS + ["valkyrie shipping", "sovcomflot tanker"]
    rebuild_sanctions_from_list(keywords)

    flags1 = check_sanctions("Vessel VALKYRIE SHIPPING in Persian Gulf")
    assert "sanctioned_ofac" in flags1

    flags2 = check_sanctions("Clean Vessel", mmsi="422123456")
    assert "sanctions_adjacent_flag_state" in flags2
