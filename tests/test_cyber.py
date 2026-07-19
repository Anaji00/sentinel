import asyncio
import pytest
import time
import os
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from services.enrichment.enrichers.cyber import CyberEnricher, CyberThreatScorer
from shared.models import RawEvent, NormalizedEvent
from shared.kafka import Topics

# Dynamically import collector-cyber/main.py due to hyphen in folder name
cyber_collector_path = Path(__file__).resolve().parents[1] / "services" / "collector-cyber" / "main.py"
spec = importlib.util.spec_from_file_location("collector_cyber_main", cyber_collector_path)
collector_cyber_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector_cyber_main)

poll_censys = collector_cyber_main.poll_censys
poll_cisa_kev = collector_cyber_main.poll_cisa_kev
poll_ransomware = collector_cyber_main.poll_ransomware

def test_cyber_scorer_threshold_caching():
    """Verify that CyberThreatScorer caches configurations and reduces Redis roundtrips."""
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{"cyber": {"cyber_exposure": {"ics_score": 0.85}}}')
        
        scorer = CyberThreatScorer(redis_mock)
        
        # Initial load (cache miss)
        await scorer.load_thresholds()
        assert scorer.cfg.get("cyber_exposure", {}).get("ics_score") == 0.85
        assert redis_mock.raw.get.call_count == 1
        
        # Second load (cache hit - within TTL)
        await scorer.load_thresholds()
        assert redis_mock.raw.get.call_count == 1  # call count remains 1
        
        # Force cache expiration by setting last_loaded to 0
        scorer.last_loaded = 0.0
        await scorer.load_thresholds()
        assert redis_mock.raw.get.call_count == 2  # call count incremented
        
    asyncio.run(run_test())


def test_cyber_enricher_exposure():
    """Verify that CyberEnricher correctly processes exposed infrastructure raw events."""
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{}')
        graph_mock = MagicMock()
        graph_mock.producer = AsyncMock()
        
        enricher = CyberEnricher(scorer=None, redis_client=redis_mock, graph_writer=graph_mock)
        
        raw_event = RawEvent(
            source="censys",
            raw_payload={
                "ip_address": "1.2.3.4",
                "port": 502,
                "protocol": "modbus",
                "product": "Siemens S7-1200",
                "org": "Siemens Energy Grid Corp", # Contains "siemens" (ICS vendor) and "grid" (critical org)
                "country_code": "US"
            }
        )
        
        normalized = await enricher.enrich(raw_event)
        
        assert normalized is not None
        assert normalized.anomaly_score == 0.80  # Elevated ICS Vendor score
        assert "critical_infrastructure" in normalized.tags
        assert "ics_vendor" in normalized.tags
        assert normalized.primary_entity.id == "1.2.3.4"
        assert int(normalized.security_data.port) == 502
        
    asyncio.run(run_test())


def test_collector_concurrency_and_awaiting():
    """Verify that collector poll tasks gather and await all event publications."""
    async def run_test():
        session_mock = MagicMock()
        producer_mock = AsyncMock()
        seen_ips = set()
        
        # Set mock environment variables for Censys auth bypass directly in the module
        collector_cyber_main.CENSYS_API_ID = "mock_id"
        collector_cyber_main.CENSYS_API_SECRET = "mock_secret"
        
        # Mock Censys hosts search response
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={
            "result": {
                "hits": [
                    {
                        "ip": "1.1.1.1",
                        "autonomous_system": {"name": "Test AS", "country_code": "US"},
                        "services": [{"port": 502, "service_name": "modbus"}]
                    }
                ]
            }
        })
        
        # Setup context manager mocking for session.post
        session_post_ctx = MagicMock()
        session_post_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        session_post_ctx.__aexit__ = AsyncMock(return_value=None)
        session_mock.post = MagicMock(return_value=session_post_ctx)
        
        # Trigger Censys polling
        await poll_censys(session_mock, producer_mock, seen_ips)
        
        # Verify that the message was sent and awaited via gather
        producer_mock.send.assert_called()
        args, kwargs = producer_mock.send.call_args
        assert args[0] == Topics.RAW_CYBER
        assert kwargs.get("key") == "1.1.1.1"

    asyncio.run(run_test())
