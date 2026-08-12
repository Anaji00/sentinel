"""
tests/test_api_gateway.py

Consolidated Test Suite: API Gateway Metrics, Route Helpers, Market Series & Raw/Normalized Event Models.
Combines:
  - test_api_gateway_metrics.py
  - test_entity_normalization_and_routes.py
  - test_market_series_route.py
  - test_events.py
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from pydantic import ValidationError
from fastapi.testclient import TestClient

from services.api_gateway.routes.main import app
from services.api_gateway.helpers import get_clean_entity_label
from services.api_gateway.routes.radar import get_market_series
from shared.models.events import RawEvent, NormalizedEvent, EventType, Entity, EntityType, CorrelationCluster, AlertTier, FinancialData, RawIngestEnvelope, compute_payload_hash
from shared.utils.equities import parse_occ_option_symbol


# ── 1. API GATEWAY METRICS & AUTH TEST ───────────────────────────────────────

def test_api_gateway_metrics_endpoints_bypass_auth():
    client = TestClient(app)
    res_metrics = client.get("/metrics")
    assert res_metrics.status_code == 200

    res_json = client.get("/metrics/json")
    assert res_json.status_code == 200
    assert isinstance(res_json.json(), dict)

    res_health_metrics = client.get("/api/v1/health/metrics")
    assert res_health_metrics.status_code == 200

def test_api_gateway_protected_endpoint_requires_auth():
    client = TestClient(app)
    res = client.get("/api/v1/events/recent")
    assert res.status_code == 403


# ── 2. ROUTE HELPERS & ENTITY NORMALIZATION ──────────────────────────────────

def test_get_clean_entity_label():
    assert get_clean_entity_label("Apple Inc", "AAPL") == "Apple Inc"
    assert get_clean_entity_label(None, "NVDA") == "NVDA"
    assert get_clean_entity_label(None, None) == "Global Entity"

def test_native_model_primary_entity_auto_derivation():
    cluster = CorrelationCluster(
        rule_id="RULE_01", rule_name="Test Rule", alert_tier=AlertTier.ALERT,
        trigger_event_id="evt_01", entity_ids=["NVDA"], entity_names=["NVIDIA Corp"],
        description="Test correlation"
    )
    assert cluster.primary_entity_id == "NVDA"
    assert cluster.primary_entity_name == "NVIDIA Corp"


# ── 3. MARKET SERIES ROUTE TEST ──────────────────────────────────────────────

def test_get_market_series_with_mock_db():
    async def _run():
        mock_db = AsyncMock()
        mock_db.query.return_value = [
            {"primary_entity_id": "BTCUSD", "primary_entity_name": "Bitcoin / USD", "occurred_at": "2026-08-05T12:00:00Z", "anomaly_score": 0.75, "financial_data": {"current_price": 67800.0, "volume": 12500}, "crypto_data": None},
            {"primary_entity_id": "TLT", "primary_entity_name": "iShares 20+ Year Treasury Bond ETF", "occurred_at": "2026-08-05T12:00:00Z", "anomaly_score": 0.40, "financial_data": {"current_price": 92.80, "volume": 45000}, "crypto_data": None}
        ]
        res = await get_market_series(symbols="BTCUSD,TLT", limit=20, db=mock_db)
        assert "BTCUSD" in res["symbols"]
        assert "TLT" in res["symbols"]
        assert "BTCUSD" in res["series"]

    asyncio.run(_run())


# ── 4. RAW & NORMALIZED EVENT MODEL CONTRACTS ────────────────────────────────

def test_raw_event_auto_generates_fields():
    payload = {"source": "news_scraper", "raw_payload": {"title": "Test Headline"}}
    event = RawEvent(**payload)
    assert event.event_id is not None
    assert event.source == "news_scraper"
    assert event.envelope.payload_hash != ""

def test_raw_ingest_envelope_auto_generates_fields_and_sha256_hash():
    raw_payload = {"ticker": "AAPL", "price": 180.5, "volume": 1000}
    env = RawIngestEnvelope(source_id="alpaca_options", payload=raw_payload)
    assert env.source_id == "alpaca_options"
    assert env.payload_hash == compute_payload_hash(raw_payload)

def test_normalized_event_rejects_invalid_anomaly_score():
    valid_data = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000", "type": EventType.HEADLINE,
        "occurred_at": datetime.now(timezone.utc), "collected_at": datetime.now(timezone.utc),
        "source": "reuters", "primary_entity": {"id": "ent_1", "name": "Taiwan", "type": "country", "flags": []},
        "anomaly_score": 1.5
    }
    with pytest.raises(ValidationError) as exc_info:
        NormalizedEvent(**valid_data)
    assert "anomaly_score" in str(exc_info.value)

def test_parse_occ_option_symbol():
    call_res = parse_occ_option_symbol("AAPL240816C00220000")
    assert call_res == {"ticker": "AAPL", "expiry": "2024-08-16", "option_type": "CALL", "strike": 220.0}

def test_payload_readability_and_summaries():
    now = datetime.now(timezone.utc)
    event = NormalizedEvent(
        type=EventType.OPTIONS_FLOW, occurred_at=now, source="alpaca_options",
        primary_entity=Entity(id="AAPL", name="Apple Inc.", type=EntityType.COMPANY),
        headline="Apple Inc $1.5M Call Option Sweep", anomaly_score=0.88,
        financial_data=FinancialData(ticker="AAPL", premium_usd=1500000.0)
    )
    summary_text = event.to_readable_summary()
    assert "[OPTIONS_FLOW]" in summary_text
    assert "Apple Inc." in summary_text
