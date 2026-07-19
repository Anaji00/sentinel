# tests/test_events.py
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from pydantic import ValidationError
from shared.models.events import RawEvent, NormalizedEvent, EventType


def test_raw_event_auto_generates_fields():
    """Test that RawEvent automatically generates an ID and Timestamp if missing."""
    payload = {
        "source": "news_scraper",
        "raw_payload": {"title": "Test Headline"}
    }
    
    event = RawEvent(**payload)
    
    assert event.event_id is not None
    assert type(event.event_id) == str
    assert event.collected_at is not None
    assert event.source == "news_scraper"


def test_normalized_event_rejects_invalid_anomaly_score():
    """Test that the data contract rejects scores outside the 0.0-1.0 range."""
    # Build a valid baseline event
    valid_data = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000",
        "type": EventType.HEADLINE,
        "occurred_at": datetime.now(timezone.utc),
        "collected_at": datetime.now(timezone.utc),
        "source": "reuters",
        "primary_entity": {"id": "ent_1", "name": "Taiwan", "type": "country", "flags": []},
        "anomaly_score": 1.5  # <--- INVALID SCORE! Must be 0.0 to 1.0
    }
    
    # Pydantic should instantly throw a ValidationError
    with pytest.raises(ValidationError) as exc_info:
        NormalizedEvent(**valid_data)
    
    # Verify the error is specifically about the anomaly_score
    assert "anomaly_score" in str(exc_info.value)


def test_options_flow_enrichment():
    """Verify that TradFiEnricher correctly processes options flow raw events."""
    from services.enrichment.enrichers.tradfi import TradFiEnricher
    
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{}')
        graph_mock = MagicMock()
        graph_mock.producer = AsyncMock()
        scorer_mock = MagicMock()
        scorer_mock.check_watchlist = AsyncMock(return_value=True)
        scorer_mock.track_frequency = AsyncMock(return_value=0.10)
        
        enricher = TradFiEnricher(scorer=scorer_mock, redis_client=redis_mock, graph_writer=graph_mock)
        
        raw_event = RawEvent(
            source="alpaca_options",
            raw_payload={
                "ticker": "AAPL",
                "option_symbol": "AAPL240621C00200000",
                "price": 5.20,
                "volume": 200.0,
                "premium_usd": 104000.0
            }
        )
        
        normalized = await enricher.enrich(raw_event)
        
        assert normalized is not None
        assert normalized.type == EventType.OPTIONS_FLOW
        assert normalized.primary_entity.id == "AAPL"
        assert normalized.financial_data.instrument_type == "option"
        assert normalized.financial_data.premium_usd == 104000.0
        assert "options_sweep" in normalized.tags
        assert normalized.anomaly_score > 0.0
        
    asyncio.run(run_test())