# tests/test_events.py
import pytest
from datetime import datetime, timezone
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
        "type": EventType.NEWS_GEOPOLITICAL,
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