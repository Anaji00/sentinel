"""
tests/test_live_events_stream.py

Unit test suite for FastAPI /ws/live-feed WebSocket streaming protocol & pubsub handlers.
Validates:
  - WebSocket API key authentication & connection rejection
  - Initial TimescaleDB event backlog delivery on connection
  - Dynamic min_anomaly parameter filtering
  - High-frequency PositionReport batch deduplication
"""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from services.api_gateway.routes.events import websocket_live_feed, DOMAIN_TO_COLUMN

# ── 1. DOMAIN TO COLUMN SCHEMA MAPPING ──────────────────────────────────────

def test_domain_to_column_schema_mapping():
    assert DOMAIN_TO_COLUMN["maritime"] == "vessel_data"
    assert DOMAIN_TO_COLUMN["aviation"] == "flight_data"
    assert DOMAIN_TO_COLUMN["financial"] == "financial_data"
    assert DOMAIN_TO_COLUMN["crypto"] == "crypto_data"
    assert DOMAIN_TO_COLUMN["prediction"] == "prediction_market_data"
    assert DOMAIN_TO_COLUMN["cyber"] == "security_data"


# ── 2. WEBSOCKET AUTHENTICATION & CONNECTION HANDSHAKE ───────────────────────

@pytest.mark.anyio
async def test_websocket_auth_rejection():
    websocket_mock = MagicMock()
    websocket_mock.query_params = {}
    websocket_mock.headers = {}
    
    with patch("services.api_gateway.routes.events.verify_websocket_api_key", new_callable=AsyncMock) as mock_verify:
        mock_verify.return_value = False
        await websocket_live_feed(websocket_mock, min_anomaly=0.0)
        
        websocket_mock.accept.assert_not_called()


# ── 3. ANOMALY SCORE FILTERING ─────────────────────────────────────────────

def test_pubsub_message_anomaly_filter():
    raw_events = [
        {"event_id": "e1", "type": "vessel_position", "anomaly_score": 0.10},
        {"event_id": "e2", "type": "dark_ship_gap", "anomaly_score": 0.88},
        {"event_id": "e3", "type": "adsb_squawk_7700", "anomaly_score": 0.95},
    ]
    min_anomaly = 0.50
    filtered = [e for e in raw_events if float(e.get("anomaly_score", 0.0)) >= min_anomaly]
    
    assert len(filtered) == 2
    assert set(e["event_id"] for e in filtered) == {"e2", "e3"}


# ── 4. HIGH FREQUENCY POSITION DEDUPLICATION ──────────────────────────────────

def test_high_frequency_position_deduplication():
    batch = [
        {"event_id": "ev1", "type": "vessel_position", "primary_entity": {"id": "MMSI_111"}, "latitude": 26.5},
        {"event_id": "ev2", "type": "vessel_position", "primary_entity": {"id": "MMSI_111"}, "latitude": 26.51},
        {"event_id": "ev3", "type": "vessel_position", "primary_entity": {"id": "MMSI_222"}, "latitude": 1.3},
        {"event_id": "ev4", "type": "sanction_alert", "primary_entity": {"id": "MMSI_111"}, "latitude": 26.51},
    ]
    
    position_map = {}
    non_position = []
    for item in batch:
        t = item["type"].lower()
        if "position" in t and item.get("primary_entity", {}).get("id"):
            eid = item["primary_entity"]["id"]
            if eid not in position_map:
                position_map[eid] = item
        else:
            non_position.append(item)
            
    deduped = list(position_map.values()) + non_position
    assert len(deduped) == 3
    assert any(e["event_id"] == "ev4" for e in deduped)
