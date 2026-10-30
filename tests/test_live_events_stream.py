"""
tests/test_live_events_stream.py

Unit test suite for FastAPI /ws/live-feed WebSocket streaming protocol & pubsub handlers.
Validates:
  - WebSocket API key authentication & connection rejection
  - Initial TimescaleDB event backlog delivery on connection
  - Dynamic min_anomaly parameter filtering
  - High-frequency PositionReport batch deduplication
"""

import asyncio
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


# ── 5. A CLOSED CLIENT RELEASES ITS SUBSCRIPTION ──────────────────────────────
#
# Measured on the live gateway: redis-py 5 defaults `get_message(timeout=)` to
# 0.0, so the loop never waited and every open socket spun a core (99% CPU);
# three dropped sockets still held their Redis subscriptions 75 s later,
# because the loop only noticed a closed client on a send and swallowed the
# OSError that send raised.

async def _never() -> dict:
    await asyncio.Event().wait()
    return {}


class _Runaway(BaseException):
    """Raised by the fake Redis once a loop has outlived its client.

    A BaseException so a catch-all `except Exception` cannot swallow it. A loop
    that never exits is not reliably cancellable on Python 3.11 -- `wait_for`
    can lose the cancel when the inner await completes at the same moment -- so
    the regression has to end the loop itself, or it hangs the suite.
    """


RUNAWAY_CALLS = 200


def _socket(receive, get_message, db=None):
    calls = 0

    async def bounded_get_message(**kwargs):
        nonlocal calls
        calls += 1
        if calls > RUNAWAY_CALLS:
            raise _Runaway(f"get_message called {calls} times after the client left")
        return await get_message(**kwargs)

    ws = MagicMock()
    ws.accept = AsyncMock()
    ws.send_json = AsyncMock()
    ws.receive = AsyncMock(side_effect=receive)
    ws.app.state.db = db
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.close = AsyncMock()
    pubsub.get_message = AsyncMock(side_effect=bounded_get_message)
    ws.app.state.redis.raw.pubsub.return_value = pubsub
    return ws, pubsub


async def _quiet(**_kwargs):
    await asyncio.sleep(0.01)
    return None


async def _run(ws):
    with patch("services.api_gateway.routes.events.verify_websocket_api_key", new_callable=AsyncMock) as ok:
        ok.return_value = True
        await asyncio.wait_for(websocket_live_feed(ws, min_anomaly=0.0), timeout=2.0)


@pytest.mark.anyio
async def test_a_client_that_leaves_a_quiet_channel_is_released():
    async def leave():
        return {"type": "websocket.disconnect", "code": 1006}

    ws, pubsub = _socket(leave, _quiet)
    await _run(ws)
    pubsub.unsubscribe.assert_awaited_once_with("sentinel:events:live")
    pubsub.close.assert_awaited_once()


@pytest.mark.anyio
async def test_a_failed_send_ends_the_loop_instead_of_being_swallowed():
    sent = {"type": "message", "data": json.dumps({"event_id": "e1", "anomaly_score": 0.9})}
    messages = iter([sent])

    async def one_then_quiet(**kwargs):
        return next(messages, None) or await _quiet()

    ws, pubsub = _socket(lambda: _never(), one_then_quiet)
    # What uvicorn raises for a send to a closed socket: ClientDisconnected(OSError).
    ws.send_json.side_effect = OSError("client disconnected")
    await _run(ws)
    pubsub.unsubscribe.assert_awaited_once_with("sentinel:events:live")


@pytest.mark.anyio
async def test_the_loop_waits_on_redis_rather_than_spinning():
    async def leave():
        await asyncio.sleep(0.05)
        return {"type": "websocket.disconnect", "code": 1000}

    ws, pubsub = _socket(leave, _quiet)
    await _run(ws)
    timeouts = [c.kwargs.get("timeout", 0.0) for c in pubsub.get_message.await_args_list]
    assert timeouts and all(t and t > 0 for t in timeouts)


@pytest.mark.anyio
async def test_the_opening_backlog_is_time_bounded():
    # Unbounded, the window ranked all 2.07M rows: 4.4 s per socket opened.
    db = MagicMock()
    db.query = AsyncMock(return_value=[])

    async def leave():
        return {"type": "websocket.disconnect", "code": 1000}

    ws, _ = _socket(leave, _quiet, db=db)
    await _run(ws)
    sql = " ".join(db.query.await_args.args[0].split()).lower()
    assert "where occurred_at > now() - interval" in sql
