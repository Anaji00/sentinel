"""
services/api_gateway/routes/events.py

This file defines the API endpoints for raw domain events.
It allows users to fetch lists of recent events filtered by domain (maritime, cyber, etc.)
and anomaly score, or dive deep into the full JSON payload of a single specific event.

ARCHITECTURAL UPGRADES:
- Removed `async def` from routes performing synchronous `db.query()` operations. 
  FastAPI will now safely offload these to a background thread pool, preventing ASGI Event Loop starvation.
- Updated DOMAIN_TO_COLUMN mapping to support the full Multi-Domain architecture.
"""

import asyncio
import json
import logging
from fastapi import APIRouter, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from services.api_gateway.dependencies import get_db, get_redis_client

logger = logging.getLogger("api-gateway.events")
router = APIRouter(prefix="/api/v1/events", tags=["Domain Events"])

# ARCHITECTURAL FIX: Explicitly map API Domain routes to strict DB Schema Columns
# Updated to support Sentinel's full multi-domain expansion
DOMAIN_TO_COLUMN = {
    "maritime": "vessel_data",
    "aviation": "flight_data",
    "financial": "financial_data",
    "tradfi": "financial_data",          # Route traditional finance to the financial_data column
    "crypto": "crypto_data",             # New Multi-Domain Schema
    "prediction": "prediction_market_data", # New Multi-Domain Schema
    "cyber": "security_data",
    "news": "headline" # News doesn't have a JSONB column, it uses the headline/summary natively
}

@router.get("/{domain}")
async def get_domain_events(
    domain: str, 
    limit: int = Query(50, le=500),
    min_anomaly: float = Query(0.0, ge=0.0, le=1.0),
    db = Depends(get_db)
):
    """Dynamic endpoint to fetch events for a specific domain or all domains."""
    domain = domain.lower()
    
    try:
        if domain == "all" or domain not in DOMAIN_TO_COLUMN:
            query = """
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name, primary_entity_name as entity_name, region, anomaly_score, latitude, longitude, summary as domain_data
                FROM events 
                WHERE anomaly_score >= $1
                ORDER BY occurred_at DESC LIMIT $2
            """
            return await db.query(query, min_anomaly, limit)
            
        target_column = DOMAIN_TO_COLUMN[domain]
        if domain == "news":
            query = """
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name, primary_entity_name as entity_name, region, anomaly_score, latitude, longitude, summary as domain_data
                FROM events 
                WHERE anomaly_score >= $1
                ORDER BY occurred_at DESC LIMIT $2
            """
            return await db.query(query, min_anomaly, limit)
        else:
            query = f"""
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name, primary_entity_name as entity_name, region, anomaly_score, latitude, longitude, COALESCE({target_column}, '{{}}'::jsonb) as domain_data
                FROM events 
                WHERE anomaly_score >= $1
                ORDER BY occurred_at DESC LIMIT $2
            """
            return await db.query(query, min_anomaly, limit)
    
    except Exception as e:
        logger.error(f"Failed to fetch {domain} events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database query failed")
    
from typing import Optional

@router.get("/detail/{event_id}")
async def get_event_detail(
    event_id: str, 
    lookback_days: Optional[int] = Query(None, ge=1, le=365, description="Optional lookback window in days to constrain hypertable chunk scans"),
    db = Depends(get_db)
):
    """
    Fetch the complete JSON payload for a single specific event.
    """
    try:
        if lookback_days:
            query = f"SELECT * FROM events WHERE event_id = $1 AND occurred_at >= NOW() - INTERVAL '{int(lookback_days)} days'"
            result = await db.query(query, event_id)
        else:
            result = await db.query("SELECT * FROM events WHERE event_id = $1", event_id)

        if not result:
            raise HTTPException(status_code=404, detail="Event not found")
        
        return result[0]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch event details: {e}")
        raise HTTPException(status_code=500, detail="Database query failed")


# ── REAL-TIME WEBSOCKET LIVE FEED ─────────────────────────────────────────────

from fastapi import WebSocket, WebSocketDisconnect
import json

@router.websocket("/ws/live-feed")
async def websocket_live_feed(websocket: WebSocket, min_anomaly: float = Query(0.0)):
    """Real-time WebSocket event stream for zero-latency dashboard visualization."""
    await websocket.accept()
    redis = websocket.app.state.redis
    db = getattr(websocket.app.state, "db", None)
    pubsub = redis.raw.pubsub()
    await pubsub.subscribe("sentinel:events:live")
    logger.info("Client connected to /ws/live-feed streaming endpoint.")
    
    # 1. Immediately send recent live events from TimescaleDB on connection
    if db:
        try:
            recent_rows = await db.query(
                """
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name,
                       primary_entity_name as entity_name, region, anomaly_score, source, summary as headline
                FROM events
                ORDER BY occurred_at DESC
                LIMIT 5;
                """
            )
            for r in recent_rows:
                e_name = r["primary_entity_name"] or r["primary_entity_id"] or "ENTITY_LIVE"
                evt = {
                    "event_id": str(r["event_id"]),
                    "type": r["type"],
                    "occurred_at": r["occurred_at"].isoformat() if hasattr(r["occurred_at"], "isoformat") else str(r["occurred_at"]),
                    "source": r["source"] or "sentinel_stream",
                    "anomaly_score": float(r["anomaly_score"] or 0.5),
                    "region": r["region"] or "Global",
                    "headline": r["headline"] or f"Event {r['event_id']} recorded",
                    "primary_entity_name": e_name,
                    "entity_name": e_name,
                    "primary_entity_id": str(r["primary_entity_id"] or e_name)
                }
                await websocket.send_json(evt)
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.warning(f"Failed to fetch initial events for WebSocket connection: {e}")

    # 2. Continuous real-time streaming loop
    try:
        while True:
            try:
                message = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=4.0)
                if message and message["type"] == "message":
                    data = json.loads(message["data"])
                    score = float(data.get("anomaly_score", 0.0) or 0.0)
                    if score >= min_anomaly:
                        await websocket.send_json(data)
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break
            except Exception as ex:
                logger.debug(f"PubSub message error: {ex}")
    except WebSocketDisconnect:
        logger.info("Client disconnected from /ws/live-feed")
    except Exception as e:
        logger.error(f"WebSocket live feed error: {e}")
    finally:
        await pubsub.unsubscribe("sentinel:events:live")