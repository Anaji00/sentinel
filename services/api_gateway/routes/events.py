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
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends, WebSocket, WebSocketDisconnect
from services.api_gateway.dependencies import get_db, get_redis_client, verify_websocket_api_key

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

from datetime import datetime

@router.get("/{domain}")
async def get_domain_events(
    domain: str, 
    limit: int = Query(50, le=500),
    min_anomaly: float = Query(0.0, ge=0.0, le=1.0),
    start_time: Optional[str] = Query(None, description="Optional ISO start timestamp"),
    end_time: Optional[str] = Query(None, description="Optional ISO end timestamp"),
    db = Depends(get_db)
):
    """Dynamic endpoint to fetch events for a specific domain or all domains with time-window filtering."""
    domain = domain.lower()
    
    try:
        where_clauses = ["anomaly_score >= $1"]
        params = [min_anomaly]
        param_idx = 2

        if start_time:
            where_clauses.append(f"occurred_at >= ${param_idx}")
            params.append(datetime.fromisoformat(start_time.replace("Z", "+00:00")))
            param_idx += 1

        if end_time:
            where_clauses.append(f"occurred_at <= ${param_idx}")
            params.append(datetime.fromisoformat(end_time.replace("Z", "+00:00")))
            param_idx += 1

        where_sql = " AND ".join(where_clauses)
        limit_idx = f"${param_idx}"
        params.append(limit)

        if domain == "all" or domain not in DOMAIN_TO_COLUMN or domain == "news":
            query = f"""
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name, primary_entity_name as entity_name, region, anomaly_score, 
                       COALESCE(latitude, ST_Y(coordinates::geometry)) as latitude, 
                       COALESCE(longitude, ST_X(coordinates::geometry)) as longitude, 
                       summary as domain_data
                FROM events 
                WHERE {where_sql}
                ORDER BY occurred_at DESC LIMIT {limit_idx}
            """
        else:
            # `target_column` comes from the fixed DOMAIN_TO_COLUMN mapping, never
            # from the request, so interpolating it carries no injection risk.
            target_column = DOMAIN_TO_COLUMN[domain]

            # Restrict to rows that actually belong to this domain. Without this
            # the endpoint projected the domain column but filtered on nothing,
            # so /events/maritime returned the newest events of ANY type -- news
            # headlines with an empty domain_data -- and the map had no
            # coordinates to plot even though vessel rows existed.
            domain_sql = (
                f"{where_sql} AND {target_column} IS NOT NULL "
                f"AND {target_column}::text NOT IN ('{{}}', 'null')"
            )
            query = f"""
                SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name, primary_entity_name as entity_name, region, anomaly_score, 
                       COALESCE(latitude, ST_Y(coordinates::geometry)) as latitude, 
                       COALESCE(longitude, ST_X(coordinates::geometry)) as longitude, 
                       {target_column} as domain_data
                FROM events 
                WHERE {domain_sql}
                ORDER BY occurred_at DESC LIMIT {limit_idx}
            """

        return await db.query(query, *params)
    
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
        
        event_dict = dict(result[0])
        # Optimize payload: ensure top-level latitude & longitude are populated
        vd = event_dict.get("vessel_data")
        fd = event_dict.get("flight_data")
        if event_dict.get("latitude") is None:
            if isinstance(vd, dict) and vd.get("latitude") is not None:
                event_dict["latitude"] = vd["latitude"]
            elif isinstance(fd, dict) and fd.get("latitude") is not None:
                event_dict["latitude"] = fd["latitude"]
        if event_dict.get("longitude") is None:
            if isinstance(vd, dict) and vd.get("longitude") is not None:
                event_dict["longitude"] = vd["longitude"]
            elif isinstance(fd, dict) and fd.get("longitude") is not None:
                event_dict["longitude"] = fd["longitude"]

        return event_dict
        
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
    # Authenticate BEFORE accepting the connection (Phase 1.1)
    if not await verify_websocket_api_key(websocket):
        return
    await websocket.accept()
    redis = websocket.app.state.redis
    db = getattr(websocket.app.state, "db", None)
    pubsub = redis.raw.pubsub()
    await pubsub.subscribe("sentinel:events:live")
    logger.info("Client connected to /ws/live-feed streaming endpoint.")
    
    # 1. Immediately send recent multi-domain live events from TimescaleDB on connection
    if db:
        try:
            recent_rows = await db.query(
                """
                WITH ranked_events AS (
                    SELECT event_id, type, occurred_at, primary_entity_id, primary_entity_name,
                           primary_entity_name as entity_name, region, anomaly_score, source, summary as headline,
                           tags, crypto_data, financial_data, vessel_data, flight_data, security_data, prediction_market_data,
                           ST_Y(coordinates::geometry) as latitude, ST_X(coordinates::geometry) as longitude,
                           ROW_NUMBER() OVER (PARTITION BY type ORDER BY occurred_at DESC) as rn
                    FROM events
                )
                SELECT * FROM ranked_events WHERE rn <= 4
                ORDER BY occurred_at DESC
                LIMIT 30;
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
                    "primary_entity_id": str(r["primary_entity_id"] or e_name),
                    "tags": r.get("tags") or [],
                    "crypto_data": r.get("crypto_data"),
                    "financial_data": r.get("financial_data"),
                    "vessel_data": r.get("vessel_data"),
                    "flight_data": r.get("flight_data"),
                    "security_data": r.get("security_data"),
                    "prediction_market_data": r.get("prediction_market_data"),
                    "latitude": r.get("latitude"),
                    "longitude": r.get("longitude")
                }
                await websocket.send_json(evt)
                await asyncio.sleep(0.05)
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
        await pubsub.close()