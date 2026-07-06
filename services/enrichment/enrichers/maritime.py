"""
services/enrichment/enrichers/maritime.py
 
THE TRANSLATOR
==============
Converts raw AIS (Automatic Identification System) messages into rich,
structured events that the rest of the system can understand.
"""
 
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List
 
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
from shared.utils.regions import classify_region, decode_nav_status, decode_vessel_type
from shared.utils.sanctions import check_sanctions
 
logger = logging.getLogger("enrichment.maritime")
 
class MaritimeEnricher:
    # ── STRICT DI ALIGNMENT ──
    def __init__(self, scorer, redis_client, graph_writer, resolver=None):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer
        self.resolver = resolver

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        payload = raw.raw_payload
        msg_type = payload.get("MessageType", "")
        meta = payload.get("MetaData", {})
        mmsi = str(meta.get("MMSI", "")).strip()

        if not mmsi or mmsi == "0":
            return None 
        
        if msg_type == "PositionReport":
            return await self._position(raw, payload, meta, mmsi)
        elif msg_type == "ShipStaticData":
            return await self._static(raw, payload, meta, mmsi)
        return None
    
    # ── Position ──────────────────────────────────────────────────────────────

    async def _position(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        pos = payload.get("Message", {}).get("PositionReport", {})
        lat = pos.get("Latitude")
        lon = pos.get("Longitude")
        if lat is None or lon is None:
            return None
        
        speed = float(pos.get("Sog") or 0)
        heading = int(pos.get("TrueHeading") or 0)
        nav_status = decode_nav_status(pos.get("NavigationalStatus") or 0)
        region = classify_region(lat, lon)

        vessel = await self._get_vessel(mmsi, meta)
        flags = vessel.get("flags", [])
        vtype = vessel.get("vessel_type", "Unknown")
        
        # ── CRITICAL FIX: ROUTE THROUGH MASTER SCORER ──
        is_sanctioned = 1.0 if any("sanctioned" in f.lower() for f in flags) else 0.0
        features = [speed, is_sanctioned, 0.5, 0.0, 0.0]
        
        # Call score_event and extract the score from the returned dictionary
        scoring_result = await self.scorer.score_event("vessel_position", mmsi, features)
        anomaly = scoring_result.get("score", 0.0)
        # ───────────────────────────────────────────────

        await self.redis.raw.set(
            f"vessel:last_seen:{mmsi}",
            json.dumps({
                "lat": lat, "lon": lon, "heading": heading,
                "region": region, "speed": speed, "ts": (raw.occurred_at or datetime.now(timezone.utc)).isoformat(),
            }),
            ex = 172800 
        )
        
        await self.graph.upsert_vessel(mmsi, {
            "name": vessel.get("name", ""),
            "vessel_type": vtype,
            "flag_state": vessel.get("flag_state", ""),
            "flags": flags,
        })

        return NormalizedEvent(
            event_id = raw.event_id,
            type = EventType.VESSEL_POSITION,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            primary_entity = Entity(
                id = mmsi, type=EntityType.VESSEL, 
                name=vessel.get("name") or meta.get("ShipName", ""),
                flags=flags,
            ),
            latitude = lat,
            longitude = lon,
            region = region,
            vessel_data = VesselData(
                mmsi=mmsi, 
                speed_knots = speed,
                heading=heading,
                nav_status=nav_status,
                vessel_type = vtype,
                flag_state = vessel.get("flag_state"),
                destination = vessel.get("destination"),
            ),
            tags = self._tags(region, vtype, flags),
            anomaly_score = anomaly,
        )
    
    # ── Static ────────────────────────────────────────────────────────────────
    async def _static(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        s = payload.get("Message", {}).get("ShipStaticData", {})
        name = str(s.get("Name", meta.get("ShipName", ""))).strip()
        dest = str(s.get("Destination", "")).strip()
        code = int(s.get("Type") or 0)
        vtype = decode_vessel_type(code)
        
        flags = check_sanctions(name, mmsi)

        await self.redis.raw.set(
            f"vessel:info:{mmsi}",
            json.dumps({ "name": name, "destination": dest,
                        "vessel_type": vtype, "flags": flags }),
            ex = 864000 
        )
        await self.graph.upsert_vessel(mmsi, {"name": name, "vessel_type": vtype, "flags": flags})

        return NormalizedEvent(
            event_id = raw.event_id,
            type = EventType.VESSEL_STATIC,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            primary_entity = Entity(id=mmsi, type=EntityType.VESSEL, name=name, flags=flags),
            vessel_data = VesselData(mmsi=mmsi, vessel_type=vtype, destination=dest, cargo_type=code),
            tags = [vtype.lower(), "static_data"],
            anomaly_score = 0.0,
        )

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def _get_vessel(self, mmsi: str, meta: dict) -> dict:
        if self.resolver:
            try:
                return await self.resolver.resolve_vessel(mmsi, ais_meta=meta)
            except Exception as e:
                logger.debug(f"Resolver failed for {mmsi}: {e}")
        
        cached = await self.redis.raw.get(f"vessel:info:{mmsi}")
        if cached:
            return json.loads(cached)
        name = meta.get("ShipName", "")
        return {
            "name":        name,
            "vessel_type": "Unknown",
            "flags":       check_sanctions(name, mmsi),
            "flag_state":  "",
        }
 
    def _tags(self, region, vtype, flags) -> list:
        tags = []
        if region:
            tags.append(region.lower().replace(" ", "_"))
        if vtype and vtype != "Unknown":
            tags.append(vtype.lower())
        if any("sanctioned" in f for f in flags):
            tags.append("sanctions_risk")
        return tags