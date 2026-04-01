"""
services/enrichment/enrichers/maritime.py
 
THE TRANSLATOR
==============
Converts raw AIS (Automatic Identification System) messages into rich,
structured events that the rest of the system can understand.

Handles AIS PositionReport and ShipStaticData messages.
 
FIX (code review): MaritimeEnricher now accepts and uses EntityResolver.
  Previously resolver was instantiated in main.py but never passed anywhere —
  the vessel lookup went directly to Redis, skipping the Neo4j ownership chain
  and the full MMSI→country mapping in entity_resolver.py.
  Now: resolver.resolve_vessel() → Redis → Neo4j → inline fallback.
 
FIX (code review): check_sanctions() and MMSI_COUNTRY moved to shared/utils/
  to eliminate the duplicate definition between this file and entity_resolver.
  Both files now import from shared.utils.sanctions.
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
    def __init__(self, scorer, graph_writer, db_writer, redis_client, resolver=None):
        # DEPENDENCY INJECTION:
        # We pass in all the tools this class needs so it doesn't create them itself.
        # scorer:       Calculates anomaly scores (The Judge).
        # graph_writer: Updates Neo4j (The Graph).
        # db_writer:    Updates TimescaleDB (The Archive).
        # redis:        Fast caching.
        # resolver:     Finds vessel details (The Detective).
        self.scorer = scorer
        self.graph_writer = graph_writer
        self.db_writer = db_writer
        self.redis = redis_client
        self.resolver = resolver # Used for vessel lookup

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        """
        Main entry point.
        Decides if the message is a Moving Report or a Static Info Report.
        """
        payload = raw.raw_payload
        msg_type = payload.get("MessageType", "")
        meta = payload.get("MetaData", {})
        mmsi = payload.get("MMSI", "").strip()

        # Filter: Garbage data often has MMSI 0 or empty.
        if not mmsi or mmsi == "0":
            return None  # Invalid MMSI, skip
        
        if msg_type == "PositionReport":
            return self._position(raw, payload, meta, mmsi)
        elif msg_type == "ShipStaticData":
            return self._static(raw, payload, meta, mmsi)
        return None
    
    # ── Position ──────────────────────────────────────────────────────────────

    def _position(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        # 1. PARSE RAW DATA
        # Extract speed, heading, location from the raw JSON.
        pos = payload.get("Message", {}).get("PositionReport", {})
        lat = pos.get("Latitude")
        lon = pos.get("Longitude")
        if lat is None or lon is None:
            return None  # Invalid position, skip
        
        speed = float(pos.get("Sog") or 0)
        heading = int(pos.get("TrueHeading") or 0)
        nav_status = decode_nav_status(pos.get("NavigationalStatus") or 0)
        region = classify_region(lat, lon)

        # 2. RESOLVE IDENTITY (Who is this?)
        # We ask the resolver: "Who owns MMSI 123456789?"
        # Returns a dict with name, type, flags, etc.
        vessel = self._get_vessel(mmsi, meta)
        flags = vessel.get("flags", [])
        vtype = vessel.get("vessel_type", "Unknown")
        
        # 3. SCORE ANOMALY (Is this weird?)
        # "Is a Tanker moving at 20 knots in the Strait of Hormuz suspicious?"
        anomaly = self.scorer.score_vessel_position(mmsi, speed, region, flags, nav_status, vtype)

        # 4. UPDATE BASELINE
        # "Remember that this ship usually moves at X speed."
        self.scorer.update_vessel_baseline(mmsi, vtype, speed)

        # 5. WRITE TO DB (The Archive)
        # Store the raw ping in the massive TimescaleDB table.
        self.db_writer.write_vessel_position(
            mmsi, lat, lon, speed, heading, nav_status,
            raw.occurred_at or datetime.now(timezone.utc),
        )

        # 6. UPDATE CACHE (The Hot Path)
        # Save "Last Seen" to Redis. This is used by the Gap Detector to find
        # ships that suddenly go dark.
        self.redis.set(
            f"vessel:last_seen:{mmsi}",
            json.dumps({
                "lat": lat, "lon": lon, "heading": heading,
                "region": region, "speed": speed, "ts": (raw.occurred_at or datetime.now(timezone.utc)).isoformat(),
            }),
            ttl = 172800 # 48 hours (if we don't see it for 2 days, the key expires)
        )
        
        # 7. UPDATE GRAPH (The Knowledge Base)
        # Update the Node in Neo4j. This is "upsert" - creates it if missing.
        self.graph_writer.upsert_vessel(mmsi, {
            "name": vessel.get("name", ""),
            "vessel_type": vtype,
            "flag_state": vessel.get("flag_state", ""),
            "flags": flags,
        })

        # 8. PRODUCE EVENT
        # Create the standardized "NormalizedEvent" to send down the pipeline.
        # This is what the Correlation Engine will look at.
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
    def _static(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        # Static messages contain the Ship Name, Destination, Dimensions, etc.
        # They don't update position, but they update Identity.
        s = payload.get("Message", {}).get("ShipStaticData", {})
        name = s.get("Name", meta.get("ShipName", "")).strip()
        dest = s.get("Destination", "").strip()
        code = int(s.get("Type") or 0)
        vtype = decode_vessel_type(code)
        
        # Check if the new name is on a sanctions list (e.g., ship changed name).
        flags = check_sanctions(name, mmsi)

        # Update Redis Cache with new identity info.
        self.redis.set(
            f"vessel:info:{mmsi}",
            json.dumps({ "name": name, "destination": dest,
                        "vessel_type": vtype, "flags": flags }),
            ttl = 864000 # 24 hours
        )
        # Update Graph with new identity info.
        self.graph.upsert_vessel(mmsi, {"name": name, "vessel_type": vtype, "flags": flags})

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

    def _get_vessel(self, mmsi: str, meta: dict) -> dict:
        """
        Determines who a vessel is.
        Strategy:
          1. Ask EntityResolver (Redis Cache -> Neo4j Graph).
          2. If that fails, build a temporary profile from the raw AIS data.
        """
        if self.resolver:
            try:
                return self.resolver.resolve_vessel(mmsi, ais_meta=meta)
            except Exception as e:
                logger.debug(f"Resolver failed for {mmsi}: {e}")
        # Inline fallback — same logic as before
        cached = self.redis.get(f"vessel:info:{mmsi}")
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