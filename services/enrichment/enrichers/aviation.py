"""
services/enrichment/enrichers/aviation.py
 
THE AIR TRAFFIC CONTROLLER
==========================
Process:
  1. Ingests raw ADS-B vectors from OpenSky.
  2. Filters out "noise" (planes taxiing on the ground).
  3. Detects "Squawk" codes (International emergency signals).
  4. Enriches with geographic context (Is this plane over a conflict zone?).
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.kafka import Topics
from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType, FlightData,
)
from shared.utils.regions import classify_region, get_region_sensitivity_multiplier
from shared.utils.sanctions import check_sanctions

logger = logging.getLogger("enrichment.aviation")

EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}
# Map numeric codes to human-readable tags
SQUAWK_LABELS = {
    "7500": "hijacking",
    "7600": "radio_failure",
    "7700": "general_emergency",
}


class AviationEnricher:

    def __init__(self, scorer, redis_client, graph_writer, resolver=None):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer
        self.resolver = resolver

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        """Legacy single-event enrichment. Useful for fallback."""
        res = await self.enrich_batch([raw])
        return res[0] if res else None

    async def enrich_batch(self, events: list) -> list:
        """
        Main bulk entry point. Groups events, performs pipelined 
        external queries (Redis/Neo4j), and yields normalized events.
        """
        if not events: return []
        
        normalized_events = []
        ontology_proposals = []
        upsert_tasks = []
        redis_pipeline = self.redis.pipeline()
        
        for raw in events:
            p      = raw.raw_payload
            icao24 = (p.get("icao24") or "").strip()
            if not icao24:
                return None
    
            # 1. PARSE POSITION
            lat       = p.get("latitude")
            lon       = p.get("longitude")
            on_ground = p.get("on_ground", False)
    
            # FILTER: Skip surface reports.
            # We care about airborne threats/anomalies. Taxiing aircraft and
            # ground support vehicles (which also have transponders) create noise.
            if on_ground:
                return None
    
            callsign = (p.get("callsign") or "").strip() or None
            squawk   = str(p.get("squawk") or "").strip()
            
            # check if this is a known emergency code OR if the collector flagged it
            is_emerg = squawk in EMERGENCY_SQUAWKS or p.get("is_emergency", False)
            
            # Geo-tagging: "Where is this plane?" (e.g., "Taiwan Strait")
            region   = classify_region(lat, lon) if (lat and lon) else None
    
            # Determine event type (Position vs Anomaly)
            event_type = EventType.FLIGHT_ANOMALY if is_emerg else EventType.FLIGHT_POSITION
    
            # SANCTIONS CHECK
            country = p.get("origin_country", "")
            flags = check_sanctions(f"{callsign or ''} {country}", "")
            is_sanctioned = len(flags) > 0
            
            # 2. CALCULATE ANOMALY SCORE
            if is_emerg:
                # CRITICAL: Squawks override everything.
                score = {"7500": 1.0, "7700": 0.85, "7600": 0.70}.get(squawk, 0.60)
            elif is_sanctioned:
                score = 0.80 # High alert for OFAC sanctioned flight
            elif region:
                # REGIONAL RISK: Flying over a conflict zone increases the score.
                score = round((get_region_sensitivity_multiplier(region) - 1.0) * 0.15, 3)
            else:
                score = 0.0
    
            # 3. UPDATE GRAPH (Knowledge Base)
            # Upsert the Aircraft node in Neo4j so we track its history.
            upsert_tasks.append(self.graph.upsert_aircraft(icao24, {
                "callsign":       callsign,
                "origin_country": p.get("origin_country"),
            }))
    
            alt_m  = p.get("baro_altitude") or p.get("geo_altitude")
            alt_ft = round(alt_m * 3.28084) if alt_m else None
    
            ontology_proposals.append({
                "entity_id": icao24,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {
                    "label": "Aircraft", 
                    "primary_domain": "aviation",
                    "confidence": min(1.0, score + 0.5)
                }
            })
    
            # FIX: Update hot-cache state for downstream trackers (Native Async Pipeline)
            if lat and lon:
                redis_pipeline.set(
                    f"aircraft:last_seen:{icao24}",
                    json.dumps({
                        "lat": lat, 
                        "lon": lon, 
                        "alt": alt_ft,
                        "ts": datetime.now(timezone.utc).isoformat()
                    }),
                    ex=86400 # 24 hours
                )
                    
            # 4. TAGGING
            tags = []
            if region:
                tags.append(region.lower().replace(" ", "_"))
            if is_emerg:
                tags += [f"squawk_{squawk}", "aviation_emergency",
                         SQUAWK_LABELS.get(squawk, "emergency")]
            if is_sanctioned:
                tags += ["sanctioned_ofac"]
    
            cc = country[:2].upper() if len(country) >= 2 else None
    
            # 5. PRODUCE NORMALIZED EVENT
            normalized_events.append(NormalizedEvent(
                event_id=raw.event_id,
                type=event_type,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source,
                primary_entity=Entity(
                    id=icao24,
                    type=EntityType.AIRCRAFT,
                    name=callsign,
                    country_code=cc,
                    flags=flags,
                ),
                latitude=lat,
                longitude=lon,
                altitude_ft=alt_ft,
                region=region,
                flight_data=FlightData(
                    icao24=icao24,
                    callsign=callsign,
                    origin_country=p.get("origin_country"),
                    baro_altitude_m=p.get("baro_altitude"),
                    velocity_ms=p.get("velocity"),
                    true_track=p.get("true_track"),
                    vertical_rate=p.get("vertical_rate"),
                    on_ground=on_ground,
                    squawk=squawk or None,
                ),
                tags=tags,
                anomaly_score=round(min(1.0, score), 3),
            ))

        # EXECUTE BULK OPS
        if len(redis_pipeline.command_stack) > 0:
            try:
                await redis_pipeline.execute()
            except Exception as e:
                logger.error(f"Redis pipeline failed in aviation: {e}")

        if upsert_tasks:
            import asyncio
            await asyncio.gather(*upsert_tasks, return_exceptions=True)

        for prop in ontology_proposals:
            try:
                await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, prop, key=prop["entity_id"])
            except Exception as e:
                logger.error(f"Failed to send ontology proposal: {e}")

        return normalized_events