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

import logging
from datetime import datetime, timezone
from typing import Optional

from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType, FlightData,
)
from shared.utils.regions import classify_region, get_region_sensitivity_multiplier

logger = logging.getLogger("enrichment.aviation")

EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}
# Map numeric codes to human-readable tags
SQUAWK_LABELS = {
    "7500": "hijacking",
    "7600": "radio_failure",
    "7700": "general_emergency",
}


class AviationEnricher:

    def __init__(self, scorer, graph_writer):
        # Scorer: Calculates risk (0.0 - 1.0)
        # Graph: Updates the Neo4j database (Aircraft nodes)
        self.scorer = scorer
        self.graph  = graph_writer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        """
        Main processing loop for aviation data.
        Returns None if the event is irrelevant (e.g., ground traffic).
        """
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

        # 2. CALCULATE ANOMALY SCORE
        if is_emerg:
            # CRITICAL: Squawks override everything.
            # 7500 (Hijack) = 1.0 (Max Panic)
            # 7700 (General) = 0.85 (High Alert)
            score = {"7500": 1.0, "7700": 0.85, "7600": 0.70}.get(squawk, 0.60)
        elif region:
            # REGIONAL RISK: Flying over a conflict zone increases the score.
            # If multiplier is 3.0 (War zone), score becomes (3.0 - 1.0) * 0.15 = 0.30.
            score = round((get_region_sensitivity_multiplier(region) - 1.0) * 0.15, 3)
        else:
            score = 0.0

        # Determine event type (Position vs Anomaly)
        event_type = EventType.FLIGHT_ANOMALY if is_emerg else EventType.FLIGHT_POSITION

        # 3. UPDATE GRAPH (Knowledge Base)
        # Upsert the Aircraft node in Neo4j so we track its history.
        self.graph.upsert_aircraft(icao24, {
            "callsign":       callsign,
            "origin_country": p.get("origin_country"),
        })

        alt_m  = p.get("baro_altitude") or p.get("geo_altitude")
        alt_ft = round(alt_m * 3.28084) if alt_m else None

        # 4. TAGGING
        tags = []
        if region:
            tags.append(region.lower().replace(" ", "_"))
        if is_emerg:
            tags += [f"squawk_{squawk}", "aviation_emergency",
                     SQUAWK_LABELS.get(squawk, "emergency")]

        country = p.get("origin_country", "")
        cc      = country[:2].upper() if len(country) >= 2 else None

        # 5. PRODUCE NORMALIZED EVENT
        return NormalizedEvent(
            event_id=raw.event_id,
            type=event_type,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=Entity(
                id=icao24,
                type=EntityType.AIRCRAFT,
                name=callsign,
                country_code=cc,
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
        )