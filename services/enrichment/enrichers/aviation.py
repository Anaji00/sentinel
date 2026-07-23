"""
services/enrichment/enrichers/aviation.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.models import NormalizedEvent, EventType, Entity, EntityType
from shared.utils.regions import classify_region
from shared.utils.sanctions import check_sanctions
from shared.kafka import Topics

logger = logging.getLogger("enrichment.aviation")

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
        """
        Enriches a single aviation event.
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
        if on_ground:
            return None

        callsign = (p.get("callsign") or "").strip() or None
        squawk   = str(p.get("squawk") or "").strip()
        
        # check if this is a known emergency code OR if the collector flagged it
        is_emerg = squawk in SQUAWK_LABELS or p.get("is_emergency", False)
        
        # Geo-tagging
        region   = classify_region(lat, lon) if (lat and lon) else None

        event_type = EventType.FLIGHT_ANOMALY if is_emerg else EventType.FLIGHT_POSITION

        # SANCTIONS CHECK
        country = p.get("origin_country", "")
        flags = check_sanctions(f"{callsign or ''} {country}", "")
        is_sanctioned = len(flags) > 0
        
        # 2. CALCULATE ANOMALY SCORE
        if is_emerg:
            score = {"7500": 1.0, "7700": 0.85, "7600": 0.70}.get(squawk, 0.60)
        elif is_sanctioned:
            score = 0.80 # High alert for OFAC sanctioned flight
        else:
            # Baseline flight behavior (no ML scoring for simple positions)
            score = 0.10

        is_watched = await self.scorer.check_watchlist(icao24, "aircraft") or (callsign and await self.scorer.check_watchlist(callsign, "aircraft"))
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(icao24, "aviation_position")
        score = min(1.0, score + w_boost + f_boost)

        # Only process high priority events to reduce noise
        if score < 0.6:
            return None

        alt_ft = int(p.get("baro_altitude", 0) * 3.28084) if p.get("baro_altitude") else 0
        
        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": icao24,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {
                "label": "Aircraft",
                "primary_domain": "physical",
                "properties": {
                    "callsign": callsign,
                    "origin_country": country,
                },
                "confidence": score
            }
        }, key=icao24)

        if lat and lon:
            await self.redis.raw.set(
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
        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=event_type,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=Entity(
                id=icao24,
                type=EntityType.AIRCRAFT,
                name=callsign or f"FLIGHT_{icao24}",
                country_code=cc,
                flags=flags,
            ),
            latitude=lat,
            longitude=lon,
            region=region,
            headline=f"Aviation Alert: {callsign or icao24} | {SQUAWK_LABELS.get(squawk, 'Alert')}",
            tags=tags,
            anomaly_score=score,
        )