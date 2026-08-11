"""
services/enrichment/aviation_gap_detector.py

Runs as an asyncio background task inside the enrichment service.
Scans Redis for aircraft last seen in watch zones.
If silence exceeds threshold → emits a FLIGHT_DARK event.

Structural mirror of VesselGapDetector, calibrated for aviation reporting frequencies.
Integrates with shared heartbeat checks for collector-adsb to suppress false dark floods.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict

from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType
from shared.utils.heartbeat import is_component_healthy

logger = logging.getLogger("enrichment.aviation_gap_detector")

# Aviation dark gap thresholds in hours (aircraft transmit far more frequently than ships)
DARK_THRESHOLDS_AIRCRAFT = {
    "Strait of Hormuz":     0.5,
    "Iranian Airspace":     0.5,
    "Bab-el-Mandeb":        0.5,
    "Ukrainian Airspace":   0.5,
    "Israeli Airspace":     0.5,
    "Black Sea":            1.0,
    "South China Sea":      1.0,
    "Taiwan Strait":        1.0,
    "Red Sea":              1.0,
    "Default":              2.0,
}

class AviationGapDetector:
    def __init__(self, producer, scorer, db_writer, redis_client):
        self.producer = producer
        self.scorer = scorer
        self.db_writer = db_writer
        self.redis = redis_client
        self._seen_gaps = set()  # Deduplication set: {icao24}:{region}

    async def run(self):
        logger.info("Starting Aviation Gap Detector background task")
        while True:
            await asyncio.sleep(600)  # Check every 10 minutes
            try:
                await self._check()
            except Exception as e:
                logger.error(f"Error in aviation gap detector: {e}", exc_info=True)
            await asyncio.sleep(120)

    async def _check(self):
        now = datetime.now(timezone.utc)

        # 1. Liveness check: Verify collector-adsb is healthy before scanning
        is_healthy = await is_component_healthy(self.redis, "collector-adsb", max_staleness_seconds=300)
        if not is_healthy:
            logger.warning("Collector collector-adsb heartbeat is stale/missing. Suppressing FLIGHT_DARK scan.")
            degraded_event = NormalizedEvent(
                type=EventType.INFRASTRUCTURE_DEGRADED,
                occurred_at=now,
                source="aviation_gap_detector",
                primary_entity=Entity(id="collector-adsb", name="collector-adsb", type=EntityType.INFRASTRUCTURE, flags=["heartbeat_stale"]),
                headline="ADS-B Collector heartbeat stale — suppressing false FLIGHT_DARK alerts",
                anomaly_score=0.85,
                region="Global"
            )
            if self.db_writer:
                await self.db_writer.write_events_batch([degraded_event])
            if self.producer:
                await self.producer.send(Topics.ALERTS, degraded_event.model_dump(mode="json"))
            return

        fired = 0
        has_keys = False
        batch_events_to_write: List[NormalizedEvent] = []
        
        batch_size = 500
        current_keys = []
        
        async for key in self.redis.raw.scan_iter("aircraft:last_seen:*", count=1000):
            has_keys = True
            current_keys.append(key)
            
            if len(current_keys) >= batch_size:
                fired_in_batch, events = await self._process_batch(current_keys, now)
                fired += fired_in_batch
                batch_events_to_write.extend(events)
                current_keys = []
                
        if current_keys:
            fired_in_batch, events = await self._process_batch(current_keys, now)
            fired += fired_in_batch
            batch_events_to_write.extend(events)

        if batch_events_to_write:
            await self.db_writer.write_events_batch(batch_events_to_write)

        if fired:
            logger.info(f"Aviation Gap Detector: {fired} FLIGHT_DARK events safely emitted & persisted.")
        elif not has_keys:
            logger.info("Aviation Gap Detector: No aircraft tracked in Redis, skipping check.")

    async def _process_batch(self, keys: List[str], now: datetime):
        fired = 0
        events_to_write = []
        
        pipe = self.redis.raw.pipeline()
        for k in keys:
            pipe.get(k)
        last_seen_results = await pipe.execute()
        
        for key, raw_val in zip(keys, last_seen_results):
            if not raw_val:
                continue
            
            try:
                val = json.loads(raw_val) if isinstance(raw_val, (str, bytes)) else raw_val
                if not isinstance(val, dict):
                    continue
                
                icao24 = key.replace("aircraft:last_seen:", "")
                ts_str = val.get("ts", "")
                region = val.get("region", "Default")
                callsign = val.get("callsign", "")
                
                if not ts_str:
                    continue
                    
                last_seen = datetime.fromisoformat(ts_str)
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                    
                gap_hours = (now - last_seen).total_seconds() / 3600.0
                threshold = DARK_THRESHOLDS_AIRCRAFT.get(region, DARK_THRESHOLDS_AIRCRAFT["Default"])
                
                dedup_key = f"{icao24}:{region}"
                if gap_hours < threshold:
                    self._seen_gaps.discard(dedup_key)
                    continue
                    
                if dedup_key in self._seen_gaps:
                    continue
                    
                self._seen_gaps.add(dedup_key)
                
                event = NormalizedEvent(
                    type=EventType.FLIGHT_DARK,
                    occurred_at=now,
                    source="aviation_gap_detector",
                    primary_entity=Entity(
                        id=icao24,
                        name=callsign or icao24.upper(),
                        type=EntityType.AIRCRAFT,
                        flags=["dark_aircraft", f"gap_{gap_hours:.1f}h"]
                    ),
                    headline=f"Aircraft '{callsign or icao24.upper()}' went dark in {region} (silent for {gap_hours:.1f}h)",
                    anomaly_score=min(1.0, 0.60 + (gap_hours / 10.0)),
                    region=region
                )
                
                events_to_write.append(event)
                if self.producer:
                    await self.producer.send(Topics.ALERTS, event.model_dump(mode="json"))
                fired += 1
                
            except Exception as e:
                logger.debug(f"Failed parsing aircraft key {key}: {e}")

        return fired, events_to_write
