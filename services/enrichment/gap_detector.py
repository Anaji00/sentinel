"""
services/enrichment/gap_detector.py
 
Runs as an asyncio background task inside the enrichment service.
Every 15 minutes, scans Redis for vessels last seen in watch zones.
If silence exceeds the region threshold → emits a VESSEL_DARK event.
 
FIX (code review): Redis KEYS command replaced with SCAN.
  redis.keys("vessel:last_seen:*") is O(N) across all Redis keys and blocks
  the Redis server for the entire scan duration. With thousands of tracked
  vessels and other keys this causes latency spikes every 15 minutes.
  scan_iter() iterates in small batches (default 10 keys per round-trip)
  and yields keys lazily without blocking Redis.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional
 
from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
 
logger = logging.getLogger("enrichment.gap_detector")
 
DARK_THRESHOLDS = {
    "Strait of Hormuz":    4,
    "Iranian Territorial": 4,
    "Bab-el-Mandeb":       4,
    "Suez Canal":          4,
    "Taiwan Strait":       4,
    "North Korean Waters": 4,
    "Persian Gulf":        6,
    "Black Sea":           6,
    "Ukrainian Waters":    6,
    "Red Sea":             8,
    "South China Sea":     8,
    "Default":             24,
}

class VesselGapDetector:
    def __init__(self, producer, scorer, db_writer, redis_client):
        self.producer = producer
        self.scorer = scorer
        self.db_writer = db_writer
        self.redis = redis_client
        self._seen_gaps = set()  # To avoid duplicate gap events for the same vessel

    async def run(self):
        logger.info("Starting Vessel Gap Detector background task")
        while True:
            await asyncio.sleep(900)  # Wait 15 minutes between scans
            try:
                await self._check()
            except Exception as e:
                logger.error(f"Error in gap detector: {e}", exc_info=True)
    async def _check(self):
        keys = list(self.redis.raw.scan_iter("vessel:last_seen:*"))
        if not keys:
            logger.info("Gap Detector: No vessels tracked in Redis, skipping check")
            return
        now = datetime.now(timezone.utc)
        fired = 0

        for key in keys:
            try:
                mmsi = key.replace("vessel:last_seen:", "")
                raw_val = self.redis.get(key)
                if not raw_val:
                    continue
                val = json.loads(raw_val)
                ts_str = val.get("ts", "")
                region = val.get("region")
                try:
                    last_seen = datetime.fromisoformat(ts_str)
                    if last_seen.tzinfo is None:
                        last_seen = last_seen.replace(tzinfo=timezone.utc)
                    gap_hours = (now - last_seen).total_seconds() / 3600
                except ValueError:
                    continue

                threshold = DARK_THRESHOLDS.get(region, DARK_THRESHOLDS["Default"])
                if gap_hours < threshold:
                    continue  # Not dark yet

                # Deduplicate: bucket by integer hour — fire at most once per hour
                bucket = int(gap_hours)
                dedup_key = f"{mmsi}:{region}:{bucket}"
                if dedup_key in self._seen_gaps:
                    continue  # Already fired for this vessel-region-hour
                self._seen_gaps.add(dedup_key)

                # trim the set to prevent unbounded growth (keep last 1000 entries)
                if len(self._seen_gaps) > 10_000:
                    self._seen_gaps = set(list(self._seen_gaps)[-5_000:])
                
                info_raw = self.redis.get(f"vessel:info:{mmsi}")
                info = json.loads(info_raw) if info_raw else {}
                flags = info.get("flags", [])
                vtype = info.get("vessel_type", "Unknown")
                score = self.scorer.score_vessel_dark(
                    mmsi, gap_hours, region, val.get("heading"), flags
                )
                event = NormalizedEvent(
                    type=EventType.VESSEL_DARK, 
                    occurred_at=now, 
                    source="gap_detector", 
                    primary_entity=Entity(
                        id=mmsi, 
                        type=EntityType.VESSEL, 
                        name=info.get("name", f"MMSI:{mmsi}"),
                        flags=flags,
                    ),
                    latitude = val.get("lat"),
                    longitude = val.get("lon"),
                    region = region,
                    vessel_data = VesselData(
                        mmsi=mmsi, 
                        gap_hours = round(gap_hours, 1), 
                        last_seen_region = region, 
                        heading=val.get("heading"), 
                        vessel_type = vtype,
                    ),
                    tags = list(filter(None, [
                        "dark_vessel",
                        "ais_gap", 
                        region.lower().replace(" ", "_") if region else None,
                    ])),
                    anomaly_score=score,
                )

                self.db.write_event(event)
                self.producer.send(Topics.ENRICHED_EVENTS, event.dict(), key=mmsi)
                fired += 1

                if score >= 0.6:
                    logger.warning(
                        f"🚢 VESSEL DARK  {event.primary_entity.name} "
                        f"gap={gap_hours:.1f}h  region={region}  score={score:.2f}"
                    )
            
            except Exception as e:
                logger.error(f"Gap check error for key {key}; {e})")

 
        if fired:
            logger.info(f"Gap detector: {fired} VESSEL_DARK events emitted")
        
        