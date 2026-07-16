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
from typing import Optional, List
 
from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
 
logger = logging.getLogger("enrichment.gap_detector")
 
DARK_THRESHOLDS = {
    "Strait of Hormuz":    1,
    "Iranian Territorial": 1,
    "Bab-el-Mandeb":       1,
    "Suez Canal":          4,
    "Taiwan Strait":       4,
    "North Korean Waters": 4,
    "Persian Gulf":        6,
    "Black Sea":           6,
    "Ukrainian Waters":    6,
    "Red Sea":             8,
    "South China Sea":     2,
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
            await asyncio.sleep(300) # Additional sleep to prevent tight loop on errors
    async def _check(self):
        now = datetime.now(timezone.utc)
        fired = 0
        has_keys = False
        batch_events_to_write: List[NormalizedEvent] = []
        
        # 1. Gather keys in batches to prevent event loop starvation
        batch_size = 500
        current_keys = []
        
        # Pull larger chunks from Redis to reduce round-trips
        async for key in self.redis.raw.scan_iter("vessel:last_seen:*", count=1000):
            has_keys = True
            current_keys.append(key)
            
            if len(current_keys) >= batch_size:
                fired_in_batch, events = await self._process_batch(current_keys, now)
                fired += fired_in_batch
                batch_events_to_write.extend(events)
                current_keys = []
                
        # Process remainder
        if current_keys:
            fired_in_batch, events = await self._process_batch(current_keys, now)
            fired += fired_in_batch
            batch_events_to_write.extend(events)

        if batch_events_to_write:
            await self.db_writer.write_events_batch(batch_events_to_write)

        if fired:
            logger.info(f"Gap detector: {fired} VESSEL_DARK events safely emitted & persisted.")
        elif not has_keys:
            logger.info("Gap Detector: No vessels tracked in Redis, skipping check")
    
    async def _process_batch(self, keys: List[str], now: datetime):
        fired = 0
        events_to_write = []
        
        # 2. Redis Pipeline: Fetch ALL last_seen payloads in 1 round trip
        pipe = self.redis.raw.pipeline()
        for k in keys:
            pipe.get(k)
        last_seen_results = await pipe.execute()
        
        anomalous_mmsis = []
        parsed_data = {}
        
        for key, raw_val in zip(keys, last_seen_results):
            if not raw_val: continue
            
            try:
                val = json.loads(raw_val) if isinstance(raw_val, (str, bytes)) else raw_val
                if not isinstance(val, dict): continue
                
                mmsi = key.replace("vessel:last_seen:", "")
                ts_str = val.get("ts", "")
                region = val.get("region")
                
                last_seen = datetime.fromisoformat(ts_str)
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
                    
                gap_hours = (now - last_seen).total_seconds() / 3600
                threshold = DARK_THRESHOLDS.get(region, DARK_THRESHOLDS["Default"])
                
                # 3. FIXED DEDUPLICATION LOGIC: Self-Healing
                dedup_key = f"{mmsi}:{region}"
                if gap_hours < threshold:
                    # Ship is active. Remove from seen_gaps if it was previously dark.
                    self._seen_gaps.discard(dedup_key)
                    continue
                    
                if dedup_key in self._seen_gaps:
                    continue # Ship is STILL dark. Do not fire a duplicate alert.
                    
                self._seen_gaps.add(dedup_key)
                
                # Queue for enrichment
                anomalous_mmsis.append(mmsi)
                parsed_data[mmsi] = {"val": val, "gap": gap_hours, "region": region}
                
            except Exception as e:
                logger.debug(f"Failed parsing key {key}: {e}")

        if not anomalous_mmsis:
            return 0, []

        # 4. Redis Pipeline: Fetch ALL info payloads for anomalous ships in 1 round trip
        pipe = self.redis.raw.pipeline()
        for mmsi in anomalous_mmsis:
            pipe.get(f"vessel:info:{mmsi}")
        info_results = await pipe.execute()
        
        # 5. Process and Emit
        for mmsi, info_raw in zip(anomalous_mmsis, info_results):
            data = parsed_data[mmsi]
            val = data["val"]
            info = json.loads(info_raw) if info_raw else {}
            
            flags = info.get("flags", []) if isinstance(info, dict) else []
            vtype = info.get("vessel_type", "Unknown") if isinstance(info, dict) else "Unknown"
            
            score = await self.scorer.score_vessel_dark(
                mmsi, data["gap"], data["region"], flags, val.get("heading", 0)
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

            events_to_write.append(event)
            await self.producer.send(Topics.ENRICHED_EVENTS, event.model_dump(), key=mmsi)
            fired += 1

        return fired, events_to_write