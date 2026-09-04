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

# Where the empirical gap distribution for each region is kept.
#
# An absolute hour count cannot say whether silence is unusual. ADS-B is
# received by ground stations, so coverage -- not the aircraft -- decides
# whether a position is heard. Over open ocean there are no receivers, and a
# multi-hour gap is the normal state of every flight on the route. The previous
# score, min(1.0, 0.60 + gap_hours/10.0), reached the ceiling at four hours and
# pinned 8,064 of 9,056 events at exactly 1.00 in six hours -- one maximally
# anomalous aircraft every 2.7 seconds.
#
# The question is not "how long has it been silent" but "how long is silent, for
# here". That is measurable: record what gaps this region actually produces and
# score against them. A gap at the median of its own region is by definition
# unremarkable, however many hours it is.
_GAP_SAMPLES_KEY = "sentinel:aviation:gap_samples:{region}"

# Samples retained per region, and the minimum before the distribution is
# allowed to decide anything.
_GAP_SAMPLE_CAP = 500
_MIN_GAP_SAMPLES = 60

# How long an already-reported gap stays reported.
#
# Long enough to survive a deploy, which is the failure this replaces, and
# short enough that an aircraft dark for days is eventually raised again.
SEEN_GAP_TTL_SEC = 7 * 86400

# Where a gap has to sit in its region's own distribution before it is an event.
# At or below the median it is what this airspace does all day.
_NOTABLE_PERCENTILE = 0.90

# Score band for gaps that clear the percentile bar. The ceiling is deliberately
# not 1.0: an aircraft losing ADS-B is a prompt to look, not a certainty, and
# the top of the scale should be reachable by something that is.
_SCORE_FLOOR = 0.35
_SCORE_CEILING = 0.92


def _percentile_rank(samples: List[float], value: float) -> float:
    """Fraction of observed gaps in this region shorter than `value`."""
    if not samples:
        return 0.0
    below = sum(1 for s in samples if s < value)
    return below / len(samples)


class AviationGapDetector:
    def __init__(self, producer, scorer, db_writer, redis_client):
        self.producer = producer
        self.scorer = scorer
        self.db_writer = db_writer
        self.redis = redis_client
        # Deduplication lives in Redis, not in this process.
        #
        # An in-memory set is empty after every restart, so the first scan of a
        # new container re-emits every aircraft currently in a gap as though it
        # had just gone dark. Measured: 200 events in one hour, 262 in the next
        # and 15 in a single second on the first scan after a deploy, against a
        # steady-state rate of one or two an hour. 462 of 464 "detections" in a
        # six-hour window were an artifact of redeploying, and they are
        # indistinguishable in the database from real ones.
        #
        # The TTL is what makes an aircraft eligible to be reported again after
        # it has genuinely returned and gone dark a second time.
        self._seen_key = "sentinel:aviation:seen_gaps"

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
                payload = degraded_event.model_dump(mode="json")
                # See the note at the emission site below: alerts.outbound has
                # no consumer, so a dark event that goes only there is invisible
                # to every rule written about it.
                await self.producer.send(Topics.ENRICHED_EVENTS, payload)
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

    async def _load_region_samples(self) -> Dict[str, List[float]]:
        """The observed gap distribution for every region, newest first.

        Read once per batch rather than per aircraft: a scan covers thousands of
        keys and there are a dozen regions.
        """
        regions = set(DARK_THRESHOLDS_AIRCRAFT) | {"Default"}
        pipe = self.redis.raw.pipeline()
        ordered = sorted(regions)
        for region in ordered:
            pipe.lrange(_GAP_SAMPLES_KEY.format(region=region), 0, _GAP_SAMPLE_CAP - 1)
        rows = await pipe.execute()

        out: Dict[str, List[float]] = {}
        for region, raw_samples in zip(ordered, rows):
            values = []
            for item in raw_samples or []:
                try:
                    values.append(float(item))
                except (TypeError, ValueError):
                    continue
            out[region] = values
        return out

    async def _record_gaps(self, observations: List[tuple]) -> None:
        """Adds this scan's gaps to each region's distribution.

        Every gap is recorded, not only the ones that fired. A distribution
        built from alerts alone is truncated at the threshold and would report
        every observation as extreme -- the same circularity that let an
        absolute cutoff call routine oceanic silence maximally anomalous.
        """
        if not observations:
            return
        pipe = self.redis.raw.pipeline()
        for region, gap_hours in observations:
            key = _GAP_SAMPLES_KEY.format(region=region)
            pipe.lpush(key, gap_hours)
            pipe.ltrim(key, 0, _GAP_SAMPLE_CAP - 1)
            # Long enough to survive a restart, short enough that a route or
            # receiver change is not argued against by last month's coverage.
            pipe.expire(key, 30 * 86400)
        try:
            await pipe.execute()
        except Exception as e:
            logger.debug(f"Failed recording aviation gap samples: {e}")

    async def _process_batch(self, keys: List[str], now: datetime):
        fired = 0
        unmeasured = 0
        ordinary = 0
        events_to_write = []
        observations: List[tuple] = []
        region_samples = await self._load_region_samples()

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
                
                # Recorded before any threshold is applied, so the
                # distribution describes the airspace rather than the alerts.
                if 0.0 <= gap_hours < 48.0:
                    observations.append((region, round(gap_hours, 3)))

                dedup_key = f"{icao24}:{region}"
                if gap_hours < threshold:
                    # Back inside its normal range: forget it, so a later gap is
                    # reported as the new event it is.
                    try:
                        await self.redis.raw.hdel(self._seen_key, dedup_key)
                    except Exception:
                        pass
                    continue

                # Marked seen only once an event is actually emitted, not here.
                #
                # The set was recorded before the percentile test, so an
                # aircraft whose gap was ordinary for its airspace was still
                # marked as reported -- and when that same aircraft later went
                # genuinely dark, it was skipped as a duplicate of an alert that
                # was never raised. The suppression outlived the reason for it.
                try:
                    if await self.redis.raw.hexists(self._seen_key, dedup_key):
                        continue
                except Exception:
                    pass
                
                # How unusual is this silence, for this airspace?
                samples = region_samples.get(region, [])
                if len(samples) < _MIN_GAP_SAMPLES:
                    # Not enough history to say. Refusing is the honest output:
                    # the alternative is asserting an anomaly from a threshold
                    # nobody measured, which is what produced 8,064 ceilings.
                    unmeasured += 1
                    continue

                rank = _percentile_rank(samples, gap_hours)
                if rank < _NOTABLE_PERCENTILE:
                    # More ordinary than 90% of what this region produces.
                    ordinary += 1
                    continue

                # Rescale the surviving tail across the score band, so the
                # ranking within it still carries information.
                tail = (rank - _NOTABLE_PERCENTILE) / max(1e-9, 1.0 - _NOTABLE_PERCENTILE)
                score = round(_SCORE_FLOOR + tail * (_SCORE_CEILING - _SCORE_FLOOR), 4)

                event = NormalizedEvent(
                    type=EventType.FLIGHT_DARK,
                    occurred_at=now,
                    source="aviation_gap_detector",
                    primary_entity=Entity(
                        id=icao24,
                        name=callsign or icao24.upper(),
                        type=EntityType.AIRCRAFT,
                        flags=["dark_aircraft", f"gap_{gap_hours:.1f}h", f"pctile_{rank:.2f}"]
                    ),
                    headline=(
                        f"Aircraft '{callsign or icao24.upper()}' silent {gap_hours:.1f}h in {region} "
                        f"— longer than {rank * 100:.0f}% of gaps observed here"
                    ),
                    anomaly_score=score,
                    region=region
                )
                
                try:
                    await self.redis.raw.hset(self._seen_key, dedup_key, now.isoformat())
                    await self.redis.raw.expire(self._seen_key, SEEN_GAP_TTL_SEC)
                except Exception as e:
                    logger.debug(f"Could not record seen gap {dedup_key}: {e}")

                events_to_write.append(event)
                if self.producer:
                    # Dark events reach the correlation window as well as the
                    # alert topic.
                    #
                    # These were sent only to Topics.ALERTS (alerts.outbound),
                    # which has no consumer anywhere in the tree, while the
                    # correlation service subscribes to ENRICHED_EVENTS alone.
                    # Measured: 0 of 214,360 correlation-window members were
                    # flight_dark, and the chokepoint rule's 1,640 supporting
                    # events were 100% flight_anomaly despite its clause naming
                    # flight_dark explicitly. The detector this audit spent its
                    # longest stretch recalibrating was emitting into a dead end.
                    # All three ALERTS sends are gone -- this one, the
                    # heartbeat-degraded send above, and the matching pair in
                    # gap_detector.py. ENRICHED_EVENTS is what carries these,
                    # and was already carrying them: the dual-send was the
                    # transitional half of an earlier repair, kept while it was
                    # unclear whether anything downstream still wanted the
                    # alert topic. Nothing does.
                    #
                    # alerts.outbound has accumulated 143,580 messages
                    # and has zero consumer groups -- verified against the
                    # broker, not inferred from the tree. Writing to it costs
                    # broker storage and replication for a topic nothing reads.
                    #
                    # Kept as a constant rather than deleted: if an alerting
                    # consumer is ever built, this is the topic it subscribes
                    # to, and the alert manager is the service that would do it.
                    # Subscribing it today would be wrong -- it would alert on
                    # every dark vessel and aircraft, thousands a day, which is
                    # the noise its rate limiter exists to suppress.
                    payload = event.model_dump(mode="json")
                    await self.producer.send(Topics.ENRICHED_EVENTS, payload)
                fired += 1
                
            except Exception as e:
                logger.debug(f"Failed parsing aircraft key {key}: {e}")

        await self._record_gaps(observations)
        if unmeasured or ordinary:
            logger.debug(
                "Aviation gaps: %s fired, %s ordinary for their region, "
                "%s with too little regional history to judge.",
                fired, ordinary, unmeasured,
            )
        return fired, events_to_write
