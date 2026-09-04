"""
services/enrichment/enrichers/aviation.py

ADS-B Aviation Enricher
Translates raw ADS-B telemetry state vectors into NormalizedEvent instances.
Features:
  - Batching via Redis pipelining & asyncio.gather parallel scoring.
  - Routine telemetry guard: Sub-threshold events write calm baselines (capped at score 0.15) instead of dropping.
  - Ontology proposals & aircraft:last_seen updates in Redis for dark flight gap detection.
"""

import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, List

from shared.models import NormalizedEvent, EventType, Entity, EntityType, FlightData
from shared.utils.regions import classify_region
from shared.utils.sanctions import check_sanctions
from shared.utils.regions import routine_band_score
from shared.kafka import Topics
from shared.utils.source_scorecard import baseline_reliability

from services.enrichment.anomaly_scorer import lift_score

logger = logging.getLogger("enrichment.aviation")

# How hard a sanctions match lifts an aircraft's measured behaviour.
#
# A lift, not a replacement: a sanctioned aircraft flying an ordinary cruise
# should not rank with one manoeuvring off its filed route, and the flat 0.80
# this replaced meant they always did.
SANCTIONED_LIFT_WEIGHT = 0.55



def _as_float(v) -> Optional[float]:
    """OpenSky sends nulls and occasional strings; a bad field must not drop a fix."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None



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
        """Backward compatibility for single event enrich call."""
        res = await self.enrich_batch([raw])
        return res[0] if res else None

    async def enrich_batch(self, events: list) -> list:
        if not events:
            return []

        parsed = []
        for raw in events:
            p = raw.raw_payload or {}
            icao24 = (p.get("icao24") or "").strip()
            if not icao24:
                continue

            lat = p.get("latitude")
            lon = p.get("longitude")
            on_ground = p.get("on_ground", False)

            if on_ground or lat is None or lon is None:
                continue

            callsign = (p.get("callsign") or "").strip() or None
            squawk = str(p.get("squawk") or "").strip()
            is_emerg = squawk in SQUAWK_LABELS or p.get("is_emergency", False)
            region = classify_region(lat, lon) if (lat and lon) else None
            country = p.get("origin_country", "")
            flags = check_sanctions(f"{callsign or ''} {country}", "")
            is_sanctioned = len(flags) > 0

            parsed.append((raw, p, icao24, lat, lon, callsign, squawk, is_emerg, region, country, flags, is_sanctioned))

        if not parsed:
            return []

        # The kinematic scores for the whole batch, in one call.
        #
        # This built one _score_flight coroutine per aircraft, each of which
        # called score_kinematic_event -- a singular wrapper whose entire body
        # constructs a one-item batch and unwraps the result. Aviation is the
        # highest-volume domain in the platform at 240,984 flight_position
        # events in 48 hours, nearly three times maritime's 85,115, and it was
        # the one paying a Redis pipeline round trip and an executor dispatch
        # per event while maritime called the batch API directly.
        entities, lats, lons, speeds, headings, timestamps, extras = [], [], [], [], [], [], []
        for (raw, p, icao24, lat, lon, callsign, squawk, is_emerg, region, country, flags, is_sanctioned) in parsed:
            entities.append(icao24)
            lats.append(lat)
            lons.append(lon)
            speeds.append(float(p.get("velocity") or p.get("speed") or 0.0))
            headings.append(float(p.get("true_track") or p.get("heading") or 0.0))
            timestamps.append((raw.occurred_at or datetime.now(timezone.utc)).timestamp())
            extras.append([float(p.get("baro_altitude") or p.get("geo_altitude") or 0.0) / 45000.0])

        try:
            kinematic_results = await self.scorer.score_kinematic_event_batch(
                entities, lats, lons, speeds, headings, timestamps, extras,
            )
        except Exception as e:
            # A failed batch must not lose the batch. Each aircraft falls back
            # to the floor the singular path used, and the reason is recorded
            # rather than swallowed.
            logger.error(f"Kinematic batch scoring failed for {len(entities)} aircraft: {e}", exc_info=True)
            kinematic_results = [{} for _ in entities]

        # Parallelize the per-aircraft work that genuinely is per-aircraft:
        # squawk floors, watchlist lookups and frequency tracking.
        scoring_tasks = []
        for (raw, p, icao24, lat, lon, callsign, squawk, is_emerg, region, country, flags, is_sanctioned), kin in zip(parsed, kinematic_results):
            scoring_tasks.append(self._score_flight(
                icao24=icao24, callsign=callsign, squawk=squawk, is_emerg=is_emerg,
                is_sanctioned=is_sanctioned,
                kinematic=float((kin or {}).get("score", 0.10) or 0.10),
            ))

        scoring_results = await asyncio.gather(*scoring_tasks, return_exceptions=True)

        results = []
        graph_tasks = []
        pipe = self.redis.raw.pipeline()
        now_iso = datetime.now(timezone.utc).isoformat()

        for (raw, p, icao24, lat, lon, callsign, squawk, is_emerg, region, country, flags, is_sanctioned), score_res in zip(parsed, scoring_results):
            speed = _as_float(p.get("velocity") or p.get("speed"))
            heading = _as_float(p.get("true_track") or p.get("heading"))
            if isinstance(score_res, Exception):
                logger.error(f"Scoring error for flight {icao24}: {score_res}")
                score, is_watched = 0.10, False
            else:
                score, is_watched = score_res

            # ROUTINE TELEMETRY GUARD:
            # Routine pings are held below the alerting band -- but *ordered*
            # within it, which `min(ROUTINE_CEILING, score * 0.3)` was not.
            #
            # That expression clamps to exactly the ceiling for any score at or
            # above 0.5, so 291 of 1,204 consecutive enriched events -- 205
            # aircraft over the Mediterranean and 86 vessels in the South China
            # Sea, Taiwan Strait, Black Sea and Turkish Straits -- carried the
            # identical 0.15. Within the watched geographies the system could
            # not rank one contact above another, and the region's own
            # sensitivity, which the platform measures, reached the number not
            # at all.
            #
            # Compressed into the band instead of clamped to its top, and scaled
            # by the region multiplier so a contact in a chokepoint outranks one
            # in open water without either leaving the routine band.
            if not is_emerg and not is_sanctioned and not is_watched:
                anomaly = routine_band_score(score, region)
            else:
                anomaly = min(1.0, score)

            alt_ft = int(p.get("baro_altitude", 0) * 3.28084) if p.get("baro_altitude") else 0

            # Store last seen in Redis pipeline for gap detector
            pipe.set(
                f"aircraft:last_seen:{icao24}",
                json.dumps({
                    "lat": lat,
                    "lon": lon,
                    "alt": alt_ft,
                    "callsign": callsign,
                    "region": region,
                    "ts": now_iso
                }),
                ex=86400  # 24 hours
            )

            event_type = EventType.FLIGHT_ANOMALY if (is_emerg or anomaly >= 0.60) else EventType.FLIGHT_POSITION

            tags = []
            if region:
                tags.append(region.lower().replace(" ", "_"))
            if is_emerg:
                tags += [f"squawk_{squawk}", "aviation_emergency", SQUAWK_LABELS.get(squawk, "emergency")]
            if is_sanctioned:
                tags += ["sanctioned_ofac"]

            cc = country[:2].upper() if len(country) >= 2 else None
            headline = f"Aviation Alert: {callsign or icao24} | {SQUAWK_LABELS.get(squawk, 'Emergency Alert')}" if (is_emerg or is_sanctioned or anomaly >= 0.50) else f"Flight {callsign or icao24} position in {region or 'global airspace'}"

            # Typed aviation payload. Without this the flight_data column stays
            # NULL on every flight event, so /api/v1/events/aviation returns
            # nothing while tens of thousands of positions are ingested -- the
            # domain is invisible to the API and the map. FlightData was defined
            # in the schema but constructed nowhere in the codebase.
            flight_data = FlightData(
                icao24=icao24,
                callsign=callsign or None,
                origin_country=country or None,
                baro_altitude_m=_as_float(p.get("baro_altitude")),
                geo_altitude_m=_as_float(p.get("geo_altitude")),
                velocity_ms=speed,
                true_track=heading,
                vertical_rate=_as_float(p.get("vertical_rate")),
                on_ground=bool(p["on_ground"]) if p.get("on_ground") is not None else None,
                squawk=squawk or None,
            )

            event = NormalizedEvent(
                event_id=raw.event_id,
                trace_id=raw.trace_id,
                type=event_type,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source,
                source_reliability=baseline_reliability(raw.source),
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
                headline=headline,
                tags=tags,
                anomaly_score=anomaly,
                flight_data=flight_data,
            )
            results.append(event)

            # The aviation enricher never touched the knowledge graph, which is
            # why it held a single Aircraft node against ~7,000 flight events per
            # ten minutes. Registration country and region are observed here, so
            # the aircraft arrives connected rather than as an orphan.
            if self.graph is not None:
                graph_tasks.append(self.graph.upsert_aircraft(icao24, {
                    "callsign": callsign,
                    "origin_country": country,
                    "region": region,
                }))

        if graph_tasks:
            await asyncio.gather(*graph_tasks, return_exceptions=True)

        await pipe.execute()
        return results

    async def _score_flight(self, icao24: str, callsign: Optional[str], squawk: str,
                            is_emerg: bool, is_sanctioned: bool, kinematic: float):
        """Combines a precomputed kinematic score with this aircraft's context.

        The kinematic score arrives already measured, from one batch call for
        the whole scan rather than a one-item batch per aircraft.

        It is always measured, including for aircraft that already qualify on
        their squawk or their operator. `elif is_sanctioned: raw_score = 0.80`
        replaced the measurement with a category, so all 85 flight_anomaly
        events in a 45-minute window shared one score: a sanctioned aircraft
        holding a normal cruise was ranked identically to one manoeuvring off
        its filed route. Being sanctioned is a reason to look, not a description
        of what the aircraft is doing.

        This is the same correction already made to crypto transfers, where a
        watched counterparty was overwriting the size signal instead of raising
        it, and 39,262 transfers shared one score for the same reason.
        """
        if is_emerg:
            # Squawk codes are standardised and their meanings genuinely differ,
            # so these are floors rather than assertions: 7500 is a hijack, 7700
            # a general emergency, 7600 a radio failure. Kinematics can still
            # lift one above its floor.
            floor = {"7500": 1.0, "7700": 0.85, "7600": 0.70}.get(squawk, 0.60)
            raw_score = lift_score(floor, SANCTIONED_LIFT_WEIGHT * kinematic)
        elif is_sanctioned:
            raw_score = lift_score(kinematic, SANCTIONED_LIFT_WEIGHT)
        else:
            raw_score = kinematic

        # Check watchlist in parallel
        watch_tasks = [self.scorer.check_watchlist(icao24, "aircraft")]
        if callsign:
            watch_tasks.append(self.scorer.check_watchlist(callsign, "aircraft"))
        
        watch_res = await asyncio.gather(*watch_tasks, return_exceptions=True)
        is_watched = any(r is True for r in watch_res if not isinstance(r, Exception))

        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(icao24, "aviation_position")
        # Headroom lift, not addition. `raw + w + f` puts every boosted event on
        # the ceiling and makes a 0.85 indistinguishable from a 0.99.
        final_score = lift_score(raw_score, w_boost)
        final_score = lift_score(final_score, f_boost, w_boost)

        return final_score, is_watched