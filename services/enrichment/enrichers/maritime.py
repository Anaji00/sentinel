"""
services/enrichment/enrichers/maritime.py
 
THE TRANSLATOR
==============
Converts raw AIS (Automatic Identification System) messages into rich,
structured events that the rest of the system can understand.
"""
 
import json
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, List
 
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
from shared.utils.source_scorecard import baseline_reliability
from shared.utils.regions import (
    classify_region, decode_nav_status, decode_vessel_type, is_restricted_nav_status,
)
from shared.utils.sanctions import check_sanctions, mmsi_to_country
from shared.utils.regions import routine_band_score
 
logger = logging.getLogger("enrichment.maritime")
 
# AIS transmits "unknown" as an in-band value rather than an absence, and both
# were reaching the payload and the summary prose as though they were bearings:
# a live event carried "heading": 511 and rendered "Heading: 511 degrees".
#
#   511 deci-degrees is the heading-not-available code (ITU-R M.1371).
#   3600 deci-degrees -- 360.0 after scaling -- is the same for course.
#
# A bearing that is not known is None, which every consumer already handles,
# rather than a number no compass can show.
AIS_HEADING_UNAVAILABLE = 511
AIS_COG_UNAVAILABLE = 360.0


def _ais_heading(value):
    """True heading in degrees, or None where AIS said it does not know."""
    if value is None:
        return None
    try:
        h = int(value)
    except (TypeError, ValueError):
        return None
    if h == AIS_HEADING_UNAVAILABLE or not (0 <= h <= 359):
        return None
    return h


def _ais_cog(value):
    """Course over ground in degrees, or None where AIS said it does not know."""
    if value is None:
        return None
    try:
        c = float(value)
    except (TypeError, ValueError):
        return None
    if c >= AIS_COG_UNAVAILABLE or c < 0:
        return None
    return c


def _as_float(value) -> Optional[float]:
    """Best-effort float, or None. AIS fields arrive absent and arrive as text."""
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class MaritimeEnricher:
    # ── STRICT DI ALIGNMENT ──
    def __init__(self, scorer, redis_client, graph_writer, resolver=None):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer
        self.resolver = resolver

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        # Backward compatibility for direct calls
        res = await self.enrich_batch([raw])
        return res[0] if res else None
        
    async def enrich_batch(self, events: list) -> list:
        if not events: return []
        
        positions = []
        statics = []
        for raw in events:
            payload = raw.raw_payload
            msg_type = payload.get("MessageType", "")
            if msg_type == "PositionReport":
                positions.append(raw)
            elif msg_type == "ShipStaticData":
                statics.append(raw)
                
        results = []
        if statics:
            tasks = [self._static(e, e.raw_payload, e.raw_payload.get("MetaData", {}), str(e.raw_payload.get("MetaData", {}).get("MMSI", "")).strip()) for e in statics]
            s_res = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend([r for r in s_res if isinstance(r, NormalizedEvent)])
            
        if positions:
            p_res = await self._position_batch(positions)
            results.extend(p_res)
            
        return results
    
    # ── Position ──────────────────────────────────────────────────────────────

    async def _position_batch(self, events: list) -> list:
        parsed = []
        mmsi_list = []
        meta_list = []
        for raw in events:
            payload = raw.raw_payload or {}
            meta = payload.get("MetaData") or {}
            mmsi = str(meta.get("MMSI", "")).strip()
            
            if not mmsi or mmsi == "0": continue
            
            msg = payload.get("Message") or {}
            pos = msg.get("PositionReport") or {}
            lat = pos.get("Latitude")
            lon = pos.get("Longitude")
            if lat is None or lon is None: continue
            
            speed = float(pos.get("Sog") or 0)
            heading = int(pos.get("TrueHeading") or 0)
            nav_code = pos.get("NavigationalStatus") or 0
            nav_status = decode_nav_status(nav_code)
            region = classify_region(lat, lon)
            
            parsed.append((raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region))
            mmsi_list.append(mmsi)
            meta_list.append(meta)
            
        if not parsed: return []
        
        # Batch resolve vessels
        if self.resolver and hasattr(self.resolver, "resolve_vessel_batch"):
            vessels = await self.resolver.resolve_vessel_batch(mmsi_list, meta_list)
        else:
            vessels = await asyncio.gather(*[self._get_vessel(m, mt) for m, mt in zip(mmsi_list, meta_list)])
                
        entities = []
        lats_list = []
        lons_list = []
        speeds_list = []
        headings_list = []
        timestamps_list = []
        extra_features_list = []
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region), vessel in zip(parsed, vessels):
            from shared.utils.regions import get_region_sensitivity_multiplier
            reg_mult = get_region_sensitivity_multiplier(region) if region else 1.0
            # Matched on the AIS code, not on prose in the display label.
            # The old test asked whether "not under command" appeared in
            # "notundercommand" and it never did, so status 2 -- a vessel that
            # cannot manoeuvre -- raised nothing. Three of the four terms were
            # single words and matched by luck.
            nav_anomaly = 1.0 if is_restricted_nav_status(nav_code) else 0.0
            is_sanctioned = 1.0 if vessel.get("flags") else 0.0
            
            entities.append(mmsi)
            lats_list.append(lat)
            lons_list.append(lon)
            speeds_list.append(speed)
            headings_list.append(heading)
            timestamps_list.append((raw.occurred_at or datetime.now(timezone.utc)).timestamp())
            extra_features_list.append([float(is_sanctioned), float(reg_mult), float(nav_anomaly)])
            
        scores = await self.scorer.score_kinematic_event_batch(
            entities, lats_list, lons_list, speeds_list, headings_list,
            timestamps_list, extra_features_list,
        )
        
        # Batch watchlist & frequency checks concurrently to avoid sequential awaits blocking
        check_tasks = []
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region), vessel, score_dict in zip(parsed, vessels, scores):
            check_tasks.append(asyncio.gather(
                self.scorer.check_watchlist(mmsi, "vessels"),
                self.scorer.track_frequency(mmsi, "vessel_position")
            ))
        check_results = await asyncio.gather(*check_tasks)
        
        results = []
        pipe = self.redis.raw.pipeline()
        for idx, ((raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region), vessel, score_dict) in enumerate(zip(parsed, vessels, scores)):
            raw_anomaly = score_dict.get("score", 0.0)
            is_watched, f_boost = check_results[idx]
            w_boost = 0.15 if is_watched else 0.0

            flags = vessel.get("flags", [])
            vtype = vessel.get("vessel_type", "Unknown")
            vname = (vessel.get("name") or meta.get("ShipName") or "").upper()
            if vtype == "Unknown" or not vtype:
                if any(k in vname for k in ("TANKER", "OIL", "CRUDE", "PETRO", "LNG", "LPG", "CHEM")):
                    vtype = "Tanker"

            is_sanctioned = bool(flags)
            # By code, not by prose in the label -- see the note at the
            # nav_anomaly assignment above.
            is_emergency_nav = is_restricted_nav_status(nav_code)

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
            if not is_sanctioned and not is_emergency_nav and not is_watched:
                anomaly = routine_band_score(raw_anomaly, region)
            else:
                anomaly = min(1.0, raw_anomaly + w_boost + f_boost)

            pipe.set(
                f"vessel:last_seen:{mmsi}",
                json.dumps({
                    "lat": lat, "lon": lon, "heading": heading,
                    "region": region, "speed": speed, "ts": (raw.occurred_at or datetime.now(timezone.utc)).isoformat(),
                }),
                ex = 172800 
            )
            
            results.append((raw, meta, mmsi, lat, lon, speed, heading, nav_status, nav_code, region, vessel, flags, vtype, anomaly))
            
        await pipe.execute()
        
        # Batch graph updates
        graph_tasks = []
        for (_, _, mmsi, _, _, _, _, _, _, region, vessel, flags, vtype, _) in results:
            graph_tasks.append(self.graph.upsert_vessel(mmsi, {
                "name": vessel.get("name", ""),
                "vessel_type": vtype,
                "flag_state": vessel.get("flag_state", ""),
                # Region was already computed for the event; passing it here is
                # what lets the vessel be joined to anything in the graph.
                "region": region,
                "flags": flags,
            }))
            
        if graph_tasks:
            await asyncio.gather(*graph_tasks, return_exceptions=True)
            
        final_events = []
        # nav_code travels with the row rather than being read from the
        # enclosing scope. It was not in this tuple, so `is_restricted_nav_status(nav_code)`
        # below resolved to whatever the *previous* loop had left bound -- the
        # last vessel's status, applied to every vessel in this one. Python does
        # not complain, and the value is a plausible integer, so the emergency
        # flag was simply wrong rather than absent.
        for (raw, meta, mmsi, lat, lon, speed, heading, nav_status, nav_code, region, vessel, flags, vtype, anomaly) in results:
            is_sanctioned = bool(flags)
            # By code, not by prose in the label -- see the note at the
            # nav_anomaly assignment above.
            is_emergency_nav = is_restricted_nav_status(nav_code)
            is_watched = bool(anomaly > 0.15)
            
            headline_str = (
                f"{vtype or 'Vessel'} '{vessel.get('name') or meta.get('ShipName') or f'MMSI:{mmsi}'}' "
                f"{nav_status.lower() if nav_status else 'transiting'} in {region or 'unknown waters'}"
                + (" — sanctioned/flagged vessel" if is_sanctioned else "")
                + (" — emergency navigation status" if is_emergency_nav else "")
            ) if (is_sanctioned or is_emergency_nav or is_watched) else None

            final_events.append(NormalizedEvent(
                event_id = raw.event_id, trace_id = raw.trace_id,
                type = EventType.VESSEL_POSITION,
                occurred_at = raw.occurred_at or datetime.now(timezone.utc),
                source = raw.source,
                source_reliability=baseline_reliability(raw.source),
                primary_entity = Entity(
                    id=mmsi,
                    type=EntityType.VESSEL,
                    name=vessel.get("name") or meta.get("ShipName") or f"VESSEL_{mmsi}",
                    flags=flags,
                    country_code=vessel.get("flag_state") or mmsi_to_country(mmsi) or None,
                ),
                latitude = lat,
                longitude = lon,
                region = region,
                country_code = vessel.get("flag_state") or mmsi_to_country(mmsi) or None,
                headline = headline_str,
                vessel_data = VesselData(
                    mmsi=mmsi, 
                    latitude=lat,
                    longitude=lon,
                    speed_knots = speed,
                    heading=_ais_heading(heading),
                    nav_status=nav_status,
                    vessel_type = vtype,
                    flag_state = vessel.get("flag_state"),
                    destination = vessel.get("destination"),
                    # The region is classified for the headline and the tags and
                    # was not written to the payload, so a consumer reading the
                    # record could not tell where the vessel was without parsing
                    # prose. It is the field chokepoint analysis keys on.
                    last_seen_region = region,
                    course_over_ground = _ais_cog(_as_float(pos.get("Cog"))),
                ),
                tags = self._tags(region, vtype, flags),
                anomaly_score = anomaly,
            ))
            
        return final_events
    
    # ── Static ────────────────────────────────────────────────────────────────
    async def _static(self, raw, payload, meta, mmsi) -> Optional[NormalizedEvent]:
        if not mmsi or mmsi == "0": return None
        msg = payload.get("Message") or {}
        s = msg.get("ShipStaticData") or {}
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
            event_id = raw.event_id, trace_id = raw.trace_id,
            type = EventType.VESSEL_STATIC,
            occurred_at = raw.occurred_at or datetime.now(timezone.utc),
            source = raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity = Entity(
                id=mmsi, type=EntityType.VESSEL, name=name, flags=flags,
                country_code=mmsi_to_country(mmsi) or None,
            ),
            vessel_data = VesselData(
                mmsi=mmsi, vessel_type=vtype, destination=dest, cargo_type=code,
                flag_state=mmsi_to_country(mmsi) or None,
            ),
            country_code = mmsi_to_country(mmsi) or None,
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