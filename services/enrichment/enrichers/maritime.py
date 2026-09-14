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
import time
from shared.utils.tasks import safe_create_task
from shared.utils.quiet_failures import dropped, swallowed
from datetime import datetime, timezone
from typing import Optional, List
 
from shared.models import NormalizedEvent, EventType, Entity, EntityType, VesselData
from shared.utils.source_scorecard import baseline_reliability
from shared.utils.regions import (
    classify_region, decode_nav_status, decode_vessel_type, is_restricted_nav_status,
)
from shared.utils.sanctions import check_sanctions, mmsi_to_country

# The vessel watchlist the news scorer reads and nothing wrote.
#
# `check_watchlist(entity, "vessels")` and two zscore lookups in anomaly_scorer
# all point at this key; it did not exist. Flagged hulls are written here so a
# vessel named in a headline is recognised the way a watched ticker is.
from shared.utils.watchlists import WATCHED_VESSELS_KEY, WATCHED_VESSELS_TTL_SEC
from shared.utils.regions import classify_region, routine_band_score
from services.enrichment.anomaly_scorer import lift_score
 
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


# Speed over ground is transmitted in tenths of a knot, and 1023 -- 102.3
# after scaling -- is the not-available code. 342 live events carry it. No
# helper existed for it at all; the detector happens to be robust to the value,
# which is why it was never noticed.
AIS_SOG_UNAVAILABLE = 102.3


def _ais_sog(value):
    """Speed over ground in knots, or None where AIS said it does not know."""
    if value is None:
        return None
    try:
        sog = float(value)
    except (TypeError, ValueError):
        return None
    if sog >= AIS_SOG_UNAVAILABLE or sog < 0:
        return None
    return sog


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
        chokepoints = []
        for raw in events:
            payload = raw.raw_payload
            msg_type = payload.get("MessageType", "")
            if msg_type == "PositionReport":
                positions.append(raw)
            elif msg_type == "ShipStaticData":
                statics.append(raw)
            elif payload.get("instrument") == "sentinel-1-sar":
                # Radar, not AIS.
                #
                # The SAR collector publishes to this topic and its payload has
                # no MessageType, so it matched neither branch above and left
                # the loop -- with no counter, no log and no dead letter, which
                # is the one failure mode this platform is least able to see.
                # Every chokepoint reading Sentinel-1 has ever produced was
                # discarded on arrival, including the traffic assessments the
                # collector logs at WARNING when they are notable.
                chokepoints.append(raw)
            else:
                # Counted, for the same reason the crypto and tradfi enrichers
                # count theirs: one unmatched message is a probe and ten
                # thousand is a feed being thrown away, and a bare `continue`
                # says neither.
                dropped(
                    "enrichment.maritime.unrouted_message",
                    f"no branch for source={raw.source!r} MessageType={msg_type!r}",
                    logger,
                )

        results = []
        if chokepoints:
            c_res = await asyncio.gather(
                *[self._chokepoint_reading(e, e.raw_payload) for e in chokepoints],
                return_exceptions=True,
            )
            results.extend([r for r in c_res if isinstance(r, NormalizedEvent)])

        if statics:
            tasks = [self._static(e, e.raw_payload, e.raw_payload.get("MetaData", {}), str(e.raw_payload.get("MetaData", {}).get("MMSI", "")).strip()) for e in statics]
            s_res = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend([r for r in s_res if isinstance(r, NormalizedEvent)])
            
        if positions:
            p_res = await self._position_batch(positions)
            results.extend(p_res)
            
        return results
    
    # ── Chokepoint radar ──────────────────────────────────────────────────────

    # Sigma at which a chokepoint reading is worth reporting as a finding.
    #
    # `shared.utils.chokepoints` already calls two sigma "notable" and says so
    # in the assessment's own `direction`. This scales that judgement onto the
    # platform's score rather than re-deciding it: two sigma reaches the floor
    # every rule can see, four saturates.
    CHOKEPOINT_SIGMA_FLOOR = 2.0
    CHOKEPOINT_SIGMA_CEILING = 4.0

    async def _chokepoint_reading(self, raw, payload) -> Optional[NormalizedEvent]:
        """A Sentinel-1 look at a chokepoint, as a supply-chain measurement.

        The first producer of SUPPLY_CHAIN_METRIC. The type was declared, named
        by `rule_physical_disruption_repricing` as its evidence, and constructed
        nowhere -- so a rule about a strait emptying and freight repricing had
        no way to learn that a strait had emptied.

        Radar is the measurement AIS cannot be: a vessel that has switched off
        its transponder still returns like metal. The collector is careful to
        say the reading is a target density and not a vessel count
        (`is_vessel_count: False`), and that distinction is carried through
        here rather than quietly upgraded.
        """
        name = payload.get("chokepoint")
        if not name:
            return None

        assessment = payload.get("traffic_assessment")
        tags = ["maritime", "chokepoint", "sar", str(name).lower()]

        if not assessment:
            # No baseline yet. A chokepoint that has not been measured has not
            # been quiet, and scoring it as calm would be the more damaging of
            # the two errors.
            anomaly = 0.0
            tags.append("no_baseline")
            direction = "unmeasured"
            z_score = None
        else:
            z_score = float(assessment.get("z_score") or 0.0)
            direction = str(assessment.get("direction") or "normal")
            span = self.CHOKEPOINT_SIGMA_CEILING - self.CHOKEPOINT_SIGMA_FLOOR
            anomaly = max(0.0, min(1.0, (abs(z_score) - self.CHOKEPOINT_SIGMA_FLOOR) / span))
            tags.append(direction)

        # The region AIS would give the same water, not the collector's label
        # for it.
        #
        # The SAR collector images four chokepoints under its own names, and the
        # region a vessel gets comes from `classify_region` on its position.
        # Three of the four agree by luck; "Gulf of Guinea" resolves to
        # "Nigerian Territorial", so a radar reading labelled with the
        # collector's name could never join the vessels inside it -- and the
        # region join is the whole basis of the chokepoint rules.
        #
        # Running the centroid through the same function the AIS path uses
        # makes the two agree by construction rather than by coincidence, for
        # the chokepoints that exist now and any added later.
        bbox = payload.get("bbox") or {}
        lat = lon = None
        try:
            lat = (float(bbox["south"]) + float(bbox["north"])) / 2.0
            lon = (float(bbox["west"]) + float(bbox["east"])) / 2.0
        except (KeyError, TypeError, ValueError):
            lat = lon = None
        region = classify_region(lat, lon) if lat is not None else None

        observed = payload.get("observed_on")
        headline = (
            f"{name}: radar target density {payload.get('target_density')} "
            + (f"({direction}, z={z_score:+.2f})" if z_score is not None
               else "(no baseline yet)")
        )

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.SUPPLY_CHAIN_METRIC,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=baseline_reliability(raw.source),
            primary_entity=Entity(
                id=str(name), type=EntityType.INFRASTRUCTURE, name=str(name),
            ),
            # Resolved above, so it matches what a vessel in the same water
            # carries. Falls back to the collector's own name only when the
            # reading has no usable bounding box, which is the honest answer
            # when there is nothing to classify.
            region=region or str(name),
            latitude=lat,
            longitude=lon,
            headline=headline,
            summary=(
                f"Sentinel-1 SAR over {name} on {observed}: "
                f"{payload.get('target_pixels')} target pixels of "
                f"{payload.get('water_pixels')} water pixels. "
                f"{payload.get('method')}. This is a target density, not a vessel count."
            ),
            anomaly_score=anomaly,
            tags=tags,
            named_entities=[str(name)],
        )

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
            
            # The in-band "not available" codes, decoded here rather than only
            # on the display path.
            #
            # `_ais_heading` and `_ais_cog` were written for exactly this and
            # were called at two sites, both of which render the payload. The
            # parse path forty lines earlier kept `int(pos.get("TrueHeading") or
            # 0)`, so the raw 511 -- ITU-R M.1371's heading-not-available code --
            # went straight into the kinematic batch scorer. Measured over three
            # days: 228 vessel events carry heading 511 and average 0.600
            # anomaly against a 0.126 baseline, 4.8x, and above the 0.5 that
            # counts as a reaction in edge validation.
            #
            # `or 0` was the second half of it: a genuine heading of due north
            # and a missing one were the same value.
            speed = _ais_sog(pos.get("Sog"))
            heading = _ais_heading(pos.get("TrueHeading"))
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

            # A flagged vessel joins the vessel watchlist.
            #
            # `anomaly_scorer` reads `sentinel:watched:vessels` at two sites --
            # it is how a vessel named in a headline earns the same boost a
            # watched ticker does -- and nothing in the tree had ever written
            # that key. It did not exist at all, so the lookup returned None on
            # every call and the maritime half of that check was dead, on a
            # platform whose maritime domain is its largest.
            #
            # Populated from what the platform already determined rather than
            # from a list somebody has to maintain: a sanctioned or flagged hull
            # is exactly the vessel whose mention in the news should carry
            # weight. The same key and the same zset `check_watchlist` reads.
            if is_sanctioned and getattr(self, "redis", None) is not None:
                safe_create_task(
                    self._watch_vessel(mmsi, vname),
                    name="watch-flagged-vessel",
                )

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
                # Headroom lift, not addition -- the same composition every
                # other enricher uses. `min(1.0, a + b)` has no notion of how
                # much room is left, so any boosted event above ~0.85 lands on
                # the ceiling and stops being distinguishable from one at 0.99.
                #
                # Only the *composition* is shared. The base score, the
                # features behind it and the weights below stay this domain's
                # own: what counts as anomalous differs by domain, how boosts
                # combine does not.
                anomaly = lift_score(raw_anomaly, w_boost)
                anomaly = lift_score(anomaly, f_boost, w_boost)

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

    async def _watch_vessel(self, mmsi, name) -> None:
        """Record a flagged hull on the vessel watchlist, by MMSI and by name.

        Both, because the reader looks up whatever token a headline produced --
        a news story names a ship, an AIS feed names an MMSI, and they have to
        meet somewhere.
        """
        raw_redis = getattr(getattr(self, "redis", None), "raw", None)
        if raw_redis is None:
            return
        members = {str(m).upper(): time.time() for m in (mmsi, name) if m}
        if not members:
            return
        try:
            pipe = raw_redis.pipeline()
            pipe.zadd(WATCHED_VESSELS_KEY, mapping=members)
            pipe.expire(WATCHED_VESSELS_KEY, WATCHED_VESSELS_TTL_SEC)
            await pipe.execute()
        except Exception as _exc:
            swallowed("enrichment.enrichers.maritime._watch_vessel", _exc, logger)
