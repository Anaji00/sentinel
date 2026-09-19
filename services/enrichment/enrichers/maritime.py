"""
services/enrichment/enrichers/maritime.py
 
THE TRANSLATOR
==============
Converts raw AIS (Automatic Identification System) messages into rich,
structured events that the rest of the system can understand.
"""
 
import json
import uuid
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
from shared.utils.maritime_behaviour import (
    STS_MAX_SPEED_KNOTS,
    STS_MAX_SEPARATION_NM,
    implausible_position,
    impossible_transit,
    sts_pair,
)
from shared.utils.regions import classify_region, routine_band_score
from services.enrichment.anomaly_scorer import breakdown_from_score, lift_score
 
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

# Which AIS message types carry a position, and which carry an identity.
#
# Class A (PositionReport) is the only one the collector used to subscribe to,
# so these branches only ever had to name it. Class B carries the same geometry
# under a different key -- and ExtendedClassBPositionReport carries the ship's
# name and type as well, which Class A position reports do not.
#
# Ordered most common first; the lookup is a set, but the order is how a reader
# learns which of these actually turns up.
_POSITION_MESSAGE_TYPES = frozenset({
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
})

_STATIC_MESSAGE_TYPES = frozenset({
    "ShipStaticData",
    "StaticDataReport",
})

# The block inside `Message` that holds the geometry, per type. AISStream keys
# the body by the message type name, so this is a lookup rather than a chain of
# `or {}` that would silently prefer whichever key happened to come first.
_POSITION_BODY_KEYS = (
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
)
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


def _ais_imo(static: dict) -> Optional[str]:
    """The IMO number from a ShipStaticData block, or None.

    The one identifier on an AIS message that does not change. MMSI is assigned
    by the flag state and is reassigned with the flag -- re-registration is the
    ordinary way a vessel breaks continuity with its own history, and it is the
    first thing a sanctioned owner does. IMO is issued once, to the hull, for
    the life of the hull.

    This platform runs `check_sanctions` and a flags path over exactly that
    population and was discarding the field at parse time, keeping the name
    (which changes), the destination (which changes) and the type.

    Zero is AIS for "not stated" and is not an IMO.
    """
    for key in ("ImoNumber", "IMONumber", "Imo", "imo"):
        raw = static.get(key)
        if raw in (None, "", 0, "0"):
            continue
        try:
            number = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if number > 0:
            return str(number)
    return None


def _ais_draught(static: dict) -> Optional[float]:
    """Maximum static draught in metres, or None.

    Loaded or in ballast, which is the question a tanker transit turns on: a
    VLCC at 22m is carrying and the same hull at 9m is not. AIS reports it in
    decimetres in the raw NMEA and AISStream decodes it to metres; values
    outside a plausible hull range are dropped rather than stored, because 0 is
    the "not available" sentinel and would read as "riding empty".
    """
    for key in ("MaximumStaticDraught", "Draught", "draught"):
        raw = static.get(key)
        if raw in (None, ""):
            continue
        try:
            metres = float(raw)
        except (TypeError, ValueError):
            continue
        if 0.1 <= metres <= 30.0:
            return round(metres, 2)
    return None


def _ais_length(static: dict) -> Optional[float]:
    """Overall length in metres from the AIS dimension block, or None.

    AIS reports the antenna's offsets from bow, stern, port and starboard
    rather than a length; the hull is the sum of the fore and aft offsets. A
    zero in both is "not available".
    """
    dim = static.get("Dimension") or static.get("dimension") or {}
    if not isinstance(dim, dict):
        return None
    total = 0.0
    for key in ("A", "B", "a", "b"):
        raw = dim.get(key)
        if raw in (None, ""):
            continue
        try:
            total += float(raw)
        except (TypeError, ValueError):
            continue
    return round(total, 1) if 1.0 <= total <= 500.0 else None


def _ais_eta(static: dict) -> Optional[str]:
    """The reported ETA as an ISO-like string, or None.

    AIS carries month, day, hour and minute with no year -- the sender does not
    state one -- so this is rendered as the partial value it is rather than
    guessed into a full timestamp. Month 0 or day 0 is "not available".
    """
    eta = static.get("Eta") or static.get("ETA") or static.get("eta")
    if not isinstance(eta, dict):
        return None
    try:
        month = int(eta.get("Month") or eta.get("month") or 0)
        day = int(eta.get("Day") or eta.get("day") or 0)
        hour = int(eta.get("Hour") or eta.get("hour") or 0)
        minute = int(eta.get("Minute") or eta.get("minute") or 0)
    except (TypeError, ValueError):
        return None
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"--{month:02d}-{day:02d}T{hour:02d}:{minute:02d}Z"


# Vessels currently reporting stationary, as a Redis GEO set. One bounded
# radius query answers "who else is stopped within a cable of this hull",
# against a scan of every `vessel:last_seen:*` key -- which the gap detector's
# own docstring records as the mistake it had to stop making.
STOPPED_VESSELS_GEO_KEY = "sentinel:vessels:stopped"
STOPPED_GEO_TTL_SEC = 6 * 3600

# An STS claim names both hulls, so it is written once per pair rather than once
# per report. Without this a two-hour transfer emits an event every few seconds
# from both transponders.
STS_DEDUP_TTL_SEC = 6 * 3600

# How many co-location checks one batch may spend. See the call site.
STS_CHECKS_PER_BATCH = 40


async def _previous_positions(redis_client, mmsis: list) -> dict:
    """The last stored report for each hull, keyed by MMSI. One mget."""
    out = {}
    if redis_client is None or not mmsis:
        return out
    try:
        raw = getattr(redis_client, "raw", redis_client)
        keys = [f"vessel:last_seen:{m}" for m in mmsis]
        values = await raw.mget(keys)
        for mmsi, blob in zip(mmsis, values or []):
            if not blob:
                continue
            text = blob if isinstance(blob, str) else blob.decode("utf-8")
            row = json.loads(text)
            # `ts` is an ISO string for the gap detector; the behaviours need a
            # number. Written as `epoch` since this change, parsed from the ISO
            # form for rows stored before it.
            if row.get("epoch") is None and row.get("ts"):
                try:
                    row["epoch"] = datetime.fromisoformat(row["ts"]).timestamp()
                except (TypeError, ValueError):
                    continue
            row["ts"] = row.get("epoch")
            out[mmsi] = row
    except Exception as _exc:
        swallowed("enrichment.maritime.previous_positions", _exc, logger)
    return out


async def _stopped_neighbour(redis_client, mmsi: str, lat: float, lon: float):
    """The nearest other stopped hull that satisfies `sts_pair`, or None."""
    if redis_client is None:
        return None
    try:
        raw = getattr(redis_client, "raw", redis_client)
        near = await raw.geosearch(
            STOPPED_VESSELS_GEO_KEY,
            longitude=lon, latitude=lat,
            radius=STS_MAX_SEPARATION_NM * 1852.0, unit="m",
            count=8, sort="ASC",
        )
    except Exception as _exc:
        swallowed("enrichment.maritime.stopped_neighbour", _exc, logger)
        return None

    candidates = []
    for entry in near or []:
        name = entry[0] if isinstance(entry, (list, tuple)) else entry
        name = name.decode() if isinstance(name, bytes) else str(name)
        if name != str(mmsi):
            candidates.append(name)
    if not candidates:
        return None

    rows = await _previous_positions(redis_client, [str(mmsi)] + candidates)
    mine = rows.get(str(mmsi))
    if not mine:
        return None
    now = datetime.now(timezone.utc).timestamp()
    for other_mmsi in candidates:
        theirs = rows.get(other_mmsi)
        if not theirs:
            continue
        evidence = sts_pair(mine, theirs, now=now)
        if not evidence:
            continue
        # One claim per pair per window, whichever transponder reports first.
        pair_key = ":".join(sorted((str(mmsi), other_mmsi)))
        try:
            claimed = await getattr(redis_client, "raw", redis_client).set(
                f"sentinel:vessels:sts_seen:{pair_key}", "1",
                ex=STS_DEDUP_TTL_SEC, nx=True,
            )
        except Exception as _exc:
            swallowed("enrichment.maritime.sts_dedup", _exc, logger)
            claimed = True
        if not claimed:
            return None
        return {"mmsi": other_mmsi, **evidence}
    return None


def _spoof_event(raw, mmsi, vessel, lat, lon, region, flags, vtype, transit):
    """A transponder reported a position it could not have reached."""
    name = (vessel or {}).get("name") or mmsi
    return NormalizedEvent(
        event_id=str(uuid.uuid4()),
        trace_id=raw.trace_id,
        type=EventType.VESSEL_SPOOF,
        occurred_at=raw.occurred_at or datetime.now(timezone.utc),
        source=raw.source,
        source_reliability=baseline_reliability(raw.source),
        primary_entity=Entity(
            id=mmsi, type=EntityType.VESSEL, name=name, flags=flags,
            country_code=mmsi_to_country(mmsi) or None,
        ),
        latitude=lat, longitude=lon, region=region,
        country_code=mmsi_to_country(mmsi) or None,
        headline=(
            f"AIS identity anomaly: {name} implies "
            f"{transit['implied_speed_knots']} kn over {transit['distance_nm']} nm"
        ),
        summary=(
            f"Two consecutive AIS reports for MMSI {mmsi} are {transit['distance_nm']} nm "
            f"apart {transit['elapsed_seconds']:.0f} seconds apart, implying "
            f"{transit['implied_speed_knots']} knots against a "
            f"{transit['threshold_knots']}-knot plausibility bound. Either two "
            f"transmitters share this identity or one position is fabricated. "
            f"This is a statement about the data, not about the vessel."
        ),
        vessel_data=VesselData(
            mmsi=mmsi, latitude=lat, longitude=lon,
            vessel_type=vtype, last_seen_region=region,
        ),
        tags=["maritime", "vessel_spoof", "ais_integrity"] + (["sanctioned"] if flags else []),
        named_entities=[str(name)],
        anomaly_score=0.82,
    )


def _inland_event(raw, mmsi, vessel, lat, lon, region, flags, vtype, evidence):
    """A transponder reported a position that is not water."""
    name = (vessel or {}).get("name") or mmsi
    return NormalizedEvent(
        event_id=str(uuid.uuid4()),
        trace_id=raw.trace_id,
        type=EventType.VESSEL_SPOOF,
        occurred_at=raw.occurred_at or datetime.now(timezone.utc),
        source=raw.source,
        source_reliability=baseline_reliability(raw.source),
        primary_entity=Entity(
            id=mmsi, type=EntityType.VESSEL, name=name, flags=flags,
            country_code=mmsi_to_country(mmsi) or None,
        ),
        latitude=lat, longitude=lon, region=region,
        country_code=mmsi_to_country(mmsi) or None,
        headline=(
            f"AIS position anomaly: {name} reporting inland in "
            f"{evidence['reported_region']}"
        ),
        summary=(
            f"MMSI {mmsi} reported {lat:.3f}, {lon:.3f}, which falls inside "
            f"{evidence['reported_region']} and inside no maritime region. A hull "
            f"cannot be there. Unlike the transit check this needs no previous "
            f"report, so it catches a transponder that sits still in the wrong "
            f"place as well as one that jumps. This is a statement about the "
            f"data, not about the vessel."
        ),
        vessel_data=VesselData(
            mmsi=mmsi, latitude=lat, longitude=lon,
            vessel_type=vtype, last_seen_region=region,
        ),
        tags=["maritime", "vessel_spoof", "ais_integrity", "position_inland"]
             + (["sanctioned"] if flags else []),
        named_entities=[str(name)],
        # Below the transit anomaly's 0.82. A jump between two reports is two
        # observations contradicting each other; this is one observation
        # contradicting the coastline, which is a weaker claim -- a coarse
        # polygon near a river mouth or a port basin can put a real hull just
        # inside a land region.
        anomaly_score=0.74,
    )


def _sts_event(raw, mmsi, vessel, lat, lon, region, partner, vtype=None):
    """Two hulls stopped alongside each other for long enough to move cargo."""
    name = (vessel or {}).get("name") or mmsi
    flags = (vessel or {}).get("flags") or []
    return NormalizedEvent(
        event_id=str(uuid.uuid4()),
        trace_id=raw.trace_id,
        type=EventType.VESSEL_STS,
        occurred_at=raw.occurred_at or datetime.now(timezone.utc),
        source=raw.source,
        source_reliability=baseline_reliability(raw.source),
        primary_entity=Entity(
            id=mmsi, type=EntityType.VESSEL, name=name, flags=flags,
            country_code=mmsi_to_country(mmsi) or None,
        ),
        latitude=partner["midpoint"]["lat"], longitude=partner["midpoint"]["lon"],
        region=region, country_code=mmsi_to_country(mmsi) or None,
        headline=(
            f"Possible STS: {name} alongside MMSI {partner['mmsi']} for "
            f"{partner['dwell_hours']}h at {partner['separation_nm']} nm"
        ),
        summary=(
            f"MMSI {mmsi} and MMSI {partner['mmsi']} have been within "
            f"{partner['separation_nm']} nm of each other, both under "
            f"{max(partner['speed_knots'])} knots, for {partner['dwell_hours']} hours in "
            f"{region or 'open water'}. That geometry is a ship-to-ship transfer or a "
            f"rendezvous; it is not a berth, because neither hull is alongside a quay, "
            f"and it is not traffic, because neither is making way."
        ),
        vessel_data=VesselData(
            mmsi=mmsi, latitude=lat, longitude=lon,
            vessel_type=vtype, last_seen_region=region,
        ),
        tags=["maritime", "vessel_sts", "co_location"] + (["sanctioned"] if flags else []),
        named_entities=[str(name), str(partner["mmsi"])],
        anomaly_score=0.78,
    )


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
            if msg_type in _POSITION_MESSAGE_TYPES:
                positions.append(raw)
            elif msg_type in _STATIC_MESSAGE_TYPES:
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
            pos = {}
            for _key in _POSITION_BODY_KEYS:
                candidate = msg.get(_key)
                if candidate:
                    pos = candidate
                    break
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
            # `or 0` here would read a Class B report -- which has no
            # NavigationalStatus field at all -- as status 0, "Under way using
            # engine". That is an assertion about a vessel that made none, and
            # it is the same defect as the map's invented 12.4 knots. Class A
            # always carries the field, so its genuine 0 is still a 0.
            _raw_nav = pos.get("NavigationalStatus")
            nav_code = _raw_nav if isinstance(_raw_nav, int) else None
            nav_status = decode_nav_status(nav_code) if nav_code is not None else None
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
            domain="maritime",
        )
        
        # Batch watchlist & frequency checks concurrently to avoid sequential awaits blocking
        check_tasks = []
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region), vessel, score_dict in zip(parsed, vessels, scores):
            check_tasks.append(asyncio.gather(
                self.scorer.check_watchlist(mmsi, "vessels"),
                self.scorer.track_frequency(mmsi, "vessel_position")
            ))
        check_results = await asyncio.gather(*check_tasks)
        
        # Each hull's previous report, for the two behaviours below. One mget
        # for the batch rather than a get per vessel.
        previous_positions = await _previous_positions(
            self.redis, [row[3] for row in parsed]
        )

        results = []
        spoof_events: list = []
        sts_events: list = []
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

            observed_at = (raw.occurred_at or datetime.now(timezone.utc))

            # The epoch beside the ISO string, and when this hull last started
            # sitting still.
            #
            # `ts` was written as an ISO string for the gap detector to read as
            # a timestamp. Two behaviours need it as a number and need one more
            # fact besides: a transfer is not "these two are close", it is
            # "these two have been stopped together for hours", and that cannot
            # be recovered from a single report.
            previous = previous_positions.get(mmsi) or {}
            was_stopped = float(previous.get("speed") or 99.0) <= STS_MAX_SPEED_KNOTS
            now_stopped = (speed is not None) and float(speed) <= STS_MAX_SPEED_KNOTS
            stationary_since = (
                previous.get("stationary_since") if (was_stopped and now_stopped)
                else (observed_at.timestamp() if now_stopped else None)
            )

            pipe.set(
                f"vessel:last_seen:{mmsi}",
                json.dumps({
                    "lat": lat, "lon": lon, "heading": heading,
                    "region": region, "speed": speed, "ts": observed_at.isoformat(),
                    "epoch": observed_at.timestamp(),
                    "stationary_since": stationary_since,
                }),
                ex = 172800
            )

            # A geo index of vessels currently stopped, so co-location is a
            # bounded radius query rather than a scan of every hull afloat.
            # Redis GEO was available and unused; the alternative is O(N) over
            # `vessel:last_seen:*`, which the gap detector's own docstring
            # records as the mistake it had to stop making.
            if now_stopped and lat is not None and lon is not None:
                pipe.geoadd(STOPPED_VESSELS_GEO_KEY, (lon, lat, mmsi))
                pipe.expire(STOPPED_VESSELS_GEO_KEY, STOPPED_GEO_TTL_SEC)
            else:
                pipe.zrem(STOPPED_VESSELS_GEO_KEY, mmsi)
            
            # The breakdown travels with the row, for the reason the note above
            # the next loop gives: that loop does not unpack the scorer's
            # output, so reading `score_dict` there would silently bind the last
            # vessel's score to every event -- which is exactly what `nav_code`
            # did before it was moved into this tuple.
            results.append((raw, meta, mmsi, lat, lon, speed, heading, nav_status, nav_code, region, vessel, flags, vtype, anomaly,
                            breakdown_from_score(score_dict, "maritime")))

            # -- identity spoofing -------------------------------------------
            #
            # Two reports that cannot both be true. The Kalman filter already
            # turns this into a residual and the residual into a score; what was
            # missing is the claim. A score says "unusual"; VESSEL_SPOOF says
            # "this transponder reported a position it could not have reached",
            # which is a different sentence and the one the rule asks for.
            prev = previous_positions.get(mmsi)
            if prev and lat is not None and lon is not None:
                transit = impossible_transit(
                    prev,
                    {"lat": lat, "lon": lon, "ts": observed_at.timestamp()},
                )
                if transit:
                    spoof_events.append(_spoof_event(
                        raw, mmsi, vessel, lat, lon, region, flags, vtype, transit
                    ))

            # A position that is not water, checked with no previous report.
            #
            # The transit test above needs two reports and measures the speed
            # between them, so a transponder that simply sits inland is passed:
            # 65 vessel positions in seven days classified into an airspace
            # region and tripped nothing, including one 200km from the Red Sea
            # coast and one on land north of Hormuz.
            if lat is not None and lon is not None:
                inland = implausible_position(lat, lon)
                if inland:
                    spoof_events.append(_inland_event(
                        raw, mmsi, vessel, lat, lon, region, flags, vtype, inland
                    ))

        await pipe.execute()

        # -- ship-to-ship transfer -------------------------------------------
        #
        # Co-location is checked after the batch is written, so a pair reported
        # in the same batch is visible to the query. Only for hulls that are
        # themselves stopped: a moving vessel cannot be alongside.
        # Bounded per batch. Each check is a geosearch plus an mget, and while
        # the stopped filter already removes most reports -- a vessel under way
        # cannot be alongside -- an anchorage-heavy feed can leave a lot of
        # them. A transfer lasts hours and both hulls report every few minutes,
        # so a pair missed in this batch is seen in the next one; spending the
        # whole batch budget on one crowded anchorage is the worse trade.
        checked = 0
        for (raw, payload, meta, mmsi, pos, lat, lon, speed, heading, nav_status, nav_code, region), vessel in zip(parsed, vessels):
            if checked >= STS_CHECKS_PER_BATCH:
                break
            if speed is None or float(speed) > STS_MAX_SPEED_KNOTS:
                continue
            if lat is None or lon is None:
                continue
            checked += 1
            partner = await _stopped_neighbour(self.redis, mmsi, lat, lon)
            if partner:
                sts_events.append(_sts_event(
                    raw, mmsi, vessel, lat, lon, region, partner, vtype=None
                ))

        behaviour_events = [e for e in (spoof_events + sts_events) if e]
        
        # Batch graph updates
        graph_tasks = []
        for (_, _, mmsi, _, _, _, _, _, _, region, vessel, flags, vtype, _, _) in results:
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
        for (raw, meta, mmsi, lat, lon, speed, heading, nav_status, nav_code, region, vessel, flags, vtype, anomaly, breakdown) in results:
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
                # What backed this score, carried onto the event.
                #
                # The scorer has always returned coverage, significance and its
                # own domain for kinematic events, and the Kalman residual is a
                # genuine spatial measurement. None of it reached the event, so
                # `/explain/event/{id}` had nothing to read for any vessel --
                # 7,370 of them in the hour this was measured, against 198
                # equity events that did carry a breakdown.
                anomaly_breakdown = breakdown,
            ))

        # The two behaviours detected above, alongside the position reports.
        #
        # `rule_maritime_chokepoint_evasion` triggers on vessel_dark, vessel_sts
        # and vessel_spoof and correlates on the same three. Two of the three
        # had no producer at either end, so the rule was a dark-gap rule wearing
        # the name of an evasion rule.
        final_events.extend(behaviour_events)

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

        # The rest of the block. `VesselData` has declared all four of these
        # since it was written; the handler read three fields out of the
        # message and dropped the ones that identify the hull and say whether
        # it is loaded.
        imo = _ais_imo(s)
        draught = _ais_draught(s)
        length_m = _ais_length(s)
        eta = _ais_eta(s)
        
        flags = check_sanctions(name, mmsi)
        if imo:
            # A hull that has changed MMSI keeps its IMO, which is the whole
            # reason the field is worth carrying on this platform.
            flags = list(dict.fromkeys(list(flags or []) + list(check_sanctions(name, imo) or [])))

        # The IMO travels with the cached identity, so a later position report
        # -- which carries no static block at all -- can be joined to the hull
        # rather than only to the MMSI that happens to be transmitting it.
        await self.redis.raw.set(
            f"vessel:info:{mmsi}",
            json.dumps({ "name": name, "destination": dest,
                        "vessel_type": vtype, "flags": flags,
                        "imo": imo, "draught": draught }),
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
                imo=imo, draught=draught, length_meters=length_m, eta=eta,
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
