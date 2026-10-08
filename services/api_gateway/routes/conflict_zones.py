"""
services/api_gateway/routes/conflict_zones.py

Active and potential combat zones, and every vessel the platform is tracking.

/conflict-zones joins four things per zone that previously lived apart:
  - the standing assessment, drivers and ripple effects (shared/utils/conflict_zones.py)
  - live news intensity and headlines from GDELT, and the NGA navigational
    warnings that fall inside the zone (written by collector-news/conflict.py)
  - what the platform itself observed there in 24 hours: dark and spoofing
    vessels, ship-to-ship transfers, aviation emergencies and radar passes
  - this platform's own headlines naming the zone, and the latest recorded
    move in each instrument the zone bears on

/vessels/live returns the last known position of every vessel in the AIS
cache, marked dark when its last report is older than a dark-vessel event
raised for it, plus dark vessels whose cached position has already expired.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from services.api_gateway.dependencies import get_db_optional, get_redis_client
from shared.utils.conflict_zones import (
    NAV_WARNINGS_KEY, ZONE_LIVE_KEY, ZONES, ConflictZone,
    keyword_regex,
)
from shared.utils.maritime_behaviour import could_have_left_coverage, is_tanker
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.vessel_feeds import VIIRS_DETECTIONS_KEY
from shared.utils.ownership import holders_line, read_holders

logger = logging.getLogger("api.conflict_zones")

router = APIRouter(prefix="/api/v1", tags=["Conflict Zones & Vessels"])

# Observation types counted per zone, in the order a reader would scan them.
ACTIVITY_TYPES = (
    "vessel_dark", "vessel_spoof", "vessel_sts", "flight_anomaly", "supply_chain_metric",
)
MOVE_TYPES = ("market_anomaly", "price_anomaly")
HEADLINE_LIMIT = 10

_VESSEL_CACHE: Dict[str, Any] = {"at": 0.0, "body": None}
VESSEL_CACHE_SEC = 30.0


async def _redis_json(redis_client, key: str) -> Optional[Any]:
    try:
        blob = await getattr(redis_client, "raw", redis_client).get(key)
    except Exception as exc:
        swallowed("api.conflict_zones.redis_get", exc, logger, detail=key)
        return None
    if not blob:
        return None
    try:
        return json.loads(blob if isinstance(blob, str) else blob.decode("utf-8", "replace"))
    except ValueError as exc:
        swallowed("api.conflict_zones.redis_decode", exc, logger, detail=key)
        return None


def _zone_static(zone: ConflictZone) -> Dict[str, Any]:
    return {
        "zone_id": zone.zone_id,
        "name": zone.name,
        "prior": zone.prior,
        "bbox": list(zone.bbox),
        "center": list(zone.center),
        "regions": list(zone.regions),
        "parties": list(zone.parties),
        "drivers": list(zone.drivers),
        "ripple": [{"target": r.target, "kind": r.kind, "why": r.why} for r in zone.ripple],
    }


async def _activity(db, zones: List[ConflictZone]) -> Dict[str, Dict[str, int]]:
    """24-hour counts of each observation type in each zone's regions."""
    out: Dict[str, Dict[str, int]] = {z.zone_id: {} for z in zones}
    pairs = [(z.zone_id, r) for z in zones for r in z.regions]
    if db is None or not pairs:
        return out
    rows = await db.query(
        """
        SELECT m.zone_id, e.type, COUNT(*) AS n
        FROM unnest($1::text[], $2::text[]) AS m(zone_id, region)
        JOIN events e ON e.region = m.region
        WHERE e.occurred_at > NOW() - INTERVAL '24 hours'
          AND e.type = ANY($3::text[])
        GROUP BY m.zone_id, e.type
        """,
        [p[0] for p in pairs], [p[1] for p in pairs], list(ACTIVITY_TYPES),
    )
    for row in rows:
        out[row["zone_id"]][row["type"]] = int(row["n"])
    return out


async def _headlines(db, zone: ConflictZone, hours: int) -> List[Dict[str, Any]]:
    """This platform's own headlines naming the zone, newest first."""
    if db is None:
        return []
    rows = await db.query(
        """
        SELECT occurred_at, source, headline, url, anomaly_score
        FROM events
        WHERE type IN ('headline', 'social_signal')
          AND occurred_at > NOW() - make_interval(hours => $2)
          AND headline ~* $1
        ORDER BY occurred_at DESC
        LIMIT $3
        """,
        keyword_regex(zone), int(hours), HEADLINE_LIMIT,
    )
    return [dict(r) for r in rows]


async def _latest_moves(db, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """The newest recorded move per instrument in three days."""
    if db is None or not symbols:
        return {}
    rows = await db.query(
        """
        SELECT DISTINCT ON (primary_entity_id)
               primary_entity_id AS symbol, occurred_at, headline, anomaly_score
        FROM events
        WHERE type = ANY($2::text[])
          AND primary_entity_id = ANY($1::text[])
          AND occurred_at > NOW() - INTERVAL '72 hours'
        ORDER BY primary_entity_id, occurred_at DESC
        """,
        symbols, list(MOVE_TYPES),
    )
    return {r["symbol"]: dict(r) for r in rows}


def _warnings_by_zone(payload: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for w in (payload or {}).get("warnings") or []:
        if w.get("zone_id"):
            out.setdefault(w["zone_id"], []).append(w)
    return out


@router.get("/conflict-zones")
async def list_conflict_zones(
    hours: int = Query(72, ge=1, le=336, description="Window for this platform's own headlines"),
    db=Depends(get_db_optional),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Every zone with its status, reasons, ripple effects, news and activity."""
    zones = list(ZONES)
    warnings_payload = await _redis_json(redis_client, NAV_WARNINGS_KEY)
    warnings = _warnings_by_zone(warnings_payload)
    viirs = await _redis_json(redis_client, VIIRS_DETECTIONS_KEY) or {}
    viirs_counts = viirs.get("zone_counts") or {}
    try:
        activity = await _activity(db, zones)
    except Exception as exc:
        swallowed("api.conflict_zones.activity", exc, logger)
        activity = {z.zone_id: {} for z in zones}
    symbols = sorted({s for z in zones for s in z.instruments})
    try:
        moves = await _latest_moves(db, symbols)
    except Exception as exc:
        swallowed("api.conflict_zones.moves", exc, logger)
        moves = {}

    results = []
    for zone in zones:
        live = await _redis_json(redis_client, ZONE_LIVE_KEY.format(zone_id=zone.zone_id)) or {}
        try:
            own = await _headlines(db, zone, hours)
        except Exception as exc:
            swallowed("api.conflict_zones.headlines", exc, logger, detail=zone.zone_id)
            own = []
        entry = _zone_static(zone)
        entry["status"] = live.get("status") or ("active" if zone.prior == "active" else "watch")
        entry["trend"] = live.get("trend") or "unmeasured"
        entry["news_volume_24h"] = live.get("volume_24h")
        entry["news_baseline_daily"] = live.get("baseline_daily")
        entry["news_ratio"] = live.get("ratio")
        entry["volume_series"] = live.get("volume_series") or []
        entry["live_updated_at"] = live.get("updated_at")
        entry["news"] = live.get("articles") or []
        entry["platform_headlines"] = own
        entry["activity_24h"] = dict(activity.get(zone.zone_id, {}))
        if zone.zone_id in viirs_counts:
            # Last night's lit vessels from VIIRS, when that feed is configured.
            entry["activity_24h"]["viirs_lit_vessels"] = int(viirs_counts[zone.zone_id])
        entry["nav_warnings"] = warnings.get(zone.zone_id, [])
        for r in entry["ripple"]:
            if r["kind"] == "instrument":
                r["latest_move"] = moves.get(r["target"])
        results.append(entry)

    order = {"active-escalating": 0, "escalating": 1, "active": 2, "watch": 3}
    results.sort(key=lambda z: (order.get(z["status"], 9), -(z["news_volume_24h"] or 0)))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "zones": results,
        "nav_warnings_updated_at": (warnings_payload or {}).get("updated_at"),
        "military_warnings_outside_zones": [
            w for w in (warnings_payload or {}).get("warnings") or []
            if not w.get("zone_id") and w.get("military")
        ],
    }


def _epoch(value: Any) -> Optional[float]:
    """Seconds since the epoch from a datetime or an ISO string.

    The Timescale client returns timestamps as strings, not datetimes.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


# How much a zone's status counts toward an instrument's exposure.
STATUS_WEIGHT = {"active-escalating": 1.0, "escalating": 0.8, "active": 0.6, "watch": 0.3}


def exposure_score(zone_weights: List[float], mentions: int, observations: int) -> float:
    """Zones that bear on an instrument, and how much they are in the news.

    The zone weights carry the standing and live status; mentions and
    observations are counted on a log scale so one busy feed cannot dominate.
    """
    import math

    return round(sum(zone_weights) + 0.25 * math.log1p(mentions) + 0.15 * math.log1p(observations), 3)


async def _zone_tagged_counts(db, hours: int) -> Dict[str, Dict[str, int]]:
    """Per instrument: zone-tagged headlines and zone-tagged observations naming it."""
    if db is None:
        return {}
    rows = await db.query(
        """
        SELECT substr(t, 9) AS symbol,
               COUNT(*) FILTER (WHERE e.type IN ('headline', 'social_signal')) AS mentions,
               COUNT(*) FILTER (WHERE e.type NOT IN ('headline', 'social_signal')) AS observations
        FROM events e, unnest(e.tags) AS t
        WHERE e.occurred_at > NOW() - make_interval(hours => $1)
          AND t LIKE 'exposes:%'
          AND EXISTS (SELECT 1 FROM unnest(e.tags) z WHERE z LIKE 'zone:%')
        GROUP BY 1
        """,
        int(hours),
    )
    return {r["symbol"]: {"mentions": int(r["mentions"]), "observations": int(r["observations"])} for r in rows}


@router.get("/conflict-zones/exposure")
async def conflict_exposure(
    hours: int = Query(72, ge=1, le=336),
    limit: int = Query(30, ge=1, le=100),
    db=Depends(get_db_optional),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Instruments ranked by how much the conflict zones bear on them.

    Each row names the zones and why, how often zone-tagged news and
    observations named it, its latest recorded move, and -- for listed
    companies -- its largest reported holders.
    """
    body = await list_conflict_zones(hours=hours, db=db, redis_client=redis_client, user=user)
    try:
        counts = await _zone_tagged_counts(db, hours)
    except Exception as exc:
        swallowed("api.conflict_zones.exposure_counts", exc, logger)
        counts = {}

    rows: Dict[str, Dict[str, Any]] = {}
    for z in body["zones"]:
        weight = STATUS_WEIGHT.get(z["status"], 0.3)
        for r in z["ripple"]:
            if r["kind"] != "instrument":
                continue
            row = rows.setdefault(r["target"], {
                "symbol": r["target"], "zones": [], "latest_move": r.get("latest_move"),
            })
            row["zones"].append({
                "zone_id": z["zone_id"], "name": z["name"], "status": z["status"],
                "weight": weight, "why": r["why"],
            })
    # Instruments the news mapped to a zone that no ripple names.
    for sym, n in counts.items():
        rows.setdefault(sym, {"symbol": sym, "zones": [], "latest_move": None})

    raw = getattr(redis_client, "raw", redis_client)
    out = []
    for sym, row in rows.items():
        n = counts.get(sym, {"mentions": 0, "observations": 0})
        row["mentions"] = n["mentions"]
        row["observations"] = n["observations"]
        row["score"] = exposure_score([zz["weight"] for zz in row["zones"]], n["mentions"], n["observations"])
        out.append(row)
    out.sort(key=lambda r: -r["score"])
    out = out[:limit]
    for row in out:
        if row["symbol"].endswith("=F"):
            row["holders"] = None
            continue
        try:
            row["holders"] = holders_line(row["symbol"], await read_holders(raw, row["symbol"])) or None
        except Exception as exc:
            swallowed("api.conflict_zones.exposure_holders", exc, logger, detail=row["symbol"])
            row["holders"] = None
    return {"generated_at": body["generated_at"], "window_hours": hours, "instruments": out}


async def _dark_vessels(db) -> Dict[str, Dict[str, Any]]:
    """Dark-vessel events in 48 hours, newest per hull."""
    if db is None:
        return {}
    rows = await db.query(
        """
        SELECT DISTINCT ON (primary_entity_id)
               primary_entity_id AS mmsi, primary_entity_name AS name, occurred_at,
               region, anomaly_score,
               COALESCE(latitude, ST_Y(coordinates::geometry)) AS lat,
               COALESCE(longitude, ST_X(coordinates::geometry)) AS lon
        FROM events
        WHERE type = 'vessel_dark' AND occurred_at > NOW() - INTERVAL '48 hours'
        ORDER BY primary_entity_id, occurred_at DESC
        """
    )
    return {str(r["mmsi"]): dict(r) for r in rows}


async def _build_vessels(db, redis_client) -> Dict[str, Any]:
    raw = getattr(redis_client, "raw", redis_client)
    keys: List[str] = []
    async for k in raw.scan_iter("vessel:last_seen:*", count=5000):
        keys.append(k if isinstance(k, str) else k.decode())
    mmsis = [k.rsplit(":", 1)[1] for k in keys]
    positions = await raw.mget(keys) if keys else []
    infos = await raw.mget([f"vessel:info:{m}" for m in mmsis]) if mmsis else []
    try:
        dark = await _dark_vessels(db)
    except Exception as exc:
        swallowed("api.conflict_zones.dark_vessels", exc, logger)
        dark = {}

    now = time.time()
    vessels = []
    for mmsi, pos_blob, info_blob in zip(mmsis, positions, infos):
        if not pos_blob:
            continue
        try:
            pos = json.loads(pos_blob)
            info = json.loads(info_blob) if info_blob else {}
        except ValueError:
            continue
        lat, lon = pos.get("lat"), pos.get("lon")
        if lat is None or lon is None:
            continue
        epoch = pos.get("epoch")
        dark_event = dark.get(mmsi)
        dark_at = _epoch(dark_event["occurred_at"]) if dark_event else None
        is_dark = bool(dark_at is not None and epoch is not None and dark_at >= float(epoch))
        # Raised before the detector learned the coverage edges, a dark event
        # can describe a hull that simply sailed out of AIS range.
        if is_dark and could_have_left_coverage(
            lat, lon, pos.get("speed"), (now - float(epoch)) / 3600.0
        ):
            is_dark = False
        vtype = info.get("vessel_type")
        vessels.append({
            "mmsi": mmsi,
            "name": info.get("name") or None,
            "lat": round(float(lat), 5),
            "lon": round(float(lon), 5),
            "speed": pos.get("speed"),
            "heading": pos.get("heading"),
            "region": pos.get("region"),
            "vessel_type": vtype,
            "tanker": is_tanker(vtype, info.get("name")),
            "sanctioned": bool(info.get("flags")),
            "age_seconds": round(now - float(epoch)) if epoch is not None else None,
            "dark": is_dark,
        })
    seen = {v["mmsi"] for v in vessels}
    # Dark hulls whose cached position has expired are still dark, and their
    # last known position is on the event.
    for mmsi, ev in dark.items():
        if mmsi in seen or ev.get("lat") is None or ev.get("lon") is None:
            continue
        vessels.append({
            "mmsi": mmsi, "name": ev.get("name"),
            "lat": round(float(ev["lat"]), 5), "lon": round(float(ev["lon"]), 5),
            "speed": None, "heading": None, "region": ev.get("region"),
            "vessel_type": None, "tanker": False, "sanctioned": False,
            "age_seconds": round(now - (_epoch(ev["occurred_at"]) or now)),
            "dark": True,
        })
    viirs = await _redis_json(redis_client, VIIRS_DETECTIONS_KEY) or {}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # Ships seen by their lights at night (NOAA VIIRS), no transponder
        # needed. Empty until EOG credentials are set.
        "light_detections": viirs.get("detections") or [],
        "light_detections_file": viirs.get("file"),
        "count": len(vessels),
        "dark": sum(1 for v in vessels if v["dark"]),
        "tankers": sum(1 for v in vessels if v["tanker"]),
        "vessels": vessels,
        "note": "AIS is terrestrial and volunteer-fed: the Gulf and Red Sea are barely covered. "
                "See /chokepoints for radar coverage of those straits.",
    }


@router.get("/vessels/live")
async def live_vessels(
    db=Depends(get_db_optional),
    redis_client=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Every vessel the platform is tracking, with dark hulls marked."""
    if _VESSEL_CACHE["body"] is not None and time.monotonic() - _VESSEL_CACHE["at"] < VESSEL_CACHE_SEC:
        return _VESSEL_CACHE["body"]
    body = await _build_vessels(db, redis_client)
    _VESSEL_CACHE.update(at=time.monotonic(), body=body)
    return body
