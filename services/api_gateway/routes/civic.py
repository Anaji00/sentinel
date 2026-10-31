"""
services/api_gateway/routes/civic.py

Watching the watchers (shared/utils/surveillance.py, services/collector-civic):
mapped licence-plate readers and gunshot detectors, which agency runs which
surveillance technology, a report card per agency, and what watches a place.

Read-only public-record data about agencies and vendors. No plate, face or
person is looked up or stored here, and a place someone looks up is not kept.
"""
import asyncio
import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_db_optional, get_redis_optional
from shared.utils.response_cache import cache_key, cached_json
from shared.utils import surveillance as sv
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role

logger = logging.getLogger("api.civic")

router = APIRouter(prefix="/api/v1/civic", tags=["Civic"])

MAX_DEVICES = 8000
MAX_RADIUS_KM = 50.0
# Degrees of latitude per km, for a bounding box before the exact distance.
_KM_PER_DEG_LAT = 111.0

_SUMMARY_SQL = {
    "devices_by_kind": "SELECT kind AS k, count(*) AS n FROM surveillance_devices GROUP BY 1 ORDER BY 2 DESC",
    "devices_by_maker": """SELECT coalesce(manufacturer, 'unknown') AS k, count(*) AS n FROM surveillance_devices
                           GROUP BY 1 ORDER BY 2 DESC LIMIT 12""",
    "tech_by_type": "SELECT technology AS k, count(DISTINCT agency || state) AS n FROM surveillance_agency_tech GROUP BY 1 ORDER BY 2 DESC",
    "vendors": """SELECT vendor AS k, count(DISTINCT agency || state) AS n FROM surveillance_agency_tech
                  WHERE vendor IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15""",
    "states": """SELECT state AS k, count(DISTINCT agency) AS n FROM surveillance_agency_tech
                 WHERE state <> '' GROUP BY 1 ORDER BY 2 DESC""",
}
_TOTALS_SQL = """
    SELECT (SELECT count(*) FROM surveillance_devices) AS devices,
           (SELECT count(DISTINCT agency || state) FROM surveillance_agency_tech) AS agencies,
           (SELECT count(*) FROM surveillance_agency_tech) AS records,
           (SELECT max(last_seen) FROM surveillance_devices) AS devices_updated,
           (SELECT max(updated_at) FROM surveillance_agency_tech) AS agencies_updated
"""
_DEVICES_SQL = """
    SELECT osm_id, lat, lon, kind, manufacturer, operator, direction FROM surveillance_devices
    WHERE lat BETWEEN $1 AND $3 AND lon BETWEEN $2 AND $4 LIMIT $5
"""
_AGENCIES_SQL = """
    SELECT agency, state, city, county, min(lat) AS lat, min(lon) AS lon,
           array_agg(DISTINCT technology) AS technologies, array_agg(DISTINCT vendor) FILTER (WHERE vendor IS NOT NULL) AS vendors
    FROM surveillance_agency_tech
    WHERE ($1::text IS NULL OR state = $1) AND ($2::text IS NULL OR technology = $2)
      AND ($3::text IS NULL OR agency ILIKE '%' || $3 || '%' OR city ILIKE '%' || $3 || '%')
    GROUP BY agency, state, city, county
    ORDER BY agency LIMIT $4
"""
_AGENCY_ROWS_SQL = """
    SELECT agency, state, city, county, lea_type, juris_type, technology, vendor, summary, link, link_date, lat, lon,
           geo_level
    FROM surveillance_agency_tech WHERE agency = $1 AND state = $2 ORDER BY technology, link_date DESC NULLS LAST
"""
_NEAR_DEVICES_SQL = """
    SELECT osm_id, lat, lon, kind, manufacturer, operator, direction FROM surveillance_devices
    WHERE lat BETWEEN $1 AND $3 AND lon BETWEEN $2 AND $4
"""
_NEAR_AGENCIES_SQL = """
    SELECT agency, state, city, lat, lon, technology, vendor FROM surveillance_agency_tech
    WHERE lat BETWEEN $1 AND $3 AND lon BETWEEN $2 AND $4
"""


def _box(lat: float, lon: float, km: float) -> List[float]:
    import math
    dlat = km / _KM_PER_DEG_LAT
    dlon = km / (_KM_PER_DEG_LAT * max(0.1, math.cos(math.radians(lat))))
    return [lat - dlat, lon - dlon, lat + dlat, lon + dlon]


def _plain(r: Dict[str, Any]) -> Dict[str, Any]:
    return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}


@router.get("/summary")
async def summary(db=Depends(get_db_optional), redis=Depends(get_redis_optional),
                  user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Totals: mapped devices by kind and maker; agencies by technology, vendor and state.

    Cached five minutes: up to 1.3 s to compute, and the totals move daily."""
    return await cached_json(getattr(redis, "raw", None), cache_key("civic_summary"),
                             lambda: _summary(db), ttl=300, max_stale=86400)


async def _summary(db) -> Dict[str, Any]:
    out: Dict[str, Any] = {"totals": {}}
    if not db:
        return out
    try:
        out["totals"] = _plain((await db.query(_TOTALS_SQL) or [{}])[0])
        for key, sql in _SUMMARY_SQL.items():
            out[key] = [{"name": r["k"], "count": int(r["n"])} for r in await db.query(sql) or []]
    except Exception as e:
        swallowed("api_gateway.civic.summary", e, logger)
    return out


@router.get("/devices")
async def devices(bbox: str = Query(..., description="south,west,north,east"),
                  db=Depends(get_db_optional),
                  user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Mapped surveillance devices inside a map box (at most MAX_DEVICES)."""
    try:
        s, w, n, e = (float(x) for x in bbox.split(","))
    except ValueError:
        raise HTTPException(status_code=422, detail="bbox must be south,west,north,east")
    if not (-90 <= s < n <= 90 and -180 <= w < e <= 180):
        raise HTTPException(status_code=422, detail="bbox out of range")
    if not db:
        return {"devices": [], "truncated": False}
    rows = await db.query(_DEVICES_SQL, s, w, n, e, MAX_DEVICES + 1) or []
    return {"devices": [_plain(r) for r in rows[:MAX_DEVICES]], "truncated": len(rows) > MAX_DEVICES}


@router.get("/agencies")
async def agencies(state: Optional[str] = Query(None, min_length=2, max_length=2),
                   technology: Optional[str] = Query(None, max_length=60),
                   q: Optional[str] = Query(None, max_length=80),
                   limit: int = Query(2000, ge=1, le=10000),
                   db=Depends(get_db_optional),
                   user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Agencies with the technologies the Atlas documents, filterable."""
    if not db:
        return {"agencies": []}
    rows = await db.query(_AGENCIES_SQL, state.upper() if state else None, technology, q, limit) or []
    out = []
    for r in rows:
        techs = list(r["technologies"] or [])
        out.append({**_plain(r), "technologies": techs, "vendors": list(r["vendors"] or []),
                    "intensity": sum(sv.TECH_WEIGHT.get(t, 1) for t in techs)})
    return {"agencies": out}


@router.get("/agency")
async def agency(name: str = Query(..., max_length=160), state: str = Query(..., min_length=2, max_length=2),
                 db=Depends(get_db_optional),
                 user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """One agency's report card: each technology, what it means, its vendors and
    the evidence (source links), plus mapped devices within 15 km of its centre."""
    if not db:
        raise HTTPException(status_code=503, detail="database unavailable")
    rows = await db.query(_AGENCY_ROWS_SQL, name, state.upper()) or []
    if not rows:
        raise HTTPException(status_code=404, detail="agency not in the Atlas of Surveillance")
    head = rows[0]
    nearby: Dict[str, int] = {}
    if head["lat"] is not None:
        for d in await db.query(_NEAR_DEVICES_SQL, *_box(head["lat"], head["lon"], 15.0)) or []:
            if sv.haversine_km(head["lat"], head["lon"], d["lat"], d["lon"]) <= 15.0:
                nearby[d["kind"]] = nearby.get(d["kind"], 0) + 1
    slug = await portal_for(db, head["state"], head["city"], head["county"])
    from shared.utils import oversight as ov
    return {"portal_slug": slug,
            "agency": head["agency"], "state": head["state"], "city": head["city"], "county": head["county"],
            "type": head["lea_type"], "jurisdiction": head["juris_type"], "lat": head["lat"], "lon": head["lon"],
            "placed_by": head["geo_level"],
            "oversight": ov.summary(ov.law_for_agency(head["agency"], head["state"])),
            "oversight_campaign": ov.CAMPAIGN_URL,
            **sv.report_card([dict(r) for r in rows], nearby)}


@router.get("/near")
async def near(lat: float = Query(..., ge=-90, le=90), lon: float = Query(..., ge=-180, le=180),
               radius_km: float = Query(5.0, gt=0, le=MAX_RADIUS_KM),
               db=Depends(get_db_optional),
               user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """What watches a place: mapped devices within radius_km (nearest first) and
    agencies centred within twice that, with their technologies. The place is
    not logged or stored."""
    if not db:
        return {"devices": [], "agencies": []}
    devs = []
    for d in await db.query(_NEAR_DEVICES_SQL, *_box(lat, lon, radius_km)) or []:
        km = sv.haversine_km(lat, lon, d["lat"], d["lon"])
        if km <= radius_km:
            devs.append({**_plain(d), "km": round(km, 2)})
    devs.sort(key=lambda d: d["km"])
    ags: Dict[str, Dict[str, Any]] = {}
    for a in await db.query(_NEAR_AGENCIES_SQL, *_box(lat, lon, radius_km * 2)) or []:
        km = sv.haversine_km(lat, lon, a["lat"], a["lon"])
        if km > radius_km * 2:
            continue
        fresh = {"agency": a["agency"], "state": a["state"], "city": a["city"], "km": round(km, 1),
                 "technologies": [], "vendors": []}
        e = ags.setdefault(f"{a['agency']}|{a['state']}", fresh)
        if a["technology"] not in e["technologies"]:
            e["technologies"].append(a["technology"])
        if a["vendor"] and a["vendor"] not in e["vendors"]:
            e["vendors"].append(a["vendor"])
    by_kind: Dict[str, int] = {}
    for d in devs:
        by_kind[d["kind"]] = by_kind.get(d["kind"], 0) + 1
    return {"radius_km": radius_km, "device_counts": by_kind, "devices": devs[:500],
            "agencies": sorted(ags.values(), key=lambda e: e["km"]),
            "caveat": "Devices are volunteer-mapped in OpenStreetMap: an empty area may be unmapped, not unwatched."}


@router.get("/privacy")
async def privacy(state: Optional[str] = Query(None, min_length=2, max_length=2),
                  user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Free steps to protect your data, most effective first, state-aware."""
    return {"steps": sv.privacy_steps(state)}


# ── Map tiles, served from this deployment ───────────────────────────────────
# The browser never contacts a tile server: each tile is fetched once by the
# gateway and cached on disk. Viewers' IPs and the places they look at stay
# here -- on a page about surveillance, the map must not announce its readers.
# OpenStreetMap's tile policy asks for an identifying User-Agent, caching and
# attribution; tiles are kept TILE_TTL_SEC as it recommends.
TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
TILE_DIR = Path(os.getenv("CIVIC_TILE_DIR", "/tmp/sentinel-tiles"))
TILE_TTL_SEC = 7 * 24 * 3600
TILE_MAX_FILES = 20000
TILE_MAX_ZOOM = 19
TILE_UA = "Sentinel-Intelligence-Platform/1.0 (self-hosted civic map; tile cache)"
_tile_session: Optional[aiohttp.ClientSession] = None
_tile_lock = asyncio.Lock()


def _tile_path(z: int, x: int, y: int) -> Path:
    return TILE_DIR / str(z) / str(x) / f"{y}.png"


def _prune_tiles() -> None:
    """Oldest tiles out once the cache passes TILE_MAX_FILES."""
    files = list(TILE_DIR.rglob("*.png"))
    if len(files) <= TILE_MAX_FILES:
        return
    files.sort(key=lambda f: f.stat().st_mtime)
    for f in files[: len(files) - TILE_MAX_FILES]:
        try:
            f.unlink()
        except OSError as e:          # already gone: another request pruned it
            swallowed("api_gateway.civic.tile_prune", e, logger)


async def _session() -> aiohttp.ClientSession:
    global _tile_session
    async with _tile_lock:
        if _tile_session is None or _tile_session.closed:
            _tile_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20),
                                                  headers={"User-Agent": TILE_UA})
    return _tile_session


@router.get("/tiles/{z}/{x}/{y}.png")
async def tile(z: int, x: int, y: int, user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Response:
    if not (0 <= z <= TILE_MAX_ZOOM and 0 <= x < 2 ** z and 0 <= y < 2 ** z):
        raise HTTPException(status_code=404, detail="no such tile")
    path = _tile_path(z, x, y)
    headers = {"Cache-Control": "private, max-age=86400"}
    try:
        if path.exists() and time.time() - path.stat().st_mtime < TILE_TTL_SEC:
            return Response(content=await asyncio.to_thread(path.read_bytes), media_type="image/png", headers=headers)
    except OSError as e:
        swallowed("api_gateway.civic.tile_read", e, logger)
    try:
        session = await _session()
        async with session.get(TILE_URL.format(z=z, x=x, y=y)) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=502, detail="tile unavailable")
            body = await resp.read()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        if path.exists():           # stale beats nothing
            return Response(content=await asyncio.to_thread(path.read_bytes), media_type="image/png", headers=headers)
        raise HTTPException(status_code=502, detail="tile unavailable")

    def _store() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{hashlib.md5(body).hexdigest()[:8]}.tmp")
        tmp.write_bytes(body)
        tmp.replace(path)
        if hash((z, x, y)) % 200 == 0:
            _prune_tiles()

    try:
        await asyncio.to_thread(_store)
    except OSError as e:
        swallowed("api_gateway.civic.tile_write", e, logger)
    return Response(content=body, media_type="image/png", headers=headers)


# ── "Is it legal here?" (Phase 2; shared/utils/civic_law.py) ─────────────────
_FINDINGS_SQL = """
    SELECT f.slug, f.code, f.severity, f.state, f.agency, f.title, f.detail, f.citation, f.source, f.evidence,
           f.first_seen, p.portal_url, p.data_last_updated, p.lat, p.lon
    FROM civic_findings f LEFT JOIN flock_portals p ON p.slug = f.slug
    WHERE ($1::text IS NULL OR f.state = $1) AND ($2::text IS NULL OR f.severity = $2)
      AND ($3::text IS NULL OR f.code = $3)
    ORDER BY CASE f.severity WHEN 'conflict' THEN 0 ELSE 1 END, f.state, f.agency LIMIT $4
"""
_FINDING_COUNTS_SQL = """
    SELECT code, severity, state, count(*) AS n FROM civic_findings GROUP BY 1, 2, 3 ORDER BY 4 DESC
"""
_PORTAL_SQL = """
    SELECT slug, portal_url, city, county, state, agency_type, population, total_cameras, total_searches,
           data_retention, vehicles_captured, hotlist_hits, hotlist_hit_rate, shared_with_count, received_from_count,
           shared_with, prohibited_uses, public_search_audit, data_last_updated, lat, lon
    FROM flock_portals WHERE slug = $1
"""
_PORTAL_FINDINGS_SQL = "SELECT code, severity, title, detail, citation, source, evidence FROM civic_findings WHERE slug = $1"
_PORTAL_TOTALS_SQL = """
    SELECT count(*) AS portals, sum(total_cameras) AS cameras, sum(vehicles_captured) AS vehicles,
           count(*) FILTER (WHERE data_retention > 30) AS over_30_days, max(updated_at) AS updated
    FROM flock_portals
"""
_287G_COUNT_SQL = "SELECT count(DISTINCT (state, agency)) AS agencies, count(*) AS agreements FROM ice_287g_agreements"
_PORTAL_MATCH_SQL = """
    SELECT slug FROM flock_portals WHERE state = $1 AND (lower(city) = lower($2) OR lower(county) = lower($3))
    ORDER BY CASE WHEN lower(city) = lower($2) THEN 0 ELSE 1 END LIMIT 5
"""


def _json_field(v: Any) -> Any:
    import json as _json
    return _json.loads(v) if isinstance(v, str) else v


@router.get("/laws")
async def laws(user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Each state's licence-plate-reader rules as the checks apply them, with citations."""
    from shared.utils import civic_law as cl
    out = []
    for st, law in sorted(cl.STATE_LAWS.items()):
        out.append({"state": st, "citation": law.citation, "retention_days": law.retention_days,
                    "retention_text": law.retention_text, "immigration_use_banned": law.immigration_use_banned,
                    "immigration_sharing_banned": law.immigration_sharing_banned,
                    "out_of_state_sharing_banned": law.out_of_state_sharing_banned,
                    "out_of_state_allowed": sorted(law.out_of_state_allowed),
                    "declaration_required": law.declaration_required,
                    "federal_sharing_banned": law.federal_sharing_banned, "note": law.note, "source": law.source})
    return {"laws": out, "source": cl.NCSL_URL,
            "caveat": "Statutes carry exceptions (warrants, active investigations, declarations on file) that "
                      "published portal data cannot show. Findings are questions to ask, not legal conclusions."}


@router.get("/legal")
async def legal(state: Optional[str] = Query(None, min_length=2, max_length=2),
                severity: Optional[str] = Query(None, pattern="^(conflict|check)$"),
                code: Optional[str] = Query(None, max_length=40),
                limit: int = Query(500, ge=1, le=5000),
                db=Depends(get_db_optional),
                user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Possible conflicts between agencies' published Flock practice and state law,
    conflicts first, with evidence and citations; plus national counts."""
    out: Dict[str, Any] = {"findings": [], "counts": [], "totals": {}}
    if not db:
        return out
    try:
        rows = await db.query(_FINDINGS_SQL, state.upper() if state else None, severity, code, limit) or []
        out["findings"] = [{**_plain(r), "evidence": _json_field(r["evidence"])} for r in rows]
        out["counts"] = [_plain(r) for r in await db.query(_FINDING_COUNTS_SQL) or []]
        totals = (await db.query(_PORTAL_TOTALS_SQL) or [{}])[0]
        g = (await db.query(_287G_COUNT_SQL) or [{}])[0]
        out["totals"] = {**_plain(totals), "ice_287g_agencies": g.get("agencies"), "ice_287g_agreements": g.get("agreements")}
    except Exception as e:
        swallowed("api_gateway.civic.legal", e, logger)
    return out


@router.get("/portal")
async def portal(slug: str = Query(..., max_length=160), db=Depends(get_db_optional),
                 user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """One agency's published Flock practice and its findings. The sharing list
    is returned in full: which agencies can search this one's plate data."""
    if not db:
        raise HTTPException(status_code=503, detail="database unavailable")
    row = await db.query_one(_PORTAL_SQL, slug)
    if not row:
        raise HTTPException(status_code=404, detail="no such portal")
    from shared.utils import civic_law as cl
    from shared.utils import oversight as ov
    shared = _json_field(row["shared_with"]) or []
    st = row["state"]
    by_state: Dict[str, int] = {}
    for o in shared:
        k = "federal" if cl.is_federal(o) else (cl.org_state(o) or "unknown")
        by_state[k] = by_state.get(k, 0) + 1
    findings = [{**_plain(f), "evidence": _json_field(f["evidence"])} for f in await db.query(_PORTAL_FINDINGS_SQL, slug) or []]
    law = cl.STATE_LAWS.get(st)
    audit = await db.query_one(_AUDIT_ONE_SQL, slug)
    return {**_plain({k: v for k, v in dict(row).items() if k != "shared_with"}), "shared_with": shared,
            "audit": _plain(audit) if audit else None,
            "shared_by_state": dict(sorted(by_state.items(), key=lambda kv: -kv[1])),
            "out_of_state_share": sum(n for k, n in by_state.items() if k not in (st, "unknown")),
            "findings": findings, "state_law": law.citation if law else None,
            "oversight": ov.summary(ov.law_for_portal(row["city"], row["county"], row["agency_type"], st)),
            "oversight_campaign": ov.CAMPAIGN_URL,
            "attribution": "Portal data: EyesOnFlock (CC BY-SA 4.0), from Flock Safety transparency portals."}


async def portal_for(db: Any, state: str, city: Optional[str], county: Optional[str]) -> Optional[str]:
    """The Flock portal slug for an Atlas agency's place, if one is published."""
    rows = await db.query(_PORTAL_MATCH_SQL, state, city or "", county or "") or []
    return rows[0]["slug"] if rows else None


@router.get("/records-request")
async def records_request(slug: str = Query(..., max_length=160), db=Depends(get_db_optional),
                          user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """A public-records request letter for one agency, citing its open findings.
    Drafted only: the person sends it themselves."""
    from datetime import date as _date
    from shared.utils import civic_law as cl
    if not db:
        raise HTTPException(status_code=503, detail="database unavailable")
    row = await db.query_one(_PORTAL_SQL, slug)
    if not row:
        raise HTTPException(status_code=404, detail="no such portal")
    findings = [dict(f) for f in await db.query(_PORTAL_FINDINGS_SQL, slug) or []]
    from shared.utils import oversight as ov
    agency = " ".join(x for x in (row["city"] or row["county"], row["agency_type"]) if x) or slug
    law = ov.law_for_portal(row["city"], row["county"], row["agency_type"], row["state"])
    return cl.records_request(agency, row["state"], findings, _date.today().isoformat(),
                              [ov.records_ask(law)] if law else ())


_AUDIT_ONE_SQL = """
    SELECT days, searches, first_search, last_search, immigration_hits, immigration_terms, first_amendment_hits,
           first_amendment_terms, wide_searches, no_reason, updated_at
    FROM flock_audit_stats WHERE slug = $1
"""
_AUDITS_SQL = """
    SELECT a.slug, coalesce(p.city, p.county, initcap(replace(a.slug, '-', ' '))) AS city, p.county, p.state,
           p.agency_type, a.days, a.searches, a.immigration_hits,
           a.immigration_terms, a.first_amendment_hits, a.first_amendment_terms, a.wide_searches, a.no_reason
    FROM flock_audit_stats a JOIN flock_portals p ON p.slug = a.slug
    WHERE a.searches > 0 AND ($1::text IS NULL OR p.state = $1)
    ORDER BY CASE $2 WHEN 'first_amendment' THEN a.first_amendment_hits WHEN 'wide' THEN a.wide_searches
             ELSE a.immigration_hits END DESC, a.searches DESC
    LIMIT $3
"""
_DEPARTED_SQL = """
    SELECT slug, coalesce(city, county, initcap(replace(slug, '-', ' '))) AS city, county, state, agency_type,
           total_cameras, first_seen, removed_at
    FROM flock_portals WHERE removed_at IS NOT NULL ORDER BY removed_at DESC LIMIT 500
"""


@router.get("/audits")
async def audits(state: Optional[str] = Query(None, min_length=2, max_length=2),
                 sort: str = Query("immigration", pattern="^(immigration|first_amendment|wide)$"),
                 limit: int = Query(100, ge=1, le=1000),
                 db=Depends(get_db_optional),
                 user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Agencies' released search logs, summarised: searches whose stated reason
    cites immigration or First Amendment activity (by the published keyword
    list), and nationwide lookups. Counts only; no reason text is kept."""
    from shared.utils import civic_law as cl
    terms = {cat: [t for t, _rx in v] for cat, v in cl.REASON_TERMS.items()}
    if not db:
        return {"agencies": [], "keywords": terms}
    rows = await db.query(_AUDITS_SQL, state.upper() if state else None, sort, limit) or []
    return {"agencies": [_plain(r) for r in rows], "keywords": terms,
            "wide_search_networks": cl.WIDE_SEARCH_NETWORKS,
            "caveat": "Keyword matches can include unrelated uses, and many searches give no reason. Source: "
                      "released Flock audit logs via EyesOnFlock (CC BY-SA 4.0)."}


@router.get("/departures")
async def departures(db=Depends(get_db_optional),
                     user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Agencies whose Flock transparency portal stopped being listed: likely
    contract cancellations, or a gap in the aggregator -- each worth confirming."""
    if not db:
        return {"departed": []}
    return {"departed": [_plain(r) for r in await db.query(_DEPARTED_SQL) or []],
            "caveat": "A portal missing from the weekly list may mean a cancelled contract, a renamed portal or an "
                      "aggregator gap. Confirm with the city's council record."}


_DRONES_SQL = """
    SELECT applicant, city, state, category, status, received, completed, start_date, altitude, lat, lon
    FROM drone_waivers WHERE ($1::text IS NULL OR state = $1) ORDER BY state, applicant LIMIT $2
"""
_DRONE_COUNTS_SQL = "SELECT state, count(*) AS n FROM drone_waivers GROUP BY 1 ORDER BY 2 DESC"
_MONEY_TOTALS_SQL = """
    SELECT kind, matched, count(*) AS awards, sum(amount) AS total FROM federal_awards GROUP BY 1, 2 ORDER BY 4 DESC NULLS LAST
"""
_MONEY_AGENCIES_SQL = """
    SELECT agency, sub_agency, count(*) AS awards, sum(amount) AS total FROM federal_awards
    WHERE kind = 'contract' GROUP BY 1, 2 ORDER BY 4 DESC NULLS LAST LIMIT 25
"""
_MONEY_TOP_SQL = """
    SELECT award_id, kind, matched, recipient, amount, agency, sub_agency, start_date, description
    FROM federal_awards WHERE ($1::text IS NULL OR matched = $1) ORDER BY amount DESC NULLS LAST LIMIT $2
"""


@router.get("/drones")
async def drones(state: Optional[str] = Query(None, min_length=2, max_length=2),
                 limit: int = Query(2000, ge=1, le=5000), db=Depends(get_db_optional),
                 user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """FAA drone-as-first-responder waivers (EFF's FOIA release). A waiver
    permits a program; it does not show one is flying."""
    if not db:
        return {"waivers": [], "by_state": []}
    rows = await db.query(_DRONES_SQL, state.upper() if state else None, limit) or []
    return {"waivers": [_plain(r) for r in rows],
            "by_state": [_plain(r) for r in await db.query(_DRONE_COUNTS_SQL) or []],
            "source": "https://www.eff.org/deeplinks/2026/07/hundreds-drone-first-responder-programs-could-soon-be-launched-across-country"}


@router.get("/money")
async def money(matched: Optional[str] = Query(None, max_length=80), limit: int = Query(50, ge=1, le=500),
                db=Depends(get_db_optional),
                user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Federal money for surveillance (USAspending): contracts to surveillance
    vendors and grants whose descriptions name a surveillance technology."""
    if not db:
        return {"totals": [], "agencies": [], "awards": []}
    return {"totals": [_plain(r) for r in await db.query(_MONEY_TOTALS_SQL) or []],
            "agencies": [_plain(r) for r in await db.query(_MONEY_AGENCIES_SQL) or []],
            "awards": [_plain(r) for r in await db.query(_MONEY_TOP_SQL, matched, limit) or []],
            "caveat": "Contracts are matched by recipient name and grants by description keywords; a grant that "
                      "mentions a technology may fund other things too. Source: USAspending.gov."}


_BROKERS_SQL = """
    SELECT name, dba, website, privacy_url, city, state, flags, registry FROM data_brokers
    WHERE ($1::text IS NULL OR (flags ->> $1)::boolean IS TRUE)
      AND ($2::text IS NULL OR name ILIKE '%' || $2 || '%' OR dba ILIKE '%' || $2 || '%')
      AND ($4::text IS NULL OR registry = $4 OR
           (left($4, 4) = 'any:' AND ',' || registry || ',' LIKE '%,' || substr($4, 5) || ',%'))
    ORDER BY name LIMIT $3
"""
_BROKER_REGISTRIES_SQL = "SELECT registry, count(*) AS n FROM data_brokers GROUP BY registry"
_BROKER_COUNTS_SQL = """
    SELECT k AS flag, count(*) FILTER (WHERE v = 'true') AS yes, count(*) AS answered
    FROM data_brokers, jsonb_each_text(flags) AS e(k, v) WHERE v IS NOT NULL GROUP BY k
"""


@router.get("/brokers")
async def brokers(flag: Optional[str] = Query(None, max_length=40), q: Optional[str] = Query(None, max_length=80),
                  registry: Optional[str] = Query(None, pattern="^(any:)?[A-Z]{2}(,[A-Z]{2})*$", max_length=40),
                  limit: int = Query(200, ge=1, le=1000), db=Depends(get_db_optional),
                  user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Registered data brokers (California and Oregon): what each declares it
    collects and who it sold data to, with its opt-out page. `registry`: an
    exact set ("OR" = Oregon only, "CA,OR" = both) or "any:OR" (on Oregon's,
    whatever else). Declarations come from California's registry only."""
    if flag is not None and flag not in sv.BROKER_FLAGS:
        raise HTTPException(status_code=422, detail="unknown flag")
    if not db:
        return {"brokers": [], "counts": [], "registries": []}
    rows = await db.query(_BROKERS_SQL, flag, q, limit, registry) or []
    counts = await db.query(_BROKER_COUNTS_SQL) or []
    regs = await db.query(_BROKER_REGISTRIES_SQL) or []
    return {"brokers": [{**_plain(r), "flags": _json_field(r["flags"])} for r in rows],
            "registries": sorted(({"registry": r["registry"], "brokers": r["n"]} for r in regs),
                                 key=lambda r: -r["brokers"]),
            "oregon_source": sv.OR_BROKER_PAGE,
            "counts": sorted(({"flag": c["flag"], "label": sv.BROKER_FLAG_TEXT.get(c["flag"], c["flag"]),
                               "yes": c["yes"], "answered": c["answered"]} for c in counts), key=lambda c: -c["yes"]),
            "source": sv.CA_BROKER_PAGE,
            "note": "Each broker's own answers to California's registry (Delete Act); Oregon's registry lists "
                    "who registered there, without declarations. A broker on one state's list but not the other's "
                    "may simply not operate there, or may not have registered. Californians can delete themselves "
                    "from all California brokers at once through the state's DROP portal."}


# ── Phase 3: a personal privacy plan ─────────────────────────────────────────
class PlanRequest(BaseModel):
    """Answers for a privacy plan. Sent in the body (not the URL, which access
    logs keep) and never stored."""
    state: Optional[str] = Field(None, min_length=2, max_length=2, pattern="^[A-Za-z]{2}$")
    has_car: bool = False
    phone: Optional[Literal["android", "iphone"]] = None
    has_doorbell_camera: bool = False
    concerns: List[Literal["immigration", "reproductive", "activism_press", "domestic_violence"]] = Field(
        default_factory=list, max_length=4)


_POLICE_BROKERS_SQL = """
    SELECT name, privacy_url FROM data_brokers
    WHERE (flags ->> 'sold_to_law_enforcement')::boolean IS TRUE AND privacy_url IS NOT NULL ORDER BY name
"""


@router.post("/plan")
async def plan(body: PlanRequest, db=Depends(get_db_optional),
               user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """A ranked, free privacy plan for these answers. Nothing is stored."""
    from shared.utils import civic_law as cl
    from shared.utils import privacy_plan as pp
    answers = pp.parse_answers(body.model_dump())
    brokers: List[Dict[str, Any]] = []
    if db:
        try:
            brokers = [dict(r) for r in await db.query(_POLICE_BROKERS_SQL) or []]
        except Exception as e:
            swallowed("api_gateway.civic.plan_brokers", e, logger)
    has_law = (answers.state in cl.STATE_LAWS) if answers.state else None
    return {"steps": pp.build_plan(answers, brokers, has_law),
            "note": "General guidance, not legal advice. Your answers were used for this response only and were not saved."}


# ── Phase 3: the surveillance watch feed (shared/utils/watch_feed.py) ────────
_FEED_NEWS_SQL = """
    SELECT source, headline, summary, url, occurred_at FROM events
    WHERE occurred_at > NOW() - make_interval(days => $1) AND headline IS NOT NULL
      AND (source = ANY($2::text[]) OR headline ~* $3)
    ORDER BY occurred_at DESC LIMIT 3000
"""
# A loose pre-filter for SQL; watch_feed.include makes the real decision.
_FEED_PREFILTER = ("flock|plate|alpr|facial|face recog|clearview|pimeyes|shotspotter|soundthinking|drone|skydio|fusus|"
                   "crime cent|ring|cell.site|imsi|geofence|location data|pegasus|nso group|paragon|stalkerware|"
                   "cellebrite|extraction|data broker|people.search|venntel|fog |babel street|gravy|kochava|lexisnexis|"
                   "palantir|ice|fisa|702|dhs|surveillance|privacy|delete act|ccops|rekor|vigilant")
# Civic changes after each table's first load (the first load is the baseline, not news).
_FEED_CIVIC_SQL = """
    WITH base AS (
        SELECT (SELECT min(first_seen) FROM civic_findings) + INTERVAL '1 day' AS findings,
               (SELECT min(first_seen) FROM flock_portals) + INTERVAL '1 day' AS portals
    )
    SELECT 'finding' AS kind, f.first_seen AS at, f.state, f.title || ' — ' || coalesce(f.agency, f.slug) AS title,
           f.detail, NULL AS url, f.slug AS ref
    FROM civic_findings f, base WHERE f.first_seen > base.findings AND f.first_seen > NOW() - make_interval(days => $1)
      AND ($2::text IS NULL OR f.state = $2)
    UNION ALL
    SELECT 'portal_dropped', p.removed_at, p.state,
           'Flock portal no longer published — ' || coalesce(p.city, p.county, p.slug) || ' ' || coalesce(p.agency_type, ''),
           'A likely contract cancellation (or a renamed portal); confirm with the council record.', p.portal_url, p.slug
    FROM flock_portals p WHERE p.removed_at > NOW() - make_interval(days => $1) AND ($2::text IS NULL OR p.state = $2)
    UNION ALL
    SELECT 'portal_new', p.first_seen, p.state,
           'New Flock transparency portal — ' || coalesce(p.city, p.county, p.slug) || ' ' || coalesce(p.agency_type, ''),
           coalesce(p.total_cameras::text, '?') || ' cameras; keeps data ' || coalesce(p.data_retention::text, '?') || ' days.',
           p.portal_url, p.slug
    FROM flock_portals p, base WHERE p.first_seen > base.portals AND p.first_seen > NOW() - make_interval(days => $1)
      AND ($2::text IS NULL OR p.state = $2)
    UNION ALL
    SELECT 'drone_waiver', d.received::timestamptz, d.state, 'Drone-as-first-responder waiver — ' || d.applicant,
           coalesce(d.status, '') || coalesce(' · ' || d.city, ''), NULL, NULL
    FROM drone_waivers d WHERE d.received > (NOW() - make_interval(days => $1))::date AND d.received <= CURRENT_DATE
      AND ($2::text IS NULL OR d.state = $2)
    UNION ALL
    SELECT 'federal_award', a.start_date::timestamptz, NULL,
           'Federal ' || a.kind || ' — ' || coalesce(a.recipient, '?') || ' (' || a.matched || ')',
           coalesce(a.agency, '') || coalesce(' · ' || a.sub_agency, '') || coalesce(' · $' || round(a.amount::numeric)::text, ''),
           NULL, a.award_id
    FROM federal_awards a WHERE a.start_date > (NOW() - make_interval(days => $1))::date AND a.start_date <= CURRENT_DATE
      AND a.amount >= 100000 AND $2::text IS NULL
    UNION ALL
    SELECT 'deployment', t.link_date::timestamptz, t.state, t.technology || ' — ' || t.agency, t.summary, t.link, t.aos_id
    FROM surveillance_agency_tech t WHERE t.link_date > (NOW() - make_interval(days => $1))::date
      AND t.link_date <= CURRENT_DATE   -- the Atlas has typo dates (3023, 2029)
      AND ($2::text IS NULL OR t.state = $2)
    UNION ALL
    SELECT * FROM (
        SELECT DISTINCT ON (lower(c.case_name), c.court_id, c.date_filed)  -- one row per multi-defendant case
               'court_case', c.date_filed::timestamptz, NULL::text, 'New federal case — ' || c.case_name,
               array_to_string(c.topics, ', ') || ' · ' || upper(coalesce(c.court_id, '?'))
                   || coalesce(' · Judge ' || c.judge, ''), c.url, c.docket_id::text
        FROM court_cases c WHERE c.date_filed > (NOW() - make_interval(days => $1))::date
          AND c.date_filed <= CURRENT_DATE AND $2::text IS NULL
        ORDER BY lower(c.case_name), c.court_id, c.date_filed, c.docket_id
    ) cc
    UNION ALL
    SELECT 'lobbying', l.posted::timestamptz, NULL,
           'Lobbying — ' || l.vendor || ' · ' || coalesce(l.registrant, '?'),
           coalesce(l.period || ' ' || l.year::text, '') || coalesce(' · $' || round(l.amount::numeric)::text, '')
               || coalesce(' · ' || nullif(array_to_string(l.issues, ', '), ''), ''),
           l.url, l.filing_uuid
    FROM lobbying_filings l WHERE l.posted > (NOW() - make_interval(days => $1))::date
      AND l.posted <= CURRENT_DATE AND $2::text IS NULL
    ORDER BY 2 DESC LIMIT 500
"""


@router.get("/feed")
async def feed(days: int = Query(30, ge=1, le=365), state: Optional[str] = Query(None, min_length=2, max_length=2),
               db=Depends(get_db_optional), redis=Depends(get_redis_optional),
               user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Cached five minutes per window and state: 2-3 s to compute (30 October)."""
    return await cached_json(getattr(redis, "raw", None), cache_key("civic_feed", days=days, state=state),
                             lambda: _feed(days, state, db), ttl=300, max_stale=86400)


async def _feed(days: int, state: Optional[str], db) -> Dict[str, Any]:
    """What is changing in surveillance: news about it (precise topics, curated
    civil-liberties sources), and changes in the civic data -- new findings,
    dropped or new Flock portals, drone waivers, federal awards, newly
    documented deployments, new federal cases, lobbying filings -- newest first."""
    from shared.utils import watch_feed as wf
    items: List[Dict[str, Any]] = []
    if not db:
        return {"items": items}
    try:
        for r in await db.query(_FEED_NEWS_SQL, days, list(wf.CIVIC_SOURCES), _FEED_PREFILTER) or []:
            ok, found = wf.include(r["source"], r["headline"], r["summary"])
            if ok:
                items.append({"kind": "news", "at": str(r["occurred_at"]), "state": None, "title": r["headline"],
                              "detail": None, "url": r["url"], "source": r["source"], "topics": found})
        for r in await db.query(_FEED_CIVIC_SQL, days, state.upper() if state else None) or []:
            items.append({**_plain(r), "topics": []})
    except Exception as e:
        swallowed("api_gateway.civic.feed", e, logger)
    items.sort(key=lambda i: str(i.get("at") or ""), reverse=True)
    return {"items": items[:400], "topics": list(wf.TOPICS), "sources": list(wf.CIVIC_SOURCES)}


# ── Federal courts (shared/utils/courts.py) ──────────────────────────────────
_CASES_SQL = """
    SELECT docket_id, case_name, court_id, date_filed, date_terminated, judge, judge_id, suit_nature, docket_number,
           url, topics
    FROM court_cases WHERE ($1::text IS NULL OR $1 = ANY(topics))
    ORDER BY date_filed DESC NULLS LAST LIMIT $2
"""
_JUDGE_HOLDINGS_SQL = """
    SELECT d.person_id, d.year, h.vendor, h.description, h.gross_value_code
    FROM vendor_holdings h JOIN judge_disclosures d ON d.disclosure_id = h.disclosure_id
    WHERE d.person_id = ANY($1::bigint[])
"""
_COURT_TOTALS_SQL = """
    SELECT (SELECT count(*) FROM court_cases) AS cases,
           (SELECT count(DISTINCT judge_id) FROM court_cases WHERE judge_id IS NOT NULL) AS judges,
           (SELECT count(*) FROM judge_checked) AS judges_checked,
           (SELECT count(*) FROM vendor_holdings) AS vendor_holdings
"""


@router.get("/courts")
async def courts(topic: Optional[str] = Query(None, max_length=40),
                 kind: Optional[str] = Query(None, pattern="^(civil|criminal)$"),
                 limit: int = Query(200, ge=1, le=1000),
                 db=Depends(get_db_optional),
                 user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Federal cases over surveillance (CourtListener) with each assigned judge's
    disclosed holdings in surveillance vendors. A possible §455(b)(4) conflict
    is flagged only when the vendor held is a party to the case."""
    from shared.utils import courts as ct
    if not db:
        return {"cases": [], "totals": {}}
    rows = ct.collapse([dict(r) for r in await db.query(_CASES_SQL, topic, limit * 3) or []])
    for r in rows:
        r["kind"] = ct.case_kind(r.get("case_name") or "", r.get("docket_number"), r.get("suit_nature"))
    counts = {"civil": sum(1 for r in rows if r["kind"] == "civil"), "criminal": sum(1 for r in rows if r["kind"] == "criminal")}
    rows = [r for r in rows if kind is None or r["kind"] == kind][:limit]
    judge_ids = sorted({int(r["judge_id"]) for r in rows if r.get("judge_id")})
    held: Dict[int, List[Dict[str, Any]]] = {}
    if judge_ids:
        for h in await db.query(_JUDGE_HOLDINGS_SQL, judge_ids) or []:
            held.setdefault(int(h["person_id"]), []).append(dict(h))
    out = []
    for r in rows:
        ties = ct.judge_ties(r, held.get(int(r["judge_id"]), [])) if r.get("judge_id") else None
        out.append({**_plain(r), "topics": list(r.get("topics") or []), "judge_ties": ties})
    totals = {**_plain((await db.query(_COURT_TOTALS_SQL) or [{}])[0]), **counts}
    totals["cases_with_tied_judge"] = sum(1 for c in out if c["judge_ties"] and c["judge_ties"]["ties"])
    totals["possible_455_conflicts"] = sum(1 for c in out if c["judge_ties"] and c["judge_ties"]["possible_455_conflict"])
    return {"cases": out, "totals": totals,
            "caveat": "Disclosures lag by a year or more, give value ranges, and do not itemise fund holdings. A tie "
                      "is context; only a holding in a party to the case is a possible disqualifying interest. "
                      "Source: CourtListener (Free Law Project)."}


# ── Lobbying (shared/utils/lobbying.py) ──────────────────────────────────────
_LOBBY_TOTALS_SQL = """
    SELECT vendor, year, sum(amount) AS total, count(*) AS filings
    FROM lobbying_filings WHERE amount IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2
"""
_LOBBY_RECENT_SQL = """
    SELECT vendor, registrant, year, period, amount, in_house, issues, entities, description, url
    FROM lobbying_filings WHERE ($1::text IS NULL OR vendor = $1)
    ORDER BY year DESC NULLS LAST, posted DESC NULLS LAST LIMIT $2
"""


@router.get("/lobbying")
async def lobbying(vendor: Optional[str] = Query(None, max_length=60), limit: int = Query(50, ge=1, le=500),
                   db=Depends(get_db_optional),
                   user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Federal lobbying by surveillance vendors: spend by year and the latest
    filings -- who lobbied, on what, whom. Source: Senate LDA filings."""
    if not db:
        return {"by_vendor": [], "filings": []}
    by: Dict[str, Dict[str, Any]] = {}
    for r in await db.query(_LOBBY_TOTALS_SQL) or []:
        e = by.setdefault(r["vendor"], {"vendor": r["vendor"], "total": 0.0, "years": {}})
        e["total"] += float(r["total"] or 0)
        e["years"][str(r["year"])] = float(r["total"] or 0)
    filings = [{**_plain(r), "issues": list(r["issues"] or []), "entities": list(r["entities"] or [])}
               for r in await db.query(_LOBBY_RECENT_SQL, vendor, limit) or []]
    return {"by_vendor": sorted(by.values(), key=lambda e: -e["total"]), "filings": filings,
            "caveat": "A lobbying firm reports its income from the client; in-house lobbying reports expenses. "
                      "Spending through trade associations is not included. Source: lda.gov."}


# ── Local oversight laws (shared/utils/oversight.py) ─────────────────────────
_OVERSIGHT_AGENCIES_SQL = """
    SELECT agency, state, count(DISTINCT technology) AS technologies,
           array_agg(DISTINCT technology ORDER BY technology) AS tech
    FROM surveillance_agency_tech WHERE state = ANY($1::text[]) GROUP BY 1, 2
"""
_OVERSIGHT_PORTALS_SQL = """
    SELECT slug, city, county, agency_type, state, total_cameras FROM flock_portals
    WHERE removed_at IS NULL AND state = ANY($1::text[])
"""
_AGENCY_COUNT_SQL = "SELECT count(DISTINCT (agency, state)) AS n FROM surveillance_agency_tech"


@router.get("/oversight")
async def oversight(db=Depends(get_db_optional),
                    user: Dict[str, Any] = Depends(require_role(Role.VIEWER))) -> Dict[str, Any]:
    """Where a council must approve surveillance before police use it (CCOPS
    laws), each with its source, the documented technology at the agencies it
    covers, and any Flock portal there."""
    from shared.utils import oversight as ov
    laws = {x["place"]: {**x, "agencies": [], "portals": []} for x in ov.laws()}
    total = 0
    if db:
        states = sorted({x.state for x in ov.OVERSIGHT_LAWS})
        for r in await db.query(_OVERSIGHT_AGENCIES_SQL, states) or []:
            law = ov.law_for_agency(r["agency"], r["state"])
            if law:
                laws[law.place]["agencies"].append({"agency": r["agency"], "state": r["state"],
                                                    "tech": list(r["tech"] or [])})
        for r in await db.query(_OVERSIGHT_PORTALS_SQL, states) or []:
            law = ov.law_for_portal(r["city"], r["county"], r["agency_type"], r["state"])
            if law:
                laws[law.place]["portals"].append({"slug": r["slug"], "cameras": r["total_cameras"]})
        total = int(((await db.query(_AGENCY_COUNT_SQL) or [{}])[0]).get("n") or 0)
    rows = list(laws.values())
    covered = sum(len(x["agencies"]) for x in rows)
    return {"laws": rows, "covered_agencies": covered, "documented_agencies": total, "campaign": ov.CAMPAIGN_URL,
            "caveat": "Verified laws only: EFF's 2021 list (each linked to its ordinance) plus later laws with "
                      "their own source. The ACLU counts 26 jurisdictions. A law covers that government's own "
                      "agencies, not universities, school districts, state or federal offices in the same city."}
