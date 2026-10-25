"""
services/collector-civic/main.py

Watching the watchers. Collects the public record of who surveils the public
(shared/utils/surveillance.py):

  - OpenStreetMap's licence-plate readers and gunshot detectors across the US,
    tile by tile through Overpass, weekly. A tile the server will not answer
    whole is split into quarters once. Devices not seen in a full pass keep
    their last_seen date, so removals show rather than vanish.
  - EFF's Atlas of Surveillance, weekly: which agency runs which technology,
    placed at its city's (or county's) Census centroid.

Agencies and vendors are the subject. Nothing here tracks a person.
"""
from __future__ import annotations

import asyncio
import ctypes
import gc
import io
import json
import re
import logging
import os
import sys
import tempfile
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.db import get_redis, get_timescale                # noqa: E402
from shared.utils import civic_law as cl                      # noqa: E402
from shared.utils import courts as ct                         # noqa: E402
from shared.utils import lobbying as lb                       # noqa: E402
from shared.utils import surveillance as sv                   # noqa: E402
from shared.utils.xlsx_lite import read_rows                  # noqa: E402
from shared.utils.heartbeat import start_heartbeat_task       # noqa: E402
from shared.utils.logging import setup_sentinel_logging       # noqa: E402
from shared.utils.quiet_failures import swallowed             # noqa: E402
from shared.utils.tasks import safe_create_task               # noqa: E402

logger = setup_sentinel_logging("collector.civic", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

USER_AGENT = "Sentinel-Intelligence-Platform/1.0 (civic transparency research)"
DEVICE_PASS_SEC = 7 * 24 * 3600
ATLAS_PASS_SEC = 7 * 24 * 3600
# Overpass is a shared community resource: one tile at a time, with a pause.
TILE_PAUSE_SEC = 10.0
BACKOFF_START_SEC = 60.0
BACKOFF_MAX_SEC = 15 * 60.0
OVERPASS_TIMEOUT_SEC = 150
STATE_KEY = "sentinel:civic:passes"

_DEVICE_UPSERT_SQL = """
    INSERT INTO surveillance_devices (osm_id, lat, lon, kind, manufacturer, operator, direction, tags, last_seen)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, NOW())
    ON CONFLICT (osm_id) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon, kind = EXCLUDED.kind,
        manufacturer = EXCLUDED.manufacturer, operator = EXCLUDED.operator, direction = EXCLUDED.direction,
        tags = EXCLUDED.tags, last_seen = NOW()
"""
_AGENCY_UPSERT_SQL = """
    INSERT INTO surveillance_agency_tech (aos_id, agency, city, county, state, lea_type, juris_type, technology,
        vendor, summary, link, link_date, lat, lon, geo_level, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW())
    ON CONFLICT (aos_id) DO UPDATE SET agency = EXCLUDED.agency, city = EXCLUDED.city, county = EXCLUDED.county,
        state = EXCLUDED.state, lea_type = EXCLUDED.lea_type, juris_type = EXCLUDED.juris_type,
        technology = EXCLUDED.technology, vendor = EXCLUDED.vendor, summary = EXCLUDED.summary, link = EXCLUDED.link,
        link_date = EXCLUDED.link_date, lat = EXCLUDED.lat, lon = EXCLUDED.lon, geo_level = EXCLUDED.geo_level,
        updated_at = NOW()
"""


class Refused(Exception):
    """Every mirror refused (429/5xx at once): the servers are busy, not the tile too big."""


async def _overpass(session: aiohttp.ClientSession, box: sv.Box) -> Optional[List[sv.Device]]:
    """One tile's devices, trying each mirror. None when a mirror ran the query
    and timed out (the tile is too dense: split it); Refused when every mirror
    turned the request away (back off and retry the same tile)."""
    q = sv.overpass_query(box, sv.OVERPASS_KINDS, timeout=OVERPASS_TIMEOUT_SEC - 20)
    timed_out = False
    for url in sv.OVERPASS_URLS:
        started = time.monotonic()
        status: Optional[int] = None
        body = b""
        try:
            async with session.post(url, data={"data": q}) as resp:
                status, body = resp.status, await resp.read()
        except asyncio.TimeoutError:
            pass
        except aiohttp.ClientError as e:
            swallowed("collector.civic.overpass", e, logger)
        outcome = sv.overpass_outcome(status, body, time.monotonic() - started)
        if outcome == "ok":
            try:
                return sv.parse_overpass(json.loads(body))
            except ValueError as e:
                swallowed("collector.civic.overpass_json", e, logger)
        elif outcome == "dense":
            timed_out = True
    if timed_out:
        return None
    raise Refused()


async def device_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    stored, failed, done = 0, 0, 0
    queue: List[Tuple[sv.Box, bool]] = [(b, True) for b in sv.tiles()]
    total = len(queue)
    backoff = BACKOFF_START_SEC
    while queue:
        box, may_split = queue[0]
        try:
            devices = await _overpass(session, box)
        except Refused:
            logger.info("Overpass busy at tile %s of %s; waiting %.0f s", done + 1, total, backoff)
            await asyncio.sleep(backoff)
            backoff = min(BACKOFF_MAX_SEC, backoff * 2)
            continue
        backoff = BACKOFF_START_SEC
        queue.pop(0)
        done += 1
        if devices is None:
            if may_split:
                queue[0:0] = [(b, False) for b in sv.split(box)]
                total += 4
            else:
                failed += 1
                logger.warning("Overpass: tile %s timed out even quartered", box)
        elif devices:
            await db.execute_many(_DEVICE_UPSERT_SQL, [
                (d.osm_id, d.lat, d.lon, d.kind, d.manufacturer, d.operator, d.direction, d.tags)
                for d in devices])
            stored += len(devices)
        if done % 25 == 0:
            logger.info("Overpass: %s of %s tiles, %s devices so far", done, total, stored)
        await asyncio.sleep(TILE_PAUSE_SEC)
    return stored, failed


async def _zip_text(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as resp:
        resp.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(await resp.read()))
        return z.read(z.namelist()[0]).decode("latin-1")


ATLAS_BATCH = 1000
DEFLOCK_BATCH = 2000
DEFLOCK_TIMEOUT_SEC = 600


async def deflock_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """DeFlock's national licence-plate-reader file, streamed to disk and read
    back feature by feature (38 MB whole would not fit), upserted in batches."""
    stored = skipped = 0
    with tempfile.TemporaryFile(mode="w+b") as tmp:
        async with session.get(sv.DEFLOCK_URL, timeout=aiohttp.ClientTimeout(total=DEFLOCK_TIMEOUT_SEC)) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(1 << 16):
                tmp.write(chunk)
        tmp.seek(0)
        batch: List[Tuple] = []
        for feat in sv.iter_geojson_features(tmp):
            d = sv.parse_deflock(feat)
            if d is None:
                skipped += 1
                continue
            batch.append((d.osm_id, d.lat, d.lon, d.kind, d.manufacturer, d.operator, d.direction, d.tags))
            if len(batch) >= DEFLOCK_BATCH:
                await db.execute_many(_DEVICE_UPSERT_SQL, batch)
                stored += len(batch)
                batch = []
        if batch:
            await db.execute_many(_DEVICE_UPSERT_SQL, batch)
            stored += len(batch)
    return stored, skipped


async def atlas_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """The Atlas streamed to a temporary file and read back row by row, written
    in batches: held whole it pushed the 160M service over its limit."""
    places = sv.parse_gazetteer(await _zip_text(session, sv.GAZ_PLACES_URL))
    counties = sv.parse_gazetteer(await _zip_text(session, sv.GAZ_COUNTIES_URL), county=True)
    geo = [(st, n, "place", la, lo) for (st, n), (la, lo) in places.items()]
    geo += [(st, n, "county", la, lo) for (st, n), (la, lo) in counties.items()]
    for i in range(0, len(geo), 2000):
        await db.execute_many(_GEO_UPSERT_SQL, geo[i:i + 2000])
    del geo
    stored = placed = 0
    with tempfile.TemporaryFile(mode="w+b") as tmp:
        async with session.get(sv.ATLAS_URL) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(1 << 16):
                tmp.write(chunk)
        tmp.seek(0)
        text = io.TextIOWrapper(tmp, encoding="utf-8", newline="")
        batch: List[Tuple] = []
        for r in sv.iter_atlas(text, places, counties):
            batch.append((r.aos_id, r.agency, r.city, r.county, r.state, r.lea_type, r.juris_type, r.technology,
                          r.vendor, r.summary, r.link, r.link_date, r.lat, r.lon, r.geo_level))
            placed += r.lat is not None
            if len(batch) >= ATLAS_BATCH:
                await db.execute_many(_AGENCY_UPSERT_SQL, batch)
                stored += len(batch)
                batch = []
        if batch:
            await db.execute_many(_AGENCY_UPSERT_SQL, batch)
            stored += len(batch)
        text.detach()
    return stored, placed


# ── "Is it legal here?" (Phase 2) ─────────────────────────────────────────────
EYESONFLOCK_URL = "https://eyesonflock.com/api/v1/data"
ICE_287G_PAGE = "https://www.ice.gov/identify-and-arrest/287g"
LEGAL_PASS_SEC = 7 * 24 * 3600
PORTAL_BATCH = 200
# A pass that stored fewer portals than this is treated as incomplete: no
# portal is marked departed from it (EyesOnFlock listed 1,528 on 10 Oct 2026).
MIN_PORTALS_FOR_DEPARTURES = 1200
_XLSX_LINK = re.compile(r'href="([^"]+participatingAgencies[^"]*\.xlsx)"', re.I)

_287G_UPSERT_SQL = """
    INSERT INTO ice_287g_agreements (state, agency, agency_type, county, support_type, signed, moa, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    ON CONFLICT (state, agency, support_type) DO UPDATE SET agency_type = EXCLUDED.agency_type,
        county = EXCLUDED.county, signed = EXCLUDED.signed, moa = EXCLUDED.moa, updated_at = NOW()
"""
_GEO_UPSERT_SQL = """
    INSERT INTO geo_places (state, norm, kind, lat, lon) VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (state, norm, kind) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon
"""
_GEO_LOOKUP_SQL = """
    SELECT g.state, g.norm, g.kind, g.lat, g.lon
    FROM unnest($1::text[], $2::text[], $3::text[]) AS k(state, norm, kind)
    JOIN geo_places g ON g.state = k.state AND g.norm = k.norm AND g.kind = k.kind
"""
_287G_ALL_SQL = "SELECT state, agency, agency_type, county, support_type, signed, moa FROM ice_287g_agreements"
_PORTAL_UPSERT_SQL = """
    INSERT INTO flock_portals (slug, portal_url, city, county, state, agency_type, population, total_cameras,
        total_searches, data_retention, vehicles_captured, hotlist_hits, hotlist_hit_rate, shared_with_count,
        received_from_count, shared_with, prohibited_uses, public_search_audit, data_last_updated, lat, lon, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb, $17, $18, $19, $20, $21, NOW())
    ON CONFLICT (slug) DO UPDATE SET portal_url = EXCLUDED.portal_url, city = EXCLUDED.city, county = EXCLUDED.county,
        state = EXCLUDED.state, agency_type = EXCLUDED.agency_type, population = EXCLUDED.population,
        total_cameras = EXCLUDED.total_cameras, total_searches = EXCLUDED.total_searches,
        data_retention = EXCLUDED.data_retention, vehicles_captured = EXCLUDED.vehicles_captured,
        hotlist_hits = EXCLUDED.hotlist_hits, hotlist_hit_rate = EXCLUDED.hotlist_hit_rate,
        shared_with_count = EXCLUDED.shared_with_count, received_from_count = EXCLUDED.received_from_count,
        shared_with = EXCLUDED.shared_with, prohibited_uses = EXCLUDED.prohibited_uses,
        public_search_audit = EXCLUDED.public_search_audit, data_last_updated = EXCLUDED.data_last_updated,
        lat = EXCLUDED.lat, lon = EXCLUDED.lon, updated_at = NOW()
"""
_FINDING_UPSERT_SQL = """
    INSERT INTO civic_findings (slug, code, severity, state, agency, title, detail, citation, source, evidence, last_seen)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, NOW())
    ON CONFLICT (slug, code) DO UPDATE SET severity = EXCLUDED.severity, state = EXCLUDED.state,
        agency = EXCLUDED.agency, title = EXCLUDED.title, detail = EXCLUDED.detail, citation = EXCLUDED.citation,
        source = EXCLUDED.source, evidence = EXCLUDED.evidence, last_seen = NOW()
"""
# A finding the latest pass no longer produces (the agency changed its
# practice, or the law did) is resolved, not kept as if it still stood.
_FINDINGS_STALE_SQL = "DELETE FROM civic_findings WHERE last_seen < $1 AND code <> 'immigration_searches'"
_AUDIT_FINDINGS_STALE_SQL = "DELETE FROM civic_findings WHERE last_seen < $1 AND code = 'immigration_searches'"


def _excel_date(x: Optional[str]) -> Optional[date]:
    try:
        return date(1899, 12, 30) + timedelta(days=int(float(x)))
    except (TypeError, ValueError):
        return None


def parse_287g(rows: List[List[Optional[str]]]) -> List[Tuple]:
    """ICE's spreadsheet rows -> (state, agency, type, county, support type, signed, MOA link)."""
    head = [str(c or "").strip().upper() for c in rows[0]] if rows else []
    col = {name: head.index(name) for name in ("STATE", "LAW ENFORCEMENT AGENCY", "TYPE", "COUNTY",
                                                "SUPPORT TYPE", "SIGNED", "MOA") if name in head}
    out = []
    for r in rows[1:]:
        def g(name: str) -> Optional[str]:
            i = col.get(name)
            return str(r[i]).strip() if i is not None and i < len(r) and r[i] is not None else None
        state = cl.STATE_NAMES.get((g("STATE") or "").lower())
        agency, support = g("LAW ENFORCEMENT AGENCY"), g("SUPPORT TYPE")
        if not state or not agency or not support:
            continue
        moa = g("MOA")
        out.append((state, agency, g("TYPE"), g("COUNTY"), support, _excel_date(g("SIGNED")),
                    moa if moa and moa.startswith("http") else None))
    return out


async def ice_287g_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """ICE's 287(g) participating-agency spreadsheet (its file name carries
    the update date, so the link is read from the page each pass)."""
    async with session.get(ICE_287G_PAGE) as resp:
        resp.raise_for_status()
        page = await resp.text()
    m = _XLSX_LINK.search(page)
    if not m:
        raise RuntimeError("287(g) spreadsheet link not found on ICE's page")
    url = m.group(1) if m.group(1).startswith("http") else "https://www.ice.gov" + m.group(1)
    async with session.get(url) as resp:
        resp.raise_for_status()
        out = parse_287g(read_rows(await resp.read()))
    await db.execute_many(_287G_UPSERT_SQL, out)
    return len(out), len({(r[0], r[1]) for r in out})


def portal_agreements(p: Dict[str, Any], st: str,
                      agreements: Dict[Tuple[str, str], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """The 287(g) rows for a portal's agency, matched on state and a loose name key."""
    kind = p.get("type") or ""
    for place in (p.get("city"), p.get("county")):
        if place:
            hit = agreements.get((st, cl.norm_agency(f"{place} {kind}")))
            if hit:
                return hit
    return []


async def legal_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Flock transparency-portal data (EyesOnFlock, CC BY-SA 4.0), streamed:
    each agency stored, placed, and checked against its state's law."""
    started = datetime.now(timezone.utc)
    agreements: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for r in await db.query(_287G_ALL_SQL) or []:
        agreements.setdefault((r["state"], cl.norm_agency(r["agency"])), []).append(dict(r))
    stored = 0
    frow: List[Tuple] = []
    with tempfile.TemporaryFile(mode="w+b") as tmp:
        async with session.get(EYESONFLOCK_URL, timeout=aiohttp.ClientTimeout(total=600)) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(1 << 16):
                tmp.write(chunk)
        tmp.seek(0)
        prow: List[Tuple] = []
        for p in sv.iter_json_array(tmp, "portals"):
            slug, st = p.get("slug"), (p.get("state") or "").upper()
            if not slug or not st:
                continue
            city, county, kind = p.get("city") or "", p.get("county") or "", p.get("type") or ""
            lat = lon = None                      # placed after the stream (place_portals)
            shared = list(p.get("organizations_shared_with") or [])
            prow.append((slug, p.get("portal_url"), city or None, county or None, st, kind or None,
                         p.get("population"), p.get("total_cameras"), p.get("total_searches"),
                         p.get("data_retention"), p.get("vehicles_captured"), p.get("hotlist_hits"),
                         p.get("hotlist_hit_rate"), p.get("organization_count"),
                         p.get("receiving_organization_count"), shared, p.get("prohibited_uses"),
                         bool(p.get("public_search_audit")), p.get("data_last_updated"), lat, lon))
            agency = f"{city or county} {kind}".strip()
            for f in cl.check_portal({**p, "state": st}, cl.STATE_LAWS.get(st), portal_agreements(p, st, agreements)):
                frow.append((slug, f.code, f.severity, st, agency, f.title, f.detail, f.citation, f.source,
                             f.evidence))
            if len(prow) >= PORTAL_BATCH:
                await db.execute_many(_PORTAL_UPSERT_SQL, prow)
                stored += len(prow)
                prow = []
        if prow:
            await db.execute_many(_PORTAL_UPSERT_SQL, prow)
            stored += len(prow)
    if frow:
        await db.execute_many(_FINDING_UPSERT_SQL, frow)
    # Portals the aggregator no longer lists: marked, kept, and back to live if
    # they reappear. Only after a pass that returned a full set (a truncated
    # download must not mark the country as departed).
    if stored >= MIN_PORTALS_FOR_DEPARTURES:
        await db.execute("UPDATE flock_portals SET removed_at = NOW() WHERE updated_at < $1 AND removed_at IS NULL",
                         started)
        await db.execute("UPDATE flock_portals SET removed_at = NULL WHERE updated_at >= $1", started)
    await db.execute(_FINDINGS_STALE_SQL, started)
    await place_portals(db)
    return stored, len(frow)


# ── Released search logs (EyesOnFlock /api/audit/{state}/{slug}) ─────────────
AUDIT_URL = "https://eyesonflock.com/api/audit/{state}/{slug}"
AUDIT_DAYS = 90
AUDIT_PAGE = 500                      # the server's maximum
AUDIT_PAUSE_SEC = 1.5
AUDIT_MAX_PAGES = 40                  # 20,000 searches per agency per pass
_AUDIT_PORTALS_SQL = "SELECT slug, state FROM flock_portals WHERE public_search_audit AND removed_at IS NULL ORDER BY slug"
_AUDIT_UPSERT_SQL = """
    INSERT INTO flock_audit_stats (slug, days, searches, first_search, last_search, immigration_hits, immigration_terms,
        first_amendment_hits, first_amendment_terms, wide_searches, no_reason, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10, $11, NOW())
    ON CONFLICT (slug) DO UPDATE SET days = EXCLUDED.days, searches = EXCLUDED.searches,
        first_search = EXCLUDED.first_search, last_search = EXCLUDED.last_search,
        immigration_hits = EXCLUDED.immigration_hits, immigration_terms = EXCLUDED.immigration_terms,
        first_amendment_hits = EXCLUDED.first_amendment_hits, first_amendment_terms = EXCLUDED.first_amendment_terms,
        wide_searches = EXCLUDED.wide_searches, no_reason = EXCLUDED.no_reason, updated_at = NOW()
"""


def _parse_ts(x: Any) -> Optional[datetime]:
    """The log mixes '2026-09-21T19:03:48.787Z' and '2025-11-19 01:51:46.548'."""
    if not x:
        return None
    try:
        t = datetime.fromisoformat(str(x).replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def summarize_audit(rows: List[List[Any]], headers: List[str], since: datetime) -> Optional[Dict[str, Any]]:
    """Counts from released search rows newer than `since`. No reason text kept."""
    idx = {h: i for i, h in enumerate(headers)}
    if "reason" not in idx or "searchDate" not in idx:
        return None
    acc: Dict[str, Any] = {"searches": 0, "first": None, "last": None, "immigration": {}, "first_amendment": {},
                           "wide": 0, "no_reason": 0}
    for r in rows:
        t = _parse_ts(r[idx["searchDate"]])
        if t is None or t < since:
            continue
        acc["searches"] += 1
        acc["first"] = t if acc["first"] is None or t < acc["first"] else acc["first"]
        acc["last"] = t if acc["last"] is None or t > acc["last"] else acc["last"]
        reason = r[idx["reason"]]
        if not (reason or "").strip():
            acc["no_reason"] += 1
        for cat, terms in cl.classify_reason(reason).items():
            for term in terms:
                acc[cat][term] = acc[cat].get(term, 0) + 1
            acc[cat + "_n"] = acc.get(cat + "_n", 0) + 1
        nets = r[idx["networkCount"]] if "networkCount" in idx else None
        if isinstance(nets, (int, float)) and nets >= cl.WIDE_SEARCH_NETWORKS:
            acc["wide"] += 1
    return acc


async def audit_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Each agency's released search log for the last AUDIT_DAYS, summarised
    into counts, and checked against its state's immigration rules."""
    started = datetime.now(timezone.utc)
    since = started - timedelta(days=AUDIT_DAYS)
    portals = await db.query(_AUDIT_PORTALS_SQL) or []
    done = flagged = 0
    for p in portals:
        rows: List[List[Any]] = []
        headers: List[str] = []
        for page in range(1, AUDIT_MAX_PAGES + 1):
            url = AUDIT_URL.format(state=p["state"].lower(), slug=p["slug"])
            try:
                async with session.get(url, params={"page": page, "limit": AUDIT_PAGE}) as resp:
                    if resp.status != 200:
                        break
                    body = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                swallowed("collector.civic.audit", e, logger)
                break
            headers = body.get("headers") or headers
            page_rows = body.get("rows") or []
            rows.extend(page_rows)
            await asyncio.sleep(AUDIT_PAUSE_SEC)
            oldest = _parse_ts(page_rows[-1][headers.index("searchDate")]) if page_rows and "searchDate" in headers else None
            if not (body.get("pagination") or {}).get("has_more") or (oldest is not None and oldest < since):
                break
        acc = summarize_audit(rows, headers, since) if rows else None
        del rows
        if acc is None:
            continue
        await db.execute(_AUDIT_UPSERT_SQL, p["slug"], AUDIT_DAYS, acc["searches"], acc["first"], acc["last"],
                         acc.get("immigration_n", 0), acc["immigration"], acc.get("first_amendment_n", 0),
                         acc["first_amendment"], acc["wide"], acc["no_reason"])
        stats = {"immigration_hits": acc.get("immigration_n", 0), "immigration_terms": acc["immigration"],
                 "days": AUDIT_DAYS}
        for f in cl.check_audit(stats, cl.STATE_LAWS.get(p["state"])):
            await db.execute(_FINDING_UPSERT_SQL, p["slug"], f.code, f.severity, p["state"], None, f.title, f.detail,
                             f.citation, f.source, f.evidence)
            flagged += 1
        done += 1
    await db.execute(_AUDIT_FINDINGS_STALE_SQL, started)
    return done, flagged


async def place_portals(db) -> int:
    """Portal agencies at their city's (else county's) Census centroid, looked up
    in geo_places (written by the Atlas pass)."""
    rows = await db.query("SELECT slug, state, city, county FROM flock_portals") or []
    keys = {(r["state"], sv.norm_place(r["city"] or ""), "place") for r in rows if r["city"]}
    keys |= {(r["state"], sv.norm_county(r["county"] or ""), "county") for r in rows if r["county"]}
    keys |= {(r["state"], sv.norm_county(r["city"] or ""), "county") for r in rows if r["city"]}
    if not keys:
        return 0
    st, nm, kd = zip(*keys)
    geo = {(g["state"], g["norm"], g["kind"]): (g["lat"], g["lon"])
           for g in await db.query(_GEO_LOOKUP_SQL, list(st), list(nm), list(kd)) or []}
    upd = []
    for r in rows:
        hit = None
        if r["city"]:
            hit = geo.get((r["state"], sv.norm_place(r["city"]), "place"))
        if hit is None and r["county"]:
            hit = geo.get((r["state"], sv.norm_county(r["county"]), "county"))
        if hit is None and r["city"]:          # sheriffs list their county as the city
            hit = geo.get((r["state"], sv.norm_county(r["city"]), "county"))
        if hit:
            upd.append((r["slug"], hit[0], hit[1]))
    if upd:
        await db.execute_many("UPDATE flock_portals SET lat = $2, lon = $3 WHERE slug = $1", upd)
    return len(upd)


# ── Drone waivers and federal money (Phase 2c) ───────────────────────────────
MONEY_PASS_SEC = 7 * 24 * 3600
MONEY_PAUSE_SEC = 1.0
MONEY_MAX_PAGES = 20
_DFR_UPSERT_SQL = """
    INSERT INTO drone_waivers (applicant, city, state, category, status, received, completed, start_date, altitude,
        lat, lon, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
    ON CONFLICT (applicant, state) DO UPDATE SET city = EXCLUDED.city, category = EXCLUDED.category,
        status = EXCLUDED.status, received = EXCLUDED.received, completed = EXCLUDED.completed,
        start_date = EXCLUDED.start_date, altitude = EXCLUDED.altitude, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
        updated_at = NOW()
"""
_AWARD_UPSERT_SQL = """
    INSERT INTO federal_awards (award_id, kind, matched, recipient, amount, agency, sub_agency, start_date, description,
        updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
    ON CONFLICT (award_id) DO UPDATE SET amount = EXCLUDED.amount, recipient = EXCLUDED.recipient,
        description = EXCLUDED.description, updated_at = NOW()
"""


async def dfr_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """EFF's sheet of FAA drone-as-first-responder waivers, placed by city."""
    async with session.get(sv.DFR_SHEET_CSV) as resp:
        resp.raise_for_status()
        rows = sv.parse_dfr(await resp.text())
    keys = {(w.state, sv.norm_place(w.city), "place") for w in rows if w.city}
    geo: Dict[Tuple[str, str, str], Tuple[float, float]] = {}
    if keys:
        st, nm, kd = zip(*keys)
        geo = {(g["state"], g["norm"], g["kind"]): (g["lat"], g["lon"])
               for g in await db.query(_GEO_LOOKUP_SQL, list(st), list(nm), list(kd)) or []}
    out = []
    for w in rows:
        hit = geo.get((w.state, sv.norm_place(w.city), "place")) if w.city else None
        out.append((w.applicant, w.city, w.state, w.category, w.status, w.received, w.completed, w.start_date,
                    w.altitude, hit[0] if hit else None, hit[1] if hit else None))
    if out:
        await db.execute_many(_DFR_UPSERT_SQL, out)
    return len(out), sum(1 for o in out if o[9] is not None)


async def money_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Federal contracts to surveillance vendors and grants naming a surveillance
    technology, from USAspending (no key), paged politely."""
    stored = calls = 0
    jobs = [("contract", v) for v in sv.SURVEILLANCE_VENDORS] + [("grant", k) for k in sv.GRANT_KEYWORDS]
    for kind, term in jobs:
        for page in range(1, MONEY_MAX_PAGES + 1):
            try:
                async with session.post(sv.USASPENDING_URL, json=sv.usaspending_query(kind, term, page)) as resp:
                    calls += 1
                    if resp.status != 200:
                        logger.warning("USAspending %s %s page %s: HTTP %s", kind, term, page, resp.status)
                        break
                    body = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                swallowed("collector.civic.usaspending", e, logger)
                break
            rows = sv.parse_awards(body, kind, term)
            if rows:
                await db.execute_many(_AWARD_UPSERT_SQL, rows)
                stored += len(rows)
            await asyncio.sleep(MONEY_PAUSE_SEC)
            if not (body.get("page_metadata") or {}).get("hasNext"):
                break
    return stored, calls


_BROKER_UPSERT_SQL = """
    INSERT INTO data_brokers (name, dba, website, privacy_url, email, city, state, flags, registry, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, 'CA', NOW())
    ON CONFLICT (name) DO UPDATE SET dba = EXCLUDED.dba, website = EXCLUDED.website,
        privacy_url = EXCLUDED.privacy_url, email = EXCLUDED.email, city = EXCLUDED.city, state = EXCLUDED.state,
        flags = EXCLUDED.flags, updated_at = NOW(),
        registry = CASE WHEN ',' || data_brokers.registry || ',' LIKE '%,CA,%' THEN data_brokers.registry
                        ELSE data_brokers.registry || ',CA' END
"""
_BROKER_OR_SQL = """
    INSERT INTO data_brokers (name, dba, city, state, registry, updated_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (name) DO UPDATE SET registry = EXCLUDED.registry, dba = COALESCE(data_brokers.dba, EXCLUDED.dba),
        city = COALESCE(data_brokers.city, EXCLUDED.city), state = COALESCE(data_brokers.state, EXCLUDED.state),
        updated_at = NOW()
"""


async def brokers_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """California's data broker registry: declared collection and sales."""
    async with session.get(sv.CA_BROKER_CSV) as resp:
        resp.raise_for_status()
        rows = sv.parse_broker_registry(await resp.text(encoding="utf-8", errors="replace"))
    if rows:
        await db.execute_many(_BROKER_UPSERT_SQL, [(r["name"], r["dba"], r["website"], r["privacy_url"], r["email"],
                                                    r["city"], r["state"], r["flags"]) for r in rows])
    stored = len(rows)
    try:
        stored += await _oregon_brokers(db, session)
    except Exception as e:
        swallowed("collector.civic.brokers.oregon", e, logger)
    return stored, sum(1 for r in rows if r["flags"].get("sold_to_law_enforcement"))


async def _oregon_brokers(db, session: aiohttp.ClientSession) -> int:
    """Oregon's registry, joined to the stored brokers by normalised legal name:
    a broker on both lists gains "OR"; one only Oregon lists is added."""
    async with session.post(sv.OR_BROKER_FORM, data=sv.OR_BROKER_FORM_DATA) as resp:
        resp.raise_for_status()
        text = await resp.text(encoding="latin-1", errors="replace")
    rows = sv.parse_or_broker_registry(text)
    if not rows:
        return 0
    known = {sv.broker_key(r["name"]): (r["name"], r["registry"])
             for r in await db.query("SELECT name, registry FROM data_brokers") or []}
    params = []
    for r in rows:
        name, registry = known.get(sv.broker_key(r["name"]), (r["name"], None))
        params.append((name, r["dba"], r["city"], r["state"], sv.add_registry(registry, "OR")))
    await db.execute_many(_BROKER_OR_SQL, params)
    return len(rows)


# ── Federal courts (CourtListener; COURTLISTENER_API_TOKEN in .env) ──────────
COURTS_PASS_SEC = 24 * 3600
HOLDINGS_PASS_SEC = 7 * 24 * 3600
# Free accounts are rate limited (about 5 a minute, 125 a day): one call every
# 13 s, at most CL_DAILY_BUDGET a day across passes, resuming the next day.
CL_PAUSE_SEC = 13.0
CL_DAILY_BUDGET = 115              # under the reported 125/day free limit
# Split so the holdings scan cannot starve the judges' disclosures (on 15 Oct
# 2026 it spent the whole day paging Motorola, held by half the bench).
CL_BUDGETS = {"courts": 70, "holdings": CL_DAILY_BUDGET - 70}
CL_BUDGET_KEY = "sentinel:civic:courtlistener:{bucket}:{day}"
HOLDINGS_MAX_PAGES = 200            # per prefix, across days (the scan resumes)
HOLDINGS_CURSOR_KEY = "sentinel:civic:holdings:cursor"
# Rarest first: Motorola, the most widely held, last.
HOLDINGS_ORDER = ("Palantir", "Rekor", "Cellebrite", "SoundThinking", "Axon", "RELX (LexisNexis)",
                  "Thomson Reuters (CLEAR)", "Motorola Solutions")
JUDGE_RECHECK_DAYS = 30
_CASE_UPSERT_SQL = """
    INSERT INTO court_cases (docket_id, case_name, court_id, date_filed, date_terminated, judge, judge_id, suit_nature,
        docket_number, url, topics, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, ARRAY[$11]::text[], NOW())
    ON CONFLICT (docket_id) DO UPDATE SET case_name = EXCLUDED.case_name, date_terminated = EXCLUDED.date_terminated,
        judge = EXCLUDED.judge, judge_id = EXCLUDED.judge_id, suit_nature = EXCLUDED.suit_nature,
        topics = (SELECT array_agg(DISTINCT t) FROM unnest(court_cases.topics || EXCLUDED.topics) t), updated_at = NOW()
"""
_HOLDING_UPSERT_SQL = """
    INSERT INTO vendor_holdings (investment_id, disclosure_id, vendor, description, gross_value_code, updated_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (investment_id) DO UPDATE SET vendor = EXCLUDED.vendor, description = EXCLUDED.description,
        gross_value_code = EXCLUDED.gross_value_code, updated_at = NOW()
"""
_JUDGES_DUE_SQL = """
    SELECT DISTINCT c.judge_id FROM court_cases c LEFT JOIN judge_checked j ON j.person_id = c.judge_id
    WHERE c.judge_id IS NOT NULL AND (j.checked_at IS NULL OR j.checked_at < NOW() - make_interval(days => $1))
"""
_DISCLOSURE_UPSERT_SQL = """
    INSERT INTO judge_disclosures (disclosure_id, person_id, year) VALUES ($1, $2, $3)
    ON CONFLICT (disclosure_id) DO UPDATE SET person_id = EXCLUDED.person_id, year = EXCLUDED.year
"""
_JUDGE_CHECKED_SQL = """
    INSERT INTO judge_checked (person_id, checked_at) VALUES ($1, NOW())
    ON CONFLICT (person_id) DO UPDATE SET checked_at = NOW()
"""


class CourtListener:
    """A rate-limited client with a daily budget kept in Redis, so a pass that
    runs out resumes tomorrow instead of hammering the API."""

    def __init__(self, session: aiohttp.ClientSession, raw: Any, token: str, bucket: str = "courts"):
        self.session, self.raw, self.bucket = session, raw, bucket
        self.limit = CL_BUDGETS[bucket]
        self.headers = {"Authorization": f"Token {token}"}

    async def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        key = CL_BUDGET_KEY.format(bucket=self.bucket, day=datetime.now(timezone.utc).date().isoformat())
        used = int(await self.raw.incr(key))
        if used == 1:
            await self.raw.expire(key, 2 * 24 * 3600)
        if used > self.limit:
            raise BudgetSpent()
        try:
            async with self.session.get(url, params=params, headers=self.headers,
                                        timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 429:
                    raise BudgetSpent()
                if resp.status != 200:
                    logger.warning("CourtListener %s: HTTP %s", url.split("/v4/")[-1][:60], resp.status)
                    return None
                return await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            swallowed("collector.civic.courtlistener", e, logger)
            return None
        finally:
            await asyncio.sleep(CL_PAUSE_SEC)


class BudgetSpent(Exception):
    """Today's CourtListener calls are used up; the pass resumes tomorrow."""


def _cl_token() -> Optional[str]:
    return os.getenv("COURTLISTENER_API_TOKEN") or None


async def courts_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Surveillance cases from docket search, then each case judge's disclosures."""
    token = _cl_token()
    if not token:
        logger.info("COURTLISTENER_API_TOKEN not set: court cases are not collected.")
        return 0, 0
    cl = CourtListener(session, getattr(await get_redis(), "raw", None), token)
    cases = judges = 0
    try:
        for query, topic in ct.CASE_QUERIES:
            url: Optional[str] = f"{ct.API}/search/"
            params: Optional[Dict[str, Any]] = {"q": query, "type": "r", "order_by": "dateFiled desc"}
            for _ in range(ct.CASE_PAGES):
                if not url:
                    break
                body = await cl.get(url, params)
                if not body:
                    break
                rows = ct.parse_search(body, topic)
                if rows:
                    await db.execute_many(_CASE_UPSERT_SQL, [
                        (c.docket_id, c.case_name, c.court_id, c.date_filed, c.date_terminated, c.judge, c.judge_id,
                         c.suit_nature, c.docket_number, c.url, c.topic) for c in rows])
                    cases += len(rows)
                url, params = body.get("next"), None
        for r in await db.query(_JUDGES_DUE_SQL, JUDGE_RECHECK_DAYS) or []:
            pid = int(r["judge_id"])
            body = await cl.get(f"{ct.API}/financial-disclosures/", {"person": pid, "fields": "id,year"})
            if body is None:
                continue
            rows = [(int(x["id"]), pid, x.get("year")) for x in body.get("results") or [] if x.get("id")]
            if rows:
                await db.execute_many(_DISCLOSURE_UPSERT_SQL, rows)
            await db.execute(_JUDGE_CHECKED_SQL, pid)
            judges += 1
    except BudgetSpent:
        logger.info("CourtListener daily budget spent; courts pass resumes tomorrow (%s cases, %s judges so far)",
                    cases, judges)
    return cases, judges


async def holdings_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Disclosed investments in surveillance vendors, across all judges, rarest
    vendor first, resuming each prefix where the last day's budget ran out."""
    token = _cl_token()
    if not token:
        return 0, 0
    raw = getattr(await get_redis(), "raw", None)
    cl = CourtListener(session, raw, token, bucket="holdings")
    stored = calls = 0
    first = f"{ct.API}/investments/"
    for vendor in HOLDINGS_ORDER:
        for prefix in ct.VENDOR_PREFIXES[vendor]:
            saved = await raw.hget(HOLDINGS_CURSOR_KEY, prefix)
            saved = saved.decode() if isinstance(saved, bytes) else saved
            if saved == "done":
                continue
            url: Optional[str] = saved or first
            params: Optional[Dict[str, Any]] = None if saved else {
                "description__istartswith": prefix,
                "fields": "id,description,financial_disclosure,gross_value_code,redacted"}
            for _ in range(HOLDINGS_MAX_PAGES):
                if not url:
                    break
                try:
                    body = await cl.get(url, params)
                except BudgetSpent:
                    await raw.hset(HOLDINGS_CURSOR_KEY, prefix, url if params is None else first)
                    logger.info("Holdings scan paused at %s (%s stored this run)", prefix, stored)
                    raise
                calls += 1
                if not body:
                    break
                rows = ct.parse_holdings(body)
                if rows:
                    await db.execute_many(_HOLDING_UPSERT_SQL, rows)
                    stored += len(rows)
                url, params = body.get("next"), None
                if url:
                    await raw.hset(HOLDINGS_CURSOR_KEY, prefix, url)
            await raw.hset(HOLDINGS_CURSOR_KEY, prefix, "done")
    # A full scan: next week's starts from the top.
    await raw.delete(HOLDINGS_CURSOR_KEY)
    return stored, calls


# ── Lobbying (lda.gov, no key) ───────────────────────────────────────────────
LOBBY_PASS_SEC = 7 * 24 * 3600
LOBBY_PAUSE_SEC = 3.0
LOBBY_MAX_PAGES = 20
_LOBBY_UPSERT_SQL = """
    INSERT INTO lobbying_filings (filing_uuid, vendor, client_name, registrant, year, period, filing_type, amount,
        in_house, issues, entities, description, url, posted, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
    ON CONFLICT (filing_uuid) DO UPDATE SET amount = EXCLUDED.amount, issues = EXCLUDED.issues,
        entities = EXCLUDED.entities, description = EXCLUDED.description, updated_at = NOW()
"""


async def lobbying_pass(db, session: aiohttp.ClientSession) -> Tuple[int, int]:
    """Every LDA filing for each surveillance vendor client, paged politely."""
    stored = calls = 0
    for query, vendor in lb.LOBBY_CLIENTS:
        url: Optional[str] = f"{lb.LDA_API}/filings/"
        params: Optional[Dict[str, Any]] = {"client_name": query, "ordering": "-dt_posted"}
        for _ in range(LOBBY_MAX_PAGES):
            if not url:
                break
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=90)) as resp:
                    calls += 1
                    if resp.status != 200:
                        logger.warning("LDA %s: HTTP %s", query, resp.status)
                        break
                    body = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
                swallowed("collector.civic.lda", e, logger)
                break
            rows = [r for r in lb.parse_filings(body, vendor) if lb.matches_vendor(r["client_name"], query)]
            if rows:
                await db.execute_many(_LOBBY_UPSERT_SQL, [
                    (r["filing_uuid"], r["vendor"], r["client_name"], r["registrant"], r["year"], r["period"],
                     r["filing_type"], r["amount"], r["in_house"], r["issues"], r["entities"], r["description"],
                     r["url"], r["posted"]) for r in rows])
                stored += len(rows)
            url, params = body.get("next"), None
            await asyncio.sleep(LOBBY_PAUSE_SEC)
    return stored, calls


def _release_memory() -> None:
    """Hand freed heap back to the OS after a pass. CPython keeps freed arenas
    mapped, so passes stacked their peaks until the 160M limit killed the
    service (9 Oct 2026); glibc's malloc_trim returns them."""
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except (OSError, AttributeError) as e:          # not glibc (tests on Windows/macOS)
        swallowed("collector.civic.malloc_trim", e, logger)


async def _run_due(name: str, fn, every: float, db, session, raw) -> float:
    """Run `fn` if its last pass (kept in Redis, so a restart does not repeat a
    week's crawl) is older than `every`; seconds until it is next due."""
    last = await raw.hget(STATE_KEY, name)
    age = time.time() - float(last) if last else None
    if age is not None and age < every:
        return every - age
    try:
        a, b = await fn(db, session)
        _release_memory()
        await raw.hset(STATE_KEY, name, str(time.time()))
        logger.info("%s pass: %s stored, %s %s", name, a, b, {"atlas": "placed", "deflock": "skipped", "ice287g": "agencies",
                     "legal": "findings", "audits": "flagged", "dfr": "placed",
                     "money": "API calls", "brokers": "sold to police", "courts": "judges checked", "lobbying": "API calls",
                     "holdings": "API calls"}.get(name, "tiles failed"))
        return every
    except BudgetSpent:
        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
        logger.info("%s pass paused: CourtListener daily budget spent; resumes after %s", name, tomorrow.isoformat())
        return (tomorrow - now).total_seconds()
    except Exception as e:
        logger.error("%s pass failed: %s", name, e, exc_info=True)
        return 3600.0


async def main() -> None:
    logger.info("Starting civic collector (OpenStreetMap surveillance devices, EFF Atlas of Surveillance)")
    db, redis = await get_timescale(), await get_redis()
    safe_create_task(start_heartbeat_task(redis, "collector-civic"))
    timeout = aiohttp.ClientTimeout(total=OVERPASS_TIMEOUT_SEC)
    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": USER_AGENT},
                                     connector=aiohttp.TCPConnector(ttl_dns_cache=300)) as session:
        # One pass at a time: the Atlas parse and a dense Overpass tile together
        # do not fit the service's memory.
        raw = getattr(redis, "raw", redis)
        while True:
            waits = [await _run_due("atlas", atlas_pass, ATLAS_PASS_SEC, db, session, raw),
                     await _run_due("deflock", deflock_pass, DEVICE_PASS_SEC, db, session, raw),
                     await _run_due("ice287g", ice_287g_pass, LEGAL_PASS_SEC, db, session, raw),
                     await _run_due("legal", legal_pass, LEGAL_PASS_SEC, db, session, raw),
                     await _run_due("dfr", dfr_pass, MONEY_PASS_SEC, db, session, raw),
                     await _run_due("brokers", brokers_pass, MONEY_PASS_SEC, db, session, raw),
                     await _run_due("money", money_pass, MONEY_PASS_SEC, db, session, raw),
                     await _run_due("audits", audit_pass, LEGAL_PASS_SEC, db, session, raw),
                     await _run_due("lobbying", lobbying_pass, LOBBY_PASS_SEC, db, session, raw),
                     await _run_due("courts", courts_pass, COURTS_PASS_SEC, db, session, raw),
                     await _run_due("holdings", holdings_pass, HOLDINGS_PASS_SEC, db, session, raw),
                     await _run_due("devices", device_pass, DEVICE_PASS_SEC, db, session, raw)]
            await asyncio.sleep(max(60.0, min(waits)))


if __name__ == "__main__":
    asyncio.run(main())
