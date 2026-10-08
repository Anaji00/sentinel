"""
services/collector-news/conflict.py

Live readings for the conflict zones in shared/utils/conflict_zones.py.

Two sources, both public and keyless, chosen because they see what AIS cannot:
terrestrial AIS is volunteer-fed and on this deployment delivered one Strait of
Hormuz event in thirty days, while the war there was in the news daily.

  GDELT DOC 2.0   per zone, the latest English headlines that name the place
                  and a conflict term, and the hourly article volume over seven
                  days in every language -- the baseline a zone's trend is
                  measured against. GDELT asks for one request every 5 seconds.

  NGA MSI         the active US broadcast navigational warnings (HYDROPAC,
                  HYDROLANT, HYDROARC, NAVAREA IV and XII): live firing,
                  missile tests, mines and GNSS interference, with coordinates.

Readings go to Redis under the keys conflict_zones declares; the gateway reads
them. A failed fetch keeps the previous reading rather than blanking it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from shared.utils.conflict_zones import (
    LIVE_TTL_SEC, NAV_WARNINGS_KEY, ZONE_LIVE_KEY, ZONES, ConflictZone,
    assess_status, classify_warning, daily_volumes, gdelt_query,
)
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector.news.conflict")

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
NGA_WARNINGS_URL = "https://msi.nga.mil/api/publications/broadcast-warn?output=json&status=A"

# GDELT asks for one request per 5 seconds and enforces it per address with a
# cool-down: the first cycle here, at 6-second spacing with two calls per zone,
# drew 429s for every request that followed. So headlines are fetched every
# cycle and the seven-day volume only every six hours, 15 seconds apart, and a
# 429 ends the cycle and backs off.
GDELT_SPACING_SEC = 15.0
ZONE_CYCLE_SEC = 30 * 60
VOLUME_CYCLE_SEC = 6 * 3600
BACKOFF_START_SEC = 10 * 60
BACKOFF_MAX_SEC = 60 * 60
NAV_CYCLE_SEC = 60 * 60
HEADLINES_PER_ZONE = 20
USER_AGENT = "SENTINEL Intelligence Platform/1.0"


class RateLimited(Exception):
    """The upstream answered 429; the caller should stop and back off."""


async def _get_json(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    """GET and decode JSON, or None. GDELT answers some errors with plain text."""
    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=45), headers={"User-Agent": USER_AGENT},
    ) as resp:
        if resp.status == 429:
            raise RateLimited(url[:80])
        if resp.status != 200:
            logger.debug(f"{url[:80]}: HTTP {resp.status}")
            return None
        text = await resp.text(errors="ignore")
    try:
        return json.loads(text, strict=False)
    except ValueError:
        logger.debug(f"{url[:80]}: not JSON: {text[:120]!r}")
        return None


def _gdelt_url(query: str, **params: str) -> str:
    return f"{GDELT_DOC_URL}?{urlencode({'query': query, 'format': 'json', **params})}"


def _articles(payload: Any) -> List[Dict[str, Any]]:
    out = []
    for a in (payload or {}).get("articles") or []:
        title = str(a.get("title") or "").strip()
        url = str(a.get("url") or "").strip()
        if not title or not url:
            continue
        out.append({
            "title": title,
            "url": url,
            "domain": a.get("domain"),
            "seendate": a.get("seendate"),
            "sourcecountry": a.get("sourcecountry"),
        })
    return out


def _hourly(payload: Any) -> List[tuple]:
    series = ((payload or {}).get("timeline") or [{}])[0].get("data") or []
    return [(str(p.get("date")), float(p.get("value") or 0)) for p in series if p.get("date")]


async def poll_zone(
    session: aiohttp.ClientSession, redis_client, zone: ConflictZone, with_volume: bool = True,
) -> bool:
    """One zone's headlines, and its volume when asked. True when stored.

    Raises RateLimited on a 429 so the caller can stop the whole cycle.
    """
    query = gdelt_query(zone)
    # Headlines in English for the reader; the volume counts every language.
    headlines = await _get_json(session, _gdelt_url(
        f"{query} sourcelang:english", mode="artlist", timespan="24h",
        sort="datedesc", maxrecords=str(HEADLINES_PER_ZONE),
    ))
    await asyncio.sleep(GDELT_SPACING_SEC)
    volume = None
    if with_volume:
        volume = await _get_json(session, _gdelt_url(query, mode="timelinevolraw", timespan="7d"))
        await asyncio.sleep(GDELT_SPACING_SEC)

    if headlines is None and volume is None:
        return False

    key = ZONE_LIVE_KEY.format(zone_id=zone.zone_id)
    raw = getattr(redis_client, "raw", redis_client)
    previous: Dict[str, Any] = {}
    try:
        blob = await raw.get(key)
        if blob:
            previous = json.loads(blob if isinstance(blob, str) else blob.decode("utf-8"))
    except Exception as exc:
        swallowed("collector.news.conflict.previous", exc, logger, detail=zone.zone_id)

    reading: Dict[str, Any] = dict(previous)
    if headlines is not None:
        reading["articles"] = _articles(headlines)
    if volume is not None:
        hourly = _hourly(volume)
        last_24h, baseline = daily_volumes(hourly)
        reading["volume_24h"] = last_24h
        reading["baseline_daily"] = baseline
        reading["volume_series"] = [v for _, v in hourly][-168:]
    reading.update(assess_status(zone.prior, float(reading.get("volume_24h") or 0.0),
                                 float(reading.get("baseline_daily") or 0.0)))
    reading["updated_at"] = datetime.now(timezone.utc).isoformat()
    reading["query"] = query
    await raw.set(key, json.dumps(reading), ex=LIVE_TTL_SEC)
    return True


async def poll_nav_warnings(session: aiohttp.ClientSession, redis_client) -> int:
    """Active NGA warnings inside a zone or of a military kind. Count stored."""
    payload = await _get_json(session, NGA_WARNINGS_URL)
    if payload is None:
        return -1
    kept = []
    for w in payload.get("broadcast-warn") or []:
        text = str(w.get("text") or "")
        info = classify_warning(text)
        if not info["zone_id"] and not info["military"]:
            continue
        if not info["centroid"]:
            continue
        kept.append({
            "id": f"{w.get('navArea')} {w.get('msgNumber')}/{w.get('msgYear')}",
            "nav_area": w.get("navArea"),
            "area": info["area"],
            "text": text[:900],
            "centroid": info["centroid"],
            "points": info["points"][:16],
            "zone_id": info["zone_id"],
            "military": info["military"],
            "issued": w.get("issueDate"),
        })
    raw = getattr(redis_client, "raw", redis_client)
    await raw.set(
        NAV_WARNINGS_KEY,
        json.dumps({"updated_at": datetime.now(timezone.utc).isoformat(), "warnings": kept}),
        ex=LIVE_TTL_SEC,
    )
    return len(kept)


def next_backoff(current: float) -> float:
    """The wait after a 429: ten minutes, doubling, at most an hour."""
    return BACKOFF_START_SEC if current <= 0 else min(BACKOFF_MAX_SEC, current * 2)


async def run_conflict_monitor(redis_client) -> None:
    """Poll the zones every cycle and the warnings hourly, backing off on 429."""
    last_nav = 0.0
    volume_done: Dict[str, float] = {}
    backoff = 0.0
    async with aiohttp.ClientSession() as session:
        while True:
            started = time.monotonic()
            if started - last_nav >= NAV_CYCLE_SEC:
                try:
                    n = await poll_nav_warnings(session, redis_client)
                    logger.info(f"Navigational warnings: {n} in zones or military")
                    last_nav = started
                except Exception as exc:
                    swallowed("collector.news.conflict.nav", exc, logger)
            stored = 0
            limited = False
            for zone in ZONES:
                want_volume = time.monotonic() - volume_done.get(zone.zone_id, -1e9) >= VOLUME_CYCLE_SEC
                try:
                    if await poll_zone(session, redis_client, zone, with_volume=want_volume):
                        stored += 1
                        if want_volume:
                            volume_done[zone.zone_id] = time.monotonic()
                except RateLimited:
                    limited = True
                    break
                except Exception as exc:
                    swallowed("collector.news.conflict.zone", exc, logger, detail=zone.zone_id)
            if limited:
                backoff = next_backoff(backoff)
                logger.warning(
                    f"GDELT rate-limited after {stored}/{len(ZONES)} zones; backing off {backoff / 60:.0f} min"
                )
                await asyncio.sleep(backoff)
                continue
            backoff = 0.0
            logger.info(f"Conflict zones: {stored}/{len(ZONES)} readings stored")
            await asyncio.sleep(max(60.0, ZONE_CYCLE_SEC - (time.monotonic() - started)))


__all__ = ["RateLimited", "next_backoff", "poll_nav_warnings", "poll_zone", "run_conflict_monitor"]
