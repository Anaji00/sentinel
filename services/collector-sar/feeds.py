"""
services/collector-sar/feeds.py

Two more ways to see vessels where shore AIS is blind, run beside the radar
sweep. Both need free accounts; without credentials each task says what is
missing and idles, the way the radar sweep does without CDSE credentials.

  Global Fishing Watch   GFW_API_TOKEN
      Satellite-AIS events every six hours over the last week: AIS gaps become
      `vessel_dark`, encounters `vessel_sts`, inside the conflict zones and
      radar areas. Published through the passthrough path so they are stored
      and correlated like any other vessel event.

  NOAA VIIRS boat detection   EOG_USERNAME, EOG_PASSWORD, EOG_CLIENT_SECRET
      The newest nightly file of lit-vessel detections, filtered to the zones
      and radar areas, stored for the map and counted per zone per night.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
import re
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urljoin

import aiohttp

from shared.kafka import Topics
from shared.models import NormalizedEvent, RawEvent
from shared.utils.osint_bridges import vessel_exposure_tags
from shared.utils.quiet_failures import swallowed
from shared.utils.regions import classify_region
from shared.utils.vessel_feeds import (
    GFW_SOURCE, VIIRS_DETECTIONS_KEY, VIIRS_DETECTIONS_TTL_SEC, VIIRS_MAX_STORED,
    VIIRS_ZONE_HISTORY_KEY, gfw_event, vbd_detections, zone_counts,
)

logger = logging.getLogger("collector.sar.feeds")

FEED_INTERVAL_SEC = 6 * 3600

GFW_EVENTS_URL = os.getenv("GFW_EVENTS_URL", "https://gateway.api.globalfishingwatch.org/v3/events")
GFW_DATASETS = [
    d.strip() for d in os.getenv(
        "GFW_DATASETS",
        "public-global-gaps-events:latest,public-global-encounters-events:latest",
    ).split(",") if d.strip()
]
GFW_LOOKBACK_DAYS = 7
GFW_PAGE = 500
GFW_MAX_PER_DATASET = 5000
GFW_SEEN_KEY = "sentinel:gfw:seen"

EOG_TOKEN_URL = os.getenv(
    "EOG_TOKEN_URL", "https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token"
)
EOG_CLIENT_ID = os.getenv("EOG_CLIENT_ID", "eogdata_oidc")
VIIRS_VBD_BASE_URL = os.getenv(
    "VIIRS_VBD_BASE_URL", "https://eogdata.mines.edu/wwwdata/viirs_products/vbd-pub/v23/global-saa/nrt/"
)
VIIRS_LAST_FILE_KEY = "sentinel:viirs:last_file"
_VBD_FILE = re.compile(r'href="(VBD_[^"/]+\.csv(?:\.gz)?)"')


def _configured(*names: str) -> bool:
    """Set and not a placeholder left in .env."""
    for n in names:
        v = (os.getenv(n) or "").strip()
        if not v or v.startswith("your_") or v.lower() in {"changeme", "placeholder"}:
            return False
    return True


# -- Global Fishing Watch -----------------------------------------------------

def _tanker_like(vtype: Optional[str]) -> str:
    """GFW vessel classes onto the platform's: a bunker vessel carries fuel."""
    return "tanker" if (vtype or "").lower() in {"bunker", "tanker"} else (vtype or "")


def _normalized(fields: Dict[str, Any]) -> NormalizedEvent:
    region = classify_region(fields["latitude"], fields["longitude"])
    ent = fields["entity"]
    tags = list(fields["tags"]) + vessel_exposure_tags(region, _tanker_like(fields.get("vessel_type")))
    summary = (
        f"Global Fishing Watch event {fields.get('gfw_event_id')} from satellite AIS. "
        "GFW's coverage is strongest for fishing vessels and carriers, and events arrive "
        "with a lag of a few days."
    )
    return NormalizedEvent.model_validate({
        "type": fields["type"],
        "occurred_at": fields["occurred_at"],
        "source": GFW_SOURCE,
        "primary_entity": {"id": str(ent["id"]), "type": "vessel", "name": ent["name"]},
        "latitude": fields["latitude"],
        "longitude": fields["longitude"],
        "region": region,
        "headline": fields["headline"],
        "summary": summary,
        "vessel_data": {
            "mmsi": str(ent["id"]),
            "vessel_type": fields.get("vessel_type"),
            "flag_state": ent.get("flag"),
            "gap_hours": fields.get("gap_hours"),
            "last_seen_region": region,
            "latitude": fields["latitude"],
            "longitude": fields["longitude"],
        },
        "tags": tags,
        "anomaly_score": fields["anomaly_score"],
    })


async def _gfw_page(session: aiohttp.ClientSession, token: str, dataset: str, offset: int) -> Dict[str, Any]:
    end = date.today()
    start = end - timedelta(days=GFW_LOOKBACK_DAYS)
    params = {
        "datasets[0]": dataset,
        "start-date": start.isoformat(),
        "end-date": end.isoformat(),
        "limit": str(GFW_PAGE),
        "offset": str(offset),
    }
    async with session.get(
        f"{GFW_EVENTS_URL}?{urlencode(params)}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=aiohttp.ClientTimeout(total=90),
    ) as resp:
        if resp.status == 401:
            raise PermissionError("GFW rejected the token (401)")
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def poll_gfw(session: aiohttp.ClientSession, producer, redis_client) -> int:
    token = os.getenv("GFW_API_TOKEN", "").strip()
    raw = getattr(redis_client, "raw", redis_client)
    published = 0
    for dataset in GFW_DATASETS:
        offset = 0
        while offset < GFW_MAX_PER_DATASET:
            page = await _gfw_page(session, token, dataset, offset)
            entries = page.get("entries") or []
            for entry in entries:
                fields = gfw_event(entry)
                if not fields:
                    continue
                if not await raw.sadd(GFW_SEEN_KEY, str(fields.get("gfw_event_id"))):
                    continue
                event = _normalized(fields)
                await producer.send(
                    Topics.RAW_TRADFI,
                    RawEvent(
                        source=GFW_SOURCE,
                        occurred_at=event.occurred_at,
                        raw_payload={"normalized_event": event.model_dump(mode="json")},
                    ).model_dump(mode="json"),
                    key=str(fields["entity"]["id"]),
                )
                published += 1
            nxt = page.get("nextOffset")
            if not entries or nxt is None or int(nxt) <= offset:
                break
            offset = int(nxt)
    await raw.expire(GFW_SEEN_KEY, 30 * 86400)
    return published


# -- VIIRS boat detection -----------------------------------------------------

async def _eog_token(session: aiohttp.ClientSession) -> str:
    async with session.post(
        EOG_TOKEN_URL,
        data={
            "client_id": EOG_CLIENT_ID,
            "client_secret": os.getenv("EOG_CLIENT_SECRET", ""),
            "username": os.getenv("EOG_USERNAME", ""),
            "password": os.getenv("EOG_PASSWORD", ""),
            "grant_type": "password",
        },
        timeout=aiohttp.ClientTimeout(total=60),
    ) as resp:
        if resp.status in (400, 401):
            raise PermissionError(f"EOG rejected the credentials ({resp.status})")
        resp.raise_for_status()
        return (await resp.json(content_type=None))["access_token"]


def _parse_file(path: str, gz: bool) -> List[Dict[str, Any]]:
    opener = gzip.open if gz else open
    with opener(path, "rt", encoding="utf-8", errors="replace", newline="") as fh:
        return list(vbd_detections(fh))


async def poll_viirs(session: aiohttp.ClientSession, redis_client) -> int:
    raw = getattr(redis_client, "raw", redis_client)
    token = await _eog_token(session)
    headers = {"Authorization": f"Bearer {token}"}
    async with session.get(VIIRS_VBD_BASE_URL, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        resp.raise_for_status()
        listing = await resp.text()
    files = sorted(set(_VBD_FILE.findall(listing)))
    if not files:
        logger.info("VIIRS: no VBD files listed at %s", VIIRS_VBD_BASE_URL)
        return 0
    newest = files[-1]
    last = await raw.get(VIIRS_LAST_FILE_KEY)
    if (last.decode() if isinstance(last, bytes) else last) == newest:
        return 0

    # Streamed to disk: a global night is tens of megabytes and this container
    # has 192 MB.
    with tempfile.NamedTemporaryFile(suffix=".csv.gz" if newest.endswith(".gz") else ".csv", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        async with session.get(
            urljoin(VIIRS_VBD_BASE_URL, newest), headers=headers,
            timeout=aiohttp.ClientTimeout(total=900),
        ) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as out:
                async for chunk in resp.content.iter_chunked(1 << 16):
                    out.write(chunk)
        detections = await asyncio.to_thread(_parse_file, tmp_path, newest.endswith(".gz"))
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as exc:
            swallowed("collector.sar.feeds.viirs_tmp_cleanup", exc, logger)

    counts = zone_counts(detections)
    await raw.set(
        VIIRS_DETECTIONS_KEY,
        json.dumps({
            "file": newest,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "zone_counts": counts,
            "detections": detections[:VIIRS_MAX_STORED],
        }),
        ex=VIIRS_DETECTIONS_TTL_SEC,
    )
    for zone_id, n in counts.items():
        key = VIIRS_ZONE_HISTORY_KEY.format(zone_id=zone_id)
        await raw.lpush(key, n)
        await raw.ltrim(key, 0, 59)
        await raw.expire(key, 90 * 86400)
    await raw.set(VIIRS_LAST_FILE_KEY, newest, ex=30 * 86400)
    return len(detections)


# -- the loops ----------------------------------------------------------------

async def run_vessel_feeds(producer, redis_client) -> None:
    """GFW and VIIRS every six hours, each only when its credentials are set."""
    gfw_on = _configured("GFW_API_TOKEN")
    viirs_on = _configured("EOG_USERNAME", "EOG_PASSWORD", "EOG_CLIENT_SECRET")
    if not gfw_on:
        logger.info("Global Fishing Watch idle: set GFW_API_TOKEN in .env and restart collector-sar.")
    if not viirs_on:
        logger.info(
            "VIIRS boat detection idle: set EOG_USERNAME, EOG_PASSWORD and EOG_CLIENT_SECRET "
            "in .env and restart collector-sar."
        )
    if not (gfw_on or viirs_on):
        return
    async with aiohttp.ClientSession() as session:
        while True:
            if gfw_on:
                try:
                    n = await poll_gfw(session, producer, redis_client)
                    logger.info("Global Fishing Watch: %d new events in watched areas", n)
                except PermissionError as exc:
                    logger.error("Global Fishing Watch: %s; check GFW_API_TOKEN", exc)
                except Exception as exc:
                    swallowed("collector.sar.feeds.gfw", exc, logger)
            if viirs_on:
                try:
                    n = await poll_viirs(session, redis_client)
                    logger.info("VIIRS boat detection: %d lit vessels in watched areas", n)
                except PermissionError as exc:
                    logger.error("VIIRS boat detection: %s; check the EOG_ settings", exc)
                except Exception as exc:
                    swallowed("collector.sar.feeds.viirs", exc, logger)
            await asyncio.sleep(FEED_INTERVAL_SEC)


__all__ = ["poll_gfw", "poll_viirs", "run_vessel_feeds"]
