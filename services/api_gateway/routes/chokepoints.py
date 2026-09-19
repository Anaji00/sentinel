"""
services/api_gateway/routes/chokepoints.py

What each watched chokepoint last reported, and from which instrument.

The platform watches nine maritime chokepoints and could not answer "am I
receiving anything from the Strait of Hormuz" from any endpoint. The data to
answer it existed the whole time and was scattered across three shapes that no
single query joined:

  AIS       vessel positions, plentiful where terrestrial receivers are dense
            and absent where they are not. AISStream delivers thousands a day
            from the Taiwan Strait and, on this deployment, four in a day from
            Hormuz and none at all from Bab-el-Mandeb.

  SAR       Sentinel-1 radar target density, which does not care about
            receivers. It is the only current observation of Bab-el-Mandeb this
            platform has, it arrives roughly hourly, and nothing displayed it:
            the events carry no `vessel_data`, so /events/maritime -- the only
            maritime feed the map fetches -- cannot return them.

The distinction this endpoint exists to preserve is the one `shared/utils/
chokepoints.py` opens with: a subscribed box with no receiver coverage looks
exactly like a quiet strait. `ais_silent` says which of the two you are looking
at, so an empty Hormuz reads as "not observed by AIS" rather than "no traffic".
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query

from services.api_gateway.dependencies import get_db, get_redis_client
from shared.utils.chokepoints import grid_key
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("api.chokepoints")

router = APIRouter(prefix="/api/v1/chokepoints", tags=["Chokepoints"])

# The maritime chokepoints, in the spelling classify_region emits. Airspace and
# open-sea regions are deliberately excluded: this endpoint answers a question
# about straits.
WATCHED: List[str] = [
    "Strait of Hormuz",
    "Bab-el-Mandeb",
    "Strait of Malacca",
    "Suez Canal",
    "Taiwan Strait",
    "Turkish Straits",
    "Red Sea",
    "Black Sea",
    "South China Sea",
    "Singapore Approach",
    "Gulf of Guinea",
    "Panama Canal",
]

# How long a reading may be before it stops describing now. Generous, because
# SAR revisits on an orbit rather than on a schedule.
FRESH_WINDOW_HOURS = 24


def _age_seconds(ts: Optional[datetime]) -> Optional[float]:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds()


async def _radar_grid(redis_client, region: str) -> Optional[Dict[str, Any]]:
    """The latest radar cells for one chokepoint, or None.

    Never raises into the response: a chokepoint list that fails because the
    cache is cold is worse than one without grids.
    """
    if redis_client is None:
        return None
    try:
        raw = await getattr(redis_client, "raw", redis_client).get(grid_key(region))
    except Exception as exc:
        # Counted and escalated rather than whispered: a Redis that has stopped
        # answering makes every strait look unimaged, which is the one thing
        # this endpoint exists to distinguish from unobserved.
        swallowed("api.chokepoints.radar_grid", exc, logger, detail=region)
        return None
    if not raw:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "replace")
        return json.loads(raw)
    except (ValueError, TypeError) as exc:
        swallowed("api.chokepoints.radar_grid_decode", exc, logger, detail=region)
        return None


@router.get("")
async def get_chokepoint_status(
    hours: int = Query(FRESH_WINDOW_HOURS, ge=1, le=168),
    db=Depends(get_db),
    redis_client=Depends(get_redis_client),
) -> Dict[str, Any]:
    """The latest AIS and SAR reading for each watched chokepoint.

    Absence is reported as absence. A chokepoint with no AIS in the window
    returns `ais: null` and `ais_silent: true` rather than a count of zero,
    because zero vessels and zero coverage are different claims and this
    platform has both.
    """
    rows = await db.query(
        """
        WITH watched AS (
            SELECT unnest($1::text[]) AS region
        ), latest AS (
            SELECT w.region, s.kind, e.occurred_at, e.headline, e.source,
                   e.latitude, e.longitude, e.anomaly_score
            FROM watched w
            CROSS JOIN (VALUES ('ais'), ('sar')) AS s(kind)
            CROSS JOIN LATERAL (
                SELECT occurred_at, headline, source, anomaly_score,
                       COALESCE(latitude, ST_Y(coordinates::geometry)) AS latitude,
                       COALESCE(longitude, ST_X(coordinates::geometry)) AS longitude
                FROM events
                WHERE region = w.region
                  AND occurred_at > NOW() - make_interval(hours => $2)
                  AND (
                        (s.kind = 'ais' AND vessel_data IS NOT NULL)
                     OR (s.kind = 'sar' AND source = 'copernicus_sentinel1')
                  )
                ORDER BY occurred_at DESC
                LIMIT 1
            ) e
        )
        SELECT * FROM latest
        """,
        WATCHED,
        int(hours),
    )

    by_region: Dict[str, Dict[str, Any]] = {
        name: {"region": name, "ais": None, "sar": None} for name in WATCHED
    }
    for row in rows:
        item = dict(row)
        region = item.pop("region")
        kind = item.pop("kind")
        if region not in by_region:
            continue
        item["age_seconds"] = _age_seconds(item.get("occurred_at"))
        by_region[region][kind] = item

    results = []
    for name in WATCHED:
        entry = by_region[name]
        # Where the metal was, when radar has seen it. Cells with no return are
        # dropped here rather than sent: an imaged empty cell is information,
        # but it is not something to draw, and the count is kept below.
        grid = await _radar_grid(redis_client, name)
        if grid:
            cells = grid.get("cells") or []
            entry["radar_grid"] = {
                "observed_on": grid.get("observed_on"),
                "cells_imaged": len(cells),
                "cells": [c for c in cells if (c.get("target_pixels") or 0) > 0],
            }
        else:
            entry["radar_grid"] = None
        entry["ais_silent"] = entry["ais"] is None
        # Observed at all, by anything. This is the field a map layer should
        # colour on: a strait SAR can see is not dark just because AIS cannot.
        entry["observed"] = entry["ais"] is not None or entry["sar"] is not None
        results.append(entry)

    silent = [r["region"] for r in results if r["ais_silent"]]
    unobserved = [r["region"] for r in results if not r["observed"]]

    return {
        "window_hours": hours,
        "chokepoints": results,
        # Stated rather than left to the caller to derive, because the whole
        # point of this endpoint is that the two are not the same list.
        "ais_silent": silent,
        "unobserved_by_any_instrument": unobserved,
    }
