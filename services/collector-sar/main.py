"""
services/collector-sar/main.py

Radar coverage for the chokepoints AIS cannot see.

Four of nine watched chokepoints have never returned an AIS message: Strait of
Hormuz, Bab-el-Mandeb, Suez Canal and the Gulf of Guinea. That is a property of
the data source rather than the configuration -- AISStream aggregates volunteer
terrestrial receivers, and the Persian Gulf and West Africa have none. Widening
the Hormuz box to include Bandar Abbas, Jebel Ali, Abu Dhabi and Fujairah
returned zero over twelve minutes, which disposes of the alternative theory.

Sentinel-1 is a radar satellite and does not care whether a vessel is
transmitting, so a ship running dark is visible to it and invisible to every AIS
source in existence. It augments AIS where AIS is blind and replaces it nowhere:
the constellation flies a six-day nominal revisit, so this is a periodic look
rather than a live feed.

Without CDSE credentials this service says what is missing and exits. That is a
supported state -- the platform ran without radar before and still does.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Same bootstrap as the sibling collectors, so this runs identically whether it
# is started by compose or by hand.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics           # noqa: E402
from shared.models import RawEvent                          # noqa: E402
from shared.db import get_redis                             # noqa: E402
from shared.utils.chokepoints import (
    grid_key,                      # noqa: E402
    TrafficReading, record_and_assess,
)
from shared.utils.heartbeat import start_heartbeat_task     # noqa: E402
from shared.utils.collector_metrics import CollectorMetrics, for_service as _collector_metrics  # noqa: E402

from shared.utils.tasks import safe_create_task            # noqa: E402
from shared.utils.logging import setup_sentinel_logging
from sar_detection import (                                 # noqa: E402
    BLIND_CHOKEPOINTS, OPENEO_URL, VV_TARGET_THRESHOLD_DB,
    VV_TARGET_THRESHOLD_LINEAR, ChokepointReading,
    build_datacube, credentials, observation_window,
)

# Credential redaction rides on the shared handler.
#
# This called logging.basicConfig(), which installs a plain StreamHandler
# with no RedactingFilter -- so any credential appearing in an exception
# message reached stdout in clear. asyncpg and aioredis raise connection
# errors whose text embeds the full DSN, password included, and this
# service connects to both.
logger = setup_sentinel_logging("collector.sar", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

# One pass a day. The constellation revisits every six days, so polling faster
# spends credits re-reading the same acquisition.
POLL_INTERVAL_SEC = int(os.getenv("SAR_POLL_SECONDS", str(24 * 3600)))


def _connect():
    """An authenticated openEO connection, or None when unconfigured."""
    creds = credentials()
    if not creds:
        logger.warning(
            "CDSE credentials absent (CDSE_CLIENT_ID / CDSE_CLIENT_SECRET). "
            "Radar augmentation is off, so %s chokepoints stay AIS-blind: %s",
            len(BLIND_CHOKEPOINTS), ", ".join(BLIND_CHOKEPOINTS),
        )
        return None

    try:
        import openeo
    except ImportError:
        logger.error("openeo is not installed; radar augmentation cannot run.")
        return None

    client_id, client_secret = creds
    try:
        connection = openeo.connect(OPENEO_URL)
        connection.authenticate_oidc_client_credentials(
            client_id=client_id,
            client_secret=client_secret,
        )
        logger.info("Authenticated to Copernicus openEO at %s", OPENEO_URL)
        return connection
    except Exception as e:
        logger.error(f"Copernicus authentication failed: {e}")
        return None


def _polygon(bbox: dict) -> dict:
    return {
        "type": "Polygon",
        "coordinates": [[
            [bbox["west"], bbox["south"]],
            [bbox["east"], bbox["south"]],
            [bbox["east"], bbox["north"]],
            [bbox["west"], bbox["north"]],
            [bbox["west"], bbox["south"]],
        ]],
    }


# How many cells a chokepoint box is divided into, per axis.
#
# 6 gives 36 cells, and the cost of that is measured rather than assumed.
#
# Measured on Bab-el-Mandeb, one openEO execution each: 1 polygon 176.5s,
# 9 cells 113.2s, 36 cells 109.7s. Cost does not scale with geometry count --
# thirty-six cells came back faster than one polygon. Validated on the Strait
# of Hormuz, which is about seven times the area and is the one that matters:
#
#   gridded sum + count   1046.6s
#   today's ungridded     846.0s
#
# 24% more for a located answer, on a sweep that runs once a day. The grid
# found 35 of 36 cells carrying a return, with the densest at 0.320 against a
# box mean of 0.0179 -- a 25x spread, which is the whole point: one number for
# the box cannot tell the shipping lane from the empty water beside it.
#
# 0 disables it and restores the pre-grid behaviour exactly.
#
# An earlier revision of this comment said the grid had failed a 900-second
# budget and was probably unaffordable. That budget was set against a baseline
# of "about five minutes" inferred from the gaps between chokepoint timestamps
# in an old sweep -- which measure the second and third chokepoint, not the
# first -- so the grid was cut off seven per cent past the cost of the thing it
# replaces. The measurement above is what settled it, and it took one throwaway
# container: never this one, which is capped at 192MB and was OOM-killed
# mid-sweep by a probe loading a second copy of this module.
#
# At 6, a cell over Hormuz is roughly 65km: coarse for a ship, useful for a
# strait, since it separates the shipping lane from the anchorage from the
# empty water in a way one number for a 390km box cannot.
SAR_GRID_STEPS = int(os.getenv("SAR_GRID_STEPS", "6"))

# How long the gridded read may take before the scalar path takes over.
#
# The exception fallback below catches a backend that refuses the request. It
# does not catch one that accepts it and never answers, and that is the failure
# that matters here: this collector sweeps once a day, so a hung call does not
# retry -- it simply means no radar reading until someone restarts the service.
# Measured: the scalar pair takes 846s on Hormuz and the gridded pair 1046.6s,
# so 2700 is about two and a half times the observed cost -- room for a loaded
# backend without letting a hung request cost a day's reading.
SAR_GRID_TIMEOUT_SEC = int(os.getenv("SAR_GRID_TIMEOUT_SEC", "2700"))


class _GridDisabled(Exception):
    """SAR_GRID_STEPS is 0 or less: skip the grid, keep the scalar reading."""

# How long a cached grid stays current.
#
# Twelve days is twice the six-day nominal revisit, so a strait imaged at all in
# two cycles has an answer and one that has not expires instead of showing a
# fortnight-old pass as though it were now. The key itself is imported from
# shared.utils.chokepoints, beside the baseline key, so the writer here and the
# reader in the API cannot drift apart.
CHOKEPOINT_GRID_TTL_SEC = int(os.getenv("SAR_GRID_TTL_SEC", str(12 * 24 * 3600)))


def _grid_cells(bbox: dict, steps: int = None) -> list:
    """The box split into (steps x steps) cells, each with its centre.

    Returned as (centre_lat, centre_lon, polygon) so the caller can attach a
    position to each count without recomputing the geometry.
    """
    steps = steps or SAR_GRID_STEPS
    south, north = float(bbox["south"]), float(bbox["north"])
    west, east = float(bbox["west"]), float(bbox["east"])
    dlat = (north - south) / steps
    dlon = (east - west) / steps
    cells = []
    for i in range(steps):
        for j in range(steps):
            s0, n0 = south + i * dlat, south + (i + 1) * dlat
            w0, e0 = west + j * dlon, west + (j + 1) * dlon
            cells.append((
                round((s0 + n0) / 2.0, 5),
                round((w0 + e0) / 2.0, 5),
                {
                    "type": "Polygon",
                    "coordinates": [[
                        [w0, s0], [e0, s0], [e0, n0], [w0, n0], [w0, s0],
                    ]],
                },
            ))
    return cells


def _feature_collection(cells: list) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"cell": idx}, "geometry": geom}
            for idx, (_lat, _lon, geom) in enumerate(cells)
        ],
    }


def _as_count_list(raw) -> list:
    """One integer per geometry, from whatever shape the backend returned.

    The single-geometry helper below collapses to one number; this keeps the
    per-feature structure. Backends wrap it as a list, a list of lists (one per
    band) or a dict keyed by feature index, and a wrong guess would silently
    read an empty grid rather than fail.
    """
    if raw is None:
        return []
    if isinstance(raw, dict):
        try:
            raw = [raw[k] for k in sorted(raw, key=lambda x: int(x))]
        except (TypeError, ValueError):
            raw = list(raw.values())
    if not isinstance(raw, (list, tuple)):
        return [_as_counts(raw)]
    return [_as_counts(item) for item in raw]


def _as_counts(raw) -> int:
    """One integer from whatever shape aggregate_spatial returned.

    Backends wrap the result differently -- a bare number, a list per geometry,
    or a list of lists per band -- and a wrong guess here would silently read
    zero targets rather than fail, which is the quietest way this collector
    could lie. Anything unrecognised returns 0 and the caller treats the read as
    no acquisition.
    """
    while isinstance(raw, (list, tuple)):
        if not raw:
            return 0
        raw = raw[0]
    if raw is None:
        return 0
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0
    if value != value:            # NaN
        return 0
    return int(round(value))


async def _measure(connection, name: str, bbox: dict) -> ChokepointReading:
    """One radar look at one chokepoint, run off the event loop.

    The openEO client is synchronous and a job blocks for minutes, so it goes to
    a thread rather than stalling the heartbeat.
    """
    window = observation_window()

    def _run():
        cube = build_datacube(connection, bbox, window)
        # Reduce time first: a chokepoint may be imaged more than once in the
        # window, and the maximum return keeps the pass that saw most.
        reduced = cube.reduce_dimension(dimension="t", reducer="max")
        geometry = _polygon(bbox)

        # Threshold on the server, then aggregate the mask.
        #
        # This asked aggregate_spatial for "array_element", intending to pull
        # the raw pixels back and threshold them here. The backend refuses it:
        #
        #   [500] Internal: Unexpected error during 'aggregate_spatial'
        #   java.lang.IllegalArgumentException: Unsupported reducer for
        #   aggregate_spatial: array_element
        #
        # which is exactly the failure the audit flagged as unverifiable
        # without credentials -- a process graph the client accepts and the
        # server rejects. aggregate_spatial exists to collapse pixels, so
        # asking it to hand them back was always the wrong shape of request.
        #
        # sum over a 0/1 mask counts targets; count over the same mask counts
        # the valid pixels, because a masked pixel stays nodata through the
        # comparison and is excluded from both. That preserves the rule the
        # local counter enforced: nodata is not evidence of empty sea.
        mask = reduced > VV_TARGET_THRESHOLD_LINEAR
        target_sum = mask.aggregate_spatial(geometries=geometry, reducer="sum").execute()
        valid_count = mask.aggregate_spatial(geometries=geometry, reducer="count").execute()
        return target_sum, valid_count

    target_raw, water_raw = await asyncio.to_thread(_run)
    targets, water = _as_counts(target_raw), _as_counts(water_raw)
    return ChokepointReading(
        chokepoint=name,
        observed_on=window[1],
        target_pixels=targets,
        water_pixels=water,
        bbox=bbox,
    )


async def _measure_grid(connection, bbox: dict) -> list:
    """Per-cell target counts for one chokepoint, or [] if the backend refuses.

    Deliberately separate from _measure and deliberately allowed to fail: the
    whole-box reading is the one Bab-el-Mandeb has depended on, and a new query
    shape must not be able to take it down. A grid that errors logs and yields
    nothing; the scalar reading still publishes.
    """
    window = observation_window()
    cells = _grid_cells(bbox)

    def _run():
        cube = build_datacube(connection, bbox, window)
        reduced = cube.reduce_dimension(dimension="t", reducer="max")
        mask = reduced > VV_TARGET_THRESHOLD_LINEAR
        fc = _feature_collection(cells)
        targets = mask.aggregate_spatial(geometries=fc, reducer="sum").execute()
        valid = mask.aggregate_spatial(geometries=fc, reducer="count").execute()
        return targets, valid

    t_raw, v_raw = await asyncio.to_thread(_run)
    t_list, v_list = _as_count_list(t_raw), _as_count_list(v_raw)
    out = []
    for idx, (lat, lon, _geom) in enumerate(cells):
        if idx >= len(t_list) or idx >= len(v_list):
            break
        water = v_list[idx]
        if water <= 0:
            # Not imaged in this pass. Not an empty cell.
            continue
        out.append({
            "latitude": lat,
            "longitude": lon,
            "target_pixels": t_list[idx],
            "water_pixels": water,
            "target_density": round(t_list[idx] / water, 8),
        })
    return out


async def sweep(connection, producer: SentinelProducer, redis_client) -> int:
    """One pass over every AIS-blind chokepoint."""
    published = 0
    for name, bbox in BLIND_CHOKEPOINTS.items():
        # The grid first, because the whole-box numbers are its column sums.
        #
        # Two executions either way: this asks for per-cell counts and adds
        # them up, rather than asking once for the box and again for the cells.
        # If the backend will not take a FeatureCollection, the scalar path
        # below runs exactly as it always has.
        grid = []
        reading = None
        try:
            if SAR_GRID_STEPS <= 0:
                # Off. The collector then behaves exactly as it did before the
                # grid existed, which is the state to fall back to while the
                # cost of the gridded query is unproven on this backend.
                raise _GridDisabled()
            grid = await asyncio.wait_for(
                _measure_grid(connection, bbox), timeout=SAR_GRID_TIMEOUT_SEC
            )
        except _GridDisabled:
            # Not a failure: the operator has set SAR_GRID_STEPS to 0 and the
            # scalar path below is the whole intent. Said once per sweep rather
            # than silently, so "no grid in the payload" has a stated reason
            # rather than looking like a query that quietly returned nothing.
            if name == next(iter(BLIND_CHOKEPOINTS)):
                logger.info(
                    "Radar grid disabled (SAR_GRID_STEPS=0); "
                    "publishing whole-chokepoint readings only."
                )
        except asyncio.TimeoutError:
            logger.warning(
                "Radar grid for %s exceeded %ss (scalar reading kept). "
                "Lower SAR_GRID_STEPS or raise SAR_GRID_TIMEOUT_SEC.",
                name, SAR_GRID_TIMEOUT_SEC,
            )
        except Exception as e:
            logger.warning("Radar grid failed for %s (scalar reading kept): %s", name, e)

        if grid:
            reading = ChokepointReading(
                chokepoint=name,
                observed_on=observation_window()[1],
                target_pixels=sum(c["target_pixels"] for c in grid),
                water_pixels=sum(c["water_pixels"] for c in grid),
                bbox=bbox,
            )
        else:
            try:
                reading = await _measure(connection, name, bbox)
            except Exception as e:
                logger.error(f"Radar read failed for {name}: {e}")
                continue

        if reading.water_pixels <= 0:
            # No usable pixels means no acquisition in the window, which is the
            # ordinary state between passes and is not an empty strait.
            logger.info("%s: no Sentinel-1 acquisition in the window.", name)
            continue

        assessment = await record_and_assess(
            redis_client,
            TrafficReading(
                chokepoint=name,
                source="sar",
                value=reading.target_density,
                observed_at=datetime.now(timezone.utc).isoformat(),
                # The threshold is the whole calibration here. Changing it
                # changes what the density means, so it starts a new series
                # rather than being compared against the old one.
                calibration=f"vv{VV_TARGET_THRESHOLD_DB:g}db",
            ),
        )

        payload = reading.as_event_payload()
        payload["grid"] = grid
        payload["grid_steps"] = SAR_GRID_STEPS if grid else 0
        if grid:
            occupied = [c for c in grid if c["target_pixels"] > 0]
            logger.info(
                "%s: %d of %d imaged cells carry a radar return.",
                name, len(occupied), len(grid),
            )
            # Kept where the API can read it. Failure is logged, not raised:
            # a Redis that will not take the grid must not stop the reading
            # reaching Kafka.
            try:
                await redis_client.raw.set(
                    grid_key(name),
                    json.dumps({
                        "chokepoint": name,
                        "observed_on": reading.observed_on,
                        "grid_steps": SAR_GRID_STEPS,
                        "cells": grid,
                    }),
                    ex=CHOKEPOINT_GRID_TTL_SEC,
                )
            except Exception as e:
                logger.warning("Could not cache radar grid for %s: %s", name, e)

        if assessment:
            payload["traffic_assessment"] = assessment.as_payload()
            if assessment.is_notable:
                logger.warning(
                    "%s is %s on radar: z=%.2f against %s prior observations.",
                    name, assessment.direction, assessment.z_score,
                    assessment.observations,
                )
        else:
            # Said plainly. A chokepoint with too little history has not been
            # quiet; it has not been measured, and the two must not read alike.
            payload["traffic_assessment"] = None
            logger.info("%s: recorded, no baseline yet to judge it against.", name)

        await producer.send(
            Topics.RAW_MARITIME,
            RawEvent(
                source="copernicus_sentinel1",
                occurred_at=datetime.now(timezone.utc),
                raw_payload=payload,
            ).model_dump(mode="json"),
            key=name,
        )
        published += 1
        _collector_metrics("collector-sar").ingested()

    return published


async def main():
    logger.info("Starting SAR Chokepoint Collector (Copernicus Sentinel-1)")
    connection = _connect()
    if connection is None:
        # Idle rather than exit.
        #
        # The service is declared `restart: always`, so returning here produces
        # a crash loop that repeats the same warning every few seconds and
        # buries everything else in the log. Credentials arrive by someone
        # editing .env and restarting, which this waits for quietly.
        logger.info(
            "Idling. Add CDSE_CLIENT_ID and CDSE_CLIENT_SECRET to .env and "
            "restart this service to enable radar augmentation."
        )
        while True:
            await asyncio.sleep(3600)

    redis_client = await get_redis()
    producer = SentinelProducer(service_name="collector-sar")
    await producer.start()
    # Bound so the counters below reach the gateway's /metrics. The seeded
    # gauges also make a collector that has ingested nothing visible as 0
    # rather than absent, which is the difference between "quiet" and "not
    # running at all".
    await _collector_metrics("collector-sar").start(redis_client)
    safe_create_task(start_heartbeat_task(redis_client, "collector-sar"))

    while True:
        try:
            n = await sweep(connection, producer, redis_client)
            logger.info(
                "Radar sweep complete: %s of %s chokepoints reported.",
                n, len(BLIND_CHOKEPOINTS),
            )
        except Exception as e:
            logger.error(f"Radar sweep failed: {e}", exc_info=True)
        await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
