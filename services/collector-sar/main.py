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
from shared.utils.chokepoints import (                      # noqa: E402
    TrafficReading, record_and_assess,
)
from shared.utils.heartbeat import start_heartbeat_task     # noqa: E402

from shared.utils.tasks import safe_create_task            # noqa: E402
from sar_detection import (                                 # noqa: E402
    BLIND_CHOKEPOINTS, OPENEO_URL, VV_TARGET_THRESHOLD_DB, ChokepointReading,
    build_datacube, credentials, observation_window,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("collector.sar")

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
        mask = reduced > VV_TARGET_THRESHOLD_DB
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


async def sweep(connection, producer: SentinelProducer, redis_client) -> int:
    """One pass over every AIS-blind chokepoint."""
    published = 0
    for name, bbox in BLIND_CHOKEPOINTS.items():
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
            ),
        )

        payload = reading.as_event_payload()
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
    producer = SentinelProducer()
    await producer.start()
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
