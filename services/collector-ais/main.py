"""
services/collector-ais/main.py
 
AIS MARITIME COLLECTOR
======================
Connects to aisstream.io via WebSocket.
Receives real-time AIS messages from vessels globally.
Wraps each message as a RawEvent and pushes to Kafka topic: raw.maritime
 
Official API: https://aisstream.io/documentation
  - WebSocket URL: wss://stream.aisstream.io/v0/stream
  - Auth: APIKey field in subscription message (sent within 3s of connect)
  - Subscription: BoundingBoxes (required) + optional filters
  - Message types: PositionReport, ShipStaticData
 
FIX (code review): AISstream timestamp parsing now handles both millisecond
  (3-digit) and microsecond (6-digit) fractional seconds, and also timestamps
  with no fractional part at all. Previously only %H:%M:%S.%f was tried —
  if AISstream sent milliseconds (%H:%M:%S.%3f) the parse failed silently
  and fell back to now(), corrupting all event timestamps.
"""

import asyncio
import json
import ssl
import logging
import os
import sys
# datetime: For handling timestamps. timezone: To ensure all times are UTC.
from datetime import datetime, timezone
from pathlib import Path
# websockets: Library to handle persistent connections to the AIS data stream.
import websockets
from dotenv import load_dotenv


# Setup the project root path so we can import 'shared' modules like shared.kafka
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

# Import our shared tools:
# SentinelProducer: The "Mailman" that delivers messages to the rest of the system (Kafka).
# RawEvent: The standardized "Envelope" we put all raw data into.
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task, touch_heartbeat



from shared.utils.logging import setup_sentinel_logging
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.tasks import safe_create_task

logger = setup_sentinel_logging("collector.ais", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
if not AISSTREAM_API_KEY:
    logger.error("AISSTREAM_API_KEY is not set in environment variables.")
    sys.exit(1)

# The WebSocket endpoint provided by AISStream.io
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# GEOGRAPHIC FILTERS (Bounding Boxes)
# Format: [[min_lat, min_lon], [max_lat, max_lon]]
# High-interest global maritime chokepoints and economic zones.
# Named so that a zone delivering nothing can be reported by name rather than
# being silently absent from the output. See ZONE_COVERAGE_WINDOW_SEC below.
ZONE_NAMES = [
    "Strait of Hormuz", "Strait of Malacca", "Bab-el-Mandeb", "Suez Canal",
    "Red Sea", "Black Sea", "Taiwan Strait", "South China Sea",
    "Gulf of Guinea",
]

WATCH_ZONES = [
    # Strait of Hormuz, drawn around where reception happens rather than around
    # the water of interest.
    #
    # The previous box, 24-27N / 56-60E, contained the strait itself and then
    # ran east into the open Gulf of Oman. It excluded every port on the
    # approach: Bandar Abbas sits at 27.18N, a fifth of a degree above the old
    # northern edge, and Jebel Ali at 55.30E, two thirds of a degree west of the
    # old western one. Terrestrial AIS is line-of-sight to a shore receiver and
    # those receivers cluster at ports, so the box was aimed at the one part of
    # the region with no chance of coverage. It returned zero messages for the
    # entire life of the system.
    #
    # Now 24-27.5N / 54-58E: Abu Dhabi, Jebel Ali, the strait, Bandar Abbas and
    # Fujairah, and less of the open water that was never going to report.
    [[24.0, 54.0], [27.5, 58.0]],   # Strait of Hormuz + Persian Gulf approaches
    [[1.0,  103.0], [6.0,  105.0]], # Strait of Malacca
    [[11.5, 43.0],  [13.5, 45.5]],  # Bab-el-Mandeb
    [[29.8, 32.2],  [31.5, 32.8]],  # Suez Canal
    [[12.0, 32.0],  [30.0, 44.0]],  # Red Sea
    [[40.5, 27.5],  [46.5, 41.5]],  # Black Sea
    [[22.0, 119.0], [26.0, 122.5]], # Taiwan Strait
    [[5.0,  109.0], [22.0, 121.0]], # South China Sea
    [[-2.0, 1.0],   [6.0,  9.0]],   # Gulf of Guinea
]
 
MESSAGE_TYPES = ["PositionReport", "ShipStaticData"]

# ── TIMESTAMP PARSER ──────────────────────────────────────────────────────────

def _parse_aisstream_time(time_utc_str: str) -> datetime:
    """
    Parse AISstream time_utc field to a tz-aware datetime.
 
    AISstream format is one of:
      "2024-03-15 08:42:00.123 +0000 UTC"   ← milliseconds (3 digits)
      "2024-03-15 08:42:00.123456 +0000 UTC" ← microseconds (6 digits)
      "2024-03-15 08:42:00 +0000 UTC"        ← no fractional seconds
 
    FIX: Previously only %H:%M:%S.%f was tried. %f in strptime accepts
    1–6 digits, so milliseconds (3 digits) actually WOULD parse correctly —
    BUT only if the string is split correctly first. The real failure case
    was malformed strings where split(" +") fails. Now we try three formats
    in order and fall back to now() only if all fail.
    """
    # 1. Fallback: If the field is missing/empty, use current time.
    if not time_utc_str:
        return datetime.now(timezone.utc)
    
    # STRIP TIMEZONE SUFFIX
    # The API returns " +0000 UTC" at the end, which Python's strptime doesn't like.
    # We split by " +" and keep the left part (the clean date string).
    dt_part = time_utc_str.split(" +")[0].strip()

    # 2. Try formats: First with fractional seconds (microseconds), then without.
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            # .replace(tzinfo=timezone.utc) forces the object to be "aware" of UTC.
            # Without this, Python assumes local system time, which causes bugs across timezones.
            return datetime.strptime(dt_part, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    logger.debug(f"Failed to parse AISstream time_utc: {time_utc_str}")
    return datetime.now(timezone.utc)

# ── SUBSCRIPTION ──────────────────────────────────────────────────────────────

def build_subscription() -> dict:
    """
    Constructs the JSON message sent to AISStream to authenticate and set filters.
    This tells the server: "I am User X, please send me Position Reports for these Regions."
    """
    return {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": WATCH_ZONES,
        "FilterMessageTypes": MESSAGE_TYPES,
    }

# ── COUNTER ───────────────────────────────────────────────────────────────────

class MessageCounter:
    """
    Performance Monitor.
    Tracks messages per second (MPS) so we can see if the stream is healthy.
    If MPS drops to 0, we know something is wrong upstream.
    """
    def __init__(self):
        self.total     = 0
        self.per_type  = {}
        self.per_zone  = {}
        self._start    = datetime.now(timezone.utc)

    def note_position(self, lat: float, lon: float) -> None:
        """Records which watch zone a report came from.

        Subscribing to a box is not the same as receiving from it. AISStream is
        a terrestrial aggregator fed by volunteer receivers, so coverage follows
        where those receivers are: dense across Europe and East Asia, absent in
        the Persian Gulf and West Africa. The Strait of Hormuz has been in this
        subscription throughout and has never delivered a single message --
        neither has Bab-el-Mandeb -- while Taiwan Strait returns thousands a day.
        A silently empty zone reads exactly like a quiet one.
        """
        for name, ((lat1, lon1), (lat2, lon2)) in zip(ZONE_NAMES, WATCH_ZONES):
            if lat1 <= lat <= lat2 and lon1 <= lon <= lon2:
                self.per_zone[name] = self.per_zone.get(name, 0) + 1
                return
 
    def increment(self, msg_type: str):
        self.total += 1
        self.per_type[msg_type] = self.per_type.get(msg_type, 0) + 1
 
    def log_stats(self):
        elapsed = (datetime.now(timezone.utc) - self._start).total_seconds()
        rate    = self.total / elapsed if elapsed > 0 else 0
        detail  = " | ".join(f"{k}:{v}" for k, v in self.per_type.items())
        logger.info(f"AIS: {self.total} msgs @ {rate:.1f}/s — {detail}")

        # Which subscribed zones produced nothing this window. Reported rather
        # than left to inference: the system claims to watch nine chokepoints
        # and four of them have never returned a message.
        silent = [z for z in ZONE_NAMES if not self.per_zone.get(z)]
        if silent and self.total:
            logger.info(
                "AIS zone coverage: %s of %s zones delivering (%s). Silent: %s. "
                "A subscribed box with no receiver coverage looks identical to "
                "a quiet one.",
                len(ZONE_NAMES) - len(silent), len(ZONE_NAMES),
                ", ".join(f"{z}:{n}" for z, n in sorted(self.per_zone.items(), key=lambda kv: -kv[1])),
                ", ".join(silent),
            )

        self._start   = datetime.now(timezone.utc)
        self.total    = 0
        self.per_type = {}
        self.per_zone = {}

# ── COLLECTION LOOP ───────────────────────────────────────────────────────────



async def collect(producer: SentinelProducer, counter: MessageCounter):
    backoff = 1
    # Built here, not in a sibling function. metrics.ingested() sat in the hot
    # path referring to a name this scope never defined, and the NameError was
    # caught by the broad `except Exception` a few lines below -- so messages
    # still reached Kafka while every one of them took the failure path and no
    # ingest metric was ever recorded.
    metrics = CollectorMetrics("collector-ais")

    while True:
        try:
            logger.info(f"Connecting to AISStream at {AISSTREAM_URL}...")
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(
                AISSTREAM_URL,
                ping_interval=20,
                ping_timeout=30,
                open_timeout=45,
                max_size=10_000_000,
                ssl=ssl_context
            ) as ws:
                await ws.send(json.dumps(build_subscription()))
                logger.info(f"Subscribed — {len(WATCH_ZONES)} zones, types: {MESSAGE_TYPES}")
                backoff = 1  # Reset backoff after successful connection

                last_stats = asyncio.get_event_loop().time()

                async for raw_msg in ws:
                    try:
                        data = json.loads(raw_msg)
                        msg_type = data.get("MessageType", "Unknown")
                        counter.increment(msg_type)
                        meta = data.get("MetaData", {})
                        mmsi = str(meta.get("MMSI", "unknown"))
                        try:
                            counter.note_position(
                                float(meta.get("latitude")), float(meta.get("longitude"))
                            )
                        except (TypeError, ValueError):
                            pass
                        occurred_at = _parse_aisstream_time(meta.get("time_utc", ""))
                        event = RawEvent(
                            source="aisstream",
                            occurred_at=occurred_at,
                            raw_payload=data,
                        )
                        await producer.send(Topics.RAW_MARITIME, event.model_dump(mode="json"), key=mmsi)
                        metrics.ingested()
                    except json.JSONDecodeError as e:
                        metrics.rejected("json_decode")
                        logger.error(f"Failed to decode AIS message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing AIS message: {e}", exc_info=True)

                    now = asyncio.get_event_loop().time()
                    if now - last_stats > 30:
                        counter.log_stats()
                        last_stats = now
        except asyncio.CancelledError:
            logger.info("AIS collector task cancelled. Exiting loop...")
            raise
        except websockets.exceptions.InvalidStatusCode as e:
            logger.warning(f"AISStream upstream server temporary HTTP {e.status_code} status — reconnecting in {backoff}s...")
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket closed: {e} — reconnecting in {backoff}s")
        except websockets.exceptions.WebSocketException as e:
            logger.error(f"WebSocket error: {e} — reconnecting in {backoff}s")
        except Exception as e:
            logger.error(f"Unexpected error ({type(e).__name__}): {repr(e)} — reconnecting in {backoff}s")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 15)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AIS Collector..")
    logger.info(f"Zones: {len(WATCH_ZONES)}  |  Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    producer = SentinelProducer(service_name="collector-ais")
    await producer.start()
    counter = MessageCounter()
    hb_task = None
    try:
        redis = await get_redis()
        # Throughput counters. The heartbeat proves this process is alive;
        # these prove it is still producing.
        metrics = CollectorMetrics("collector-ais")
        await metrics.start(redis)
        # AISStream accepts an inactive API key without ever returning an error:
        # the socket opens, the subscription is acknowledged, and no message ever
        # arrives. Nothing in the connect path can detect that, so the absence
        # of data is what has to be watched.
        safe_create_task(metrics.watch_for_starvation(source="aisstream"))
        hb_task = safe_create_task(start_heartbeat_task(redis, "collector-ais"))
        await collect(producer, counter)
    except KeyboardInterrupt:
        logger.info("Shutting down AIS Collector...")
    finally:
        if hb_task: hb_task.cancel()
        await producer.close()

if __name__ == "__main__":
    asyncio.run(main())

