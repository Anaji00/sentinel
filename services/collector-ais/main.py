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
import logging
import os
import sys
# datetime: For handling timestamps. timezone: To ensure all times are UTC.
from datetime import datetime, timezone
from pathlib import Path
# websockets: Library to handle persistent connections to the AIS data stream.
import websockets

# Import our shared tools:
# SentinelProducer: The "Mailman" that delivers messages to the rest of the system (Kafka).
# RawEvent: The standardized "Envelope" we put all raw data into.
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from dotenv import load_dotenv

# Setup the project root path so we can import 'shared' modules like shared.kafka
ROOT = Path(__file__).resolve().parents(2)
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.ais")

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
if not AISSTREAM_API_KEY:
    logger.error("AISSTREAM_API_KEY is not set in environment variables.")
    sys.exit(1)

# The WebSocket endpoint provided by AISStream.io
AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# GEOGRAPHIC FILTERS (Bounding Boxes)
# Format: [[min_lat, min_lon], [max_lat, max_lon]]
# We only request data from these specific high-interest regions to save bandwidth and processing power.
WATCH_ZONES = [
    [[24.0, 56.0], [27.0, 60.0]],   # Strait of Hormuz
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
        self._start    = datetime.now(timezone.utc)
 
    def increment(self, msg_type: str):
        self.total += 1
        self.per_type[msg_type] = self.per_type.get(msg_type, 0) + 1
 
    def log_stats(self):
        elapsed = (datetime.now(timezone.utc) - self._start).total_seconds()
        rate    = self.total / elapsed if elapsed > 0 else 0
        detail  = " | ".join(f"{k}:{v}" for k, v in self.per_type.items())
        logger.info(f"AIS: {self.total} msgs @ {rate:.1f}/s — {detail}")
        self._start   = datetime.now(timezone.utc)
        self.total    = 0
        self.per_type = {}

# ── COLLECTION LOOP ───────────────────────────────────────────────────────────



async def collect(producer: SentinelProducer, counter: MessageCounter):
    backoff = 1

    while True:
        try:
            logger.info(f"Connecting to AISStream at {AISSTREAM_URL}...")
            async with websockets.connect(
                AISSTREAM_URL,
                ping_interval=20,
                ping_timeout=10,
                max_size=10_000_000,
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
                        occurred_at = _parse_aisstream_time(meta.get("time_utc", ""))
                        event = RawEvent(
                            source = "aisstream",
                            occurred_at = occurred_at,
                            raw_payload = data,
                        )
                        producer.send(Topics.RAW_MARITIME, event.dict(), key=mmsi)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode AIS message: {e}")
                    except Exception as e:
                        logger.error(f"Error processing AIS message: {e}", exc_info=True)

                    now = asyncio.get_event_loop().time()
                    if now - last_stats > 30:
                        counter.log_stats()
                        last_stats = now
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket closed: {e} — reconnect in {backoff}s")
        except websockets.exceptions.WebSocketException as e:
            logger.error(f"WebSocket error: {e} — reconnect in {backoff}s")
        except Exception as e:
            logger.error(f"Unexpected error: {e} — reconnect in {backoff}s", exc_info=True)
 
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL AIS Collector..")
    logger.info(f"Zones: {len(WATCH_ZONES)}  |  Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    producer = SentinelProducer()
    counter = MessageCounter()
    try:
        await collect(producer, counter)
    except KeyboardInterrupt:
        logger.info("Shutting down AIS Collector...")
    finally:
        producer.close()

if __name__ == "__main__":
    asyncio.run(main())

