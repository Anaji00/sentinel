"""
services/collector-adsb/main.py
 
ADS-B AVIATION COLLECTOR
========================
Polls the OpenSky Network REST API for aircraft state vectors.
Covers defined geographic watch zones.
Pushes RawEvents to Kafka topic: raw.aviation
 
Official API: https://openskynetwork.github.io/opensky-api/rest.html
  - Endpoint: GET https://opensky-network.org/api/states/all
  - Auth: OAuth2 Bearer token (new accounts)
  - Rate limits:
      Anonymous:    400 credits/day,  10s minimum between requests
      Registered:  4000 credits/day,   5s minimum
      Contributing: 8000 credits/day,  1s minimum
  - State vectors are arrays — positional indexing documented below
 
State vector field positions:
  [0]  icao24         ICAO 24-bit transponder hex  (unique aircraft ID)
  [1]  callsign       flight number / callsign
  [2]  origin_country country of registration
  [3]  time_position  Unix timestamp of last position update
  [4]  last_contact   Unix timestamp of last any contact
  [5]  longitude      decimal degrees, nullable
  [6]  latitude       decimal degrees, nullable
  [7]  baro_altitude  meters, nullable
  [8]  on_ground      bool
  [9]  velocity       m/s ground speed, nullable
  [10] true_track     degrees clockwise from north
  [11] vertical_rate  m/s climb/descent
  [12] sensors        sensor IDs (own sensors only)
  [13] geo_altitude   meters, nullable
  [14] squawk         transponder code (7500/7600/7700 = emergency)
  [15] spi            special purpose indicator
  [16] position_source 0=ADS-B, 1=ASTERIX, 2=MLAT
"""
import asyncio
import aiohttp
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict
 
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
 
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.adsb")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent

OPENSKY_CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_API_BASE = "https://opensky-network.org/api"

EMERGENCY_SQUAWKS = {"7500", "7600", "7700"}
 
# (name, lamin, lamax, lomin, lomax) — match maritime watch zones for correlation
WATCH_ZONES = [
    ("Strait of Hormuz",   24.0, 27.0,  56.0,  60.0),
    ("Strait of Malacca",   1.0,  6.0, 103.0, 105.0),
    ("Bab-el-Mandeb",      11.5, 13.5,  43.0,  45.5),
    ("Taiwan Strait",      22.0, 26.0, 119.0, 122.5),
    ("Black Sea",          40.5, 46.5,  27.5,  41.5),
    ("South China Sea",     5.0, 22.0, 109.0, 121.0),
    ("Ukrainian Airspace", 44.0, 52.5,  22.0,  40.5),
    ("Red Sea",            29.8, 31.5,  32.2,  44.0),
    ("Caspian Sea",         36.0, 47.0,  46.0,  54.0),
    ("Iranian Airspace",     24.0, 40.0,  44.0,  63.0),
    ("Israeli Airspace",      29.0, 33.0,  34.0,  36.0),
    ("Saudi Airspace",        16.0, 32.0,  34.0,  56.0),
]

# Poll interval between full zone sweeps (seconds)
# Zone polls are spaced 5s apart inside the sweep.
POLL_INTERVAL = 30
# ── AUTH ──────────────────────────────────────────────────────────────────────

class OpenSkyAuth:
    """
    OAuth2 client credentials flow.
    Token expires every 30 minutes — auto-refreshes 60s before expiry.
    Falls back to anonymous (no token) if credentials not configured.
    """
    def __init__(self):
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
    async def get_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
            logger.warning("OpenSky credentials not set. Using anonymous access (limited rate).")
            return None
        
        if time.time() < self._expires_at - 60:
            return self._token  # Still valid
        
        logger.info("Refreshing OpenSky OAuth2 token...")

        try:
            async with session.post(
                OPENSKY_TOKEN_URL,
                data = {
                    "grant_type": "client_credentials",
                    "client_id": OPENSKY_CLIENT_ID,
                    "client_secret": OPENSKY_CLIENT_SECRET,
                },
                headers = {"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._token = data["access_token"]
                    self._expires_at = time.time() + data.get("expires_in", 1800)  # Default to 30 mins if not provided 
                    logger.info("OpenSky token refreshed successfully.")
                    return self._token
                else:
                    text = await resp.text()
                    logger.error(f"Failed to refresh OpenSky token: {resp.status} {text[:200]}")

        except Exception as e:
            logger.error(f"Error during OpenSky token refresh: {e}")
            return None  # Fallback to anonymous

# ── STATE VECTOR PARSER ───────────────────────────────────────────────────────

def parse_state_vector(state: list) -> Optional[dict]:
    """
    Convert OpenSky state vector array to named dict immediately.
    State vectors use positional indexing — extract to named fields
    at the boundary so downstream code never uses magic indices.
    Returns None if the array is too short.
    """
    if len(state) < 17:
        logger.warning(f"Received malformed state vector (expected 17 fields, got {len(state)}): {state}")
        return None
    
    return {
        "icao24":          state[0],
        "callsign":        (state[1] or "").strip() or None,
        "origin_country":  state[2],
        "time_position":   state[3],   # Unix timestamp
        "last_contact":    state[4],   # Unix timestamp
        "longitude":       state[5],
        "latitude":        state[6],
        "baro_altitude":   state[7],   # meters
        "on_ground":       state[8],
        "velocity":        state[9],   # m/s
        "true_track":      state[10],  # degrees
        "vertical_rate":   state[11],  # m/s
        "geo_altitude":    state[13],  # meters (index 12 = sensors, skipped)
        "squawk":          state[14],
        "position_source": state[16],  # 0=ADS-B 1=ASTERIX 2=MLAT
    }

# ── ZONE POLLER ───────────────────────────────────────────────────────────────

async def poll_zone(
        session: aiohttp.ClientSession,
        auth: OpenSkyAuth,
        producer: SentinelProducer,
        zone_name: str,
        lamin: float,
        lamax: float,
        lomin: float,
        lomax: float,
    ):
    token = await auth.get_token(session)
    url = (
        f"{OPENSKY_API_BASE}/states/all"
        f"?lamin={lamin}&lamax={lamax}&lomin={lomin}&lomax={lomax}"
    )
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            remaining = resp.headers.get("X-Rate-Limit-Remaining", "?")

            if resp.status == 200:
                data = await resp.json()
                states = data.get("states") or []
                server_time = data.get("time", int(time.time()))

                if not states:
                    return
                
                logger.debug(f"{zone_name}: {len(states)} aircraft, credits left: {remaining}")

                for state_array in states:
                    parsed = parse_state_vector(state_array)
                    if not parsed or parsed["latitude"] is None:
                        continue  # Skip malformed or positionless vectors

                    squawk = str(parsed.get("squawk") pr "").strip()
                    is_emergency = squawk in EMERGENCY_SQUAWKS

                    event = RawEvent(
                        source = "OpenSky",
                        occurred_at = datetime.fromtimestamp(parsed["time_position"] or server_time, tz=timezone.utc),
                        raw_payload = {
                            **parsed,
                            "zone_name": zone_name,
                            "is_emergency": is_emergency,
                            "emergency_type": {
                                "7500": "Hijacking",
                                "7600": "Radio Failure",
                                "7700": "General Emergency",
                            }.get(squawk) if is_emergency else None,
                        }
                    )
                    producer.send(
                        topic = Topics.RAW_AVIATION,
                        data = event.dict(),
                        key = parsed["icao24"] or "unknown",
                    )

                    if is_emergency:
                        logger.warning(
                            f"🚨 SQUAWK {squawk} in {zone_name}: "
                            f"{parsed.get('callsign', 'UNKNOWN')} "
                            f"({parsed['icao24']}) "
                            f"@ {parsed['latitude']:.3f},{parsed['longitude']:.3f}"
                        ) 

            elif resp.status == 429:
                logger.warning(f"OpenSky rate limited — sleeping 60s (credits: {remaining})")
                await asyncio.sleep(60)

            elif resp.status == 401:
                logger.warning("OpenSky auth failed — forcing token refresh")
                auth._expires_at = 0.0
 
            else:
                logger.warning(f"OpenSky {resp.status} for {zone_name}")

    except asyncio.TimeoutError:
        logger.warning(f"Timeout polling {zone_name}")
    except Exception as e:
        logger.error(f"Error polling {zone_name}: {e}", exc_info=True)

# ── MAIN COLLECTION LOOP ──────────────────────────────────────────────────────

async def collect(producer: SentinelProducer):
    auth = OpenSkyAuth()
    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)  # Limit concurrent connections
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            for zone_name, lamin, lamax, lomin, lomax in WATCH_ZONES:
                await poll_zone(
                    session, auth, producer,
                    zone_name, lamin, lamax, lomin, lomax
                )
                await asyncio.sleep(5)  # Space out zone polls to respect rate limits

            logger.info(f"Completed sweep of all zones. Sleeping for {POLL_INTERVAL}s...")
            await asyncio.sleep(POLL_INTERVAL)

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  ADS-B Collector")
    logger.info(f"Zones: {len(WATCH_ZONES)}  |  Poll interval: {POLL_INTERVAL}s")
    logger.info(f"Auth: {'OAuth2' if OPENSKY_CLIENT_ID else 'Anonymous (400 credits/day)'}")
    logger.info("=" * 60)
 
    producer = SentinelProducer()
    try:
        await collect(producer)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        producer.close()
 
 
if __name__ == "__main__":
    asyncio.run(main())   
       