"""
services/collector-cyber/main.py

CYBER / INFRASTRUCTURE COLLECTOR — Phase 3
==========================================
Monitors: exposed infrastructure, BGP anomalies, ransomware victims, known exploited CVEs.
Pushes RawEvents to Kafka topic: raw.cyber

Data sources (all free, no Shodan required):

  Censys Search API      — exposed ICS/SCADA and critical infrastructure
    https://search.censys.io/api  (free tier: 250 queries/day)
    Strong on industrial protocols: Modbus (502), S7 (102), EtherNet/IP (44818)
    Better org attribution than Shodan for sector mapping.
    Auth: CENSYS_API_ID + CENSYS_API_SECRET (base64 Basic auth)

  CISA KEV JSON Feed     — Known Exploited Vulnerabilities catalog
    https://www.cisa.gov/known-exploited-vulnerabilities-catalog
    Free, no auth. Updated daily by CISA. We emit an event per new CVE.
    High signal-to-noise: every entry has confirmed active exploitation in the wild.
    Cross-referencing KEV additions against Censys-found exposed assets is the
    core Phase 3 cyber correlation.

  Ransomware.live        — ransomware group victim disclosures
    https://api.ransomware.live/recentvictims  (free, no auth)

  RIPE RIS BGP stream    — real-time BGP route announcements
    wss://ris-live.ripe.net/v1/stream/  (free WebSocket, no auth)

Poll intervals:
  Censys:          every 6 hours (budget: 250 queries/day → ~7 queries/cycle)
  CISA KEV:        every 4 hours (feed updates once daily, but we poll more to catch fast)
  Ransomware.live: every 5 minutes
  RIPE RIS BGP:    continuous stream
"""

# ── 1. PYTHON STANDARD & EXTERNAL LIBRARIES ───────────────────────────────────
# Built-in and third-party tools to handle web requests, parsing, and timing.
import asyncio
import aiohttp
import json
import logging
import os
import sys
import time
import websockets
from base64 import b64encode
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# ── 2. PATH SETUP & SHARED MODULES ────────────────────────────────────────────
# Dynamically set the project root so we can import our custom shared tools.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

# SentinelProducer: The Kafka messenger that sends data to the rest of the system.
# RawEvent: The standard envelope format we use for all incoming data.
from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.cyber")

CENSYS_API_ID     = os.getenv("CENSYS_API_ID")
CENSYS_API_SECRET = os.getenv("CENSYS_API_SECRET")
CENSYS_BASE       = "https://search.censys.io/api/v2"

CISA_KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
RIPE_RIS_URL = "wss://ris-live.ripe.net/v1/stream/?client=sentinel"

# ── 3. DOMAIN KNOWLEDGE (CONSTANTS) ───────────────────────────────────────────
# ICS/SCADA and critical infrastructure protocols
# TIP: ICS (Industrial Control Systems) run power plants, water facilities, etc.
# If these specific ports are openly accessible on the internet, it's almost 
# always a dangerous misconfiguration of a critical asset.
ICS_QUERIES = [
    ("modbus",      "services.port:502"),
    ("s7comm",      "services.port:102"),
    ("ethernet_ip", "services.port:44818"),
    ("dnp3",        "services.port:20000"),
    ("iec104",      "services.port:2404"),
    ("bacnet",      "services.port:47808"),
    ("open_scada",  'services.software.product:"SCADA"'),
]

# ICS/OT vendors — Censys hits against these vendors get elevated anomaly scoring
ICS_VENDORS = {"siemens", "schneider", "rockwell", "honeywell", "ge", "abb", "moxa", "emerson"}

# Keywords used to identify if a compromised company manages physical infrastructure.
CRITICAL_SECTORS = {
    "energy", "oil", "gas", "pipeline", "power", "utilities",
    "water", "transportation", "port", "airport", "rail",
    "government", "defense", "military", "nato",
}


# ── CENSYS — EXPOSED ICS/SCADA ────────────────────────────────────────────────

def _censys_auth() -> Optional[str]:
    """Helper to format the Censys API credentials into a Base64 Basic Auth string."""
    if not CENSYS_API_ID or not CENSYS_API_SECRET:
        return None
    token = b64encode(f"{CENSYS_API_ID}:{CENSYS_API_SECRET}".encode()).decode()
    return f"Basic {token}"


async def poll_censys(
    session:  aiohttp.ClientSession,
    producer: SentinelProducer,
    seen_ips: set,
):
    """
    Query Censys for exposed ICS/SCADA services.
    One query per protocol per cycle, 2s between queries to avoid bursting.
    250 query/day free budget → ~7 queries/cycle at 6h interval = well within budget.
    """
    auth = _censys_auth()
    if not auth:
        logger.debug("Censys not configured — skipping (set CENSYS_API_ID + CENSYS_API_SECRET)")
        return

    headers = {
        "Authorization": auth,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    new_total = 0

    # We loop through each protocol (like Modbus or BACnet) and ask Censys: 
    # "Show me all the IP addresses on the internet currently exposing this."
    for protocol_name, query in ICS_QUERIES:
        try:
            # aiohttp.ClientSession.post() makes a non-blocking web request.
            async with session.post(
                f"{CENSYS_BASE}/hosts/search",
                headers=headers,
                json={
                    "q":        query,
                    "per_page": 50,
                    "fields": [
                        "ip",
                        "services.port",
                        "services.service_name",
                        "services.software.product",
                        "autonomous_system.name",
                        "autonomous_system.country_code",
                        "location.country_code",
                    ],
                },
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    hits  = data.get("result", {}).get("hits", [])
                    new_c = 0

                    for hit in hits:
                        ip  = hit.get("ip", "")
                        key = f"{ip}:{protocol_name}"
                        
                        # DEDUPLICATION: We only care about NEW exposures.
                        # If we already saw this IP running this protocol today, skip it.
                        if not ip or key in seen_ips:
                            continue
                        seen_ips.add(key)

                        # Extract who owns this IP address.
                        org     = (hit.get("autonomous_system", {}).get("name") or "").lower()
                        country = (
                            hit.get("location", {}).get("country_code") or
                            hit.get("autonomous_system", {}).get("country_code") or ""
                        )[:2].upper()

                        services = hit.get("services", [])
                        port     = services[0].get("port") if services else None
                        product  = (
                            services[0].get("service_name", "") or
                            protocol_name
                        ) if services else protocol_name

                        is_critical = any(kw in org for kw in CRITICAL_SECTORS)
                        is_ics_vendor = any(v in org for v in ICS_VENDORS)

                        event = RawEvent(
                            source="censys",
                            occurred_at=datetime.now(timezone.utc),
                            raw_payload={
                                "ip_address":   ip,
                                "port":         port,
                                "protocol":     protocol_name,
                                "product":      product,
                                "org":          org,
                                "country_code": country,
                                "is_ics_vendor": is_ics_vendor,
                                "query":        query,
                            },
                        )
                        # Send to the Kafka 'raw.cyber' topic so the Enrichment 
                        # service can process it later.
                        producer.send(Topics.RAW_CYBER, event.model_dump(), key=ip)
                        new_c += 1

                        if is_critical or is_ics_vendor:
                            logger.warning(
                                f"🔴 CENSYS critical: {protocol_name} on {ip} "
                                f"({org}) [{country}]"
                            )

                    if new_c:
                        logger.info(f"Censys {protocol_name}: {new_c} new exposed hosts")
                        new_total += new_c

                elif resp.status == 401:
                    logger.error("Censys auth failed — check CENSYS_API_ID / CENSYS_API_SECRET")
                    return
                elif resp.status == 429:
                    # RATE LIMITING: HTTP 429 means "Too Many Requests".
                    # If Censys tells us to slow down, we pause this specific task for 2 minutes.
                    logger.warning("Censys rate limited — pausing 120s")
                    await asyncio.sleep(120)
                else:
                    logger.warning(f"Censys {protocol_name}: HTTP {resp.status}")

            # Polite crawling: Wait 2 seconds before asking about the next protocol.
            await asyncio.sleep(2)   # space between queries

        except asyncio.TimeoutError:
            logger.warning(f"Censys timeout ({protocol_name})")
        except Exception as e:
            logger.error(f"Censys poll error ({protocol_name}): {e}", exc_info=True)

    if new_total:
        logger.info(f"Censys cycle: {new_total} new exposed ICS hosts")

    # MEMORY MANAGEMENT: Prevent unbounded growth of the `seen_ips` set.
    # If we let this grow forever, the program would eventually crash from Out Of Memory.
    # Clearing it resets the cache. We might re-alert on an old IP, but it keeps the app alive.
    if len(seen_ips) > 100_000:
        seen_ips.clear()


# ── CISA KEV — KNOWN EXPLOITED VULNERABILITIES ────────────────────────────────

async def poll_cisa_kev(
    session:   aiohttp.ClientSession,
    producer:  SentinelProducer,
    seen_cves: set,
):
    """
    Downloads the CISA KEV catalog and emits an event for each new CVE.

    Why this matters for SENTINEL:
    A CISA KEV addition means DHS has confirmed active exploitation.
    If we also have a Censys hit showing that CVE's affected product exposed
    on the internet, that's a Tier 2+ correlation — someone's critical
    infrastructure is actively vulnerable to a known-exploited CVE.

    Feed schema (key fields):
      cveID, vendorProject, product, vulnerabilityName,
      dateAdded, shortDescription, knownRansomwareCampaignUse
    """
    try:
        # CISA provides this as one giant JSON file.
        # We download the whole thing and parse it.
        async with session.get(
            CISA_KEV_URL,
            timeout=aiohttp.ClientTimeout(total=30),
            headers={"User-Agent": "SENTINEL/1.0"},
        ) as resp:
            if resp.status != 200:
                logger.warning(f"CISA KEV: HTTP {resp.status}")
                return
            # Feed is JSON but sometimes served without content-type header
            data = await resp.json(content_type=None)

    except asyncio.TimeoutError:
        logger.warning("CISA KEV: timeout")
        return
    except Exception as e:
        logger.error(f"CISA KEV fetch error: {e}", exc_info=True)
        return

    vulns     = data.get("vulnerabilities", [])
    new_count = 0

    # Loop through every vulnerability in the catalog
    for vuln in vulns:
        cve_id = vuln.get("cveID", "")
        
        # If we've already processed this specific CVE before, skip it.
        if not cve_id or cve_id in seen_cves:
            continue
        seen_cves.add(cve_id)

        vendor         = vuln.get("vendorProject", "")
        product        = vuln.get("product", "")
        added          = vuln.get("dateAdded", "")
        ransomware_use = vuln.get("knownRansomwareCampaignUse", "Unknown")

        try:
            # Convert the string date (e.g., "2024-03-15") into a standardized 
            # UTC Python datetime object.
            occurred_at = datetime.strptime(added, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            occurred_at = datetime.now(timezone.utc)

        event = RawEvent(
            source="cisa_kev",
            occurred_at=occurred_at,
            raw_payload={
                "cve_id":          cve_id,
                "vendor":          vendor,
                "product":         product,
                "vulnerability":   vuln.get("vulnerabilityName", ""),
                "description":     vuln.get("shortDescription", ""),
                "date_added":      added,
                "due_date":        vuln.get("dueDate", ""),
                "ransomware_use":  ransomware_use,
                "required_action": vuln.get("requiredAction", ""),
                "is_ics_vendor":   any(v in vendor.lower() for v in ICS_VENDORS),
            },
        )
        producer.send(Topics.RAW_CYBER, event.model_dump(), key=cve_id)
        new_count += 1

        # High-priority: ransomware-linked CVEs or ICS/OT vendor vulnerabilities
        is_ics = any(v in vendor.lower() for v in ICS_VENDORS)
        if ransomware_use == "Known" or is_ics:
            logger.warning(
                f"🔴 CISA KEV (high priority): {cve_id} — {vendor} {product} "
                f"| ransomware:{ransomware_use} | ics:{is_ics}"
            )

    if new_count:
        logger.info(f"CISA KEV: {new_count} new CVEs (catalog total: {len(vulns)})")


# ── RANSOMWARE.LIVE ───────────────────────────────────────────────────────────

async def poll_ransomware(
    # This function checks a community-run API that scrapes Dark Web ransomware blogs
    # to see who got hacked recently.
    session:  aiohttp.ClientSession,
    producer: SentinelProducer,
    seen_ids: set,
):
    try:
        async with session.get(
            "https://api.ransomware.live/recentvictims",
            timeout=aiohttp.ClientTimeout(total=15),
            headers={"User-Agent": "SENTINEL/1.0"},
        ) as resp:
            if resp.status != 200:
                logger.debug(f"ransomware.live: {resp.status}")
                return
            victims = await resp.json()
            if not isinstance(victims, list):
                return

        new_count = 0
        for victim in victims:
            victim_id = victim.get("id") or victim.get("post_title", "")
            if victim_id in seen_ids:
                continue
            seen_ids.add(victim_id)

            sector  = (victim.get("activity") or victim.get("sector") or "").lower()
            country = (victim.get("country") or "")[:2].upper()

            event = RawEvent(
                source="ransomware_feed",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "victim":       victim.get("victim") or victim.get("post_title", ""),
                    "group":        victim.get("group_name", "Unknown"),
                    "sector":       sector,
                    "country_code": country,
                    "url":          victim.get("url", ""),
                    "data_types":   [],
                },
            )
            producer.send(Topics.RAW_CYBER, event.model_dump(), key="ransomware")
            new_count += 1

            if any(kw in sector for kw in CRITICAL_SECTORS):
                logger.warning(
                    f"🔴 RANSOMWARE (critical sector): "
                    f"{victim.get('victim')} — {victim.get('group_name')} [{sector}]"
                )

        if new_count:
            logger.info(f"Ransomware.live: {new_count} new victims")
        # Memory protection: Clear the cache if it gets too large.
        if len(seen_ids) > 10_000:
            seen_ids.clear()

    except asyncio.TimeoutError:
        logger.debug("ransomware.live: timeout")
    except Exception as e:
        logger.error(f"Ransomware poll error: {e}", exc_info=True)


# ── RIPE RIS — BGP STREAM ─────────────────────────────────────────────────────

async def stream_bgp(producer: SentinelProducer):
    backoff = 1
    while True:
        try:
            logger.info("Connecting to RIPE RIS BGP stream...")
            async with websockets.connect(RIPE_RIS_URL, ping_interval=30, ping_timeout=15) as ws:
                await ws.send(json.dumps({
                    "type": "ris_subscribe",
                    "data": {"type": "UPDATE", "require": "announcements"},
                }))
                backoff = 1
                logger.info("RIPE RIS BGP stream connected")

                async for raw_msg in ws:
                    try:
                        msg  = json.loads(raw_msg)
                        data = msg.get("data", {})
                        if msg.get("type") != "ris_message":
                            continue

                        announcements = data.get("announcements", [])
                        path          = data.get("path", [])
                        if not announcements or not path:
                            continue

                        origin_as = str(path[-1]) if path else ""
                        peer_as   = str(data.get("peer_asn", ""))

                        for ann in announcements[:5]:
                            for prefix in (ann.get("prefixes") or [])[:3]:
                                event = RawEvent(
                                    source="ripe_ris",
                                    occurred_at=datetime.fromtimestamp(
                                        data.get("timestamp", time.time()), tz=timezone.utc,
                                    ),
                                    raw_payload={
                                        "prefix":       prefix,
                                        "origin_as":    origin_as,
                                        "peer_as":      peer_as,
                                        "path":         path,
                                        "is_hijack":    False,
                                        "as_name":      "",
                                        "country_code": "",
                                    },
                                )
                                producer.send(Topics.RAW_CYBER, event.model_dump(), key=prefix)

                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        logger.debug(f"BGP message error: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"RIPE RIS disconnected: {e} — reconnect in {backoff}s")
        except Exception as e:
            logger.error(f"RIPE RIS error: {e} — reconnect in {backoff}s", exc_info=True)

        # EXPONENTIAL BACKOFF: If the server crashes, we don't want to spam it with 
        # reconnect attempts. We wait 1s, then 2s, then 4s, up to a max of 60s.
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ── MAIN ──────────────────────────────────────────────────────────────────────

async def collect(producer: SentinelProducer):
    seen_ips        = set()
    seen_cves       = set()
    seen_ransomware = set()
    connector       = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)

    # FIRE AND FORGET:
    # We create the WebSocket stream as a background task. It runs continuously 
    # alongside the `while True` polling loop below without blocking it.
    asyncio.create_task(stream_bgp(producer))

    async with aiohttp.ClientSession(connector=connector) as session:
        # Full CISA KEV catalog on startup — catches all historical entries
        logger.info("Loading CISA KEV catalog (initial run)...")
        await poll_cisa_kev(session, producer, seen_cves)

        last_censys_poll = 0.0
        last_kev_poll    = time.time()   # just ran above

        CENSYS_INTERVAL    = 21600   # 6 hours
        KEV_INTERVAL       = 14400   # 4 hours
        RANSOMWARE_INTERVAL = 300    # 5 minutes

        while True:
            now = time.time()
            await poll_ransomware(session, producer, seen_ransomware)

            # Only poll CISA KEV if 4 hours have passed since the last check.
            if now - last_kev_poll >= KEV_INTERVAL:
                await poll_cisa_kev(session, producer, seen_cves)
                last_kev_poll = now

            # Only poll Censys if 6 hours have passed since the last check.
            if now - last_censys_poll >= CENSYS_INTERVAL:
                await poll_censys(session, producer, seen_ips)
                last_censys_poll = now

            # Sleep for 5 minutes, then wake up and do the loop again.
            await asyncio.sleep(RANSOMWARE_INTERVAL)


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Cyber Collector")
    logger.info(f"Censys:   {'configured' if CENSYS_API_ID else 'not configured'}")
    logger.info("Sources:  CISA KEV (free), ransomware.live (free), RIPE RIS BGP (free)")
    logger.info("          Censys (free tier, set CENSYS_API_ID + CENSYS_API_SECRET)")
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