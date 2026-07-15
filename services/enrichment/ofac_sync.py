"""
services/enrichment/ofac_sync.py

Standalone script to fetch OFAC SDN sanctions list, parse it, and broadcast
updates to the rest of the Sentinel cluster via Redis Pub/Sub.

This implements the Control Plane for the In-Memory Aho-Corasick Automaton.
"""

import asyncio
import json
import logging
import os
import csv
from io import StringIO
import aiohttp

# We use the shared Redis client connection logic
from shared.db import get_redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ofac_sync")

OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
OFAC_ALT_URL = "https://www.treasury.gov/ofac/downloads/alt.csv"

async def fetch_ofac_keywords():
    """
    Downloads the official SDN and ALT (aliases) lists from the US Treasury.
    Returns a list of unique names/keywords.
    """
    keywords = set()
    logger.info(f"Downloading OFAC SDN from {OFAC_SDN_URL}")
    
    async with aiohttp.ClientSession() as session:
        # Download primary names
        async with session.get(OFAC_SDN_URL) as resp:
            if resp.status == 200:
                text = await resp.text()
                # OFAC CSV format: uid, last_name, type, programs, title, vessel_call_sign, vessel_type, vessel_tonnage, grt, vessel_flag, vessel_owner, remarks
                reader = csv.reader(StringIO(text))
                for row in reader:
                    if len(row) > 1 and row[1]:
                        # Basic cleanup
                        name = row[1].strip().lower()
                        # Avoid single character garbage
                        if len(name) > 3:
                            keywords.add(name)
            else:
                logger.error(f"Failed to fetch SDN list: HTTP {resp.status}")

        # Download aliases
        async with session.get(OFAC_ALT_URL) as resp:
            if resp.status == 200:
                text = await resp.text()
                # OFAC ALT CSV format: uid, ent_num, alt_type, alt_name, alt_remarks
                reader = csv.reader(StringIO(text))
                for row in reader:
                    if len(row) > 3 and row[3]:
                        name = row[3].strip().lower()
                        if len(name) > 3:
                            keywords.add(name)
                            
    return list(keywords)

async def sync_ofac_to_cluster():
    """
    Fetches the latest keywords, updates the local sanctions.json (for restarts),
    and sends a Pub/Sub message to tell all running workers to rebuild their Automatons.
    """
    redis = await get_redis()
    
    while True:
        try:
            keywords = await fetch_ofac_keywords()
            if not keywords:
                logger.warning("No keywords found, aborting sync for this cycle.")
            else:
                logger.info(f"Parsed {len(keywords)} total OFAC aliases and names.")

                payload = {"keywords": list(keywords)}
                
                # Save directly to Redis instead of a local file
                # This acts as the Single Source of Truth for all containers
                await redis.raw.set("sentinel:config:sanctions", json.dumps(payload))
                logger.info("Saved OFAC list to Redis key: sentinel:config:sanctions")
                    
                # Broadcast the update command to all running Python instances
                await redis.raw.publish("sentinel:config:updates", "ofac_rebuild")
                logger.info("Published 'ofac_rebuild' signal to Sentinel cluster via Redis Pub/Sub.")
            
        except Exception as e:
            logger.error(f"Error during OFAC sync: {e}", exc_info=True)
            
        logger.info("Sleeping for 24 hours before the next OFAC sync...")
        await asyncio.sleep(86400) # 24 hours in seconds

if __name__ == "__main__":
    import asyncio
    asyncio.run(sync_ofac_to_cluster())
