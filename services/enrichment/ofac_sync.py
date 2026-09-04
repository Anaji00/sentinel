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
from shared.utils.sanctions import is_usable_keyword, OFAC_MATCHABLE_TYPES

logger = logging.getLogger("ofac_sync")

OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
OFAC_ALT_URL = "https://www.treasury.gov/ofac/downloads/alt.csv"

async def fetch_ofac_keywords():
    """
    Downloads the official SDN and ALT (aliases) lists from the US Treasury.
    Returns a list of unique names/keywords.
    """
    keywords = set()
    # The SDN uid of every row kept, so aliases can be filtered to the same set.
    # An alias inherits the type of the entry it belongs to, and alt.csv does not
    # carry a type column of its own.
    kept_uids = set()
    skipped_individual = 0
    skipped_short = 0
    logger.info(f"Downloading OFAC SDN from {OFAC_SDN_URL}")

    strip_chars = ' "\''

    async with aiohttp.ClientSession() as session:
        # Download primary names
        async with session.get(OFAC_SDN_URL) as resp:
            if resp.status == 200:
                text = await resp.text()
                # OFAC CSV format: uid, last_name, type, programs, title, vessel_call_sign, vessel_type, vessel_tonnage, grt, vessel_flag, vessel_owner, remarks
                reader = csv.reader(StringIO(text))
                for row in reader:
                    if len(row) <= 1 or not row[1]:
                        continue
                    # row[2] is the SDN type. Individuals are people, and a
                    # person's surname matched against a ship's name is not
                    # evidence about the ship -- that is where "maria", "lily"
                    # and "star" came from.
                    sdn_type = row[2].strip(strip_chars).lower() if len(row) > 2 else ""
                    if sdn_type not in OFAC_MATCHABLE_TYPES:
                        skipped_individual += 1
                        continue
                    name = row[1].strip(strip_chars).lower()
                    # `len(name) > 3` was the only rule here, and it is what let
                    # four-character surnames and word fragments into an
                    # unanchored substring matcher. The length judgement now
                    # lives with the matcher that has to honour it.
                    if not is_usable_keyword(name):
                        skipped_short += 1
                        continue
                    keywords.add(name)
                    if row[0]:
                        kept_uids.add(row[0].strip())
            else:
                logger.error(f"Failed to fetch SDN list: HTTP {resp.status}")

        # Download aliases, restricted to the entries kept above
        async with session.get(OFAC_ALT_URL) as resp:
            if resp.status == 200:
                text = await resp.text()
                # OFAC ALT CSV format: uid, ent_num, alt_type, alt_name, alt_remarks
                reader = csv.reader(StringIO(text))
                for row in reader:
                    if len(row) <= 3 or not row[3]:
                        continue
                    # ent_num ties the alias back to its SDN entry. An alias of
                    # an individual is still an individual.
                    ent_num = row[1].strip() if len(row) > 1 else ""
                    if ent_num and ent_num not in kept_uids:
                        skipped_individual += 1
                        continue
                    name = row[3].strip(strip_chars).lower()
                    if not is_usable_keyword(name):
                        skipped_short += 1
                        continue
                    keywords.add(name)

    logger.info(
        "OFAC sync: %d usable keywords; dropped %d individual-typed and %d too short to identify.",
        len(keywords), skipped_individual, skipped_short,
    )
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
