"""
services/enrichment/ofac_sync.py

Standalone script to fetch OFAC SDN sanctions list, parse it, and broadcast
updates to the rest of the Sentinel cluster via Redis Pub/Sub.

This implements the Control Plane for the In-Memory Aho-Corasick Automaton.
"""

import asyncio
import json
import logging
import csv
from datetime import datetime, timezone
from io import StringIO

# We use the shared Redis client connection logic
from shared.db import get_redis

logging.basicConfig(level=logging.INFO)
from shared.utils import sanctions
from shared.utils.quiet_failures import dropped, swallowed
from shared.utils.sanctions import (
    OFAC_MATCHABLE_TYPES, SANCTIONED_KEYWORDS, build_vessel_index, is_usable_keyword, sync_is_plausible,
)
from shared.utils.http_client import guarded_request

logger = logging.getLogger("ofac_sync")

SANCTIONS_KEY = "sentinel:config:sanctions"
OFAC_SDN_URL = "https://www.treasury.gov/ofac/downloads/sdn.csv"
OFAC_ALT_URL = "https://www.treasury.gov/ofac/downloads/alt.csv"

async def fetch_ofac_keywords():
    """The usable names and aliases on Treasury's lists."""
    keywords, _ = await fetch_ofac_lists()
    return keywords


async def fetch_ofac_lists():
    """
    Downloads the official SDN and ALT (aliases) lists from the US Treasury.
    Returns (unique names/keywords, vessel index), or ([], {}) unless both arrive.
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

    # Behind the circuit breaker.
    #
    # `shared/utils/http_client.py` wraps outbound HTTP in the breaker, and it
    # had zero importers anywhere in the tree -- every external dependency the
    # platform has called out with raw aiohttp, so the live breaker registry
    # read `{}` and an empty object in a health payload reads as "all circuits
    # healthy" rather than "there are no circuits". OFAC is a good example of
    # why it matters: a slow or failing sanctions endpoint retried on every
    # sweep with nothing between it and the network.
    text = await guarded_request(
        "GET", OFAC_SDN_URL,
        source="ofac", service="enrichment",
        expect_json=False, timeout=60.0,
    )
    if text is not None:
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
        logger.error("Failed to fetch the OFAC SDN list; keeping the previous keyword set.")
        return [], {}

    sdn_text = text

    # Aliases, restricted to the entries kept above, behind the same breaker.
    alt_text = await guarded_request(
        "GET", OFAC_ALT_URL,
        source="ofac", service="enrichment",
        expect_json=False, timeout=60.0,
    )
    if alt_text is not None:
        text = alt_text
        # OFAC ALT CSV format: ent_num, alt_num, alt_type, alt_name, alt_remarks.
        # The owner is column 0. Column 1 is the alias's own number, and reading
        # it as the owner dropped 9,794 company and vessel aliases and let in
        # 1,214 aliases of people (measured on the 4 Nov 2026 file).
        reader = csv.reader(StringIO(text))
        for row in reader:
            if len(row) <= 3 or not row[3]:
                continue
            # ent_num ties the alias back to its SDN entry. An alias of
            # an individual is still an individual.
            ent_num = row[0].strip()
            if ent_num not in kept_uids:
                skipped_individual += 1
                continue
            name = row[3].strip(strip_chars).lower()
            if not is_usable_keyword(name):
                skipped_short += 1
                continue
            keywords.add(name)
    else:
        # All or nothing: names without their aliases would silently replace a
        # complete list with a partial one.
        logger.error("Failed to fetch the OFAC alias list; keeping the previous keyword set.")
        return [], {}

    logger.info(
        "OFAC sync: %d usable keywords; dropped %d individual-typed and %d too short to identify.",
        len(keywords), skipped_individual, skipped_short,
    )
    vessels = build_vessel_index(sdn_text, alt_text)
    logger.info("OFAC vessels: %d by IMO, %d by MMSI, %d short names.",
                len(vessels["imo"]), len(vessels["mmsi"]), len(vessels["names"]))
    return list(keywords), vessels

async def sync_once() -> bool:
    """One sync. True when a current list is in force, False to try again soon.

    A failure must not touch the list in force. It used to rebuild from the 31
    built-in terms, so one Treasury timeout dropped 13,000 names for up to a day
    while the log said the previous set was being kept. A process with only the
    built-in terms loads the last good list from Redis instead.
    """
    try:
        fetched, vessels = await fetch_ofac_lists()
    except Exception as exc:
        swallowed("enrichment.ofac_sync.fetch", exc, logger)
        fetched, vessels = [], {}
    redis = await get_redis()
    if not sync_is_plausible(len(fetched), len(sanctions._active_keywords)):
        dropped("enrichment.ofac_sync", "implausible_download", logger, detail=str(len(fetched)))
        if len(sanctions._active_keywords) <= len(SANCTIONED_KEYWORDS):
            try:
                blob = await redis.raw.get(SANCTIONS_KEY)
                saved = json.loads(blob) if blob else {}
                stored = saved.get("keywords")
                if stored:
                    sanctions.rebuild_sanctions_from_list(stored)
                    sanctions.set_vessel_index(saved.get("vessels"))
                    logger.warning("OFAC download failed; loaded %d stored keywords.", len(stored))
            except Exception as exc:
                swallowed("enrichment.ofac_sync.restore", exc, logger)
        return False
    all_keywords = list(set(SANCTIONED_KEYWORDS + fetched))
    sanctions.rebuild_sanctions_from_list(all_keywords)
    sanctions.set_vessel_index(vessels)
    logger.info("OFAC sanctions sync complete. Total entities in automaton: %d", len(all_keywords))
    try:
        await redis.raw.set(SANCTIONS_KEY, json.dumps({
            "keywords": all_keywords, "vessels": vessels, "synced_at": datetime.now(timezone.utc).isoformat(),
        }))
        await redis.raw.publish("sentinel:config:updates", "ofac_rebuild")
    except Exception as exc:
        swallowed("enrichment.ofac_sync.store", exc, logger)
    return True


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
