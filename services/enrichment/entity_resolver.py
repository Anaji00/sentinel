"""
services/enrichment/entity_resolver.py

THE DETECTIVE
=============
Resolves boring ID numbers (MMSI: 123456789) into rich profiles ("Vessel: Titanic, Flag: UK").
 
The "Waterfall" Lookup Strategy:
  1. FAST (Redis): "Have we seen this ID recently?" (Microseconds).
  2. DEEP (Neo4j): "Do we have a file on this ID in our database?" (Milliseconds).
  3. FALLBACK (Inference): "Never seen it, but let's guess based on the raw signal."
  
Why this matters:
  We process thousands of events per second. We cannot query the graph database (Neo4j)
  for every single one—it would crash. Redis acts as a "shock absorber."
 
FIX (code review): MMSI_COUNTRY and check_sanctions() now imported from
  shared/utils/sanctions.py instead of being re-defined here. Removes the
  duplicate that would drift from the maritime enricher copy.

Phase 2 additions:
  - Equasis API  (vessel ownership chain, IMO lookup)
  - OFAC SDN list sync (full sanctions matching)
"""

import json
import logging
from typing import Optional, Dict

from shared.utils.sanctions import check_sanctions, mmsi_to_country

logger = logging.getLogger("enrichment.resolver")


class EntityResolver:
    """
    Central dictionary for the system.
    Input:  ID (MMSI, ICAO24)
    Output: Dict (Name, Type, Owner, Flags)
    """

    def __init__(self, redis_client, neo4j_client):
        self.redis = redis_client
        self.neo4j = neo4j_client

    # ── Vessel ────────────────────────────────────────────────────────────────

    def resolve_vessel(self, mmsi: str, ais_meta: dict = None) -> Dict:
        """
        Finds out who a vessel is based on its MMSI number.
        """
        # ── LEVEL 1: REDIS (Hot Cache) ────────────────────────────────────────
        # Check if we looked this up in the last 24 hours.
        # If yes, return immediately. This handles 99% of traffic.
        cached = self.redis.get(f"vessel:info:{mmsi}")
        if cached:
            return json.loads(cached)

        # ── LEVEL 2: NEO4J (The Graph) ────────────────────────────────────────
        # If not in cache, ask the Graph Database.
        # This is where we store "Permanent" knowledge (Ownership, past sanctions).
        try:
            result = self.neo4j.query_one(
                "MATCH (v:Vessel {mmsi: $mmsi}) RETURN v", {"mmsi": mmsi}
            )
            if result and result.get("v"):
                data = dict(result["v"])
                
                # CACHE UPDATE:
                # We found it in the graph! Save it to Redis for 24 hours so we
                # don't have to ask Neo4j again soon.
                self.redis.set(f"vessel:info:{mmsi}", json.dumps(data), ttl=86400)
                return data
        except Exception as e:
            # If the database fails, log it but don't crash. Fall back to inference.
            logger.debug(f"Neo4j vessel lookup failed ({mmsi}): {e}")

        # ── LEVEL 3: INFERENCE (Fallback) ─────────────────────────────────────
        # We know nothing about this ship in our DB.
        # But the AIS signal itself contains some raw text (e.g., "SHIPNAME: BOaty McBoatface").
        # We use that + the MMSI country code to build a temporary profile.
        meta  = ais_meta or {}
        name  = meta.get("ShipName", "")
        data  = {
            "name":        name,
            "vessel_type": "Unknown",
            "flag_state":  mmsi_to_country(mmsi),       # e.g., "235" -> "GB"
            "flags":       check_sanctions(name, mmsi), # Check name against blacklist
        }
        
        # CACHE UPDATE (Short Term):
        # Save this "Best Guess" profile for 1 hour.
        # Why only 1 hour? Because a real analyst might add the ship to Neo4j soon,
        # and we want to pick up the "Real" data when it becomes available.
        self.redis.set(f"vessel:info:{mmsi}", json.dumps(data), ttl=3600)
        return data

    # ── Aircraft ──────────────────────────────────────────────────────────────

    def resolve_aircraft(self, icao24: str) -> Dict:
        # Same 3-step logic as vessels, but for planes.
        
        # 1. Redis
        cached = self.redis.get(f"aircraft:info:{icao24}")
        if cached:
            return json.loads(cached)
            
        # 2. Neo4j
        try:
            result = self.neo4j.query_one(
                "MATCH (a:Aircraft {icao24: $id}) RETURN a", {"id": icao24}
            )
            if result and result.get("a"):
                data = dict(result["a"])
                self.redis.set(f"aircraft:info:{icao24}", json.dumps(data), ttl=86400)
                return data
        except Exception as e:
            logger.debug(f"Neo4j aircraft lookup failed ({icao24}): {e}")
            
        # 3. No Fallback?
        # Unlike AIS, ADS-B (aviation) signals often don't contain the airline name
        # or model in every packet. If we don't know the plane, we return empty.
        return {}