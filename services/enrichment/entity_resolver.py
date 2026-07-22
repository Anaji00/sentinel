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
import time
from typing import Optional, Dict

from shared.utils.sanctions import check_sanctions, mmsi_to_country

logger = logging.getLogger("enrichment.resolver")


class AsyncTTLCache:
    """In-memory Level 0 RAM cache for entity resolution (nanoseconds)."""
    def __init__(self, maxsize: int = 5000, ttl: int = 300):
        self.maxsize = maxsize
        self.ttl = ttl
        self._cache: Dict[str, tuple] = {}

    def get(self, key: str) -> Optional[Dict]:
        if key in self._cache:
            val, expire = self._cache[key]
            if time.time() < expire:
                return val
            del self._cache[key]
        return None

    def set(self, key: str, value: Dict):
        if len(self._cache) >= self.maxsize:
            # Evict 20% oldest entries
            oldest = sorted(self._cache.items(), key=lambda x: x[1][1])[:self.maxsize // 5]
            for k, _ in oldest:
                self._cache.pop(k, None)
        self._cache[key] = (value, time.time() + self.ttl)


class EntityResolver:
    """
    Central dictionary for the system.
    Input:  ID (MMSI, ICAO24, Ticker, IP)
    Output: Dict (Name, Type, Owner, Flags)
    """

    def __init__(self, redis_client, neo4j_client):
        self.redis = redis_client
        self.neo4j = neo4j_client
        self._mem_cache = AsyncTTLCache(maxsize=5000, ttl=300)

    # ── Vessel ────────────────────────────────────────────────────────────────

    async def resolve_vessel(self, mmsi: str, ais_meta: dict = None) -> Dict:
        """
        Finds out who a vessel is based on its MMSI number.
        """
        # ── LEVEL 0: RAM CACHE (Nanoseconds) ──────────────────────────────────
        mem_cached = self._mem_cache.get(f"vessel:{mmsi}")
        if mem_cached:
            return mem_cached
        # ── LEVEL 1: REDIS (Hot Cache) ────────────────────────────────────────
        cached = await self.redis.raw.get(f"vessel:info:{mmsi}")
        if cached:
            data = json.loads(cached)
            self._mem_cache.set(f"vessel:{mmsi}", data)
            return data

        # ── LEVEL 2: NEO4J (The Graph) ────────────────────────────────────────
        try:
            cypher = "MATCH (v:Vessel {mmsi: $mmsi}) RETURN v.name as name, v.vessel_type as vessel_type, v.flags as flags, v.flag_state as flag_state, v.watchlist_tags as watchlist_tags"
            records = await self.neo4j.execute_and_fetch(cypher, {"mmsi": mmsi})
            if records:
                data = dict(records[0])
                await self.redis.raw.set(f"vessel:info:{mmsi}", json.dumps(data), ex=86400)
                self._mem_cache.set(f"vessel:{mmsi}", data)
                return data
        except Exception as e:
            logger.debug(f"Neo4j vessel lookup failed ({mmsi}): {e}")

        # ── LEVEL 3: INFERENCE (Fallback) ─────────────────────────────────────
        meta  = ais_meta or {}
        name  = str(meta.get("ShipName", "")).strip()
        data  = {
            "name":        name,
            "vessel_type": "Unknown",
            "flag_state":  mmsi_to_country(mmsi),
            "flags":       check_sanctions(name, mmsi),
        }
        
        await self.redis.raw.set(f"vessel:info:{mmsi}", json.dumps(data), ex=3600)
        self._mem_cache.set(f"vessel:{mmsi}", data)
        return data

    async def resolve_vessel_batch(self, mmsi_list: list, ais_meta_list: list) -> list:
        """
        Batch resolves vessel identities using Redis pipelining and Neo4j batch queries.
        """
        if not mmsi_list:
            return []
            
        # 1. Redis Pipeline
        pipe = self.redis.raw.pipeline()
        for mmsi in mmsi_list:
            pipe.get(f"vessel:info:{mmsi}")
        
        try:
            cached_results = await pipe.execute()
        except Exception as e:
            logger.error(f"Redis pipeline failed in resolve_vessel_batch: {e}")
            cached_results = [None] * len(mmsi_list)
        
        results = [None] * len(mmsi_list)
        missing_mmsis = []
        missing_indices = []
        
        for i, (mmsi, cached) in enumerate(zip(mmsi_list, cached_results)):
            if cached:
                results[i] = json.loads(cached)
            else:
                missing_mmsis.append(mmsi)
                missing_indices.append(i)
                
        if not missing_mmsis:
            return results
            
        # 2. Neo4j Batch Lookup
        found_in_neo4j = {}
        try:
            cypher = "MATCH (v:Vessel) WHERE v.mmsi IN $mmsis RETURN v.mmsi as mmsi, v.name as name, v.vessel_type as vessel_type, v.flags as flags, v.flag_state as flag_state, v.watchlist_tags as watchlist_tags"
            records = await self.neo4j.execute_and_fetch(cypher, {"mmsis": missing_mmsis})
            
            if records:
                for r in records:
                    found_in_neo4j[r["mmsi"]] = dict(r)
        except Exception as e:
            logger.debug(f"Neo4j vessel batch lookup failed: {e}")

        # 3. Process Neo4j hits and Fallbacks
        set_pipe = self.redis.raw.pipeline()
        for i, mmsi in zip(missing_indices, missing_mmsis):
            if mmsi in found_in_neo4j:
                data = found_in_neo4j[mmsi]
                results[i] = data
                set_pipe.set(f"vessel:info:{mmsi}", json.dumps(data), ex=86400)
            else:
                meta = ais_meta_list[i] or {}
                name = str(meta.get("ShipName", "")).strip()
                data = {
                    "name":        name,
                    "vessel_type": "Unknown",
                    "flag_state":  mmsi_to_country(mmsi),
                    "flags":       check_sanctions(name, mmsi),
                }
                results[i] = data
                set_pipe.set(f"vessel:info:{mmsi}", json.dumps(data), ex=3600)
                
        try:
            if len(set_pipe.command_stack) > 0:
                await set_pipe.execute()
        except Exception as e:
            logger.error(f"Redis pipeline set failed in resolve_vessel_batch: {e}")
            
        return results

    # ── Aircraft ──────────────────────────────────────────────────────────────

    async def resolve_aircraft(self, icao24: str) -> Dict:
        """Asynchronously resolves aircraft identity using cascading cache strategies."""
        
        # 1. REDIS (Hot Cache)
        cached = await self.redis.raw.get(f"aircraft:info:{icao24}")
        if cached:
            return json.loads(cached)
            
        # 2. NEO4J (The Graph)
        try:
            cypher = """
                MATCH (a:Aircraft {icao24: $id}) 
                RETURN a.callsign as callsign, a.origin_country as origin_country
            """
            records = await self.neo4j.execute_and_fetch(cypher, {"id": icao24})
            
            if records:
                data = dict(records[0])
                # Await the write to cache, mapping 'ex' for seconds
                await self.redis.raw.set(f"aircraft:info:{icao24}", json.dumps(data), ex=86400)
                return data
        except Exception as e:
            logger.debug(f"Neo4j async aircraft lookup failed ({icao24}): {e}")
            
        # 3. No Fallback
        # ADS-B vectors often lack contextual static payload data. Fail cleanly.
        return {}