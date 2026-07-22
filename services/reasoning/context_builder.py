"""
services/reasoning/context_builder.py
 
Assembles the full context package sent to Claude for scenario generation.
 
Takes a CorrelationCluster and fetches everything needed:
  - The trigger event and all supporting events (from TimescaleDB)
  - Entity ownership chains (from Neo4j)
  - Historical pattern matches (from pattern_library)
  - Recent relevant headlines
 
Output is a structured dict that scenario_generator.py passes to Claude.
"""

import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
 
from shared.db import get_timescale, get_neo4j
from shared.models import CorrelationCluster
 
logger = logging.getLogger("reasoning.context")

class ContextBuilder:
    def __init__(self, db_client):
        # Timescale is a thread-pooled async client now
        self._db = db_client

    # Converted core build orchestrator to async
    async def build(self, cluster: CorrelationCluster) -> Dict[str, Any]:
        """Build the full context dict for a correlation cluster."""
        # Await async database calls directly
        trigger, supporting, recent_news = await asyncio.gather(
            self._fetch_event(cluster.trigger_event_id),
            self._fetch_events(cluster.supporting_event_ids),
            self._fetch_recent_news(cluster)
        )
        
        # Natively await asynchronous Redis/Neo4j graph calls
        entity_graph = await self._fetch_entity_graph(cluster.entity_ids)
        agent_intel = await self._fetch_agent_intel(cluster)
        
        pattern_matches = self._fetch_pattern_matches(cluster) 
        
        return {
            "correlation": {
                "id": cluster.correlation_id,
                "rule": cluster.rule_name,
                "tier": cluster.alert_tier.value,
                "detected_at": cluster.detected_at.isoformat(),
                "description": cluster.description,
                "tags": cluster.tags,
            },
            "trigger_event": trigger,
            "supporting_events": supporting,
            "entity_graph": entity_graph,
            "historical_patterns": pattern_matches, 
            "recent_headlines": recent_news,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "agent_intel_briefs": agent_intel,
        }

    async def _fetch_event(self, event_id: str) -> Optional[Dict]:
        try:
            # CRITICAL THINKING: Parameterized Queries.
            # We use `$1` for asyncpg parameterized SQL queries to prevent SQL Injection 
            # attacks, even though these event_ids are generated internally.
            row = await self._db.query_one(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, prediction_market_data, crypto_data, cyber_data, tags
                   FROM events WHERE event_id = $1""",
                event_id
            )
            return self._serialize_row(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching event {event_id}: {e}")
            return None
        
    async def _fetch_events(self, event_ids: List[str]) -> List[Dict]:
        if not event_ids:
            return []
        try:
            # CRITICAL THINKING: Batch DB Queries.
            # Instead of looping and running `_fetch_event` N times (which creates an N+1 
            # query bottleneck), we use PostgreSQL's `ANY($1)` array operator. 
            # This fetches all supporting events in a single, fast database round-trip.
            rows = await self._db.query(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, prediction_market_data, crypto_data, cyber_data, tags
                   FROM events
                   WHERE event_id = ANY($1::text[])
                   ORDER BY occurred_at DESC""",
                event_ids
            )
            return [self._serialize_row(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching events {event_ids}: {e}")
            return []
        
    async def _fetch_entity_graph(self, entity_ids: List[str]) -> List[Dict]:
        """
        Batches and parallelizes Neo4j graph lookup for entities.
        Fetches up to 3 hops of relationships and active flags in just 2 parallel database round-trips.
        """
        if not entity_ids:
            return []
        
        targets = list(set(entity_ids[:5]))
        neo4j_client = await get_neo4j()
        
        try:
            rel_task = neo4j_client.query("""
                MATCH (v) WHERE v.name IN $ids OR v.mmsi IN $ids
                MATCH (v)-[r*1..3]->(n)
                RETURN coalesce(v.name, v.mmsi) as entity_id, type(r[0]) as rel, coalesce(n.name, n.mmsi) as connected,
                       labels(n) as labels
                LIMIT 100
            """, {"ids": targets})
            
            flag_task = neo4j_client.query("""
                MATCH (v) WHERE v.name IN $ids OR v.mmsi IN $ids
                MATCH (v)-[:FLAGGED_AS]->(f:Flag)
                RETURN coalesce(v.name, v.mmsi) as entity_id, f.type as flag
                LIMIT 50
            """, {"ids": targets})
            
            rel_rows, flag_rows = await asyncio.gather(rel_task, flag_task)
            
            results = []
            
            rel_by_entity = {}
            for r in (rel_rows or []):
                ent = r["entity_id"]
                rel_by_entity.setdefault(ent, []).append(r)
                
            for ent, relationships in rel_by_entity.items():
                results.append({"entity_id": ent, "relationships": relationships})
                
            flags_by_entity = {}
            for r in (flag_rows or []):
                ent = r["entity_id"]
                flags_by_entity.setdefault(ent, []).append(r["flag"])
                
            for ent, flags in flags_by_entity.items():
                results.append({"entity_id": ent, "flags": flags})
                
            return results
        except Exception as e:
            logger.debug(f"Error fetching batch graph for entities {targets}: {e}")
            return []
    
    async def _fetch_recent_news(self, cluster: CorrelationCluster) -> List[str]:
        """Fetch recent high-anomaly headlines related to the correlation."""
        try:
            # CRITICAL THINKING: Time-Bounding.
            # We only want news from 72 hours *before* the cluster was detected. 
            # Anything older is likely irrelevant noise to the LLM.
            cutoff = cluster.detected_at - timedelta(hours=72)
            rows = await self._db.query(
                """SELECT headline FROM events
                   WHERE type = 'headline'
                     AND anomaly_score >= 0.4
                     AND occurred_at BETWEEN $1 AND $2
                   ORDER BY anomaly_score DESC
                   LIMIT 10""",
                cutoff, cluster.detected_at
            )
            return [r["headline"] for r in rows if r.get("headline")]
        except Exception as e:
            logger.error(f"Error fetching recent news: {e}")
            return []
        

    def _fetch_pattern_matches(self, cluster: CorrelationCluster) -> List[Dict]:
        """
        Stub — returns empty list.
 
        Pattern matching is handled by PatternLibrary.find_similar() in
        reasoning/main.py, which passes results directly to
        scenario_generator.generate(). If you ever want the context
        builder to bundle patterns itself (e.g. for a standalone call),
        inject a PatternLibrary instance and call it here.
 
        TODO Phase 2: wire in PatternLibrary if the calling pattern changes.
        """
        return []
 
        
    def _serialize_row(self, row: Dict) -> Dict:
        """
        Make a DB row JSON-safe (convert datetime, parse JSONB).
        
        CODING CONVICTION: Data Normalization at the Edge.
        Python standard `json.dumps()` cannot handle raw `datetime` objects. 
        Since this data is destined for an LLM API (which requires JSON), we must 
        proactively stringify dates using ISO-8601 format to avoid runtime crashes 
        in the `scenario_generator.py` API call.
        """
        out = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                out[k] = v.isoformat()
            elif isinstance(v, (dict, list)):
                out[k] = v
            else:
                out[k] = v
        return out

    async def _fetch_agent_intel(self, cluster: CorrelationCluster) -> list:
        """
        Fetch recent agent-generated intel briefs from Redis.
        These are richer than raw news — already structured by the Intel Agent.
        """
        try:
            from shared.db import get_redis
            redis = await get_redis()
            
            # Fetch recent high-severity briefs
            raw = await redis.raw.get("sentinel:intel:briefs:latest")
            if raw:
                brief = json.loads(raw)
                # Check if thematically related to this cluster
                cluster_tags = set(cluster.tags)
                brief_hotspots = set(brief.get("geographic_hotspots", []))
                if cluster_tags.intersection(brief_hotspots):
                    return [brief]
        except Exception as e:
            logger.debug(f"Agent intel fetch failed: {e}")
        return []