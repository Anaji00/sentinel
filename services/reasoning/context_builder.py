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
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
 
from shared.db import get_timescale, get_neo4j
from shared.models import CorrelationCluster
 
logger = logging.getLogger("reasoning.context")

class ContextBuilder:
    def __init__(self):
        self._db = get_timescale()
        self._neo4j = get_neo4j()

    def build(self, cluster: CorrelationCluster) -> Dict[str, Any]:
        """
        Build the full context dict for a correlation cluster.
        This is what gets passed to Gemini.

        CODING CONVICTION: Aggregator Pattern.
        An aggregator pattern is a design approach where a single class or function 
        is responsible for gathering and assembling data from multiple sources into a cohesive structure.
        This class acts as a central aggregator so the LLM generation logic doesn't 
        need to know *how* to query PostgreSQL or Neo4j. It just asks for the "context package".
        """
        trigger = self._fetch_event(cluster.trigger_event_id) # Fetch the trigger event details
        supporting = self._fetch_events(cluster.supporting_event_ids) # Fetch details for all supporting events
        entity_graph = self._fetch_entity_graph(cluster.entity_ids) # Fetch ownership chains for all involved entities
        pattern_matches = self._fetch_pattern_matches(cluster) # Fetch historical pattern matches relevant to this
        recent_news = self._fetch_recent_news(cluster) # Fetch recent headlines related to the entities/events in this cluster

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
            "historical_patterns": pattern_matches, # Passed the missing fetched patterns into the final dict
            "recent_headlines": recent_news,
            # FIXED: Typo 'analysis_timestammp' changed to 'analysis_timestamp'.
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _fetch_event(self, event_id: str) -> Optional[Dict]:
        try:
            # CRITICAL THINKING: Parameterized Queries.
            # We use `%s` instead of f-strings for SQL queries to prevent SQL Injection 
            # attacks, even though these event_ids are generated internally.
            row = self._db.query_one(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, tags
                   FROM events WHERE event_id = %s""",
                (event_id,)
            )
            return self._serialize_row(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching event {event_id}: {e}")
            return None
        
    def _fetch_events(self, event_ids: List[str]) -> List[Dict]:
        if not event_ids:
            return []
        try:
            # CRITICAL THINKING: Batch DB Queries.
            # Instead of looping and running `_fetch_event` N times (which creates an N+1 
            # query bottleneck), we use PostgreSQL's `ANY(%s)` array operator. 
            # This fetches all supporting events in a single, fast database round-trip.
            rows = self._db.query(
                """SELECT event_id, type, occurred_at, source, region,
                          primary_entity_id, primary_entity_name, primary_entity_flags,
                          headline, summary, anomaly_score,
                          vessel_data, flight_data, financial_data, tags
                   FROM events
                   WHERE event_id = ANY(%s)
                   ORDER BY occurred_at DESC""",
                (event_ids,)
            )
            return [self._serialize_row(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching events {event_ids}: {e}")
            return []
        
    def _fetch_entity_graph(self, entity_ids: List[str]) -> List[Dict]:
        """
        For each entity, get up to 3 hops of relationships from Neo4j.
        Returns ownership chains, flag connections, etc.
        """
        if not entity_ids:
            return []
        results = []
        
        # CRITICAL THINKING: Token Limit & Performance Protection.
        # We slice `entity_ids[:5]` to avoid blowing up the LLM Context Window or 
        # crushing the Neo4j graph database. Graph traversal grows exponentially 
        # (1 node -> 10 nodes -> 100 nodes). Limiting to 5 entities ensures stability.
        for entity_id in entity_ids[:5]:  # Limit to first 5 entities to avoid overload
            try:
                # Query breakdown: `-[r*1..3]-` tells Neo4j to find paths between 1 and 3 hops away.
                # This reveals hidden links (e.g., Vessel -> owned_by -> Shell Company -> owns -> Other Vessel).
                # `LIMIT 20` prevents a massive network of thousands of nodes from returning.
                rows = self._neo4j.query("""
                    MATCH (v:Vessel {mmsi: $id})-[r*1..3]-(n)
                    RETURN v.name as entity, type(r[0]) as rel, n.name as connected,
                           labels(n) as labels
                    LIMIT 20
                """, {"id": entity_id})
                if rows:
                    results.append({"entity_id": entity_id, "relationships": rows})

                flags = self._neo4j.query("""
                    MATCH (v:Vessel {mmsi: $id})-[:FLAGGED_AS]->(f:Flag)
                    RETURN f.type as flag
                """, {"id": entity_id})
                if flags:
                    flag_list = [r["flag"] for r in flags]
                    results.append({"entity_id": entity_id, "flags": flag_list})
            except Exception as e:
                logger.debug(f"Error fetching graph for entity {entity_id}: {e}")
        return results
    
    def _fetch_recent_news(self, cluster: CorrelationCluster) -> List[str]:
        """Fetch recent high-anomaly headlines related to the correlation."""
        try:
            # CRITICAL THINKING: Time-Bounding.
            # We only want news from 72 hours *before* the cluster was detected. 
            # Anything older is likely irrelevant noise to the LLM.
            cutoff = cluster.detected_at - timedelta(hours=72)
            rows = self._db.query(
                """SELECT headline FROM events
                   WHERE type = 'headline'
                     AND anomaly_score >= 0.4
                     AND occurred_at BETWEEN %s AND %s
                   ORDER BY anomaly_score DESC
                   -- Capped at 10 to preserve LLM token context budget
                   LIMIT 10""",
                (cutoff, cluster.detected_at)
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