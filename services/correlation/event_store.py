"""
services/correlation/event_store.py

TimescaleDB query interface used by all correlation rules.
Rules call get_recent() to ask "what events exist in this domain, region,
and time window?" — this translates those questions into SQL.

All queries are parameterized. The only f-string interpolation is the safe
`LIMIT {int(limit)}` cast and the `AND`-join of hardcoded condition strings.
"""

import time
import json
import logging
from typing import List, Dict, Optional, Any

from shared.db import get_timescale

logger = logging.getLogger("correlation.store")


class EventStore:

    def __init__(self, redis_client):
        self._redis = redis_client
        self._db = get_timescale()
        self.cache_key = "events:recent_window"
        self.window_seconds = 48 * 3600
          
    def add_event(self, event: Any):
        """Add a normalized event to the Redis Sliding Window cache."""
        try:
            timestamp = event.occurred_at.timestamp()
            payload = json.dumps({
                "event_id": event.event_id,
                "type": event.type.value,
                "domain": event.type.value.split("_")[0],
                "anomaly_score": event.anomaly_score,
                "tags": event.tags,
                "region": event.region,
                "latitude": event.latitude,
                "longitude": event.longitude,
                "headline": event.headline,
                "named_entities": event.named_entities,
            })
            self._redis.zadd(self.cache_key, {payload: timestamp})

            # Sliding Window Maintenance
            cutoff = time.time() - self.window_seconds
            self._redis.raw.zremrangebyscore(self.cache_key, "-inf", cutoff)
        except Exception as e:
            logger.error(f"EventStore.add_event to redis cache failed: {e}")
            

    def get_recent(
        self,
        event_types: List[str],
        exclude_event_id: str = None,
        hours:       int   = 48,
        region:      str   = None,
        min_anomaly: float = 0.0,
        tags:        List[str] = None,
        limit:       int   = 50,
    ) -> List[Dict]:
        """Fetch historical events instantly from RAM instead of Postgres."""

        try:
            cutoff = time.time() - (hours * 3600)
            
            # Fetch events from 'now' down to the 'cutoff' timestamp, ordered newest to oldest
            raw_results = self._redis.zrange(
                self.cache_key, 
                "+inf", 
                cutoff,
                desc = True,
                byscore=True
            )
            
            results = []
            for raw in raw_results:
                e = json.loads(raw)
                
                # FILTER FIX: Translated SQL conditions into native Python checks
                if exclude_event_id and e["event_id"] == exclude_event_id:
                    continue
                if min_anomaly > 0 and e["anomaly_score"] < min_anomaly:
                    continue
                if event_types and e["type"] not in event_types:
                    continue
                if region and e.get("region") != region:
                    continue
                if tags:
                    # Python equivalent of PostgreSQL's "tags && %s" (array overlap check)
                    # Returns True if ANY tag in the required 'tags' list exists in the event's tags.
                    event_tags = e.get("tags") or []
                    if not any(t in event_tags for t in tags):
                        continue
                
                results.append(e)
                
                # Enforce the row limit
                if len(results) >= limit:
                    break
                    
            # The SQL query ordered by anomaly_score DESC, then occurred_at DESC.
            # Redis sorted them by occurred_at DESC natively. Now we just sort by anomaly.
            results.sort(key=lambda x: x["anomaly_score"], reverse=True)
            return results
            
        except Exception as e:
            logger.error(f"Redis cache fetch failed: {e}")
            return []

    def save_correlation(self, cluster) -> None:
        """
        Persist a CorrelationCluster to the correlations table.
        Errors are logged but not re-raised — a failed save doesn't block
        the correlation engine from processing the next event.
        """
        try:
            self._db.execute("""
                INSERT INTO correlations (
                    correlation_id, rule_id, rule_name, alert_tier,
                    detected_at, trigger_event_id, supporting_event_ids,
                    entity_ids, description, tags
                ) VALUES (%s::uuid,%s,%s,%s,%s,%s::uuid,%s::uuid[],%s,%s,%s)
            """, (
                cluster.correlation_id,
                cluster.rule_id,
                cluster.rule_name,
                cluster.alert_tier.value,
                cluster.detected_at,
                cluster.trigger_event_id,
                cluster.supporting_event_ids,
                cluster.entity_ids,
                cluster.description,
                cluster.tags,
            ))
            logger.info(f"💾 Persisted correlation {cluster.correlation_id} to TimescaleDB.")
        except Exception as e:
            logger.error(f"save_correlation failed ({cluster.correlation_id}): {e}")