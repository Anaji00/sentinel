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

# Below this an event is unreachable by every correlation rule.
#
# Kept under the lowest min_anomaly any rule uses (0.20) with margin, so a
# slightly more permissive rule added later still finds its inputs. The
# accompanying test asserts the ordering rather than trusting this comment.
RECENT_WINDOW_MIN_ANOMALY = 0.15

# Members fetched per round trip when scanning the recent window.
#
# Large enough that the scan is a handful of round trips rather than hundreds,
# small enough that the reply Redis has to build stays in the low megabytes
# instead of the 98.7 MB the unbatched read required.
RECENT_WINDOW_SCAN_BATCH = 2000



def _select_diverse_evidence(ranked: list, limit: int) -> list:
    """The top `limit` events, with every matched type represented first.

    Evidence was taken as `sorted_by_anomaly[:10]`, which lets one detector own
    every slot whenever its scores run higher than its neighbours'. That is not
    hypothetical: a rule naming four event types in one clause produced clusters
    whose evidence was entirely flight_anomaly, averaging 0.956, while
    flight_dark averaged 0.632 -- and flight_dark's 10,370 events in 48 hours
    appeared in no cluster at all. The rule matched them; the ranking hid them.

    A cluster's job is to show what co-occurred, so the first pass gives each
    distinct type its strongest example and only then fills the remaining slots
    by score. Within a type the score order is preserved, so the ranking still
    decides which flight_dark event is shown -- it just no longer decides
    whether any is.
    """
    if limit <= 0 or not ranked:
        return []
    if len(ranked) <= limit:
        return ranked

    by_type: dict = {}
    for event in ranked:
        by_type.setdefault(event.get("type") or "unknown", []).append(event)

    selected, seen = [], set()
    # Round-robin across types, strongest first within each, so the slots are
    # shared before any type takes a second one.
    while len(selected) < limit:
        progressed = False
        for bucket in by_type.values():
            if not bucket:
                continue
            event = bucket.pop(0)
            key = id(event)
            if key in seen:
                continue
            seen.add(key)
            selected.append(event)
            progressed = True
            if len(selected) >= limit:
                break
        if not progressed:
            break
    return selected


class EventStore:

    def __init__(self, redis_client, db_client):
        self._redis = redis_client
        self._db = db_client
        self.cache_key = "events:recent_window"
        self.window_seconds = 48 * 3600
          
    async def add_event(self, event: Any):
        """Add a normalized event to the Redis Sliding Window cache."""
        try:
            # Nothing stores what no rule can ask for.
            #
            # This window held 321,535 members and 196MB -- 86% of the whole
            # Redis instance, in one key with no TTL, which under volatile-lru
            # can never be reclaimed. It therefore forced everything else out
            # and writes began failing with "command not allowed when used
            # memory > 'maxmemory'", taking the supervisor's dispatch with them.
            #
            # The lowest min_anomaly any rule requests is 0.20, so an event
            # below that floor is unreachable by every rule in the system and
            # was being kept for forty-eight hours regardless. flight_position
            # averages 0.107 and vessel_static is 0.000, and between them they
            # are thousands of members an hour.
            #
            # The floor sits below the lowest rule threshold with margin, and
            # tests/test_recent_window_floor.py pins that relationship: if a
            # rule is ever written below it, the test fails rather than the
            # rule silently matching nothing.
            if (event.anomaly_score or 0.0) < RECENT_WINDOW_MIN_ANOMALY:
                return

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
                # Truncated at write. Both are read back, so they cannot be
                # dropped, but the consumer already cuts them to 200 and
                # storing them in full multiplied a 321,535-member structure.
                "headline": (event.headline or "")[:160] or None,
                "summary": (getattr(event, "summary", None) or "")[:200] or None,
                "named_entities": event.named_entities,
                "entity_name": (event.primary_entity.name if event.primary_entity and event.primary_entity.name else
                                event.primary_entity.id if event.primary_entity else None),
                "entity_type": (event.primary_entity.type.value if event.primary_entity and hasattr(event.primary_entity.type, 'value') else None),
                "entity_id": (event.primary_entity.id if event.primary_entity else None),
            })
            await self._redis.zadd(self.cache_key, {payload: timestamp})

            # Sliding Window Maintenance
            cutoff = time.time() - self.window_seconds
            await self._redis.raw.zremrangebyscore(self.cache_key, "-inf", cutoff)
        except Exception as e:
            logger.error(f"EventStore.add_event to redis cache failed: {e}")
            

    async def get_recent(
        self,
        event_types: List[str],
        exclude_event_id: str = None,
        hours:       int   = 48,
        region:      str   = None,
        min_anomaly: float = 0.0,
        tags:        List[str] = None,
        limit:       int   = 50,
        entity_id:   Optional[str] = None,
    ) -> List[Dict]:
        """Fetch historical events instantly from RAM instead of Postgres."""

        try:
            cutoff = time.time() - (hours * 3600)
            entity_id = str(entity_id).upper() if entity_id else None

            # Read in batches rather than materialising the whole window.
            #
            # This was a single ZRANGE over the entire 48-hour set: 144,212
            # members, 98.7 MB, fetched and JSON-parsed in full so that Python
            # could filter it down and return at most 50 rows. It runs once per
            # correlation clause, per rule, per event.
            #
            # Redis has to build the whole reply in the client output buffer
            # before sending it, so each call pushed used_memory from 114 MB to
            # 273-287 MB against a 419 MB ceiling. Measured live: a spike every
            # ~55 seconds, and evicted_keys rising by ~2,300 at each one --
            # about 47 keys a second, continuously.
            #
            # What that evicted is the point. events:recent_window carries no
            # TTL, so under volatile-lru it cannot be evicted; everything that
            # *can* be is the small TTL'd keys, which is where the anomaly
            # baselines live. 331 tickers traded through the financial scorers
            # in 24 hours and 4 of them still had a stored mean and variance.
            # A normaliser with no history returns 0 for its first observation,
            # so the financial z-scores were being computed from a baseline that
            # eviction kept resetting.
            #
            # The scan still reads every member -- the caller ranks by anomaly
            # score and takes the top N, so an early exit would silently change
            # which rows come back. Only the peak buffer changes, from one 98 MB
            # reply to a few megabytes at a time.
            results = []
            offset = 0
            while True:
                raw_results = await self._redis.raw.zrange(
                    self.cache_key,
                    "+inf",
                    cutoff,
                    desc=True,
                    byscore=True,
                    offset=offset,
                    num=RECENT_WINDOW_SCAN_BATCH,
                )
                if not raw_results:
                    break
                offset += len(raw_results)
                for raw in raw_results:
                    e = json.loads(raw)
                
                    # FILTER FIX: Translated SQL conditions into native Python checks
                    if exclude_event_id and e["event_id"] == exclude_event_id:
                        continue
                    if min_anomaly > 0 and e["anomaly_score"] < min_anomaly:
                        continue
                    if event_types and e["type"] not in event_types:
                        continue
                    # Same-name correlation, for rules that mean one company.
                    #
                    # "Equity Block & Options Convergence" is a claim about a
                    # block trade and options activity in the same name. Without
                    # this the rule correlated an AAPL block with whatever else
                    # had traded in 48 hours, and published the result headlined
                    # AAPL over supporting evidence reading MTZ, KKR and DELL.
                    if entity_id and str(e.get("entity_id") or "").upper() != entity_id:
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
                
            
                    
            # The SQL query ordered by anomaly_score DESC, then occurred_at DESC.
            # Redis sorted them by occurred_at DESC natively. Now we just sort by anomaly.
            results.sort(key=lambda x: x["anomaly_score"], reverse=True)
            return _select_diverse_evidence(results, limit)
            
        except Exception as e:
            logger.error(f"Redis cache fetch failed: {e}")
            return []

    async def save_correlation(self, cluster) -> None:
        """
        Persist a CorrelationCluster to the correlations table.
        Errors are logged but not re-raised — a failed save doesn't block
        the correlation engine from processing the next event.
        """
        try:
            tier_map = {"WATCH": 1, "ALERT": 2, "ELEVATED": 3, "INTELLIGENCE": 4, "CRITICAL": 5}
            tier_str = cluster.alert_tier.value if hasattr(cluster.alert_tier, 'value') else str(cluster.alert_tier)
            tier_int = tier_map.get(str(tier_str).upper(), 2)

            # Eight fields the cluster is published with were not named here,
            # so they were dropped in silence on every row. Nothing reading a
            # correlation from the database could see its confidence, its
            # domain, how many events supported it, or the headline the operator
            # was shown -- the writer names the columns it knows about, and the
            # ones it does not raise nothing.
            metrics = getattr(cluster, "metrics_summary", None)
            await self._db.execute("""
                INSERT INTO correlations (
                    correlation_id, rule_id, rule_name, alert_tier,
                    detected_at, trigger_event_id, supporting_event_ids,
                    entity_ids, description, tags,
                    confidence_score, primary_domain, summary_headline,
                    supporting_headlines, metrics_summary,
                    primary_entity_id, primary_entity_name, entity_names
                ) VALUES ($1::uuid, $2, $3, $4, $5, $6::uuid, $7::uuid[], $8, $9, $10,
                          $11, $12, $13, $14, $15::jsonb, $16, $17, $18)
            """, 
                cluster.correlation_id,
                cluster.rule_id,
                cluster.rule_name,
                tier_int,
                cluster.detected_at,
                cluster.trigger_event_id,
                cluster.supporting_event_ids,
                cluster.entity_ids,
                cluster.description,
                cluster.tags,
                getattr(cluster, "confidence_score", None),
                getattr(cluster, "primary_domain", None),
                getattr(cluster, "summary_headline", None),
                getattr(cluster, "supporting_headlines", None),
                # Passed as a mapping, not a pre-serialised string: the pool
                # registers a jsonb codec whose encoder is already json.dumps,
                # so serialising here would store a jsonb *string* rather than
                # the object -- the double-encoding defect repaired elsewhere in
                # this audit, which made half the scenario corpus unqueryable.
                metrics if isinstance(metrics, dict) else None,
                getattr(cluster, "primary_entity_id", None),
                getattr(cluster, "primary_entity_name", None),
                getattr(cluster, "entity_names", None),
            )
            logger.info(f"💾 Persisted correlation {cluster.correlation_id} to TimescaleDB.")
        except Exception as e:
            logger.error(f"save_correlation failed ({cluster.correlation_id}): {e}")