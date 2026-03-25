"""
services/correlation/event_store.py

TimescaleDB query interface used by all correlation rules.
Rules call get_recent() to ask "what events exist in this domain, region,
and time window?" — this translates those questions into SQL.

All queries are parameterized. The only f-string interpolation is the safe
`LIMIT {int(limit)}` cast and the `AND`-join of hardcoded condition strings.
"""

import logging
from typing import List, Dict, Optional

from shared.db import get_timescale

logger = logging.getLogger("correlation.store")


class EventStore:

    def __init__(self):
        self._db = get_timescale()

    def get_recent(
        self,
        event_types: List[str],
        hours:       int   = 72,
        region:      str   = None,
        min_anomaly: float = 0.0,
        tags:        List[str] = None,
        limit:       int   = 50,
    ) -> List[Dict]:
        """
        Fetch recent events. All filters are optional.

        Args:
            event_types: list of EventType.value strings to include
            hours:       look-back window
            region:      exact region string match (from regions.py)
            min_anomaly: lower bound on anomaly_score
            tags:        PostgreSQL array overlap — event must have at least
                         one of these tags
            limit:       max rows returned, ordered by anomaly DESC

        Returns:
            List of row dicts. Empty list on query error (logged).
        """
        conditions = [
            "type = ANY(%s)",
            "occurred_at > NOW() - INTERVAL '1 hour' * %s",
        ]
        params: list = [event_types, hours]

        if region:
            conditions.append("region = %s")
            params.append(region)
        if min_anomaly > 0:
            conditions.append("anomaly_score >= %s")
            params.append(min_anomaly)
        if tags:
            conditions.append("tags && %s")   # && = array overlap in PostgreSQL
            params.append(tags)

        # LIMIT uses f-string with explicit int() cast — not user input, safe
        sql = f"""
            SELECT event_id, type, occurred_at, source,
                   primary_entity_id, primary_entity_name, primary_entity_flags,
                   region, latitude, longitude, anomaly_score,
                   headline, named_entities, financial_data, vessel_data, tags
            FROM events
            WHERE {" AND ".join(conditions)}
            ORDER BY anomaly_score DESC, occurred_at DESC
            LIMIT {int(limit)}
        """
        try:
            return self._db.query(sql, tuple(params))
        except Exception as e:
            logger.error(f"EventStore.get_recent failed: {e}")
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
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
        except Exception as e:
            logger.error(f"save_correlation failed ({cluster.correlation_id}): {e}")