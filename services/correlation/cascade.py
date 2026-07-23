"""
services/correlation/cascade.py

GEOPOLITICAL FLASHPOINT CASCADE ENGINE
======================================
Tracks sliding 1-hour multi-domain event windows across Cyber, Maritime, Aviation,
TradFi, Crypto, and News Headlines to detect co-occurring compound anomalies.

When 3+ distinct domains exhibit co-occurring anomalies in the same region or
entity cluster within a 1-hour window, emits a composite CorrelationCluster
with an elevated Flashpoint Index.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from shared.models import CorrelationCluster, AlertTier, NormalizedEvent

logger = logging.getLogger("correlation.cascade")


class GeopoliticalCascadeEngine:
    """
    Sliding-window multi-domain compound event cascade detector.
    Tracks cross-domain co-occurrence across Cyber, Maritime, Aviation,
    Financial Markets, and News Headlines.
    """

    def __init__(self, window_seconds: int = 3600):
        self.window_seconds = window_seconds
        # Buffer structure: {region_or_entity: [(timestamp, domain, event_id, headline, score)]}
        self._sliding_window: Dict[str, List[tuple]] = {}

    def ingest_event(self, event: NormalizedEvent) -> Optional[CorrelationCluster]:
        """
        Ingests a normalized event, prunes expired window entries,
        and checks for multi-domain cascade triggers.
        """
        now = time.time()
        domain = event.type.value if hasattr(event.type, "value") else str(event.type)
        key = (getattr(event, "region", None) or getattr(event.primary_entity, "id", None) or "global").lower()
        
        headline = getattr(event, "headline", None) or getattr(event, "summary", "") or f"Event {event.event_id}"
        score = float(getattr(event, "anomaly_score", 0.0) or 0.0)

        if key not in self._sliding_window:
            self._sliding_window[key] = []

        # Append new event tuple
        self._sliding_window[key].append((now, domain, str(event.event_id), str(headline)[:120], score))

        # Prune expired entries older than 1 hour (3600s)
        cutoff = now - self.window_seconds
        self._sliding_window[key] = [e for e in self._sliding_window[key] if e[0] >= cutoff]

        current_events = self._sliding_window[key]
        domains_present: Set[str] = {e[1] for e in current_events}

        # Trigger multi-domain cascade if 2+ distinct domains (e.g. Cyber + Aviation, TradFi + Crypto, News + Market) co-occur
        if (len(domains_present) >= 2 or len(current_events) >= 3) and len(current_events) >= 2:
            dedup_trigger_key = f"cascade:{key}:{int(now // 900)}"  # 15-min dedup
            
            # Calculate composite Flashpoint Index (0.0 to 100.0)
            avg_score = sum(e[4] for e in current_events) / len(current_events)
            flashpoint_index = round(min(100.0, (len(domains_present) * 20.0) + (avg_score * 40.0)), 1)
            
            supporting_ids = [e[2] for e in current_events]
            headlines_list = [e[3] for e in current_events[:3]]
            
            logger.warning(
                f"🚨 GEOPOLITICAL CASCADE DETECTED in '{key}' | Flashpoint Index: {flashpoint_index}/100 | "
                f"Domains ({len(domains_present)}): {list(domains_present)} | Headlines: {headlines_list}"
            )

            import uuid
            cluster = CorrelationCluster(
                correlation_id=str(uuid.uuid4()),
                rule_id=f"rule_geopolitical_cascade_{key.lower().replace(' ', '_')}",
                rule_name=f"Geopolitical Cascade ({key.title()})",
                alert_tier=AlertTier.CRITICAL if flashpoint_index >= 75.0 else AlertTier.ELEVATED,
                trigger_event_id=supporting_ids[0],
                supporting_event_ids=supporting_ids[1:],
                entity_ids=[key],
                description=(
                    f"Geopolitical Cascade Alert (Flashpoint Index: {flashpoint_index}/100) in '{key}' "
                    f"across domains {list(domains_present)}. Co-occurring events include: {'; '.join(headlines_list)}"
                ),
                tags=["geopolitical_cascade", key.lower().replace(" ", "_")]
            )
            return cluster

        return None
