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


def _extract_event_context(event: NormalizedEvent) -> dict:
    """Extract rich context from a NormalizedEvent for the sliding window."""
    pe = event.primary_entity
    entity_name = (pe.name if pe and pe.name else None) or (pe.id if pe else None) or "Unknown"
    entity_type = (pe.type.value if pe and hasattr(pe.type, "value") else str(pe.type) if pe else "unknown")
    domain = event.type.value if hasattr(event.type, "value") else str(event.type)

    # Build a meaningful headline from available fields, never falling back to bare UUID
    headline = getattr(event, "headline", None) or getattr(event, "summary", None)
    if not headline:
        # Construct a descriptive fallback from entity + domain
        domain_label = domain.replace("_", " ").title()
        headline = f"{domain_label}: {entity_name}"
        if event.region:
            headline += f" ({event.region})"

    return {
        "entity_name": str(entity_name),
        "entity_type": entity_type,
        "entity_id": pe.id if pe else "unknown",
        "domain": domain,
        "headline": str(headline)[:200],
        "event_id": str(event.event_id),
        "score": float(getattr(event, "anomaly_score", 0.0) or 0.0),
        "region": getattr(event, "region", None),
        "summary": str(getattr(event, "summary", "") or "")[:200],
    }


class GeopoliticalCascadeEngine:
    """
    Sliding-window multi-domain compound event cascade detector.
    Tracks cross-domain co-occurrence across Cyber, Maritime, Aviation,
    Financial Markets, and News Headlines.
    """

    def __init__(self, window_seconds: int = 3600, cooldown_seconds: int = 900):
        self.window_seconds = window_seconds
        self.cooldown_seconds = cooldown_seconds
        # Buffer: {region_key: [(timestamp, context_dict)]}
        self._sliding_window: Dict[str, List[tuple]] = {}
        # Cooldown map: {region_key: last_triggered_timestamp}
        self._last_trigger: Dict[str, float] = {}

    def ingest_event(self, event: NormalizedEvent) -> Optional[CorrelationCluster]:
        """
        Ingests a normalized event, prunes expired window entries,
        and checks for multi-domain cascade triggers.
        """
        # Filter out routine position telemetry (vessels & flights with anomaly < 0.30)
        # to prevent chokepoint position flooding from entering the sliding window.
        domain = event.type.value if hasattr(event.type, "value") else str(event.type)
        score = float(getattr(event, "anomaly_score", 0.0) or 0.0)
        if domain in ("vessel_position", "flight_position") and score < 0.30:
            return None

        now = time.time()
        ctx = _extract_event_context(event)
        key = (getattr(event, "region", None) or ctx["entity_id"] or "global").lower()

        # Deduplication Cooldown check: prevent duplicate cascades for the same key within cooldown window
        last_fired = self._last_trigger.get(key, 0)
        if now - last_fired < self.cooldown_seconds:
            return None

        if key not in self._sliding_window:
            self._sliding_window[key] = []

        # Append timestamped context
        self._sliding_window[key].append((now, ctx))

        # Prune expired entries older than window (1 hour)
        cutoff = now - self.window_seconds
        self._sliding_window[key] = [e for e in self._sliding_window[key] if e[0] >= cutoff]

        current_entries = self._sliding_window[key]
        domains_present: Set[str] = {e[1]["domain"] for e in current_entries}

        # Calculate composite Flashpoint Index (0.0 to 100.0)
        avg_score = sum(e[1]["score"] for e in current_entries) / len(current_entries) if current_entries else 0.0
        flashpoint_index = round(min(100.0, (len(domains_present) * 25.0) + (avg_score * 50.0)), 1)

        # Tightened Cascade Trigger Rules:
        # 1. Multi-domain: at least 2 distinct domains AND flashpoint_index >= 35.0 AND total events >= 2
        # 2. High-Severity Single Domain: total events >= 4 AND avg_score >= 0.45 AND max score >= 0.60
        max_score = max((e[1]["score"] for e in current_entries), default=0.0)
        
        is_multi_domain_cascade = (len(domains_present) >= 2 and flashpoint_index >= 35.0 and len(current_entries) >= 2)
        is_single_domain_storm = (len(domains_present) == 1 and len(current_entries) >= 4 and avg_score >= 0.45 and max_score >= 0.60)

        if is_multi_domain_cascade or is_single_domain_storm:
            # Mark cooldown timestamp for this key
            self._last_trigger[key] = now

            # Collect rich context for log + description
            top_entries = current_entries[-5:]  # most recent 5
            unique_entities = list(dict.fromkeys(e[1]["entity_name"] for e in current_entries))[:5]
            headlines = [e[1]["headline"] for e in top_entries[:3]]
            supporting_ids = [e[1]["event_id"] for e in current_entries]
            domain_list = sorted(domains_present)

            logger.warning(
                f"🚨 GEOPOLITICAL CASCADE DETECTED in '{key}' | "
                f"Flashpoint Index: {flashpoint_index}/100 | "
                f"Domains ({len(domains_present)}): {domain_list} | "
                f"Entities: {unique_entities} | "
                f"Headlines: {headlines}"
            )

            # Build a readable summary
            entity_summary = ", ".join(unique_entities[:3])
            domain_summary = ", ".join(d.replace("_", " ").title() for d in domain_list)
            headline_summary = "; ".join(headlines)

            import uuid
            cluster = CorrelationCluster(
                correlation_id=str(uuid.uuid4()),
                rule_id=f"rule_geopolitical_cascade_{key.lower().replace(' ', '_')}",
                rule_name=f"Geopolitical Cascade ({key.title()})",
                alert_tier=AlertTier.CRITICAL if flashpoint_index >= 75.0 else AlertTier.ELEVATED,
                trigger_event_id=supporting_ids[0],
                supporting_event_ids=supporting_ids[1:],
                entity_ids=[e[1]["entity_id"] for e in current_entries[:10]],
                entity_names=unique_entities,
                description=(
                    f"Geopolitical Cascade Alert (Flashpoint: {flashpoint_index}/100) in '{key.title()}'. "
                    f"Entities: {entity_summary}. "
                    f"Domains: {domain_summary}. "
                    f"Activity: {headline_summary}"
                ),
                tags=["geopolitical_cascade", key.lower().replace(" ", "_")]
                    + [f"entity:{n}" for n in unique_entities[:3]]
                    + [f"domain:{d}" for d in domain_list]
            )
            return cluster

        return None

