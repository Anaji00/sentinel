"""
services/correlation/rules/news.py

NEWS_001 — Sanctions or military headline co-incident with
           flagged vessel activity in the last 24h.
"""

from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier

# 1. TRIGGER KEYWORDS
# We aren't interested in every maritime news story (e.g., "Port earnings up").
# We only care about headlines that suggest conflict, enforcement, or seizures.
TRIGGER_KEYWORDS = [
    "sanction", "irgc", "seized", "detained vessel",
    "blockade", "naval", "strait", "tanker attack",
    "missile strike", "maritime", "warship",
]


def rule_sanctions_headline(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    NEWS_001: Triggered by a news headline.
    Checks if any high-risk vessels (sanctioned/flagged) have been active
    recently, which might explain or relate to the news.
    """
    # 1. GATEKEEPING: Only process Headline events.
    if event.type != EventType.HEADLINE:
        return None

    # 2. KEYWORD FILTER
    # "Is this a scary headline?"
    headline = (event.headline or "").lower()
    if not any(kw in headline for kw in TRIGGER_KEYWORDS):
        return None

    # 3. CONTEXT QUERY
    # "Are the bad guys active right now?"
    # We query the Event Store for any vessel activity in the last 24h
    # that explicitly has the 'sanctions_risk' tag (applied by the Enricher).
    flagged = store.get_recent(
        ["vessel_position", "vessel_dark"],
        hours=24, min_anomaly=0.4,
        tags=["sanctions_risk"],
    )

    # If no high-risk vessels are active, this is just a news story. Ignore.
    if not flagged:
        return None

    # 4. BUILD RESULT
    # Create a cluster linking the News Event to the specific Vessel Events.
    desc = (
        f"Sanctions/military headline: '{event.headline[:80]}'. "
        f"{len(flagged)} flagged vessels active in last 24h."
    )

    return CorrelationCluster(
        rule_id="NEWS_001",
        rule_name="Sanctions Headline + Flagged Vessel Activity",
        alert_tier=AlertTier.ALERT,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e["event_id"] for e in flagged[:5]],
        entity_ids=(
            [event.primary_entity.id] +
            [e["primary_entity_id"] for e in flagged[:3]]
        ),
        description=desc,
        tags=["sanctions", "news", "maritime", "flagged_entity"],
    )