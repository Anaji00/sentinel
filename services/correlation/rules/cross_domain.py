"""
services/correlation/rules/cross_domain.py
 
CROSS_001 — High anomaly event co-incident with high anomaly events in
            OTHER domains. Catch-all for patterns no hard rule covers.
            Requires anomaly >= 0.70 on the trigger event and >= 2
            co-occurring anomalies in different domains.
"""

from typing import Optional, List
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier

# 1. DOMAIN DEFINITIONS
# We group event types into broad "Domains".
# Why? To ensure "Cross-Domain" correlation actually crosses domains.
# We don't want a "Vessel Dark" event triggering off a "Vessel Speeding" event
# and calling it a global crisis. We want "Vessel Dark" + "Cyber Attack".
DOMAIN_GROUPS = {
    "maritime":  ["vessel_position", "vessel_dark", "vessel_static",
                  "vessel_sts", "vessel_spoof"],
    "aviation":  ["flight_position", "flight_anomaly", "flight_dark"],
    "financial": ["options_flow", "dark_pool", "futures_cot",
                  "price_anomaly", "insider_trade"],
    "news":      ["headline", "social_signal", "narrative_cluster"],
    "cyber":     ["breach_detected", "infra_exposed", "bgp_anomaly", "ransomware"],
    "political": ["sanction_change", "regulatory_filing", "political_event"],
}


def _domain_of(event_type: str) -> str:
    """Helper: Maps a specific event type (e.g., 'vessel_dark') to its domain ('maritime')."""
    for domain, types in DOMAIN_GROUPS.items():
        if event_type in types:
            return domain
    return "other"  # For any event types not categorized

def _all_types_except(domain: str) -> List[str]:
    """Helper: Returns a list of ALL event types that belong to OTHER domains."""
    return [t for d, types in DOMAIN_GROUPS.items() if d != domain for t in types]

def rule_cross_domain_anomaly(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    CROSS_001: The Catch-All Rule.
    Looks for high-severity anomalies happening at the same time across different worlds
    (e.g., Physical + Financial, or Cyber + Kinetic).
    """
    
    # 1. GATEKEEPING: Only consider High-Risk events as triggers.
    # We don't want to run expensive cross-checks on every minor anomaly.
    if event.anomaly_score < 0.70:
        return None  # Only trigger on high-anomaly events
    
    # 2. DETERMINE SEARCH SCOPE
    # "I am a Maritime event. Find me everything that ISN'T Maritime."
    my_domain = _domain_of(event.type.value)
    other_types = _all_types_except(my_domain)

    # 3. QUERY HISTORY
    # Look back 48 hours for high-risk events in those other domains.
    others = [
        e for e in store.get_recent(
            other_types, 
            hours=48,
            min_anomaly=0.65, # Threshold is slightly lower for supporting evidence.
        )
        if e["event_id"] != event.event_id  # Exclude the trigger event itself
    ]

    # 4. MINIMUM CO-OCCURRENCE
    # We need at least 2 distinct anomalous events in other domains to call this a cluster.
    if len(others) < 2:
        return None  # Need at least 2 other high-anomaly events in different domains
    
    # 5. TIERING LOGIC
    # INTELLIGENCE (High): If we have a swarm of 4+ anomalies.
    # ALERT (Medium): If we have 2-3 anomalies.
    tier = AlertTier.INTELLIGENCE if len(others) >= 4 else AlertTier.ALERT
    domains = list({_domain_of(e["type"]) for e in others})

    # 6. BUILD DESCRIPTION
    desc = (
        f"High anomaly ({event.anomaly_score:.2f}) in {event.type.value}. "
        f"Co-occurring anomalies in: {', '.join(domains)}. "
        f"Total cross-domain signals: {len(others)}."
    )
 
    return CorrelationCluster(
        rule_id="CROSS_001",
        rule_name="Multi-Domain High Anomaly Cluster",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e["event_id"] for e in others[:5]],
        entity_ids=[event.primary_entity.id],
        description=desc,
        tags=["cross_domain", "anomaly_cluster"] + domains,
    )