"""
services/correlation/rules/aviation.py

AVIATION_001 — Emergency squawk (7500/7600/7700) in a watch zone.
               Minimum Tier 2. Escalates to Tier 3 if concurrent
               maritime anomalies exist in same region.
"""

from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

# International Civil Aviation Organization (ICAO) emergency codes.
# These are set manually by pilots on the aircraft transponder.
SQUAWK_LABELS = {
    "7500": "HIJACK",       # Unlawful interference
    "7600": "RADIO FAIL",   # Lost communications
    "7700": "EMERGENCY",    # General emergency
}


def rule_emergency_squawk(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    AVIATION_001
    Trigger: A plane broadcasts an emergency code (Squawk).
    Context: We check if ships in the water below are also acting weird.
    """
    # 1. FILTER: Only interested in Flight Anomalies (already flagged by Enricher)
    if event.type != EventType.FLIGHT_ANOMALY:
        return None
    
    # Safety check for missing data
    if not event.flight_data:
        return None

    # 2. VALIDATION: Is this a known emergency code?
    squawk = event.flight_data.squawk or ""
    if squawk not in SQUAWK_LABELS:
        return None

    # 3. CROSS-DOMAIN CORRELATION
    # "Is there chaos on the sea below the plane?"
    # If a plane declares an emergency over the Black Sea, and we also see
    # ships going dark or speeding there, it increases the likelihood of a
    # geopolitical event (e.g., jamming, conflict) rather than just a mechanical failure.
    maritime = []
    if event.region:
        maritime = store.get_recent(
            ["vessel_dark", "vessel_position"],
            hours=24,               # Look back 24 hours
            region=event.region,    # Exact region match (e.g., "Taiwan Strait")
            min_anomaly=0.5,        # Only medium-to-high risk vessels
        )

    # 4. ESCALATION LOGIC
    # - INTELLIGENCE (High): If we have corroborating maritime signals.
    # - ALERT (Medium): If it's just the plane (could be mechanical).
    tier = AlertTier.INTELLIGENCE if maritime else AlertTier.ALERT
    
    callsign = event.primary_entity.name or event.primary_entity.id
    label    = SQUAWK_LABELS[squawk]

    # Build human-readable description
    desc = f"SQUAWK {squawk} ({label}) — {callsign} in {event.region or 'unknown region'}."
    if maritime:
        desc += f" {len(maritime)} concurrent maritime anomalies in region."

    # 5. RESULT
    return CorrelationCluster(
        rule_id="AVIATION_001",
        rule_name="Emergency Squawk in Watch Zone",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e["event_id"] for e in maritime[:3]], # Link top 3 ships
        entity_ids=[event.primary_entity.id],
        description=desc,
        tags=[
            "aviation", 
            "emergency", 
            f"squawk_{squawk}",
            (event.region or "unknown").lower().replace(" ", "_"),
        ],
    )