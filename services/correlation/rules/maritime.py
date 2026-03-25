"""
services/correlation/rules/maritime.py

MARITIME_001 — Vessel goes dark near a sensitive region.
               Cross-checks financial signals + geopolitical news.
               Tier 1 (dark alone) → Tier 2 (+ one domain) → Tier 3 (+ both).

FIX (code review): removed dead ENERGY_TICKERS constant.
  It was defined at module level but never referenced — the financial query
  uses tag-based filtering (tags=["energy","crude","oil"]) instead.
"""

from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier

# 1. REGION FILTER
# We only care about "Dark" events in these high-tension chokepoints.
# A vessel going dark in the open Atlantic is usually just bad satellite coverage.
SENSITIVE_REGIONS = {
    "Strait of Hormuz", "Persian Gulf", "Iranian Territorial",
    "Bab-el-Mandeb", "Black Sea", "Ukrainian Waters",
    "North Korean Waters", "Taiwan Strait",
}

# 2. NEWS CONTEXT MAPPING
# Maps a physical region (from the event) to keywords/entities found in news.
# Example: If a ship goes dark in "Taiwan Strait", we look for news mentioning "PLA" or "China".
REGION_ENTITIES = {
    "Strait of Hormuz":    ["Iran", "IRGC", "Hormuz", "tanker", "Persian Gulf"],
    "Bab-el-Mandeb":       ["Houthi", "Yemen", "Red Sea", "shipping"],
    "Black Sea":           ["Russia", "Ukraine", "grain", "Black Sea"],
    "Ukrainian Waters":    ["Russia", "Ukraine", "Navy"],
    "North Korean Waters": ["North Korea", "DPRK", "Kim Jong"],
    "Taiwan Strait":       ["Taiwan", "China", "PLA", "strait"],
    "Persian Gulf":        ["Iran", "IRGC", "Gulf", "tanker", "oil"],
    "Iranian Territorial": ["Iran", "IRGC", "Hormuz", "Revolutionary Guard"],
}


def rule_vessel_dark(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    MARITIME_001: Detects suspicious 'AIS Gap' events in conflict zones.
    """
    # 1. GATEKEEPING: Only process VESSEL_DARK events in the listed hot zones.
    if event.type != EventType.VESSEL_DARK:
        return None
    if not event.region or event.region not in SENSITIVE_REGIONS:
        return None

    # 2. FINANCIAL CORRELATION
    # "Is the market pricing in risk?"
    # Check for large energy trades (Options/Futures) in the last 4 days (96h).
    financial = store.get_recent(
        ["options_flow", "futures_cot", "price_anomaly"],
        hours=96, min_anomaly=0.5,
        tags=["energy", "crude", "oil"],
    )

    # 3. NEWS CORRELATION
    # "Is there news about this specific conflict zone?"
    # Fetch recent headlines (last 72h) and filter them using our REGION_ENTITIES map.
    region_entities = REGION_ENTITIES.get(event.region, [])
    all_news = store.get_recent(["headline"], hours=72, min_anomaly=0.3)
    
    # Local filter: Only keep news that mentions an entity relevant to THIS region.
    relevant_news = [
        n for n in all_news
        if any(e in (n.get("named_entities") or []) for e in region_entities)
    ]

    # 4. CLUSTER CORRELATION
    # "Is this an isolated incident or a fleet movement?"
    # Check for other vessels going dark in the exact same region in the last 24h.
    other_dark = [
        e for e in store.get_recent(
            ["vessel_dark"], hours=24, region=event.region, min_anomaly=0.4
        )
        if e["event_id"] != event.event_id  # Exclude self
    ]

    # 5. TIERING LOGIC (The Decision Matrix)
    has_fin  = len(financial) > 0
    has_news = len(relevant_news) > 0

    if has_fin and has_news:
        # Highest Priority: We have a dark ship + market moves + war news.
        tier = AlertTier.INTELLIGENCE
    elif has_fin or has_news:
        # Medium Priority: Dark ship + one other signal.
        tier = AlertTier.ALERT
    else:
        # Low Priority: Just a dark ship. Worth watching, but might be technical.
        tier = AlertTier.WATCH

    # 6. BUILD DESCRIPTION
    # Create a human-readable summary for the analyst dashboard.
    name  = event.primary_entity.name or f"MMSI:{event.primary_entity.id}"
    gap_h = (event.vessel_data.gap_hours or 0) if event.vessel_data else 0
    parts = [f"Vessel {name} dark near {event.region} (gap {gap_h:.1f}h)."]

    if event.primary_entity.flags:
        parts.append(f"Flags: {', '.join(event.primary_entity.flags)}.")
    if has_fin:
        tickers = list({e.get("financial_data", {}).get("ticker", "?") for e in financial[:3]})
        parts.append(f"Energy options activity: {', '.join(tickers)}.")
    if has_news:
        parts.append(f"{len(relevant_news)} geopolitical headlines in window.")
    if other_dark:
        parts.append(f"{len(other_dark)} other vessels also dark in region.")

    # Collect IDs of related events to draw the graph edges in the UI.
    ids = (
        [e["event_id"] for e in financial[:3]] +
        [e["event_id"] for e in relevant_news[:3]] +
        [e["event_id"] for e in other_dark[:2]]
    )

    return CorrelationCluster(
        rule_id="MARITIME_001",
        rule_name="Vessel Dark Near Sensitive Region",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=ids,
        entity_ids=[event.primary_entity.id],
        description=" ".join(parts),
        tags=["maritime", "dark_vessel", "ais_gap",
              event.region.lower().replace(" ", "_")],
    )