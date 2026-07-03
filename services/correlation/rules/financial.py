"""
services/correlation/rules/financial.py
 
FINANCIAL_001 — Large options sweep on a geopolitically-sensitive instrument.
                Cross-checks vessel anomalies in relevant regions
                and geopolitical headlines in last 48h.
                Fires only when financial signal has cross-domain support.
"""

from typing import Optional

from streamlit import json
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier
from shared.db import get_redis

THEATER_TO_REGIONS = {
    "theater_middle_east": ["Strait of Hormuz", "Persian Gulf", "Red Sea", "Bab-el-Mandeb", "Iranian Territorial", "Eastern Mediterranean"],
    "theater_apac": ["Taiwan Strait", "South China Sea", "Philippine Sea", "Sea of Japan"],
    "theater_eeur": ["Black Sea", "Ukrainian Waters", "Baltic Sea", "Gulf of Finland"],
    "theater_latam": ["Panama Canal", "Venezuelan Waters", "Essequibo"]
}

async def rule_options_geo(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    FINANCIAL_001 — Large options sweep on a geopolitically-sensitive instrument.
    Cross-checks vessel anomalies in relevant regions and geopolitical headlines
    in last 48h. Fires only when financial signal has cross-domain support.
    """
    # 1. GATEKEEPER: Expand to all major institutional flow types
    valid_types = {EventType.OPTIONS_FLOW, EventType.DARK_POOL, EventType.EQUITY_BLOCK}
    if event.type not in valid_types or not event.financial_data:
        return None
    
    # 2. QUANTITATIVE THRESHOLD
    notional = event.financial_data.premium_usd or getattr(event.financial_data, 'notional_usd', 0)
    if notional < 150_000:  # Calibrated for institutional sweeps, filtering retail noise
        return None
    
    # 3. CATEGORY CHECK: Is this a sensitive instrument?
    # If it's Apple (AAPL), we ignore it (not in our GEO_INSTRUMENTS list).
    ticker = event.financial_data.ticker or "UNKNOWN"
    side = event.financial_data.side or "UNKNOWN"
    redis = await get_redis()
    record_raw = await redis.raw.get(f"sentinel:ontology:entity:{ticker.lower()}")
    if not record_raw:
        return None  # Not a known instrument in our ontology   
    
    record = json.loads(record_raw)
    concepts = record.get("macro_concepts", [])
    geo_linked = any("theater" in c or "energy" in c or "defense" in c for c in concepts)
    if not geo_linked:
        return None  # Not a geopolitically sensitive instrument
    
    target_regions = set()
    for concept in geo_linked:
        if concept in THEATER_TO_REGIONS:
            target_regions.update(THEATER_TO_REGIONS[concept])
    
    target_regions = list(target_regions)

    supporting_events = []
    domains_triggered = set()


    
    # A. Spatial Domain: Maritime
    if target_regions:
        for region in target_regions:
            maritime_hits = store.get_recent(
                ["vessel_dark", "vessel_position", "vessel_sts"],
                hours=96, region=region, min_anomaly=0.4
            )
            if maritime_hits:
                supporting_events.extend(maritime_hits)
                domains_triggered.add("maritime")
        
    # B. Spatial Domain: Aviation (e.g., emergency squawks in the theater)
    if target_regions:
        for region in target_regions:
            aviation_hits = store.get_recent(
                ["aircraft_squawk", "flight_anomaly", "flight_dark"],
                hours=96, region=region, min_anomaly=0.6
            )
            if aviation_hits:
                supporting_events.extend(aviation_hits)
                domains_triggered.add("aviation")

    # C. Informational Domain: Cyber & News
    for concept in geo_linked:
        # Cyber attacks (BGP hijacks, infra exposure) linked to the macro concept
        cyber_hits = store.get_recent(
            ["bgp_anomaly", "breach_detected", "infra_exposed"],
            hours=72, min_anomaly=0.6, tags=[concept]
        )
        if cyber_hits:
            supporting_events.extend(cyber_hits)
            domains_triggered.add("cyber")
            
        # Geopolitical News
        news_hits = store.get_recent(
            ["headline", "social_signal"], 
            hours=48, tags=[concept]
        )
        if news_hits:
            supporting_events.extend(news_hits)
            domains_triggered.add("news")

    if not supporting_events:
        return None  # No cross-domain support, don't fire
    
    # Tiering based on cross-domain convergence density
    if len(domains_triggered) >= 3:
        tier = AlertTier.CRITICAL
    elif len(domains_triggered) == 2:
        tier = AlertTier.INTELLIGENCE
    else:
        tier = AlertTier.ALERT

    # 7. METADATA CONSTRUCTION
    notional_m = notional / 1_000_000
    trade_type = getattr(event.financial_data, 'trade_type', 'SWEEP')

    desc = (
        f"Anomalous {side} {trade_type} (${notional_m:.1f}M) on {ticker}. "
        f"Ontology links asset to {geo_linked[:3]}. "
        f"Correlated with {len(supporting_events)} events across {len(domains_triggered)} external domains ({', '.join(domains_triggered)})."
    )
 
    return CorrelationCluster(
        rule_id="FINANCIAL_001",
        rule_name="Macro-Geopolitical Capital Flight",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e["event_id"] for e in supporting_events[:10]], # Limit payload size
        entity_ids=[event.primary_entity.id] if event.primary_entity else [],
        description=desc,
        tags=["financial", "cross_domain", trade_type.lower()] + geo_linked,
    )
 
