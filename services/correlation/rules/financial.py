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


async def rule_options_geo(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    FINANCIAL_001 — Large options sweep on a geopolitically-sensitive instrument.
    Cross-checks vessel anomalies in relevant regions and geopolitical headlines
    in last 48h. Fires only when financial signal has cross-domain support.
    """
    # 1. FILTER: Only look at Options Flow events.
    if event.type != EventType.OPTIONS_FLOW or not event.financial_data:
        return None
    
    # 2. THRESHOLD: Ignore small retail trades.
    # We only care if 'Smart Money' moves > $500k in a single sweep.
    premium = event.financial_data.premium_usd or 0
    if premium < 100_000:  # Threshold for "large" sweep (adjustable)
        return None
    
    # 3. CATEGORY CHECK: Is this a sensitive instrument?
    # If it's Apple (AAPL), we ignore it (not in our GEO_INSTRUMENTS list).
    ticker = event.financial_data.ticker or ""
    side = event.financial_data.side or "UNKOWN"
    redis = await get_redis()
    record_raw = await redis.raw.get(f"sentinel:ontology:entity:{ticker.lower()}")
    if not record_raw:
        return None  # Not a known instrument in our ontology   
    
    record = json.loads(record_raw)
    concepts = record.get("macro_concepts", [])
    geo_linked = any("theater" in c or "energy" in c or "defense" in c for c in concepts)
    if not geo_linked:
        return None  # Not a geopolitically sensitive instrument
    

    
    # 4. MARITIME CORRELATION
    vessel_anomalies = []
    if "theater_middle_east" in concepts:
        vessel_anomalies += store.get_recent(
            ["vessel_dark", "vessel_position"],
            hours=96, region="Strait of Hormuz", min_anomaly=0.6, # Lowered from 0.7
        )
    
    # 5. NEWS CORRELATION
    # "Is there any war news in the last 48 hours?"
    all_news = store.get_recent(["headline"], hours = 48)
    # Filter news locally for our specific keywords
    geo_news = [
        n for n in all_news
        if any(c in (n.get("tags") or []) for c in concepts)
    ]

    # 6. DECISION LOGIC
    # If we found NO supporting evidence (no weird ships, no war news),
    # then this is just a normal financial trade. Ignore it.
    if not vessel_anomalies and not geo_news:
        return None  # No cross-domain support, don't fire
    
    # Tier the alert:
    # - INTELLIGENCE (High Priority): If we have BOTH maritime anomalies AND news.
    # - ALERT (Medium Priority): If we only have one source of evidence.
    tier = AlertTier.INTELLIGENCE if (vessel_anomalies and geo_news) else AlertTier.ALERT
    premium_m = premium / 1_000_000
    side = event.financial_data.side or "?"
    ttype = event.financial_data.trade_type or "?"

    desc = (
        f"${premium_m:.1f}M {side} {ttype} on {ticker} ({category}). "
        f"{len(vessel_anomalies)} vessel anomalies in relevant regions. "
        f"{len(geo_news)} geopolitical headlines."
    )
 
    # Collect IDs of supporting events (top 3 of each) to link in the graph UI
    ids = (
        [e["event_id"] for e in vessel_anomalies[:3]] +
        [e["event_id"] for e in geo_news[:3]]
    )
 
    return CorrelationCluster(
        rule_id="FINANCIAL_001",
        rule_name="Large Options Sweep + Geopolitical Signals",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=ids,
        entity_ids=[event.primary_entity.id],
        description=desc,
        tags=["financial", "options_sweep", category, "geopolitical"],
    )
 
