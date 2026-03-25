"""
services/correlation/rules/financial.py
 
FINANCIAL_001 — Large options sweep on a geopolitically-sensitive instrument.
                Cross-checks vessel anomalies in relevant regions
                and geopolitical headlines in last 48h.
                Fires only when financial signal has cross-domain support.
"""

from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier
 
# Tickers mapped to geopolitical category.
# Category determines which maritime regions are checked for correlation.
GEO_INSTRUMENTS = {
    "USO": "oil",  "BNO": "oil",  "UCO": "oil",
    "XOP": "energy", "OIH": "energy",
    "UNG": "gas",  "BOIL": "gas",
    "LMT": "defense", "RTX": "defense", "NOC": "defense",
    "GD":  "defense", "BA":  "defense",
    "GLD": "gold", "IAU": "gold", "GOLD": "gold",
    "ZIM": "shipping", "DAC": "shipping",
    "SBLK": "shipping", "GOGL": "shipping",
}

# 2. REGION MAPPING
# Which physical chokepoints matter for which commodity?
CATEGORY_REGIONS = {
    "oil":      ["Strait of Hormuz", "Persian Gulf", "Bab-el-Mandeb", "Red Sea"],
    "energy":   ["Strait of Hormuz", "Persian Gulf", "Ukrainian Waters"],
    "gas":      ["Strait of Hormuz", "Persian Gulf"],
    "defense":  ["Ukrainian Waters", "Taiwan Strait", "South China Sea"],
    "shipping": ["Strait of Malacca", "Suez Canal", "Bab-el-Mandeb"],
    "gold":     [],   # no specific maritime region — use headlines only
    "silver":   [],   # no specific maritime region — use headlines only
    "copper":   [],   # no specific maritime region — use headlines only
    "lithium":  [],   # no specific maritime region — use headlines only
}

# 3. NEWS KEYWORDS
# Simple keyword list to identify "Scary" news headlines.
GEO_KEYWORDS = [
    "war", "attack", "sanction", "military", "missile", "OPEC",
    "conflict", "blockade", "escalation", "seized", "explosion",
]

def rule_options_geo(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    FINANCIAL_001 — Large options sweep on a geopolitically-sensitive instrument.
    Cross-checks vessel anomalies in relevant regions and geopolitical headlines
    in last 48h. Fires only when financial signal has cross-domain support.
    """
    # 1. FILTER: Only look at Options Flow events.
    if event.type != EventType.OPTIONS_FLOW:
        return None
    
    if not event.financial_data:
        return None
    
    # 2. THRESHOLD: Ignore small retail trades.
    # We only care if 'Smart Money' moves > $500k in a single sweep.
    premium = event.financial_data.premium_usd or 0
    if premium < 500_000:  # Threshold for "large" sweep (adjustable)
        return None
    
    # 3. CATEGORY CHECK: Is this a sensitive instrument?
    # If it's Apple (AAPL), we ignore it (not in our GEO_INSTRUMENTS list).
    ticker = event.financial_data.ticker or ""
    category = GEO_INSTRUMENTS.get(ticker)
    if not category:
        return None
    
    # 4. MARITIME CORRELATION
    # "Are there any weird ships in the regions that affect this stock?"
    regions = CATEGORY_REGIONS.get(category, [])
    vessel_anomalies = []
    for region in regions:
        # Query the Event Store for:
        # - Event Types: Dark Vessels (AIS off) or Position Anomalies (Speeding)
        # - Time: Last 96 hours (4 days)
        # - Region: The specific chokepoint (e.g., Strait of Hormuz)
        # - Anomaly Score: Must be high risk (>= 0.7)
        vessel_anomalies += store.get_recent(
            ["vessel_dark", "vessel_position"],
            hours = 96, region = region, min_anomaly = 0.7,
        )
    
    # 5. NEWS CORRELATION
    # "Is there any war news in the last 48 hours?"
    all_news = store.get_recent(["headline"], hours = 48)
    # Filter news locally for our specific keywords
    geo_news = [
        n for n in all_news
        if any(kw.lower() in (n.get("headline") or "").lower() for kw in GEO_KEYWORDS)
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
 
