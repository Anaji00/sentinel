"""
services/correlation/rules/cross_domain.py

CROSS_001 — High anomaly event co-incident with high anomaly events in
            OTHER domains that share a THEMATIC LINK via the Ontology Map.
"""

from typing import Optional, List
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier

DOMAIN_GROUPS = {
    "maritime":  ["vessel_position", "vessel_dark", "vessel_static", "vessel_sts", "vessel_spoof"],
    "aviation":  ["flight_position", "flight_anomaly", "flight_dark", "aircraft_squawk"],
    "financial": ["options_flow", "dark_pool", "futures_cot", "price_anomaly", "insider_trade", "equity_block", "prediction_market_trade"],
    "news":      ["headline", "social_signal", "narrative_cluster", "news_article"],
    "cyber":     ["breach_detected", "infra_exposed", "bgp_anomaly", "ransomware", "bgp_hijack"],
    "political": ["sanction_change", "regulatory_filing", "political_event"],
}

# ── THE ENTERPRISE ONTOLOGY MAP (KNOWLEDGE GRAPH) ─────────────────────────────
# This forces the engine to link financial tickers/slugs to physical world geography and events.
# In a massive production system, this would eventually live in Neo4j, but an in-memory
# dictionary is blazingly fast for the Kafka stream processing layer.

THEMATIC_LINKS = {
    # ── ENERGY & COMMODITIES ──
    "energy_oil": ["hormuz", "red_sea", "persian_gulf", "brent", "crude", "uso", "xop", "oih", "tanker", "aramco", "exxon", "xom", "cvx", "pipeline", "opec"],
    "energy_gas": ["natgas", "boil", "ung", "lng", "gazprom", "nordstream", "qatar", "baltic", "wintershall"],
    "precious_metals": ["gold", "gld", "iau", "silver", "slv", "safe_haven", "flight_to_safety", "treasury"],
    "agriculture": ["wheat", "corn", "soybeans", "black_sea", "fertilizer", "weat", "corn_futures"],

    # ── GEOPOLITICAL THEATERS ──
    "theater_middle_east": ["israel", "iran", "gaza", "lebanon", "syria", "houthi", "yemen", "idf", "irgc", "tehran", "tel_aviv", "mossad", "hezbollah", "khamenei"],
    "theater_apac": ["taiwan", "china", "beijing", "taipei", "south_china_sea", "plan", "pla", "ccp", "xi_jinping", "tsm", "semiconductor", "philippines"],
    "theater_eeur": ["ukraine", "russia", "moscow", "kyiv", "crimea", "putin", "zelensky", "black_sea", "nato", "kursk", "rostov", "sanctions", "oligarch"],
    "theater_latam": ["venezuela", "maduro", "guyana", "essequibo", "pdxv", "oil_sanctions", "cartel", "us_southern_command", "drug_trafficking", "bukele", "millei"],

    # ── SECTORS & EQUITIES ──
    "sector_defense": ["lmt", "rtx", "noc", "gd", "ba", "pltr", "kratos", "military", "war", "weapons", "missile", "drone", "uav", "dod", "pentagon"],
    "sector_semis": ["nvda", "tsm", "asml", "amd", "intel", "intc", "soxx", "smh", "microchips", "foundry", "fab", "export_controls"],
    "sector_shipping": ["suez", "bab_el_mandeb", "panama_canal", "zim", "maersk", "freight", "supply_chain", "baltic_dry", "maritime", "dark_vessel", "piracy", "logistics"],

    # ── CYBER & THREAT VECTORS ──
    "cyber_warfare": ["ransomware", "apt", "lazarus", "sandworm", "fancy_bear", "ddos", "infra_exposed", "bgp_hijack", "zero_day", "cisa", "kev", "botnet", "scada", "ics"],
    "crypto_ecosystem": ["btc", "eth", "sol", "usdc", "usdt", "binance", "polymarket", "defi", "laundering", "mixer", "tornado_cash", "on_chain"],

    # ── AVIATION & ALERTS ──
    "aviation_distress": ["squawk_7700", "squawk_7600", "squawk_7500", "emergency", "hijack", "radio_failure", "no_callsign", "military_flight", "combat_air_patrol", "intercept"],

    # ── MACRO ECONOMICS & TRADFI ──
    "macro_rates": ["fed", "fomc", "warsh", "interest_rates", "inflation", "cpi", "ppi", "treasury", "yield_curve", "tlt", "tmf"],
    "macro_volatility": ["vix", "uvxy", "market_crash", "liquidity", "margin_call", "repo_rate", "dark_pool", "options_sweep"],
    "macro_sentiment": ["consumer_confidence", "business_confidence", "gdp", "unemployment", "retail_sales", "durable_goods", "initial_claims"]
}

def get_expanded_tags(tags: set) -> set:
    """
    Takes standard tags (e.g. 'tsm', 'taiwan') and explodes them using the ontology map.
    This creates a massive 'thematic net' to catch cross-domain overlaps.
    """
    expanded = set(tags)
    for tag in tags:
        for theme, keywords in THEMATIC_LINKS.items():
            # If the tag IS the theme, or exists IN the theme's keywords
            if tag == theme or tag in keywords:
                expanded.add(theme)
                expanded.update(keywords)
    return expanded

def _domain_of(event_type: str) -> str:
    """Maps a specific event type to its macro domain."""
    for domain, types in DOMAIN_GROUPS.items():
        if event_type in types: return domain
    return "other"

def rule_cross_domain_anomaly(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    CROSS_001: Thematic Cross-Domain Cluster.
    Uses the Ontology Map to bind completely different data streams (e.g. ADSB and Stock Flow)
    into a single cohesive geopolitical narrative.
    """
    
    # 1. GATEKEEPING: Only trigger heavily anomalous events to save CPU
    if event.anomaly_score < 0.50:
        return None
    
    event_type_str = event.type.value if hasattr(event.type, 'value') else str(event.type)
    my_domain = _domain_of(event_type_str)
    
    # We want to correlate with EVERYTHING outside our own domain
    other_types = [t for d, types in DOMAIN_GROUPS.items() if d != my_domain for t in types]

    # Query the last 48 hours of anomalous events from the database
    recent_events = store.get_recent(other_types, hours=48, min_anomaly=0.65)
    
    # 2. ONTOLOGY EXPANSION
    # Expand the trigger event's tags (e.g. "lmt" expands to include "defense", "taiwan", "war", "rtx")
    my_expanded_tags = get_expanded_tags(set(event.tags))
    valid_others = []

    # 3. INTERSECTION SEARCH
    for other_event in recent_events:
        if other_event["event_id"] == event.event_id: continue
        
        # Expand the historical event's tags
        their_expanded_tags = get_expanded_tags(set(other_event.get("tags", [])))
        
        # If the expanded sets intersect, we have a confirmed geopolitical/thematic correlation!
        overlap = my_expanded_tags.intersection(their_expanded_tags)
        if overlap:
            # We attach the intersecting keywords so the LLM knows WHY we linked them
            other_event["ontology_overlap"] = list(overlap)[:5] # Keep it concise for the prompt
            valid_others.append(other_event)

    if len(valid_others) < 1:
        return None
    
    # 4. TIERING LOGIC
    # 3+ cross-domain events is a major intelligence indicator.
    tier = AlertTier.INTELLIGENCE if len(valid_others) >= 3 else AlertTier.ALERT
    domains = list({_domain_of(e.get("type", "unknown")) for e in valid_others})

    desc = (f"Thematic Cross-Domain Cluster: High anomaly ({event.anomaly_score:.2f}) in {event_type_str}. "
            f"Correlated thematically with anomalies in: {', '.join(domains)}.")
 
    return CorrelationCluster(
        rule_id="CROSS_001",
        rule_name="Thematic Cross-Domain Anomaly Cluster",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e["event_id"] for e in valid_others[:5]],
        entity_ids=[event.primary_entity.id],
        description=desc,
        tags=["cross_domain", "thematic_link"] + domains,
    )