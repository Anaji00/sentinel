"""
services/correlation/rules/news.py

NEWS_002 — Dynamic Catalyst Detection
Triggers when a headline matches the AI-curated keyword list (from the Ontology Master)
and temporally correlates with high-anomaly events across ANY domain (TradFi, Cyber, Maritime).
"""

import logging
from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier
from shared.db import get_redis

logger = logging.getLogger("correlation.rules.news")

# 1. CORE FALLBACK KEYWORDS
# We keep a foundational list for cold-starts, expanded to cover all domains.
CORE_KEYWORDS = [
    # Geopolitics/Maritime
    "sanction", "irgc", "seized", "blockade", "missile strike", "warship",
    # Finance/Macro
    "margin call", "liquidation", "chapter 11", "indictment", "sec probe",
    # Cyber
    "cyberattack", "ransomware", "data breach", "zero-day",
]

def _get_dynamic_keywords() -> set:
    """
    Pulls the live, AI-curated keyword watchlist from Redis.
    This list is actively maintained by the OntologyMasterAgent.
    """
    keywords = set(CORE_KEYWORDS)
    try:
        redis_client = get_redis()
        if redis_client:
            # Fetch the dynamic set created by the Ontology Master
            dynamic = redis_client.raw.smembers("sentinel:news:keywords")
            if dynamic:
                # Decode bytes to string and normalize
                keywords.update({k.decode('utf-8').lower().strip() for k in dynamic})
    except Exception as e:
        logger.debug(f"Failed to fetch dynamic news keywords, using fallback: {e}")
    
    return keywords

def rule_dynamic_news_catalyst(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    NEWS_002: Dynamic Catalyst Detection
    Checks if a headline contains tracked keywords and searches for corresponding 
    anomalies across ALL domains based on shared Ontology tags.
    """
    # 1. GATEKEEPING: Only process Headline events.
    if event.type != EventType.HEADLINE:
        return None

    headline = (event.headline or "").lower()
    live_keywords = _get_dynamic_keywords()

    # 2. DYNAMIC KEYWORD FILTER
    # Does this headline contain any core OR AI-generated threat words?
    matched_keywords = [kw for kw in live_keywords if kw in headline]
    if not matched_keywords:
        return None

    # 3. CROSS-DOMAIN CONTEXT QUERY
    # We don't just look for ships anymore. We look for ANY anomalous event 
    # (Equity Block, BGP Hijack, Crypto Transfer) in the last 24h that shares
    # an Ontology tag with this news story.
    if not event.tags:
        return None

    # Note: 'event.tags' have already been expanded by the SoftCorrelator via ontology.py
    correlated_events = store.get_recent(
        types=None,        # Search across ALL event types
        hours=24, 
        min_anomaly=0.30,  # Only look at significant anomalies
        tags=event.tags    # Must share at least one tag with the news event
    )

    # Filter out the news event itself
    valid_correlations = [e for e in correlated_events if e.get("event_id") != event.event_id]

    if not valid_correlations:
        return None

    # 4. BUILD RESULT
    desc = (
        f"Dynamic Catalyst Detected: '{event.headline[:80]}'. "
        f"Matched AI keywords: {matched_keywords[:3]}. "
        f"Correlated with {len(valid_correlations)} anomalous events across domains."
    )

    # Safely extract entity IDs, dropping Nones (Fixes previous KeyError/AttributeError bugs)
    safe_entities = []
    if getattr(event, "primary_entity", None):
        safe_entities.append(event.primary_entity.id)
    
    safe_entities.extend([
        e.get("primary_entity", {}).get("id") for e in valid_correlations[:5] 
        if e.get("primary_entity", {}).get("id")
    ])

    # Tiering: If the news catalyst maps to 3 or more distinct anomalies, elevate to CRITICAL
    tier = AlertTier.INTELLIGENCE if len(valid_correlations) >= 3 else AlertTier.ALERT

    return CorrelationCluster(
        rule_id="NEWS_002",
        rule_name="Dynamic News Catalyst Correlation",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e.get("event_id") for e in valid_correlations[:5]],
        entity_ids=list(set(safe_entities)), # Deduplicate entity IDs
        description=desc,
        tags=list(set(["dynamic_catalyst", "news"] + event.tags[:3])),
    )

import logging
from typing import Optional
from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier
from shared.db import get_redis

logger = logging.getLogger("correlation.rules.news")

CORE_KEYWORDS = [
    "sanction", "irgc", "seized", "blockade", "missile strike", "warship",
    "margin call", "liquidation", "chapter 11", "indictment", "sec probe",
    "cyberattack", "ransomware", "data breach", "zero-day",
]

def _get_dynamic_keywords() -> set:
    keywords = set(CORE_KEYWORDS)
    try:
        redis_client = get_redis()
        if redis_client:
            dynamic = redis_client.raw.smembers("sentinel:news:keywords")
            if dynamic:
                keywords.update({k.decode('utf-8').lower().strip() for k in dynamic})
    except Exception as e:
        logger.debug(f"Failed to fetch dynamic news keywords, using fallback: {e}")
    return keywords

def rule_dynamic_news_catalyst(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    if event.type != EventType.HEADLINE:
        return None

    headline = (event.headline or "").lower()
    live_keywords = _get_dynamic_keywords()

    matched_keywords = [kw for kw in live_keywords if kw in headline]
    if not matched_keywords:
        return None

    if not event.tags:
        return None

    # FIXED: event_types=[] keyword mapping
    correlated_events = store.get_recent(
        event_types=[], 
        hours=24, 
        min_anomaly=0.30,  
        tags=event.tags    
    )

    # FIXED: Dict lookups replacing class property access
    valid_correlations = [e for e in correlated_events if e.get("event_id") != event.event_id]

    if not valid_correlations:
        return None

    desc = (
        f"Dynamic Catalyst Detected: '{event.headline[:80]}'. "
        f"Matched AI keywords: {matched_keywords[:3]}. "
        f"Correlated with {len(valid_correlations)} anomalous events across domains."
    )

    safe_entities = []
    if getattr(event, "primary_entity", None):
        safe_entities.append(event.primary_entity.id)
    
    safe_entities.extend([
        e.get("primary_entity", {}).get("id") for e in valid_correlations[:5] 
        if e.get("primary_entity", {}).get("id")
    ])

    # Valid Tier Assignment (Mapped to Intelligence if high correlation counts)
    tier = AlertTier.INTELLIGENCE if len(valid_correlations) >= 3 else AlertTier.ALERT

    return CorrelationCluster(
        rule_id="NEWS_002",
        rule_name="Dynamic News Catalyst Correlation",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e.get("event_id") for e in valid_correlations[:5]], # FIXED: Dict key access
        entity_ids=list(set(safe_entities)), 
        description=desc,
        tags=list(set(["dynamic_catalyst", "news"] + event.tags[:3])),
    )