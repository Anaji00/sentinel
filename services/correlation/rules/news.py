"""
services/correlation/rules/news.py

NEWS_002 — Dynamic Catalyst Detection
Triggers when a headline matches the AI-curated keyword list (from the Ontology Master)
and temporally correlates with high-anomaly events across ANY domain (TradFi, Cyber, Maritime).
"""

import logging
import time
from typing import Optional
import asyncio

from shared.models import NormalizedEvent, EventType, CorrelationCluster, AlertTier
from shared.db import get_redis

logger = logging.getLogger("correlation.rules.news")

CORE_KEYWORDS = {
    "sanction", "irgc", "seized", "blockade", "missile strike", "warship",
    "margin call", "liquidation", "chapter 11", "indictment", "sec probe",
    "cyberattack", "ransomware", "data breach", "zero-day",
}

# ── IN-MEMORY TTL CACHE ───────────────────────────────────────────────────────
# Prevents hammering Redis with network requests for every single news event.
_cached_keywords = set(CORE_KEYWORDS)
_last_fetch_time = 0.0
CACHE_TTL_SECONDS = 60.0

async def _get_dynamic_keywords() -> set:
    """
    Pulls the live, AI-curated keyword watchlist from Redis with a 60-second cache.
    """
    global _cached_keywords, _last_fetch_time

    # Return local memory if the cache is still fresh
    if time.time() - _last_fetch_time < CACHE_TTL_SECONDS:
        return _cached_keywords

    keywords = set(CORE_KEYWORDS)
    try:
        redis_client = await get_redis()
        if redis_client:
            # [CRITICAL FIX 1]: Await the asynchronous Redis network call
            dynamic = await redis_client.raw.smembers("sentinel:news:keywords")
            
            if dynamic:
                # [CRITICAL FIX 2]: Removed .decode('utf-8') because decode_responses=True is active
                keywords.update({k.lower().strip() for k in dynamic})
        
        # Update Cache State
        _cached_keywords = keywords
        _last_fetch_time = time.time()
        
    except Exception as e:
        logger.debug(f"Failed to fetch dynamic news keywords, using fallback: {e}")

    return _cached_keywords


async def rule_dynamic_news_catalyst(event: NormalizedEvent, store) -> Optional[CorrelationCluster]:
    """
    NEWS_002: Dynamic Catalyst Detection
    Checks if a headline contains tracked keywords and searches for corresponding 
    anomalies across ALL domains based on shared Ontology tags.
    """
    if event.type != EventType.HEADLINE:
        return None

    headline = (event.headline or "").lower()
    
    # [CRITICAL FIX 3]: Await the keyword fetcher
    live_keywords = await _get_dynamic_keywords()

    matched_keywords = [kw for kw in live_keywords if kw in headline]
    if not matched_keywords:
        return None

    if not event.tags:
        return None

    correlated_events = await store.get_recent(
        event_types=[], 
        hours=24, 
        min_anomaly=0.30,  
        tags=event.tags    
    )

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

    tier = AlertTier.INTELLIGENCE if len(valid_correlations) >= 3 else AlertTier.ALERT

    return CorrelationCluster(
        rule_id="NEWS_002",
        rule_name="Dynamic News Catalyst Correlation",
        alert_tier=tier,
        trigger_event_id=event.event_id,
        supporting_event_ids=[e.get("event_id") for e in valid_correlations[:5]],
        entity_ids=list(set(safe_entities)), 
        description=desc,
        tags=list(set(["dynamic_catalyst", "news"] + event.tags[:3])),
    )