"""
services/enrichment/enrichers/news.py

Handles RSS/news raw events from collector-news.
Runs spaCy NER to extract named entities.
Scores sentiment and anomaly.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List

from shared.models import NormalizedEvent, EventType, Entity, EntityType
from shared.kafka import Topics

logger = logging.getLogger("enrichment.news")

_NEG = {
    "attack", "war", "kill", "explosion", "sanction", "seizure", "crash",
    "disaster", "collapse", "crisis", "threat", "missile", "detained",
    "conflict", "strike", "blockade", "arrested", "hijack", "piracy",
    "bankruptcy", "default", "liquidation", "plunge", "recession", 
    "inflation", "margin_call", "downgrade", "bearish",
    # Cyber & Infrastructure
    "breach", "ransomware", "hack", "vulnerability", "exploit", "outage", "malware",
    # Geopolitical & Kinetic
    "escalation", "invasion", "coup", "riot", "embargo", "tension", "terrorism",
    # Financial, Crypto & Legal
    "fraud", "scam", "indictment", "lawsuit", "selloff", "penalty", "rugpull",
    "delisted", "slump", "tanked", "underperform", "capitulation", "dilution", "deflation",
}
_POS = {
    "deal", "agreement", "peace", "growth", "recovery", "cooperation",
    "ceasefire", "diplomatic", "alliance", "trade", "accord",
    "bullish", "merger", "acquisition", "profit", "dividend",
    "surge", "rally", "stimulus", "breakout", "upgrade",
    # Tech, Cyber & Diplomatic
    "treaty", "negotiation", "partnership", "aid", "patched", "secured",
    "resolved", "rescued", "funding", "adoption", "approved", "breakthrough",
    # Stocks & Markets
    "outperform", "buyback", "uptrend", "skyrocket", "lucrative", "undervalued", "bull-run", "soar",
}

FINANCIAL_KEYWORDS = {
    "fed": "macro", "rates": "macro", "inflation": "macro", 
    "earnings": "corporate", "sec": "regulatory", "crypto": "crypto",
    "treasury": "macro", "opec": "energy", "fomc": "macro",
    "supply chain": "logistics", "semiconductor": "tech"
}

def _sentiment(text: str) -> float:
    t     = text.lower()
    neg   = sum(1 for w in _NEG if w in t)
    pos   = sum(1 for w in _POS if w in t)
    total = neg + pos
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 3)


class NewsEnricher:

    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p     = raw.raw_payload
        title = (p.get("title") or "").strip()
        if not title:
            return None

        summary     = (p.get("summary") or "")[:1000]
        url         = p.get("url", "")
        reliability = float(p.get("reliability", 0.8))

        named_entities: List[str] = []
        combined_text = f"{title} {summary}"
        lower_text = combined_text.lower()


        sentiment = _sentiment(combined_text[:200])

        tags = list(p.get("tags", []))
        tags.append(p.get("category", "news"))

        for kw, category in FINANCIAL_KEYWORDS.items():
            if kw in lower_text:
                if category not in tags:
                    tags.append(category)
        
        features = [abs(sentiment), len(tags)]
        anomaly = await self.scorer.score_event("news_headline", features)

        if anomaly < 0.3:
            return None
        
        logger.info(f"Enriched News | Anomaly: {anomaly} | Sentiment: {sentiment} | {title[:60]}...")

        if anomaly >=0.5 and self.graph:
            topic_str = "High_Impact_News"

            # Using try/except to prevent network timeouts from crashing the loop
            try:
                await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                    "entity_id": topic_str,
                    "action": "MERGE_ONTOLOGY_NODE",
                    "data": {
                        "label": "NewsEvent",
                        "primary_domain": "global",
                        "confidence": anomaly,
                        "sentiment": sentiment
                    }
                }, key=topic_str)
            except Exception as e:
                logger.debug(f"Failed to push to ontology proposals: {e}")

        entity = Entity(id=raw.source, type=EntityType.MEDIA_SOURCE, name=raw.source)
    
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.HEADLINE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=reliability,
            primary_entity=entity,
            headline=title,
            summary=summary,
            url=url,
            tags=tags,
            named_entities=[],
            sentiment=sentiment,
            anomaly_score=anomaly,
        )