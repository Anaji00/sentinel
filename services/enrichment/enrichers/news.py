"""
services/enrichment/enrichers/news.py

Handles RSS/news raw events from collector-news.
Runs spaCy NER to extract named entities.
Scores sentiment and anomaly.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, List

from shared.models import NormalizedEvent, EventType, Entity, EntityType

logger = logging.getLogger("enrichment.news")

_NEG = {
    "attack", "war", "kill", "explosion", "sanction", "seizure", "crash",
    "disaster", "collapse", "crisis", "threat", "missile", "detained",
    "conflict", "strike", "blockade", "arrested", "hijack", "piracy",
}
_POS = {
    "deal", "agreement", "peace", "growth", "recovery", "cooperation",
    "ceasefire", "diplomatic", "alliance", "trade", "accord",
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

    def __init__(self, scorer):
        self.scorer = scorer
        self._nlp   = None   # lazy-loaded on first use

    def _get_nlp(self):
        if self._nlp is None:
            try:
                import spacy
                self._nlp = spacy.load("en_core_web_sm")
                logger.info("spaCy model loaded")
            except Exception as e:
                logger.warning(f"spaCy unavailable ({e}) — NER disabled")
                self._nlp = False
        return self._nlp if self._nlp else None

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        p     = raw.raw_payload
        title = (p.get("title") or "").strip()
        if not title:
            return None

        summary     = (p.get("summary") or "")[:1000]
        url         = p.get("url", "")
        reliability = float(p.get("reliability", 0.8))

        named_entities: List[str] = []
        nlp = self._get_nlp()
        if nlp:
            doc = nlp(f"{title}. {summary[:400]}")
            named_entities = list(set(
                ent.text for ent in doc.ents
                if ent.label_ in ("GPE", "ORG", "PERSON", "NORP", "FAC", "LOC")
                and len(ent.text) > 2
            ))[:20]

        sentiment = _sentiment(title + " " + summary[:200])
        anomaly   = self.scorer.score_news(named_entities, sentiment, reliability)

        tags = list(p.get("tags", []))
        tags.append(p.get("category", "news"))

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.HEADLINE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            source_reliability=reliability,
            primary_entity=Entity(
                id=raw.source, type=EntityType.MEDIA_SOURCE, name=raw.source,
            ),
            headline=title,
            summary=summary,
            url=url,
            tags=tags,
            named_entities=named_entities,
            sentiment=sentiment,
            anomaly_score=anomaly,
        )