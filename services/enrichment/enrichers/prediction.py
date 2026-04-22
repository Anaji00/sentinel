import logging
from datetime import datetime, timezone
from typing import Optional
from shared.models import NormalizedEvent, EventType, Entity, EntityType

logger = logging.getLogger("enrichment.prediction")

class PredictionEnricher:
    def __init__(self, scorer):
        self.scorer = scorer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        slug = p.get("event_slug")
        
        if not slug: return None
        
        entity = Entity(id=slug, type=EntityType.INSTRUMENT, name=slug.replace("-", " ").title())

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.OPTIONS_FLOW, # Create EventType.PREDICTION_MARKET later
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            tags=["prediction_market", "probability_shift"],
            anomaly_score=0.5, # Placeholder, you can implement math later
        )