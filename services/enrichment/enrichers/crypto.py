import logging
from datetime import datetime, timezone
from typing import Optional
from shared.models import NormalizedEvent, EventType, Entity, EntityType, FinancialData

logger = logging.getLogger("enrichment.crypto")

class CryptoEnricher:
    def __init__(self, scorer):
        self.scorer = scorer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        
        # Example logic for a Whale Wallet Transfer
        wallet = p.get("to_address")
        amount = float(p.get("amount_usd", 0))
        
        if not wallet or amount < 1_000_000:
            return None
            
        entity = Entity(id=wallet, type=EntityType.ORGANIZATION, name=f"Wallet_{wallet[:6]}")
        anomaly = min(1.0, amount / 50_000_000 * 0.5)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.OPTIONS_FLOW, # Create EventType.CRYPTO_TRANSFER later
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            tags=["crypto", "whale_transfer"],
            anomaly_score=round(anomaly, 3),
        )