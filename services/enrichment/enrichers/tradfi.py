import logging
from datetime import datetime, timezone
from typing import Optional
from shared.models import NormalizedEvent, EventType, Entity, EntityType, FinancialData

logger = logging.getLogger("enrichment.tradfi")

GEO_INSTRUMENTS = {
    "LMT": "defense", "RTX": "defense", "USO": "oil", "GLD": "gold"
}

class TradFiEnricher:
    def __init__(self, scorer):
        self.scorer = scorer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        if source == "finnhub_equities":
            return self._enrich_equity(raw, p)
        elif source == "sec_form4":
            return self._enrich_insider(raw, p)
        elif source == "cftc_cot":
            return self._enrich_cot(raw, p)
            
        return None

    def _enrich_equity(self, raw, p) -> Optional[NormalizedEvent]:
        # Handle Finnhub Block Trades and Volume Spikes
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None
        
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        anomaly = min(1.0, float(p.get("notional_usd", 0)) / 10_000_000 * 0.5)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.OPTIONS_FLOW, # Or create EventType.EQUITY_BLOCK in your models
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, instrument_type="equity", 
                premium_usd=p.get("notional_usd")
            ),
            tags=["tradfi", "equity_block", ticker.lower()],
            anomaly_score=round(anomaly, 3),
        )

    def _enrich_insider(self, raw, p) -> Optional[NormalizedEvent]:
        # Exact same SEC Form 4 logic from your old financial.py
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None
        
        value = float(p.get("transaction_value_usd", 0))
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        anomaly = min(1.0, value / 10_000_000 * 0.3)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.INSIDER_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, instrument_type="equity", premium_usd=value
            ),
            headline=f"Insider Trade: {ticker} ${value/1e6:.1f}M",
            tags=["tradfi", "insider_trade", ticker.lower()],
            anomaly_score=round(anomaly, 3),
        )