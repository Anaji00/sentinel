"""
services/enrichment/enrichers/prediction.py

This module is responsible for taking raw, unstructured data from prediction markets 
(like Polymarket and Kalshi) and transforming it into a standardized `NormalizedEvent` 
that the rest of the Sentinel system can understand and analyze.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

# FIX: Imported the missing `PredictionMarketData` model.
from shared.models import NormalizedEvent, EventType, Entity, EntityType, PredictionMarketData

logger = logging.getLogger("enrichment.prediction")

class PredictionEnricher:
    """
    Standardizes events from prediction markets. It uses a routing mechanism to handle 
    different data sources (e.g., Polymarket's real-time trades vs. Kalshi's volume spikes).
    """
    def __init__(self, scorer):
        # The scorer is used to determine how "unusual" an event is, though 
        # currently this class calculates some baseline anomaly scores itself.
        self.scorer = scorer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        # Extract the raw dictionary payload and the source identifier
        p = raw.raw_payload
        source = raw.source

        # ROUTER: Decide which private processing method to use based on the source
        if source == "polymarket":
            return self._enrich_polymarket(raw, p)
        elif source == "kalshi":
            return self._enrich_kalshi(raw, p)
        
        return None

    def _enrich_polymarket(self, raw, p) -> Optional[NormalizedEvent]:
        """Processes real-time trades from Polymarket, identifying large 'whale' bets."""
        # The collector sends a human-readable label like "slug | question | outcome".
        # We parse this string to get structured data for our event.
        label = p.get("asset_label", "UNKNOWN | UNKNOWN | UNKNOWN")
        parts = label.split(" | ")
        slug = parts[0] if len(parts) > 0 else label
        question = parts[1] if len(parts) > 1 else "UNKNOWN QUESTION"
        outcome = parts[2] if len(parts) > 2 else "UNKNOWN OUTCOME"
        # Safely extract financial details, defaulting to 0 if they don't exist.
        notional = float(p.get("notional_usd", 0))
        shares = float(p.get("size_shares", 0))
        price = float(p.get("price", 0))
        is_whale = p.get("is_whale_bet", False)

        # DATA BASELINING: The collector has already done the hard work of detecting
        # if a bet is unusually large. We just check the boolean flag.
        is_baseline =  not is_whale
        
        if is_baseline:
            # If it's a normal trade, give it a very low anomaly score.
            anomaly = 0.1
            tags = ["prediction_market", "baseline_data"]
            headline = f"Market Flow on {label}: ${notional:,.2f}"
        else:
            # For whale bets, calculate a score. Start with a base of 0.5 and add more
            # based on size, maxing out the size contribution at $100k.
            anomaly = min(1.0, 0.5 + (notional / 100_000) * 0.4)
            tags = ["prediction_market", "whale_bet", slug.lower()]
            headline = f"🐋 WHALE BET on {slug}: ${notional:,.2f}"

        # The primary entity is the market outcome itself (e.g., "Will X happen? | Yes").
        entity = Entity(id=label, type=EntityType.INSTRUMENT, name=label)

        # Package everything into our system-wide NormalizedEvent format.
        return NormalizedEvent(
            event_id=raw.event_id,
            # Use a specific EventType for prediction markets.
            type=getattr(EventType, "PREDICTION_MARKET_TRADE", EventType.PREDICTION_MARKET_TRADE),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            # Use the dedicated data model for prediction market events.
            prediction_market_data=PredictionMarketData(
                market_id = slug,
                question = question,
                outcome = outcome,
                notional_usd = notional,
                shares_traded = shares,
                price_usd = price,
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )
    
    def _enrich_kalshi(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = p.get("ticker", "UNKNOWN")
        title = p.get("title", "Unknown Market")
        delta = float(p.get("volume_delta", 0))
        price = float(p.get("yes_bid") or p.get("no_bid") or 0.0)
        is_anomaly = p.get("is_anomaly", False)

        is_baseline = not is_anomaly

        if is_baseline:
            anomaly = 0.1
            tags = ["prediction_market", "baseline_data"]
            headline = f"Kalshi Volume Flow: {ticker} (+{int(delta)})"
        else:
            anomaly = 0.65
            tags = ["prediction_market", "volume_spike", ticker.lower()]
            headline = f"🚨 KALSHI SPIKE: {ticker} (+{int(delta)} contracts)"

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "PREDICTION_MARKET_TRADE", EventType.PREDICTION_MARKET_TRADE),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            prediction_market_data=PredictionMarketData(
                market_id=ticker,
                question=title,
                outcome="Volume Spike",
                shares_traded=delta,
                price_usd=price
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )