"""
services/enrichment/enrichers/prediction.py
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.models import NormalizedEvent, EventType, Entity, EntityType, PredictionMarketData

logger = logging.getLogger("enrichment.prediction")

class PredictionEnricher:
    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        if source == "polymarket":
            return await self._enrich_polymarket(raw, p)
        elif source == "kalshi":
            return await self._enrich_kalshi(raw, p)
        
        return None

    async def _enrich_polymarket(self, raw, p) -> Optional[NormalizedEvent]:
        label = p.get("asset_label", "UNKNOWN | UNKNOWN | UNKNOWN")
        parts = label.split(" | ")
        slug = parts[0] if len(parts) > 0 else label
        question = parts[1] if len(parts) > 1 else "UNKNOWN QUESTION"
        outcome = parts[2] if len(parts) > 2 else "UNKNOWN OUTCOME"
        
        notional = float(p.get("notional_usd", 0))
        shares = float(p.get("size_shares", 0))
        price = float(p.get("price", 0))
        asset_id = p.get("asset_id", slug)

        # BRAIN CHECK: Ask the AnomalyScorer if this is unusual
        anomaly = await self.scorer.score_prediction_trade(asset_id, notional)

        # GATEKEEPER: Drop normal trades. We only care about anomalies > 0.6
        if anomaly < 0.6:
            return None

        tags = ["prediction_market", "whale_bet", slug.lower()]
        headline = f"🐋 WHALE BET on {slug}: ${notional:,.2f}"

        try:
            # ASYNC REDIS RAW API - Dropped executor wrapper
            await self.redis.raw.sadd("sentinel:polymarket:watched_slugs", slug)
        except Exception:
            pass
        
        entity = Entity(id=label, type=EntityType.INSTRUMENT, name=label)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "PREDICTION_MARKET_TRADE", EventType.PREDICTION_MARKET_TRADE),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
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
            anomaly_score=anomaly,
        )
    
    async def _enrich_kalshi(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = p.get("ticker", "UNKNOWN")
        title = p.get("title", "Unknown Market")
        price = float(p.get("yes_bid") or p.get("no_bid") or p.get("price") or 0.0)
        current_vol = float(p.get("total_volume", 0))
        loop = asyncio.get_running_loop()
        # r Stateful delta calculation using Redis
        try:
            redis_key = f"sentinel:kalshi:vol:{ticker}"
            last_vol_str = await self.redis.raw.get(redis_key)
            last_vol = float(last_vol_str) if last_vol_str else current_vol
            await self.redis.raw.set(redis_key, str(current_vol), ex=86400)  # 24h expiry
        except Exception:
            last_vol = current_vol

        delta = current_vol - last_vol
        notional_usd = delta * price

        # GATEKEEPER: Ignore if there was no new volume
        if delta <= 0:
            return None
        
        # BRAIN CHECK: Await the AnomalyScorer
        anomaly_dict = await self.scorer.score_event("prediction_market_trade", ticker, [notional_usd, delta, price, 0, 0])
        anomaly_score = anomaly_dict.get("score", 0.0)

        # GATEKEEPER: Drop normal volume variance.
        if anomaly_score < 0.6:
            return None

        tags = ["kalshi_prediction", "volume_spike", ticker.lower()]
        headline = f"🚨 KALSHI SPIKE: {ticker} (+${notional_usd:,.2f})"
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        try:
            await self.redis.raw.sadd("sentinel:kalshi:watched_tickers", ticker)
        except Exception:
            pass
            
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
                notional_usd=notional_usd,
                price_usd=price
            ),
            headline=headline,
            tags=tags,
            anomaly_score=anomaly_score,
        )