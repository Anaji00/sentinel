"""
services/enrichment/enrichers/prediction.py

ENTERPRISE PREDICTION MARKET ENRICHER & VOLATILITY SCORER
=========================================================
Enriches PolyMarket & Kalshi prediction events.
Ignores zero-volume markets, delegates Z-score anomaly scoring to AnomalyScorer,
and structures full PredictionMarketData models for the Probability Radar Matrix.
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
        raw_slug = parts[0] if len(parts) > 0 else label
        question = parts[1] if len(parts) > 1 and parts[1] != "UNKNOWN QUESTION" else p.get("question", "UNKNOWN QUESTION")
        outcome = parts[2] if len(parts) > 2 else p.get("outcome_name", "UNKNOWN OUTCOME")
        
        # Clean slug to avoid generic string splits like 'tradfi' or 'prediction_market'
        if raw_slug.lower() in ("tradfi", "prediction_market", "prediction", "unknown"):
            slug = p.get("ticker") or p.get("market_id") or (question[:30].replace(" ", "-") if question != "UNKNOWN QUESTION" else "PREDICTION-CONTRACT")
        else:
            slug = raw_slug

        notional = float(p.get("notional_usd", 0))
        shares = float(p.get("size_shares", 0))
        price = float(p.get("price", 0))
        total_vol = float(p.get("total_volume", 0) or notional)
        asset_id = p.get("asset_id", slug)

        # GATEKEEPER: Filter out dead/low-volume noise (<$100 volume / trade size)
        if total_vol < 100 and notional < 50 and shares < 50:
            return None

        # Check previous price in Redis to detect odd probability shifts (Delta P >= 0.04 / 4%)
        prev_price_key = f"sentinel:prediction:last_price:{slug}"
        prev_price_raw = await self.redis.raw.get(prev_price_key)
        delta_p = 0.0
        if prev_price_raw:
            try:
                prev_price = float(prev_price_raw)
                delta_p = price - prev_price
            except Exception:
                pass
        try:
            await self.redis.raw.set(prev_price_key, price, ex=86400)
        except Exception:
            pass

        # Delegate volume anomaly scoring to central AnomalyScorer
        anomaly = await self.scorer.score_prediction_volume_anomaly(asset_id, notional, shares)

        # Frequency boost for high activity
        f_boost = await self.scorer.track_frequency(slug, "prediction_market")
        anomaly_score = min(1.0, anomaly + f_boost)

        # Record Hawkes cross-domain excitation event
        self.scorer.record_hawkes_event("prediction")

        # Classify large bid vs odd shift vs routine update
        tags = ["prediction_market", slug.lower()]
        is_large_bid = notional >= 5000 or (shares * price) >= 5000
        is_odd_shift = abs(delta_p) >= 0.04
        display_contract = question if question != "UNKNOWN QUESTION" else slug.upper()

        if is_large_bid:
            tags.extend(["large_bid", "whale_bet", "volume_spike"])
            anomaly_score = max(anomaly_score, 0.85)
            headline = f"🐋 LARGE POLYMARKET BID: {display_contract} (${notional:,.2f} on {outcome} @ {(price*100):.1f}%)"
        elif is_odd_shift:
            shift_dir = "▲ PROBABILITY SPIKE" if delta_p > 0 else "▼ PROBABILITY DROP"
            tags.extend(["odd_shift", "probability_spike"])
            anomaly_score = max(anomaly_score, 0.78)
            headline = f"🎯 {shift_dir}: {display_contract} ({outcome} shift: {(delta_p*100):+.1f}%)"
        elif notional >= 1000 or anomaly_score >= 0.70:
            tags.append("volume_spike")
            headline = f"🎯 POLYMARKET VOLUME MOVER: {display_contract} (${notional:,.2f} — {outcome})"
        else:
            tags.append("odds_update")
            headline = f"🎯 POLYMARKET ODDS: {display_contract} ({outcome} @ {(price*100):.1f}%)"

        try:
            await self.redis.raw.sadd("sentinel:polymarket:watched_slugs", slug)
        except Exception:
            pass
        
        entity_name = display_contract if display_contract != "UNKNOWN QUESTION" else slug
        entity = Entity(id=slug, type=EntityType.INSTRUMENT, name=entity_name)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.PREDICTION_MARKET_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            prediction_market_data=PredictionMarketData(
                market_id=slug,
                ticker=slug.upper(),
                question=question,
                outcome=outcome,
                notional_usd=notional,
                shares_traded=shares,
                price_usd=price,
                total_volume=total_vol,
                yes_probability=p.get("yes_probability"),
                no_probability=p.get("no_probability"),
                category=p.get("category") or "Macro & Geopolitics",
                resolution_date=p.get("resolution_date")
            ),
            headline=headline,
            tags=tags,
            anomaly_score=max(0.15, anomaly_score),
        )
    
    async def _enrich_kalshi(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = p.get("ticker", "UNKNOWN")
        title = p.get("title", "Unknown Market")
        price = float(p.get("yes_bid") or p.get("no_bid") or p.get("price") or 0.0)
        current_vol = float(p.get("total_volume", 0))

        # GATEKEEPER: Ignore dead Kalshi markets with zero volume
        if current_vol <= 0 and float(p.get("yes_bid") or 0) <= 0 and float(p.get("no_bid") or 0) <= 0:
            return None

        # Stateful volume delta calculation using Redis
        try:
            redis_key = f"sentinel:kalshi:vol:{ticker}"
            last_vol_str = await self.redis.raw.get(redis_key)
            last_vol = float(last_vol_str) if last_vol_str else current_vol
            await self.redis.raw.set(redis_key, str(current_vol), ex=86400)  # 24h expiry
        except Exception:
            last_vol = current_vol

        delta = max(0.0, current_vol - last_vol)
        notional_usd = delta * price if delta > 0 else current_vol * price
        
        # Delegate volume anomaly scoring to central AnomalyScorer
        anomaly_score = await self.scorer.score_prediction_volume_anomaly(ticker, notional_usd, delta)

        # Frequency boost for high activity
        f_boost = await self.scorer.track_frequency(ticker, "prediction_market")
        anomaly_score = min(1.0, anomaly_score + f_boost)

        # Record Hawkes cross-domain excitation event
        self.scorer.record_hawkes_event("prediction")

        is_volume_anomaly = delta >= 50000 or notional_usd >= 25000 or anomaly_score >= 0.70
        tags = ["kalshi_prediction", ticker.lower()]
        if is_volume_anomaly:
            tags.extend(["volume_spike", "whale_bet"])
            headline = f"🚨 KALSHI VOLUME SPIKE: {ticker} (+${notional_usd:,.2f})"
        else:
            tags.append("odds_update")
            headline = f"🎯 KALSHI ODDS: {ticker} ({title})"

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        try:
            await self.redis.raw.sadd("sentinel:kalshi:watched_tickers", ticker)
        except Exception:
            pass
            
        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.PREDICTION_MARKET_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            prediction_market_data=PredictionMarketData(
                market_id=ticker,
                ticker=ticker,
                question=title,
                outcome="Volume Spike" if is_volume_anomaly else "Market Odds",
                shares_traded=delta,
                notional_usd=notional_usd,
                price_usd=price,
                total_volume=current_vol,
                yes_bid=p.get("yes_bid"),
                no_bid=p.get("no_bid"),
                yes_probability=p.get("yes_probability"),
                no_probability=p.get("no_probability"),
                category=p.get("category") or "Macro & Fed Rates",
                resolution_date=p.get("resolution_date")
            ),
            headline=headline,
            tags=tags,
            anomaly_score=max(0.15, anomaly_score),
        )