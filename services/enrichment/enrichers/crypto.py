"""
services/enrichment/enrichers/crypto.py

This module is responsible for taking raw, unstructured crypto data (like blockchain 
transfers or exchange liquidations) and transforming it into a standardized 
`NormalizedEvent` that the rest of the Sentinel system can process and analyze.
"""

import logging
from datetime import datetime, timezone
from typing import Optional
from shared.models import NormalizedEvent, EventType, Entity, EntityType, CryptoData
from shared.kafka import Topics

logger = logging.getLogger("enrichment.crypto")

class CryptoEnricher:
    """
    Standardizes cryptocurrency events. It uses a routing mechanism to handle 
    different types of crypto data sources (e.g., on-chain RPCs vs. CEX WebSockets).
    """
    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer


    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        res = await self.enrich_batch([raw])
        return res[0] if res else None

    async def enrich_batch(self, events: list) -> list:
        if not events: return []
        
        spot_trades = []
        other_tasks = []
        
        for raw in events:
            p, source = raw.raw_payload, raw.source
            if source == "ethereum_rpc": other_tasks.append(self._enrich_whale_transfer(raw, p))
            elif source == "binance_futures": other_tasks.append(self._enrich_liquidation(raw, p))
            elif source == "coinbase_spot": spot_trades.append((raw, p))
            elif source == "coinbase_candles": other_tasks.append(self._enrich_candle(raw, p))
            
        import asyncio
        results = await asyncio.gather(*other_tasks, return_exceptions=True) if other_tasks else []
        
        normalized_events = []
        for res in results:
            if isinstance(res, NormalizedEvent):
                normalized_events.append(res)
            elif isinstance(res, list):
                normalized_events.extend(res)
            elif isinstance(res, Exception):
                logger.error(f"Error enriching crypto batch item: {res}")
                
        if spot_trades:
            batched_results = await self._enrich_spot_trade_batch(spot_trades)
            normalized_events.extend(batched_results)
            
        return normalized_events
        
    async def _enrich_spot_trade_batch(self, spot_trades: list) -> list:
        parsed_events = []
        trades_for_scoring = []
        for raw, p in spot_trades:
            asset = p.get("asset", "UNKNOWN").upper()
            side = p.get("side", "UNKNOWN").upper()
            try:
                price = float(p.get("price", 0))
                qty = float(p.get("size_tokens", 0))
                notional = float(p.get("notional_usd", 0))
            except (ValueError, TypeError) as e:
                logger.error(f"Failed to parse price/qty for spot trade: {e}")
                continue
            parsed_events.append((raw, p, asset, side, price, qty, notional))
            trades_for_scoring.append((asset, notional, qty))
            
        if not parsed_events: return []
        
        scores = await self.scorer.score_crypto_trade_batch(trades_for_scoring)
        
        results = []
        for i, (raw, p, asset, side, price, qty, notional) in enumerate(parsed_events):
            anomaly = scores[i]
            logger.info(f"🧠 ML INFERENCE | {asset} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
            if anomaly < 0.6: continue
            
            tags = ["crypto", "spot_trade", asset.lower(), side.lower()]
            entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)
            headline = f"🐋 ML Outlier CRYPTO Trade ({side}): ${notional/1e6:.2f}M {asset} at ${price:,.2f}"

            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": asset,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
            }, key=asset)

            results.append(NormalizedEvent(
                event_id=raw.event_id,
                type = EventType.CRYPTO_TRADE,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source,
                primary_entity=entity,
                crypto_data=CryptoData(
                    pair=asset,
                    trade_type="LARGE_SPOT",
                    side=side,
                    price=price,
                    size_tokens=qty
                ),
                headline=headline,
                tags=tags,
                anomaly_score=round(anomaly, 3),
            ))
            
        return results
    
    async def _enrich_candle(self, raw, p) -> list:
        asset = p.get("asset", "UNKNOWN").upper()
        open_p = float(p.get("open", 0))
        high_p = float(p.get("high", 0))
        low_p = float(p.get("low", 0))
        close_p = float(p.get("close", 0))
        volume = float(p.get("volume", 0))
        
        if open_p == 0 or close_p == 0:
            return []
            
        ts = raw.occurred_at or datetime.now(timezone.utc)
        
        from shared.utils.candles import evaluate_multi_timeframe
        
        anomalous_frames = await evaluate_multi_timeframe(
            self.redis, self.scorer, domain="crypto", asset=asset, 
            ts=ts, open_p=open_p, high_p=high_p, low_p=low_p, close_p=close_p, volume=volume
        )
        
        events = []
        for tf, block, features, anomaly in anomalous_frames:
            price_change_pct = features[0]
            volatility_pct = features[1]
            notional = features[2]
            
            tags = ["crypto", "market_structure", f"volatile_{tf}m_candle", asset.lower()]

            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": asset,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
            }, key=asset)
            
            entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)
            direction = "🟢 Bullish" if block["close"] >= block["open"] else "🔴 Bearish"
            headline = f"{direction} Structural Anomaly: {asset} {tf}-min moved {price_change_pct*100:.2f}% on ${notional/1e6:.1f}M vol"
    
            events.append(NormalizedEvent(
                event_id=raw.event_id,
                type=EventType.MARKET_ANOMALY,
                occurred_at=datetime.fromisoformat(block["start_ts"]),
                source=raw.source,
                primary_entity=entity,
                crypto_data=CryptoData(
                    pair=asset, 
                    trade_type=f"OHLCV_{tf}M_BAR",
                    side="MARKET",
                    price=block["close"],
                    size_tokens=block["volume"],
                    open_price=block["open"],
                    high_price=block["high"],
                    low_price=block["low"],
                    close_price=block["close"]
                ),
                headline=headline,
                tags=tags,
                anomaly_score=anomaly,
            ))
            
        return events

    async def _enrich_whale_transfer(self, raw, p) -> Optional[NormalizedEvent]:
        """Processes large on-chain token/coin movements (whale transfers)."""
        wallet = p.get("receiver_wallet", "UNKNOWN")
        sender = p.get("sender_wallet", "UNKNOWN")
        asset = p.get("asset", "UNKNOWN").upper()
        is_suspect = p.get("is_suspect_wallet", False)

        try:
            notional = float(p.get("notional_usd", 0))
        except (ValueError, TypeError):
            notional = 0.0

        is_baseline = notional < 1_000_000 and not is_suspect

        if is_baseline:
            anomaly = 0.1
            tags = ["crypto", "transfer", "baseline_data"]
            headline = f"Standard Transfer: ${notional/1e6:.2f}M {asset}"
        else:
            anomaly = min(1.0, notional / 50_000_000 * 0.5)
            if is_suspect: anomaly = min(1.0, anomaly + 0.4)
            
            tags = ["crypto", "whale_transfer", asset.lower()]
            if is_suspect: tags.append("suspect_wallet")
            headline = f"{'🚨 SUSPECT ' if is_suspect else ''}Whale Transfer: ${notional/1e6:.1f}M {asset}"

            if notional > 5_000_000:
                try:
                    await self.redis.raw.sadd("sentinel:watched:wallets", wallet)
                except Exception as e:
                    logger.error(f"Redis connection failed while saving wallet {wallet[:6]}: {e}")

            if sender != "UNKNOWN" and wallet != "UNKNOWN":
                await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                    "entity_id": sender,
                    "action": "LINK_ENTITY",
                    "data": {"target_id": wallet, "target_label": "Wallet", "relation_type": "RELATED_TO", "weight": anomaly}
                }, key=sender)

        entity = Entity(id=wallet, type=EntityType.ORGANIZATION, name=f"Wallet_{wallet[:6]}")

        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "CRYPTO_TRANSFER", EventType.CRYPTO_TRANSFER), 
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="WHALE_TRANSFER",
                side="TRANSFER",
                price=1.0, # Assumes 1.0 peg for calculation simplicity on raw transfers
                size_tokens=notional
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    async def _enrich_liquidation(self, raw, p) -> Optional[NormalizedEvent]:
        """Processes forced closures of leveraged positions on centralized exchanges."""
        asset = p.get("asset", "UNKNOWN")
        side = p.get("side", "UNKNOWN")
        
        try:
            notional = float(p.get("notional_usd", 0))
            price = float(p.get("price", 0))
            qty = float(p.get("size_tokens", 0))
        except (ValueError, TypeError):
            return None

        is_baseline = notional < 500_000

        if is_baseline:
            anomaly = 0.1
            tags = ["crypto", "liquidation", "baseline_data"]
            headline = f"Standard Liquidation: ${notional:,.2f} {asset}"
        else:
            anomaly = min(1.0, notional / 10_000_000 * 0.4)
            tags = ["crypto", "liquidation", asset.lower(), side.lower()]
            headline = f"Massive Liquidation ({side}): ${notional/1e6:.1f}M {asset}"

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": asset,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
        }, key=asset)
        
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "CRYPTO_LIQUIDATION", EventType.CRYPTO_LIQUIDATION),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="LIQUIDATION",
                side=side,
                price=price,
                size_tokens=qty
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )