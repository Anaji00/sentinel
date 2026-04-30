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


logger = logging.getLogger("enrichment.crypto")

class CryptoEnricher:
    """
    Standardizes cryptocurrency events. It uses a routing mechanism to handle 
    different types of crypto data sources (e.g., on-chain RPCs vs. CEX WebSockets).
    """
    def __init__(self, scorer, redis_client):
        # The scorer is used to determine how "unusual" an event is.
        # This class calculates baseline anomaly scores mathematically.
        self.scorer = scorer
        self.redis = redis_client
    

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        # Extract the raw dictionary payload and the source identifier
        p = raw.raw_payload
        source = raw.source

        # ROUTER: Decide which private processing method to use based on the source
        if source == "ethereum_rpc":
            return self._enrich_whale_transfer(raw, p)
        elif source == "binance_futures":
            return self._enrich_liquidation(raw, p)
        elif source == "binance_spot":
            return self._enrich_spot_trade(raw, p)
        elif source == "binance_candles":
            return self._enrich_candle(raw, p)
        
        # If the source is unknown, we drop the event by returning None
        return None
    
    def _enrich_spot_trade(self, raw, p) -> Optional[NormalizedEvent]:
        asset = p.get("asset", "UNKNOWN").upper()
        side = p.get("side", "UNKNOWN").upper()

        try:
            price = float(p.get("price", 0))
            qty = float(p.get("size_tokens", 0))
            notional = float(p.get("notional_usd", 0))
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse price/qty for spot trade: {e}")
            return None
# ISOLATION FOREST SCORING: 
        # Compare this trade's notional value against the recent historical distribution for THIS specific asset.

        anomaly = self.scorer.score_crypto_trade(asset, notional, qty)
        if anomaly < 0.6:
            return None
    
        tags = ["crypto", "spot_trade", asset.lower(), side.lower()]
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)
        headline = f"🐋 ML Outlier CRYPTO Trade ({side}): ${notional/1e6:.2f}M {asset} at ${price:,.2f}"
        return NormalizedEvent(
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
        )
    
    def _enrich_candle(self, raw, p) -> Optional[NormalizedEvent]:
        asset = p.get("asset", "UNKNOWN").upper()
        try:
            open_p = float(p.get("open", 0))
            close_p = float(p.get("close", 0))
            high_p = float(p.get("high", 0))
            low_p = float(p.get("low", 0))
            volume = float(p.get("volume", 0))
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse candle data for {asset}: {e}")
            return None
        if open_p == 0 or close_p == 0:
            return None
        price_change_pct = abs((close_p - open_p) / open_p)
        volatility_pct = (high_p - low_p) / open_p
        notional_volume = close_p * volume
        
        features = [price_change_pct, volatility_pct, notional_volume]
        anomaly = self.scorer.score_crypto_candle(asset, features)
    
        if anomaly < 0.6:
            return None
        
        direction = "🟢 Bullish" if close_p >= open_p else "🔴 Bearish"
        tags = ["crypto", "market_structure", "volatile_candle", direction, asset.lower()]
        headline = f"{direction} Crypto Anomaly: {asset} moved {price_change_pct*100:.2f}% on ${notional_volume/1e6:.1f}M vol"
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.MARKET_ANOMALY,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="OHLCV_1M",
                side="MARKET",
                price=close_p,
                size_tokens=volume,
                open_price=open_p,
                high_price=high_p,
                low_price=low_p,
                close_price=close_p
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    def _enrich_whale_transfer(self, raw, p) -> Optional[NormalizedEvent]:
        """Processes large on-chain token/coin movements (whale transfers)."""
        wallet = p.get("receiver_wallet", "UNKNOWN")
        asset = p.get("asset", "UNKNOWN").upper()
        is_suspect = p.get("is_suspect_wallet", False)

        # Safely attempt to parse the monetary value. Fallback to 0.0 if invalid.
        try:
            notional = float(p.get("notional_usd", 0))
        except (ValueError, TypeError):
            notional = 0.0

        # DATA BASELINING: Define what is "boring" normal traffic vs an anomaly
        # Transfers under $1M that aren't to suspect wallets are baseline noise.
        is_baseline = notional < 1_000_000 and not is_suspect

        if is_baseline:
            # Give normal traffic a very low anomaly score (0.1 out of 1.0)
            anomaly = 0.1
            tags = ["crypto", "transfer", "baseline_data"]
            headline = f"Standard Transfer: ${notional/1e6:.2f}M {asset}"
        else:
            # Calculate score based on size (e.g., $50M+ maxes out the size score at 0.5)
            anomaly = min(1.0, notional / 50_000_000 * 0.5)
            # Add a flat 0.4 penalty if the wallet is flagged as a suspect actor
            if is_suspect: anomaly = min(1.0, anomaly + 0.4)
            
            tags = ["crypto", "whale_transfer", asset.lower()]
            if is_suspect: tags.append("suspect_wallet")
            headline = f"{'🚨 SUSPECT ' if is_suspect else ''}Whale Transfer: ${notional/1e6:.1f}M {asset}"

            if notional > 5_000_00:
                try:
                    self.redis.sadd("sentinel:watched:wallets", wallet)
                except Exception as e:
                    logger.error(f"Redis connection failed while saving wallet {wallet[:6]}: {e}")

        # Create a unified Entity object to represent the wallet in our graph database
        entity = Entity(id=wallet, type=EntityType.ORGANIZATION, name=f"Wallet_{wallet[:6]}")

        # Package everything into our system-wide NormalizedEvent format
        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "CRYPTO_TRANSFER", EventType.CRYPTO_TRANSFER), 
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            # Package specific crypto metrics using the new CryptoData model
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

    def _enrich_liquidation(self, raw, p) -> Optional[NormalizedEvent]:
        """Processes forced closures of leveraged positions on centralized exchanges."""
        asset = p.get("asset", "UNKNOWN")
        side = p.get("side", "UNKNOWN")
        
        # Safely extract the size and price of the liquidation
        try:
            notional = float(p.get("notional_usd", 0))
            price = float(p.get("price", 0))
            qty = float(p.get("size_tokens", 0))
        except (ValueError, TypeError):
            # Drop the event if we can't do the math
            return None

        # DATA BASELINING: Keep small liquidations for market-wide volume tracking
        is_baseline = notional < 500_000

        if is_baseline:
            anomaly = 0.1
            tags = ["crypto", "liquidation", "baseline_data"]
            headline = f"Standard Liquidation: ${notional:,.2f} {asset}"
        else:
            # Scale the anomaly score up to 0.4 based on how close it is to a $10M liquidation
            anomaly = min(1.0, notional / 10_000_000 * 0.4)
            tags = ["crypto", "liquidation", asset.lower(), side.lower()]
            headline = f"Massive Liquidation ({side}): ${notional/1e6:.1f}M {asset}"

        # The primary entity here is the asset itself (e.g., BTC, ETH)
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        # Package everything into our system-wide NormalizedEvent format
        return NormalizedEvent(
            event_id=raw.event_id,
            type=getattr(EventType, "CRYPTO_LIQUIDATION", EventType.CRYPTO_LIQUIDATION),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            # Package specific crypto metrics using the new CryptoData model
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