"""
services/enrichment/enrichers/crypto.py

This module is responsible for taking raw, unstructured crypto data (like blockchain 
transfers or exchange liquidations) and transforming it into a standardized 
`NormalizedEvent` that the rest of the Sentinel system can process and analyze.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from shared.models import NormalizedEvent, EventType, Entity, EntityType, CryptoData, MarketMicrostructure
from shared.kafka import Topics
from shared.utils import quant_calc
import asyncio

logger = logging.getLogger("enrichment.crypto")

# Rolling trade buffer size for microstructure calculations (configurable)
MICRO_BUFFER_SIZE = int(os.getenv("CRYPTO_MICRO_BUFFER_SIZE", "100"))


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
            trade_type = p.get("trade_type", "")
            if source == "ethereum_rpc":
                other_tasks.append(self._enrich_whale_transfer(raw, p))
            elif source == "binance_futures" and trade_type == "LIQUIDATION":
                other_tasks.append(self._enrich_liquidation(raw, p))
            elif source == "binance_futures" and trade_type == "CRYPTO_PERP_FUNDING":
                other_tasks.append(self._enrich_funding_rate(raw, p))
            elif source == "binance_futures" and trade_type == "OPEN_INTEREST":
                other_tasks.append(self._enrich_open_interest(raw, p))
            elif source == "coinbase_spot":
                spot_trades.append((raw, p))
            elif source == "coinbase_candles":
                other_tasks.append(self._enrich_candle(raw, p))
            
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
        
        # Batch watchlist and frequency checks concurrently
        check_tasks = []
        for raw, p, asset, side, price, qty, notional in parsed_events:
            check_tasks.append(asyncio.gather(
                self.scorer.check_watchlist(asset, "wallets"),
                self.scorer.check_watchlist(asset, "equities"),
                self.scorer.track_frequency(asset, "crypto_spot")
            ))
        check_results = await asyncio.gather(*check_tasks)
        
        results = []
        for i, (raw, p, asset, side, price, qty, notional) in enumerate(parsed_events):
            anomaly = scores[i]
            is_watched_wallets, is_watched_equities, f_boost = check_results[i]
            is_watched = is_watched_wallets or is_watched_equities
            w_boost = 0.15 if is_watched else 0.0
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            
            # Hawkes cross-domain excitation: tradfi/prediction events boost crypto intensity
            hawkes_ratio = self.scorer.get_hawkes_intensity("crypto")
            if hawkes_ratio > 1.5:
                hawkes_boost = min(0.15, (hawkes_ratio - 1.0) * 0.05)
                anomaly = min(1.0, anomaly + hawkes_boost)
            
            # Record anomalous events in Hawkes tracker for reciprocal cross-excitation
            if anomaly >= 0.5:
                self.scorer.record_hawkes_event("crypto")
            
            from shared.utils.candles import get_domain_tag
            domain_tag = get_domain_tag("crypto", asset)
            logger.info(f"🧠 ML INFERENCE [{domain_tag}] | {asset} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
            if anomaly < 0.6: continue
            
            tags = ["crypto", "spot_trade", asset.lower(), side.lower()]
            entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)
            headline = f"🐋 ML Outlier CRYPTO Trade ({side}): ${notional/1e6:.2f}M {asset} at ${price:,.2f}"

            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": asset,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
            }, key=asset)

            # Phase 3: Compute microstructure from rolling trade buffer
            micro = await self._compute_crypto_microstructure(asset, price, qty, side)

            results.append(NormalizedEvent(
                event_id=raw.event_id, trace_id=raw.trace_id,
                type=EventType.CRYPTO_TRADE,
                occurred_at=raw.occurred_at or datetime.now(timezone.utc),
                source=raw.source,
                primary_entity=entity,
                crypto_data=CryptoData(
                    pair=asset,
                    trade_type="LARGE_SPOT",
                    side=side,
                    price=price,
                    size_tokens=qty,
                    market_microstructure=micro,
                ),
                headline=headline,
                tags=tags,
                anomaly_score=round(anomaly, 3),
                market_microstructure=micro,
            ))
            
        return results

    # ── Phase 3: Crypto Microstructure from Rolling Trade Buffer ───────────────

    async def _compute_crypto_microstructure(
        self, asset: str, price: float, qty: float, side: str
    ) -> Optional[MarketMicrostructure]:
        """
        Maintains a rolling trade buffer in Redis (sentinel:microstructure:crypto:{asset})
        and computes OFI, Kyle's lambda, and Amihud illiquidity from real trade sequences.
        Mirrors the tradfi enricher pattern.
        """
        redis_key = f"sentinel:microstructure:crypto:{asset}"
        try:
            # Push this trade into the rolling buffer
            trade_record = json.dumps({
                "p": price, "q": qty, "s": side,
                "ts": datetime.now(timezone.utc).isoformat(),
            })
            pipe = self.redis.raw.pipeline()
            pipe.lpush(redis_key, trade_record)
            pipe.ltrim(redis_key, 0, MICRO_BUFFER_SIZE - 1)
            pipe.expire(redis_key, 3600)
            await pipe.execute()

            # Fetch the buffer for computation
            raw_buffer = await self.redis.raw.lrange(redis_key, 0, MICRO_BUFFER_SIZE - 1)
            if not raw_buffer or len(raw_buffer) < 5:
                return None

            prices = []
            volumes = []
            buy_vol = 0.0
            sell_vol = 0.0
            notionals = []

            for entry in raw_buffer:
                try:
                    t = json.loads(entry)
                    p = float(t["p"])
                    q = float(t["q"])
                    s = t.get("s", "").upper()
                    prices.append(p)
                    volumes.append(q)
                    notionals.append(p * q)
                    if s == "BUY":
                        buy_vol += q
                    elif s == "SELL":
                        sell_vol += q
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue

            if len(prices) < 5:
                return None

            ofi = quant_calc.order_flow_imbalance(buy_vol, sell_vol)

            # Kyle's λ: ΔP vs signed volume (guarded by n≥10 price changes)
            price_changes = [prices[i] - prices[i + 1] for i in range(len(prices) - 1)]
            # Build signed volumes from buffer side info
            signed_volumes_computed = []
            for i in range(len(prices) - 1):
                try:
                    t = json.loads(raw_buffer[i])
                    s = t.get("s", "").upper()
                    sv = volumes[i] if s == "BUY" else (-volumes[i] if s == "SELL" else 0.0)
                    signed_volumes_computed.append(sv)
                except (json.JSONDecodeError, KeyError, ValueError):
                    signed_volumes_computed.append(0.0)

            k_lambda = quant_calc.kyle_lambda(price_changes, signed_volumes_computed) if len(price_changes) >= 10 else 0.0
            returns = [abs(prices[i] / prices[i + 1] - 1) for i in range(len(prices) - 1)]
            valid_notionals = notionals[:-1]
            ami = quant_calc.amihud_illiquidity(
                returns,
                valid_notionals
            ) if len(prices) >= 5 and all(n > 0 for n in valid_notionals) else 0.0
            v = quant_calc.vwap(prices, volumes) if volumes else price

            return MarketMicrostructure(
                order_flow_imbalance=ofi,
                kyle_lambda=k_lambda,
                amihud_illiquidity=ami,
                vwap=v,
            )
        except Exception as e:
            logger.debug(f"Crypto microstructure computation failed for {asset}: {e}")
            return None

    # ── Phase 2: Funding Rate Anomaly Enrichment ──────────────────────────────

    async def _enrich_funding_rate(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Enriches extreme funding rate events from the !markPrice@arr@1s stream.
        Uses the same self-calibrating RRCF/EMA gate as all other domains.
        """
        asset = p.get("asset", "UNKNOWN").upper()
        funding_rate = p.get("funding_rate", 0.0)
        mark_price = p.get("mark_price", 0.0)
        index_price = p.get("index_price", 0.0)
        basis_bps = p.get("basis_bps", 0.0)
        next_funding_time = p.get("next_funding_time", 0)

        if mark_price <= 0 or index_price <= 0:
            return None

        # Score through RRCF — features: [funding_rate_abs_bps, basis_bps_abs, mark/index ratio, 0, 0]
        features = [
            abs(funding_rate) * 10000.0,  # Scale to basis points for RRCF sensitivity
            abs(basis_bps),
            mark_price / max(index_price, 1.0),
            0.0,
            0.0,
        ]
        score_result = await self.scorer.score_event("crypto_trade", asset, features)
        anomaly = score_result["score"]

        # Watchlist boost
        is_watched = await self.scorer.check_watchlist(asset, "equities")
        if is_watched:
            anomaly = min(1.0, anomaly + 0.15)

        # Fetch latest OI from Redis to attach as context (no separate API call)
        oi_value = None
        try:
            raw_oi = await self.redis.raw.get(f"sentinel:crypto:oi:{asset}")
            if raw_oi:
                oi_value = float(raw_oi)
        except Exception:
            pass

        direction = "positive" if funding_rate > 0 else "negative"
        annualized_carry = abs(funding_rate) * 3 * 365 * 100  # 8h periods * 365 days * 100%
        headline = (
            f"⚡ FUNDING RATE EXTREME ({direction}) | {asset} | "
            f"Rate: {funding_rate:.6f} ({annualized_carry:.1f}% annualized) | "
            f"Basis: {basis_bps:.2f}bps"
        )

        tags = ["crypto", "funding_rate", asset.lower(), direction]
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": asset,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
        }, key=asset)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.CRYPTO_PERP_FUNDING,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="CRYPTO_PERP_FUNDING",
                side=direction.upper(),
                price=mark_price,
                size_tokens=0.0,
                funding_rate=funding_rate,
                mark_price=mark_price,
                index_price=index_price,
                basis_bps=round(basis_bps, 4),
                open_interest=oi_value,
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    # ── Phase 2: Open Interest Enrichment ─────────────────────────────────────

    async def _enrich_open_interest(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Enriches open interest snapshots. Detects OI surges via dynamic z-score
        (EMA mean/variance in Redis) — no hardcoded thresholds.
        """
        asset = p.get("asset", "UNKNOWN").upper()
        oi_value = float(p.get("open_interest", 0))
        symbol = p.get("symbol", asset)

        if oi_value <= 0:
            return None

        # Dynamic z-score for OI surge detection
        oi_z = 0.0
        try:
            ema_alpha = float(os.getenv("OI_EMA_ALPHA", "0.05"))
            ema_key = f"sentinel:crypto:oi_ema:{symbol}"
            var_key = f"sentinel:crypto:oi_var:{symbol}"
            raw_mean = await self.redis.raw.get(ema_key)
            raw_var = await self.redis.raw.get(var_key)
            ema_mean = float(raw_mean) if raw_mean else oi_value
            ema_var = float(raw_var) if raw_var else 1.0
            std = max(ema_var ** 0.5, 1e-5)
            oi_z = (oi_value - ema_mean) / std
            # Update EMA
            new_mean = ema_alpha * oi_value + (1 - ema_alpha) * ema_mean
            new_var = ema_alpha * (oi_value - ema_mean) ** 2 + (1 - ema_alpha) * ema_var
            pipe = self.redis.raw.pipeline()
            pipe.set(ema_key, str(new_mean), ex=604800)
            pipe.set(var_key, str(new_var), ex=604800)
            await pipe.execute()
        except Exception:
            pass

        # Only emit as anomaly if OI z-score is significant
        oi_z_threshold = float(os.getenv("OI_ZSCORE_TRIGGER", "2.0"))
        if abs(oi_z) < oi_z_threshold:
            return None

        # Fetch latest price for context
        latest_price = 0.0
        try:
            funding_raw = await self.redis.raw.get(f"sentinel:crypto:funding:{symbol}")
            if funding_raw:
                fd = json.loads(funding_raw)
                latest_price = float(fd.get("mark_price", 0))
        except Exception:
            pass

        anomaly = min(1.0, abs(oi_z) / 5.0)  # Scale z-score to 0-1 range
        direction = "surging" if oi_z > 0 else "collapsing"
        headline = (
            f"📊 OPEN INTEREST {direction.upper()} | {asset} | "
            f"OI: {oi_value:,.0f} | Z-Score: {oi_z:.2f}"
        )

        tags = ["crypto", "open_interest", asset.lower(), direction]
        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.MARKET_ANOMALY,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="OPEN_INTEREST",
                side="MARKET",
                price=latest_price,
                size_tokens=0.0,
                open_interest=oi_value,
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )

    # ── Existing enrichment methods ───────────────────────────────────────────

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
            
            # Watchlist & Frequency boost
            is_watched = await self.scorer.check_watchlist(asset, "wallets") or await self.scorer.check_watchlist(asset, "equities")
            w_boost = 0.15 if is_watched else 0.0
            f_boost = await self.scorer.track_frequency(asset, f"crypto_candle_{tf}m")
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            
            tags = ["crypto", "market_structure", f"volatile_{tf}m_candle", asset.lower()]

            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": asset,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
            }, key=asset)
            
            entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)
            direction = "🟢 Bullish" if block["close"] >= block["open"] else "🔴 Bearish"
            headline = f"{direction} Structural Anomaly: {asset} {tf}-min moved {price_change_pct*100:.2f}% (Range Vol: {volatility_pct*100:.2f}%) on ${notional/1e6:.1f}M vol"
    
            events.append(NormalizedEvent(
                event_id=raw.event_id, trace_id=raw.trace_id,
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
            
            # Watchlist & Frequency boost
            is_w_sender = await self.scorer.check_watchlist(sender, "wallets") if sender != "UNKNOWN" else False
            is_w_receiver = await self.scorer.check_watchlist(wallet, "wallets") if wallet != "UNKNOWN" else False
            w_boost = 0.15 if (is_w_sender or is_w_receiver) else 0.0
            f_boost = await self.scorer.track_frequency(wallet, "crypto_transfer")
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            
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
            event_id=raw.event_id, trace_id=raw.trace_id,
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
            
            # Liquidation events are the strongest Hawkes excitation source:
            # crypto liquidation cascades → tradfi anomaly intensity spikes
            self.scorer.record_hawkes_event("crypto")

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": asset,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "CryptoAsset", "primary_domain": "financial", "confidence": anomaly}
        }, key=asset)
        
        # Attach latest funding rate and OI context to liquidation events
        funding_rate = None
        basis_bps = None
        oi_value = None
        try:
            asset_upper = asset.upper()
            # Try common Binance symbol formats dynamically
            for sym_candidate in [asset_upper, asset_upper.replace("usdt", "").upper() + "USDT"]:
                funding_raw = await self.redis.raw.get(f"sentinel:crypto:funding:{sym_candidate}")
                if funding_raw:
                    fd = json.loads(funding_raw)
                    funding_rate = fd.get("funding_rate")
                    basis_bps = fd.get("basis_bps")
                    break
            for sym_candidate in [asset_upper, asset_upper.replace("usdt", "").upper() + "USDT"]:
                oi_raw = await self.redis.raw.get(f"sentinel:crypto:oi:{sym_candidate}")
                if oi_raw:
                    oi_value = float(oi_raw)
                    break
        except Exception:
            pass

        entity = Entity(id=asset, type=EntityType.INSTRUMENT, name=asset)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=getattr(EventType, "CRYPTO_LIQUIDATION", EventType.CRYPTO_LIQUIDATION),
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            crypto_data=CryptoData(
                pair=asset,
                trade_type="LIQUIDATION",
                side=side,
                price=price,
                size_tokens=qty,
                funding_rate=funding_rate,
                basis_bps=basis_bps,
                open_interest=oi_value,
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )