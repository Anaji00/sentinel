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
import math
from shared.utils.metrics import MetricsCollector

logger = logging.getLogger("enrichment.crypto")


# What actually counts as a whale on-chain movement, in USD.
WHALE_NOTIONAL_USD = 1_000_000.0

# Where the log scale starts. Below this a transfer is ordinary retail traffic.
_NOTIONAL_FLOOR_USD = 10_000.0

# Where it saturates. A move this large is as anomalous as the scale reports.
_NOTIONAL_CEILING_USD = 100_000_000.0


def _notional_score(notional: float) -> float:
    """Anomaly contribution of a transfer's size, on a log scale.

    Transfer sizes span nine orders of magnitude, and the previous linear map
    (`notional / 50_000_000 * 0.5`) gave a $5M movement a score of 0.05 -- below
    the noise floor of every other domain -- while reserving anything
    meaningful for $50M and up. Log scaling puts $10k at 0, $1M at about 0.5 and
    $100M at 1.0, so the ranking between two whale transfers is informative
    rather than uniformly maximal.
    """
    try:
        value = float(notional)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(value) or value <= _NOTIONAL_FLOOR_USD:
        return 0.0
    if value >= _NOTIONAL_CEILING_USD:
        return 1.0
    span = math.log10(_NOTIONAL_CEILING_USD) - math.log10(_NOTIONAL_FLOOR_USD)
    return round((math.log10(value) - math.log10(_NOTIONAL_FLOOR_USD)) / span, 4)


def _money(usd: float) -> str:
    """Formats an amount at a unit that shows it.

    Every headline read "$0.0M" because `notional/1e6` was used for all sizes,
    so a $50 transfer and a $500,000 one were displayed identically -- which is
    also why a stream of genuinely distinct transfers looked like the same event
    repeated.
    """
    try:
        value = float(usd)
    except (TypeError, ValueError):
        return "$0"
    if not math.isfinite(value):
        return "$0"
    if abs(value) >= 1e9:
        return f"${value / 1e9:.2f}B"
    if abs(value) >= 1e6:
        return f"${value / 1e6:.2f}M"
    if abs(value) >= 1e3:
        return f"${value / 1e3:.1f}K"
    return f"${value:,.0f}"

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
            # Matched on trade_type, not venue. Funding is funding whoever
            # reports it, and pinning these to "binance_futures" meant the OKX
            # poller -- added because Binance answers 451 from this host -- had
            # its events silently dropped here after being collected correctly.
            elif trade_type == "CRYPTO_PERP_FUNDING":
                other_tasks.append(self._enrich_funding_rate(raw, p))
            elif trade_type == "OPEN_INTEREST":
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
            
            # Cache latest quote for live stream APIs
            try:
                if self.redis and hasattr(self.redis, "raw") and price > 0:
                    clean_asset = asset.replace("USDT", "").replace("USD", "").upper()
                    pipe = self.redis.raw.pipeline()
                    pipe.set(f"sentinel:quotes:latest:{clean_asset}", str(price), ex=3600)
                    pipe.set(f"sentinel:quotes:latest:{clean_asset}USD", str(price), ex=3600)
                    pipe.set(f"sentinel:quotes:latest:{clean_asset}USDT", str(price), ex=3600)
                    asyncio.create_task(pipe.execute())
            except Exception as e:
                # Quote cache write. Non-fatal for this event, but a persistent
                # failure means every downstream consumer reads stale prices,
                # so it is counted rather than discarded.
                MetricsCollector.increment("enrichment_quote_cache_errors_total")
                logger.warning("Quote cache write failed for %s: %s", asset, e)
            
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
            # Amihud pairs |return| with the notional traded over the same step,
            # so the two lists are built together: dividing by prices[i + 1] was
            # unguarded, and the collectors write 0.0 for a trade that arrived
            # without a price, so a single bad tick made the estimate infinite.
            # Filtering only `returns` would desynchronise it from the notionals
            # and amihud_illiquidity would quietly answer 0.0 on the length
            # mismatch, which loses the measurement instead of fixing it.
            returns, valid_notionals = [], []
            for i in range(len(prices) - 1):
                base = prices[i + 1]
                notional = notionals[i] if i < len(notionals) else 0.0
                if base and notional > 0:
                    returns.append(abs(prices[i] / base - 1))
                    valid_notionals.append(notional)
            ami = quant_calc.amihud_illiquidity(
                returns, valid_notionals
            ) if len(returns) >= 4 else 0.0
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

        # Prefer the value the collector already put on the event. The Redis
        # lookup below reads a key written only by the Binance OI poller, which
        # cannot run from this host (HTTP 451) -- so open_interest arrived null
        # even when the event carried it, as OKX's events do.
        oi_value = None
        payload_oi = p.get("open_interest")
        if payload_oi is not None:
            try:
                oi_value = float(payload_oi)
            except (TypeError, ValueError):
                oi_value = None
        try:
            if oi_value is None:
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
                # The instrument, not the asset. Entity stays the asset (BTC),
                # which is what an analyst reasons about and what everything
                # groups by; pair says which contract produced the number, so
                # BTC-USDT-SWAP and BTC-USDC-SWAP stay distinguishable without
                # fragmenting BTC into several "entities" -- which is what the
                # Binance rows did, leaving BTCUSDT, BTCUSDC and BTCUSD_PERP as
                # three separate subjects in the graph.
                pair=(p.get("pair") or asset),
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
        except Exception as e:
            # EMA baseline write. Losing this silently means the open-interest
            # z-score is computed against a baseline that stopped updating --
            # the detector keeps reporting, on stale statistics.
            MetricsCollector.increment("enrichment_oi_baseline_errors_total")
            logger.warning("Open-interest baseline update failed: %s", e)

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

        # Size and provenance are separate signals and are no longer conflated.
        #
        # `notional < 1_000_000 and not is_suspect` meant a transfer from a
        # watched wallet skipped the size test entirely, so a 50 USDC movement
        # was published as "SUSPECT Whale Transfer" at CRITICAL. Because
        # notional/50_000_000 is ~0 at that size, every such event scored
        # 0 + 0.4 + boosts = exactly 0.75 regardless of amount -- 51,771 crypto
        # transfers in 24 hours, indistinguishable from one another and from a
        # genuine nine-figure move.
        is_whale = notional >= WHALE_NOTIONAL_USD

        # Log-scaled, because transfer sizes span nine orders of magnitude.
        # Linear scaling gave a $5M move a score of 0.05 -- less than the noise
        # floor -- while only a $50M+ move registered at all.
        size_score = _notional_score(notional)

        if not is_whale and not is_suspect:
            anomaly = round(min(0.35, size_score), 4)
            tags = ["crypto", "transfer", "baseline_data"]
            headline = f"Transfer: {_money(notional)} {asset}"
        else:
            anomaly = size_score
            if is_suspect:
                # A watched counterparty is a reason to look, not a verdict. It
                # lifts the floor rather than replacing the size signal.
                anomaly = max(anomaly, 0.45)

            is_w_sender = await self.scorer.check_watchlist(sender, "wallets") if sender != "UNKNOWN" else False
            is_w_receiver = await self.scorer.check_watchlist(wallet, "wallets") if wallet != "UNKNOWN" else False
            w_boost = 0.15 if (is_w_sender or is_w_receiver) else 0.0
            f_boost = await self.scorer.track_frequency(wallet, "crypto_transfer")
            anomaly = round(min(1.0, anomaly + w_boost + f_boost), 4)

            tags = ["crypto", asset.lower()]
            tags.append("whale_transfer" if is_whale else "watched_wallet_transfer")
            if is_suspect:
                tags.append("suspect_wallet")

            # The label states which signal fired. Calling a $50 movement a
            # whale transfer is simply untrue, and it trained the reader to
            # ignore the word.
            kind = "Whale Transfer" if is_whale else "Watched Wallet Transfer"
            headline = f"{'🚨 ' if is_suspect else ''}{kind}: {_money(notional)} {asset}"

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

        entity = Entity(id=wallet, type=EntityType.WALLET, name=f"Wallet_{wallet[:6]}")

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