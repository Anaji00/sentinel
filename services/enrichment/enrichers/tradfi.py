import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from shared.kafka import Topics
from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType, FinancialData,
    AnomalyBreakdown, MarketMicrostructure, ScoreAdjustment
)
from shared.utils import quant_calc
import re 

logger = logging.getLogger("enrichment.tradfi")


# SEC Form 4 Classifications
FORM4_CODES = {
    "P": "Open Market Buy",
    "S": "Open Market Sale",
    "A": "Grant/Award",
    "F": "Tax Withholding",
    "G": "Gift",
    "M": "Option Exercise",
    "X": "Option Exercise",
    "D": "Return to Issuer",
    "J": "Other",
    "C": "Conversion"
}

class TradFiEnricher:
    # Requires redis_client to push dynamic watchlists and train EMA
    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis_client = redis_client
        self.graph = graph_writer

    async def enrich_batch(self, events: list) -> list:
        if not events: return []
        
        equity_trades = []
        other_tasks = []
        
        for raw in events:
            source = raw.source
            if source == "finnhub_equities":
                trade_type = raw.raw_payload.get("trade_type", "RAW_TRADE")
                if trade_type != "OHLCV_MINUTE_BAR":
                    equity_trades.append(raw)
                else:
                    other_tasks.append(self.enrich(raw))
            else:
                other_tasks.append(self.enrich(raw))
                
        results = await asyncio.gather(*other_tasks, return_exceptions=True) if other_tasks else []
        normalized_events = []
        for res in results:
            if isinstance(res, NormalizedEvent):
                normalized_events.append(res)
            elif isinstance(res, list):
                normalized_events.extend(res)
            elif isinstance(res, Exception):
                logger.error(f"Error enriching tradfi batch item: {res}")
                
        if equity_trades:
            batched_results = await self._enrich_equity_trade_batch(equity_trades)
            normalized_events.extend(batched_results)
            
        return normalized_events

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        if source == "finnhub_equities":
            trade_type = p.get("trade_type", "RAW_TRADE")
            if trade_type == "OHLCV_MINUTE_BAR":
                return await self._enrich_equity_candle(raw, p)
            else:   
                # We don't usually call this anymore since enrich_batch handles it, but just in case
                res = await self._enrich_equity_trade_batch([raw])
                return res[0] if res else None
        elif source == "sec_form4":
            return await self._enrich_insider(raw, p)
        elif source == "alpaca_options":
            return await self._enrich_options_flow(raw, p)
        elif source == "alpaca_quant_radar":
            return await self._enrich_quant_radar(raw, p)
        elif source == "finnhub_earnings":
            return await self._enrich_earnings_calendar(raw, p)
            
        return None

    async def _enrich_equity_trade_batch(self, raw_events: list) -> list:
        # Phase 1: Extract Features
        parsed_events = []
        trades_for_scoring = []
        for raw in raw_events:
            p = raw.raw_payload
            ticker = (p.get("ticker") or "").upper()
            if not ticker or ticker == "UNKNOWN": continue
            
            price = float(p.get("close") or p.get("price", 0))
            volume = float(p.get("volume") or p.get("size_shares", 0))
            notional = float(p.get("notional_usd") or (price * volume))
            
            parsed_events.append((raw, p, ticker, price, volume, notional))
            trades_for_scoring.append((ticker, notional, volume))
            
        if not parsed_events: return []
        
        # Phase 2: Batch ML Scoring
        scores = await self.scorer.score_financial_trade_batch("tradfi", trades_for_scoring)
        
        # Batch watchlist and frequency checks concurrently
        check_tasks = []
        for raw, p, ticker, price, volume, notional in parsed_events:
            check_tasks.append(asyncio.gather(
                self.scorer.check_watchlist(ticker, "equities"),
                self.scorer.track_frequency(ticker, "tradfi_block")
            ))
        check_results = await asyncio.gather(*check_tasks)
        
        # Phase 3: Finalize
        results = []
        set_pipe = self.redis_client.raw.pipeline()
        for i, (raw, p, ticker, price, volume, notional) in enumerate(parsed_events):
            anomaly = scores[i]
            is_watched, f_boost = check_results[i]
            w_boost = 0.15 if is_watched else 0.0
            base_score = anomaly
            anomaly = min(1.0, anomaly + w_boost + f_boost)

            # Score adjustment provenance
            adjustments = []
            if w_boost > 0:
                adjustments.append(ScoreAdjustment(reason="watchlist_boost", delta=w_boost))
            if f_boost > 0:
                adjustments.append(ScoreAdjustment(reason="frequency_boost", delta=f_boost))
            
            # Hawkes cross-domain excitation: crypto/prediction market events boost tradfi intensity
            hawkes_ratio = self.scorer.get_hawkes_intensity("tradfi")
            if hawkes_ratio > 1.5:
                # Cross-domain excitation is active — boost anomaly proportionally
                hawkes_boost = min(0.15, (hawkes_ratio - 1.0) * 0.05)
                anomaly = min(1.0, anomaly + hawkes_boost)
                adjustments.append(ScoreAdjustment(reason="hawkes_cross_domain", delta=hawkes_boost))
            
            # Record anomalous events in Hawkes tracker for reciprocal cross-excitation
            if anomaly >= 0.5:
                self.scorer.record_hawkes_event("tradfi")
                
            if price > 0:
                set_pipe.set(f"sentinel:quotes:latest:{ticker}", price, ex=3600)
                
            logger.info(f"🧠 ML INFERENCE | {ticker} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
            
            tags = ["tradfi", "equity_block", ticker.lower()]
            
            aggressor_side = p.get("aggressor_side", p.get("side", "UNKNOWN")).upper()
            if aggressor_side == "UNKNOWN":
                tick_dir = p.get("tick_direction", "")
                if tick_dir == "DownTick":
                    aggressor_side = "SELL"
                elif tick_dir == "UpTick":
                    aggressor_side = "BUY"

            # Volume Order Imbalance (VOI) Tagging
            buy_vol = volume if aggressor_side == "BUY" else 0.0
            sell_vol = volume if aggressor_side == "SELL" else 0.0
            voi = buy_vol - sell_vol

            if volume > 0 and abs(voi) / volume >= 0.60:
                pre_voi = anomaly
                if voi > 0:
                    tags.append("institutional_accumulation")
                    anomaly = min(1.0, anomaly * 1.15)
                else:
                    tags.append("institutional_distribution")
                    anomaly = min(1.0, anomaly * 1.15)
                adjustments.append(ScoreAdjustment(reason="institutional_flow_x1.15", delta=round(anomaly - pre_voi, 6)))
                    
            conditions = str(p.get("conditions", "")).lower()
            is_dark_pool = "out of sequence" in conditions or "average price" in conditions
            if is_dark_pool:
                tags.append("dark_pool_print")
                
            direction_str = "Block Trade"
            
            if aggressor_side == "SELL":
                tags.append("aggressor_sell")
                if not is_dark_pool:
                    tags.append("lit_aggressor_sell")
                    pre_lit = anomaly
                    anomaly = min(1.0, anomaly * 1.2)
                    adjustments.append(ScoreAdjustment(reason="lit_aggressor_sell_x1.2", delta=round(anomaly - pre_lit, 6)))
                if notional > 5_000_000:
                    tags.append("institutional_distribution")
                    pre_dist = anomaly
                    anomaly = min(1.0, anomaly * 1.3)
                    adjustments.append(ScoreAdjustment(reason="large_distribution_x1.3", delta=round(anomaly - pre_dist, 6)))
                    direction_str = "🔴 INSTITUTIONAL DUMP"
            elif aggressor_side == "BUY":
                tags.append("aggressor_buy")
                if notional > 5_000_000:
                    tags.append("institutional_accumulation")
                    pre_acc = anomaly
                    anomaly = min(1.0, anomaly * 1.1)
                    adjustments.append(ScoreAdjustment(reason="accumulation_sweep_x1.1", delta=round(anomaly - pre_acc, 6)))
                    direction_str = "🟢 ACCUMULATION SWEEP"

            if anomaly < 0.35:  # Sensitive floor for correlation store ingest
                continue
                
            results.append(self._finalize_equity_trade(raw, p, ticker, price, volume, notional, tags, direction_str, anomaly, hawkes_ratio, adjustments))
            
        await set_pipe.execute()
        
        final_events = await asyncio.gather(*results) if results else []
        return [e for e in final_events if e]

    async def _finalize_equity_trade(self, raw, p, ticker, price, volume, notional, tags, direction_str, anomaly, hawkes_ratio=0.0, score_adjustments=None):
        if score_adjustments is None:
            score_adjustments = []
        try:
            baseline = await self.redis_client.raw.get(f"baseline:volume:{ticker}")
            if baseline and float(baseline) > 0 and volume > float(baseline) * 20:
                tags.append("volume_capitulation")
                pre_cap = anomaly
                anomaly = min(1.0, anomaly * 1.4)
                score_adjustments.append(ScoreAdjustment(reason="volume_capitulation_x1.4", delta=round(anomaly - pre_cap, 6)))
        except Exception as e:
            logger.debug(f"Baseline fetch failed: {e}")

        await self._sync_geo_watchlist(ticker, tags)
        await self._update_volume_baseline(ticker, volume)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
        }, key=ticker)

        # Attach reference data (sector, industry, exchange) from daily cache
        ref_data = None
        try:
            from services.enrichment.ref_data import get_reference_data
            ref_data = await get_reference_data(self.redis_client, ticker)
            if ref_data:
                sector = ref_data.get("sector", "")
                if sector:
                    # Emit deterministic SECTOR_PEER edge
                    await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                        "entity_id": ticker,
                        "action": "MERGE_SECTOR_EDGE",
                        "data": {"sector": sector, "edge_type": "SECTOR_PEER"}
                    }, key=ticker)
                # Emit INDEX_CO_MEMBER edges for each index this ticker belongs to
                for idx in ref_data.get("index_membership", []):
                    await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                        "entity_id": ticker,
                        "action": "MERGE_INDEX_EDGE",
                        "data": {"index": idx, "edge_type": "INDEX_CO_MEMBER"}
                    }, key=ticker)
        except Exception as e:
            logger.debug(f"Ref data lookup failed for {ticker}: {e}")

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        # Microstructure calculations — rolling trade buffer (mirrors crypto.py pattern)
        aggressor_side = p.get("aggressor_side", p.get("side", "UNKNOWN")).upper()
        buy_vol = volume if aggressor_side == "BUY" else 0.0
        sell_vol = volume if aggressor_side == "SELL" else 0.0
        ofi = quant_calc.order_flow_imbalance(buy_vol, sell_vol)

        # Maintain a 30-trade rolling buffer in Redis for windowed microstructure
        import json
        micro_key = f"sentinel:microstructure:tradfi:{ticker}"
        k_lambda = 0.0
        ami = 0.0
        v_wap = price  # fallback

        try:
            signed_vol = volume if aggressor_side == "BUY" else (-volume if aggressor_side == "SELL" else 0.0)
            trade_record = json.dumps({
                "p": price, "v": volume, "sv": signed_vol, "n": notional,
            })
            pipe = self.redis_client.raw.pipeline()
            pipe.lpush(micro_key, trade_record)
            pipe.ltrim(micro_key, 0, 29)
            pipe.expire(micro_key, 3600)
            await pipe.execute()

            raw_buffer = await self.redis_client.raw.lrange(micro_key, 0, 29)
            if raw_buffer and len(raw_buffer) >= 5:
                prices_buf, volumes_buf, signed_vols_buf, notionals_buf = [], [], [], []
                for entry in raw_buffer:
                    try:
                        t = json.loads(entry)
                        prices_buf.append(float(t["p"]))
                        volumes_buf.append(float(t["v"]))
                        signed_vols_buf.append(float(t["sv"]))
                        notionals_buf.append(float(t["n"]))
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue

                if len(prices_buf) >= 5:
                    # Kyle's λ: ΔP vs signed volume (guarded by n≥10 inside kyle_lambda)
                    price_changes = [prices_buf[i] - prices_buf[i + 1] for i in range(len(prices_buf) - 1)]
                    signed_flows = signed_vols_buf[:-1]  # align with price_changes
                    k_lambda = quant_calc.kyle_lambda(price_changes, signed_flows)

                    # Amihud: proper period returns |r_t| = |p_t/p_{t-1} - 1|
                    if len(prices_buf) >= 2 and all(p_val > 0 for p_val in prices_buf):
                        returns = [abs(prices_buf[i] / prices_buf[i + 1] - 1) for i in range(len(prices_buf) - 1)]
                        ami = quant_calc.amihud_illiquidity(returns, notionals_buf[:-1])

                    # Multi-trade VWAP
                    v_wap = quant_calc.vwap(prices_buf, volumes_buf)
        except Exception as e:
            logger.debug(f"Microstructure buffer computation failed for {ticker}: {e}")

        micro = MarketMicrostructure(
            order_flow_imbalance=ofi,
            vwap=v_wap,
            kyle_lambda=k_lambda,
            amihud_illiquidity=ami,
        )

        breakdown = AnomalyBreakdown(
            composite_score=anomaly,
            volume_z_score=round(anomaly * 2.0, 2),
            cross_domain_correlation_score=round(hawkes_ratio, 4),
            domain="tradfi",
            is_significant=anomaly >= 0.65,
        )

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.EQUITY_BLOCK,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, 
                instrument_type="equity",
                trade_type="RAW_TRADE", 
                premium_usd=notional,
                underlying_price=price,
                volume=volume,
                volume_oi_ratio=p.get("vol_oi_ratio"),
                sector=ref_data.get("sector") if ref_data else None,
                industry=ref_data.get("industry") if ref_data else None,
                exchange=ref_data.get("exchange") if ref_data else None,
                market_cap_tier=ref_data.get("market_cap_tier") if ref_data else None,
                index_membership=ref_data.get("index_membership", []) if ref_data else [],
            ),
            headline=f"🐋 {direction_str} | {ticker} ${notional/1e6:.2f}M | Anomaly: {anomaly:.2f}",
            tags=tags,
            anomaly_score=anomaly,
            anomaly_breakdown=breakdown,
            market_microstructure=micro,
            score_adjustments=score_adjustments,
        )

    async def _enrich_equity_candle(self, raw, p) -> Optional[NormalizedEvent]:
        # 1 minute ohcvl bars for volume spike detection
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None

        open_p = float(p.get("open", 0))
        close_p = float(p.get("close", 0))
        volume = float(p.get("volume", 0))
        low_p = float(p.get("low", 0))
        high_p = float(p.get("high", 0))
        
        # Cache the absolute latest price so the Cointegration Engine can reference it
        if close_p > 0:
            try:
                await self.redis_client.raw.set(f"sentinel:quotes:latest:{ticker}", close_p, ex=3600)
                if ticker in ("^VIX", "VIX"):
                    await self.redis_client.raw.set("sentinel:macro:vix", close_p, ex=86400)
            except Exception as e:
                logger.error(f"Failed to cache latest quote for {ticker}: {e}")
        
        if open_p == 0 or close_p == 0:
            return []
            
        from shared.utils.candles import evaluate_multi_timeframe
        
        ts = raw.occurred_at or datetime.now(timezone.utc)
        
        anomalous_frames = await evaluate_multi_timeframe(
            self.redis_client, self.scorer, domain="tradfi", asset=ticker, 
            ts=ts, open_p=open_p, high_p=high_p, low_p=low_p, close_p=close_p, volume=volume
        )
        
        events = []
        for tf, block, features, anomaly in anomalous_frames:
            price_change_pct = features[0]
            volatility_pct = features[1]
            notional = features[2]
            
            # Score adjustment provenance
            bar_adjustments = []

            # Watchlist & Frequency boost
            is_watched = await self.scorer.check_watchlist(ticker, "equities")
            w_boost = 0.15 if is_watched else 0.0
            f_boost = await self.scorer.track_frequency(ticker, f"tradfi_candle_{tf}m")
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            if w_boost > 0:
                bar_adjustments.append(ScoreAdjustment(reason="watchlist_boost", delta=w_boost))
            if f_boost > 0:
                bar_adjustments.append(ScoreAdjustment(reason="frequency_boost", delta=f_boost))

            # Hawkes cross-domain excitation
            bar_hawkes_ratio = self.scorer.get_hawkes_intensity("tradfi")
            if bar_hawkes_ratio > 1.5:
                hawkes_boost = min(0.15, (bar_hawkes_ratio - 1.0) * 0.05)
                anomaly = min(1.0, anomaly + hawkes_boost)
                bar_adjustments.append(ScoreAdjustment(reason="hawkes_cross_domain", delta=hawkes_boost))
            
            tags = ["tradfi", "market_structure", f"volatile_{tf}m_candle", ticker.lower()]
            await self._sync_geo_watchlist(ticker, tags)
            await self._update_volume_baseline(ticker, block["volume"])
    
            await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
                "entity_id": ticker,
                "action": "MERGE_ONTOLOGY_NODE",
                "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
            }, key=ticker)
            
            entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
            direction = "🟢 Bullish" if block["close"] >= block["open"] else "🔴 Bearish"
            headline = f"{direction} Structural Anomaly: {ticker} {tf}-min moved {price_change_pct*100:.2f}% on ${notional/1e6:.1f}M vol"
    
            # Compute Parkinson volatility for the bar
            parkinson = quant_calc.parkinson_volatility([block["high"]], [block["low"]])

            # Multi-bar microstructure from 15-bar rolling history
            history_vol_key = f"tradfi:history{tf}m:{ticker}:volumes"
            history_not_key = f"tradfi:history{tf}m:{ticker}:notionals"
            history_cls_key = f"tradfi:history{tf}m:{ticker}:closes"

            bar_k_lambda = 0.0
            bar_ami = 0.0
            bar_vwap = block["close"]  # fallback

            try:
                cls_bytes, vol_bytes, not_bytes = await asyncio.gather(
                    self.redis_client.raw.lrange(history_cls_key, 0, 14),
                    self.redis_client.raw.lrange(history_vol_key, 0, 14),
                    self.redis_client.raw.lrange(history_not_key, 0, 14),
                )
                hist_closes = [float(c) for c in reversed(cls_bytes)] if cls_bytes else []
                hist_volumes = [float(v) for v in reversed(vol_bytes)] if vol_bytes else []
                hist_notionals = [float(n) for n in reversed(not_bytes)] if not_bytes else []

                # Append current bar
                hist_closes.append(block["close"])
                hist_volumes.append(block["volume"])
                hist_notionals.append(block["close"] * block["volume"])

                if len(hist_closes) >= 2:
                    # Kyle's λ: ΔP vs signed volumes (guarded by n≥10 inside kyle_lambda)
                    price_changes = [hist_closes[i] - hist_closes[i - 1] for i in range(1, len(hist_closes))]
                    hist_signed_vols = [hist_volumes[i] if hist_closes[i] >= hist_closes[i - 1] else -hist_volumes[i] for i in range(1, len(hist_closes))]
                    bar_k_lambda = quant_calc.kyle_lambda(price_changes, hist_signed_vols)

                    # Amihud: proper period returns |r_t| = |(close_t - close_{t-1}) / close_{t-1}|
                    if all(c > 0 for c in hist_closes):
                        returns = [abs((hist_closes[i] - hist_closes[i - 1]) / hist_closes[i - 1]) for i in range(1, len(hist_closes))]
                        valid_notionals = hist_notionals[1:]
                        if valid_notionals and all(n > 0 for n in valid_notionals):
                            bar_ami = quant_calc.amihud_illiquidity(returns, valid_notionals)

                # Multi-bar VWAP
                if hist_volumes and sum(hist_volumes) > 0:
                    bar_vwap = quant_calc.vwap(hist_closes, hist_volumes)
            except Exception as e:
                logger.debug(f"Multi-bar microstructure failed for {ticker} {tf}m: {e}")

            micro = MarketMicrostructure(
                parkinson_volatility=parkinson,
                vwap=bar_vwap,
                twap=(block["high"] + block["low"] + block["close"]) / 3.0,
                realized_volatility=volatility_pct,
                kyle_lambda=bar_k_lambda,
                amihud_illiquidity=bar_ami,
            )

            breakdown = AnomalyBreakdown(
                composite_score=anomaly,
                volatility_z_score=round(volatility_pct * 10.0, 2),
                cross_domain_correlation_score=round(bar_hawkes_ratio, 4),
                domain="tradfi",
                is_significant=anomaly >= 0.65,
            )

            events.append(NormalizedEvent(
                event_id=raw.event_id, trace_id=raw.trace_id,
                type=EventType.MARKET_ANOMALY,
                occurred_at=datetime.fromisoformat(block["start_ts"]),
                source=raw.source,
                primary_entity=entity,
                financial_data=FinancialData(
                    ticker=ticker, 
                    instrument_type="equity",
                    trade_type=f"OHLCV_{tf}M_BAR", 
                    premium_usd=notional,
                    underlying_price=block["close"],
                    volume=block["volume"],
                    open_price=block["open"],
                    high_price=block["high"],
                    low_price=block["low"],
                    close_price=block["close"]
                ),
                headline=headline,
                tags=tags,
                anomaly_score=anomaly,
                anomaly_breakdown=breakdown,
                market_microstructure=micro,
                score_adjustments=bar_adjustments,
            ))
            
        return events

    async def _sync_geo_watchlist(self, ticker, tags):
        try:
            import json, time
            cached = await self.redis_client.raw.get(f"sentinel:ontology:entity:{ticker.lower()}")
            if cached:
                data = json.loads(cached)
                concepts = data.get("macro_concepts", [])
                if concepts:
                    tags.append("geo_linked_asset")
                    tags.extend(concepts)
                    async with self.redis_client.raw.pipeline(transaction=True) as pipe:
                        pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                        pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
                        await pipe.execute()
        except Exception as e:
            logger.error(f"Failed to update geo watchlist for {ticker}: {e}", exc_info=True)
                
    async def _update_volume_baseline(self, ticker: str, volume: float):
        """EMA baselining (α=0.001) for tick-level block sizes to handle heavy-tailed distributions."""
        try:
            key = f"baseline:volume:{ticker}"
            current = await self.redis_client.raw.get(key)
            updated = (0.999 * float(current) + 0.001 * volume) if current else volume
            await self.redis_client.raw.set(key, str(round(updated, 3)), ex=604800)
        except Exception as e:
            logger.error(f"Failed to update volume baseline for {ticker}: {e}")

    async def _enrich_insider(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker:
            title = p.get("title", "")
            match = re.search(r'\(\s*([A-Za-z]+)\s*\)', title)
            if match:
                ticker = match.group(1).upper()
            else:
                logger.debug(f"Failed to extract ticker from Form 4 payload: {title}")
                return None
        
        # Robustly parse code, value, and role from summary HTML if absent from raw_payload
        summary_html = p.get("summary") or ""
        
        code = p.get("transaction_code")
        if not code:
            code_match = re.search(r'(?:<b>Code:</b>|Code:)\s*([A-Z])', summary_html, re.IGNORECASE)
            code = code_match.group(1).upper() if code_match else "J"
            
        value = p.get("transaction_value_usd")
        if not value:
            val_match = re.search(r'(?:<b>Value:</b>|Value:)\s*\$([0-9,]+(?:\.[0-9]+)?)', summary_html, re.IGNORECASE)
            if val_match:
                try:
                    value = float(val_match.group(1).replace(",", ""))
                except ValueError:
                    value = 0.0
            else:
                value = 0.0
        else:
            value = float(value)
            
        title = p.get("role") or p.get("title") or ""
        if not title:
            rel_match = re.search(r'(?:<b>Relationship:</b>|Relationship:)\s*([^<]+)', summary_html, re.IGNORECASE)
            if rel_match:
                title = rel_match.group(1).strip()
            else:
                title = p.get("title") or ""
        title = title.upper()
        
        # Suppress noise: Ignore standard compensation & tax withholding below $500k
        if code in ("A", "F") and value < 500_000:
            return None
            
        code_label = FORM4_CODES.get(code, "Transaction")
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        
        # Role-based weighting multiplier
        anomaly = min(1.0, value / 10_000_000 * 0.3)
        if "CEO" in title:
            anomaly = min(1.0, anomaly * 1.5)
        elif "CFO" in title:
            anomaly = min(1.0, anomaly * 1.4)
        elif "COO" in title or "PRESIDENT" in title:
            anomaly = min(1.0, anomaly * 1.2)
        elif "DIRECTOR" in title:
            anomaly = min(1.0, anomaly * 1.1)
        elif any(w in title for w in ("TEN PERCENT OWNER", "10% OWNER", "10 PERCENT")):
            anomaly = min(1.0, anomaly * 1.3)

        # Open market buys are high conviction
        if code == "P":
            anomaly = min(1.0, anomaly * 1.2)

        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "insider_trade")
        anomaly = min(1.0, anomaly + w_boost + f_boost)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial"}
        }, key=ticker)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.INSIDER_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, instrument_type="equity", premium_usd=value
            ),
            headline=f"Insider {code_label}: {ticker} ${value/1e6:.1f}M by {title}",
            tags=["tradfi", "insider_trade", ticker.lower(), code_label.lower().replace(" ", "_")],
            anomaly_score=round(anomaly, 3),
        )

    async def _enrich_options_flow(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None
        
        option_symbol = p.get("option_symbol", "")
        price = float(p.get("price", 0.0))
        volume = float(p.get("volume", 0.0))
        premium = float(p.get("premium_usd", 0.0))
        
        option_type = p.get("option_type")
        strike = p.get("strike")
        expiry = p.get("expiry")
        if (not option_type or strike is None or not expiry) and option_symbol:
            from shared.utils.equities import parse_occ_option_symbol
            parsed = parse_occ_option_symbol(option_symbol)
            if parsed:
                option_type = option_type or parsed.get("option_type")
                strike = strike if strike is not None else parsed.get("strike")
                expiry = expiry or parsed.get("expiry")
        
        option_type = str(option_type or "CALL").upper()
        implied_volatility = float(p["implied_volatility"]) if p.get("implied_volatility") is not None else None
        open_interest = int(p["open_interest"]) if p.get("open_interest") is not None else None
        
        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "options_flow")
        
        # Calculate baseline anomaly score based on premium size (e.g. $1M premium -> 0.50 score)
        base_score = min(1.0, premium / 1_000_000.0 * 0.5)
        anomaly = min(1.0, base_score + w_boost + f_boost)
        
        tags = ["tradfi", "options_flow", ticker.lower(), option_type.lower()]
        if premium >= 100000.0:
            tags.append("options_sweep")
            
        entity = Entity(id=ticker, type=EntityType.COMPANY, name=ticker)
        
        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.OPTIONS_FLOW,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="option",
                side=option_type,
                trade_type="OPTIONS_FLOW",
                strike=float(strike) if strike is not None else None,
                expiry=expiry,
                premium_usd=premium,
                underlying_price=price,
                volume=int(volume),
                open_interest=open_interest,
                implied_volatility=implied_volatility,
                option_type=option_type,
            ),
            headline=f"🐋 OPTIONS FLOW {option_type} Sweep | {ticker} ({option_symbol}) | Premium: ${premium/1e3:.1f}k",
            tags=tags,
            anomaly_score=anomaly
        )

    async def _enrich_quant_radar(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker: return None

        z_score = float(p.get("z_score", 0.0))
        volume = float(p.get("volume", 0.0))
        price = float(p.get("price", 0.0))
        notional = float(p.get("notional_usd", 0.0))

        import time as _time
        try:
            await self.redis_client.raw.zadd("sentinel:watched:equities", mapping={ticker: _time.time()})
            await self.redis_client.raw.zremrangebyrank("sentinel:watched:equities", 0, -51)
        except Exception:
            pass

        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "quant_radar")

        base_score = min(1.0, z_score / 5.0)
        anomaly = min(1.0, base_score + w_boost + f_boost)

        tags = ["tradfi", "radar_anomaly", ticker.lower()]
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=EventType.MARKET_ANOMALY,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="equity",
                trade_type="RADAR_ANOMALY",
                premium_usd=notional,
                underlying_price=price,
                volume=volume,
            ),
            headline=f"⚡ QUANT RADAR VOLUME SPIKE | {ticker} | Z-Score: {z_score:.2f} | Flow: ${notional/1e6:.2f}M",
            tags=tags,
            anomaly_score=anomaly,
        )

    async def _enrich_earnings_calendar(self, raw, p) -> Optional[NormalizedEvent]:
        """Enriches Finnhub earnings calendar events into NormalizedEvents.
        Uses dynamic EMA z-score on surprise % for anomaly scoring."""
        import os

        ticker = (p.get("ticker") or "").upper()
        if not ticker:
            return None

        trade_type = p.get("trade_type", "EARNINGS_REPORT")
        report_date = p.get("report_date", "")
        session = p.get("session", "")
        eps_estimate = p.get("eps_estimate")
        eps_actual = p.get("eps_actual")
        eps_surprise_pct = p.get("eps_surprise_pct")
        revenue_estimate = p.get("revenue_estimate")
        revenue_actual = p.get("revenue_actual")

        # Determine event type
        if trade_type == "EARNINGS_SURPRISE" and eps_actual is not None:
            event_type = EventType.EARNINGS_SURPRISE
        else:
            event_type = EventType.EARNINGS_REPORT

        # Anomaly scoring — dynamic EMA z-score on abs(surprise_pct)
        anomaly = 0.3  # Baseline for pre-announcement
        if eps_surprise_pct is not None:
            abs_surprise = abs(eps_surprise_pct)
            # Dynamic z-score against historical surprises for this ticker
            try:
                ema_alpha = float(os.getenv("EARNINGS_EMA_ALPHA", "0.1"))
                ema_key = f"sentinel:earnings:surprise_ema:{ticker}"
                var_key = f"sentinel:earnings:surprise_var:{ticker}"
                raw_mean = await self.redis_client.raw.get(ema_key)
                raw_var = await self.redis_client.raw.get(var_key)
                ema_mean = float(raw_mean) if raw_mean else abs_surprise
                ema_var = float(raw_var) if raw_var else 1.0
                std = max(ema_var ** 0.5, 0.01)
                z = (abs_surprise - ema_mean) / std
                # Update EMA
                new_mean = ema_alpha * abs_surprise + (1 - ema_alpha) * ema_mean
                new_var = ema_alpha * (abs_surprise - ema_mean) ** 2 + (1 - ema_alpha) * ema_var
                pipe = self.redis_client.raw.pipeline()
                pipe.set(ema_key, str(new_mean), ex=604800 * 4)
                pipe.set(var_key, str(new_var), ex=604800 * 4)
                await pipe.execute()
                # Map z-score to anomaly: z>=2 is significant
                anomaly = min(1.0, max(0.3, abs(z) / 4.0))
            except Exception:
                # Fallback: simple scaled surprise
                anomaly = min(1.0, abs_surprise / 50.0)

        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "earnings")
        anomaly = min(1.0, anomaly + w_boost + f_boost)

        # Direction tags
        direction = "beat" if (eps_surprise_pct or 0) > 0 else "miss" if (eps_surprise_pct or 0) < 0 else "inline"
        tags = ["tradfi", "earnings", ticker.lower(), direction]

        if event_type == EventType.EARNINGS_SURPRISE:
            surprise_str = f"{eps_surprise_pct:+.1f}%" if eps_surprise_pct is not None else "N/A"
            headline = f"📊 EARNINGS {'BEAT' if direction == 'beat' else 'MISS' if direction == 'miss' else 'INLINE'} | {ticker} | EPS: {eps_actual} vs Est {eps_estimate} ({surprise_str})"
        else:
            session_label = {"bmo": "Pre-Market", "amc": "After-Close", "dmh": "During Hours"}.get(session, session.upper() if session else "TBD")
            headline = f"📅 EARNINGS UPCOMING | {ticker} | Date: {report_date} ({session_label}) | Est EPS: {eps_estimate}"

        entity = Entity(id=ticker, type=EntityType.COMPANY, name=ticker)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial"}
        }, key=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            trace_id=raw.trace_id,
            type=event_type,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="equity",
                trade_type=trade_type,
                earnings_report_date=report_date,
                earnings_session=session,
                eps_estimate=float(eps_estimate) if eps_estimate is not None else None,
                eps_actual=float(eps_actual) if eps_actual is not None else None,
                eps_surprise_pct=float(eps_surprise_pct) if eps_surprise_pct is not None else None,
                revenue_estimate=float(revenue_estimate) if revenue_estimate is not None else None,
                revenue_actual=float(revenue_actual) if revenue_actual is not None else None,
            ),
            headline=headline,
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )