import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, FinancialData
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
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            
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
                if voi > 0:
                    tags.append("institutional_accumulation")
                    anomaly = min(1.0, anomaly * 1.15)
                else:
                    tags.append("institutional_distribution")
                    anomaly = min(1.0, anomaly * 1.15)
                    
            conditions = str(p.get("conditions", "")).lower()
            is_dark_pool = "out of sequence" in conditions or "average price" in conditions
            if is_dark_pool:
                tags.append("dark_pool_print")
                
            direction_str = "Block Trade"
            
            if aggressor_side == "SELL":
                tags.append("aggressor_sell")
                if not is_dark_pool:
                    tags.append("lit_aggressor_sell")
                    anomaly = min(1.0, anomaly * 1.2)
                if notional > 5_000_000:
                    tags.append("institutional_distribution")
                    anomaly = min(1.0, anomaly * 1.3)
                    direction_str = "🔴 INSTITUTIONAL DUMP"
            elif aggressor_side == "BUY":
                tags.append("aggressor_buy")
                if notional > 5_000_000:
                    tags.append("institutional_accumulation")
                    anomaly = min(1.0, anomaly * 1.1)
                    direction_str = "🟢 ACCUMULATION SWEEP"

            if anomaly < 0.6:  # Strict floor
                continue
                
            results.append(self._finalize_equity_trade(raw, p, ticker, price, volume, notional, tags, direction_str, anomaly))
            
        await set_pipe.execute()
        
        final_events = await asyncio.gather(*results) if results else []
        return [e for e in final_events if e]

    async def _finalize_equity_trade(self, raw, p, ticker, price, volume, notional, tags, direction_str, anomaly):
        try:
            baseline = await self.redis_client.raw.get(f"baseline:volume:{ticker}")
            if baseline and float(baseline) > 0 and volume > float(baseline) * 20:
                tags.append("volume_capitulation")
                anomaly = min(1.0, anomaly * 1.4)
        except Exception as e:
            logger.debug(f"Baseline fetch failed: {e}")

        await self._sync_geo_watchlist(ticker, tags)
        await self._update_volume_baseline(ticker, volume)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
        }, key=ticker)

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

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
                volume_oi_ratio=p.get("vol_oi_ratio")
            ),
            headline=f"🐋 {direction_str} | {ticker} ${notional/1e6:.2f}M | Anomaly: {anomaly:.2f}",
            tags=tags,
            anomaly_score=anomaly,
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
            
            # Watchlist & Frequency boost
            is_watched = await self.scorer.check_watchlist(ticker, "equities")
            w_boost = 0.15 if is_watched else 0.0
            f_boost = await self.scorer.track_frequency(ticker, f"tradfi_candle_{tf}m")
            anomaly = min(1.0, anomaly + w_boost + f_boost)
            
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
        
        # Watchlist & Frequency boost
        is_watched = await self.scorer.check_watchlist(ticker, "equities")
        w_boost = 0.15 if is_watched else 0.0
        f_boost = await self.scorer.track_frequency(ticker, "options_flow")
        
        # Calculate baseline anomaly score based on premium size (e.g. $1M premium -> 0.50 score)
        base_score = min(1.0, premium / 1_000_000.0 * 0.5)
        anomaly = min(1.0, base_score + w_boost + f_boost)
        
        tags = ["tradfi", "options_flow", ticker.lower()]
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
                trade_type="OPTIONS_FLOW",
                premium_usd=premium,
                underlying_price=price,
                volume=int(volume)
            ),
            headline=f"🐋 OPTIONS FLOW Sweep | {ticker} ({option_symbol}) | Premium: ${premium/1e3:.1f}k",
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