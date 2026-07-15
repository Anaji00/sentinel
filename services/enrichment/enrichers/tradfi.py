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
        tasks = [self.enrich(raw) for raw in events]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        normalized_events = []
        for res in results:
            if isinstance(res, NormalizedEvent):
                normalized_events.append(res)
            elif isinstance(res, list):
                normalized_events.extend(res)
            elif isinstance(res, Exception):
                logger.error(f"Error enriching tradfi batch item: {res}")
                
        return normalized_events

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        if source == "finnhub_equities":
            trade_type = p.get("trade_type", "RAW_TRADE")
            if trade_type == "OHLCV_MINUTE_BAR":
                return await self._enrich_equity_candle(raw, p)
            else:   
                return await self._enrich_equity_trade(raw, p)
        elif source == "sec_form4":
            return await self._enrich_insider(raw, p)
            
        return None

    async def _enrich_equity_trade(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = (p.get("ticker") or "").upper()
        if not ticker or ticker == "UNKNOWN": return None
        
        price = float(p.get("close") or p.get("price", 0))
        volume = float(p.get("volume") or p.get("size_shares", 0))
        notional = float(p.get("notional_usd") or (price * volume))
        
        # Cache the absolute latest price so the Cointegration Engine can reference it
        if price > 0:
            try:
                await self.redis_client.raw.set(f"sentinel:quotes:latest:{ticker}", price, ex=3600)
            except Exception as e:
                logger.error(f"Failed to cache latest quote for {ticker}: {e}")
        
        
        # Send to Anomaly Scorer for ML isolation forest & volume ratio check
        anomaly = await self.scorer.score_financial_trade("tradfi", ticker, notional, volume)
        logger.info(f"🧠 ML INFERENCE | {ticker} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
        
        tags = ["tradfi", "equity_block", ticker.lower()]
        
        # Determine Aggressor Side (Institutional Distribution vs Accumulation)
        aggressor_side = p.get("aggressor_side", p.get("side", "UNKNOWN")).upper()
        if aggressor_side == "UNKNOWN":
            tick_dir = p.get("tick_direction", "")
            if tick_dir == "DownTick":
                aggressor_side = "SELL"
            elif tick_dir == "UpTick":
                aggressor_side = "BUY"
                
        # Dark pool logic
        conditions = str(p.get("conditions", "")).lower()
        is_dark_pool = "out of sequence" in conditions or "average price" in conditions
        if is_dark_pool:
            tags.append("dark_pool_print")
            
        direction_str = "Block Trade"
        
        if aggressor_side == "SELL":
            tags.append("aggressor_sell")
            # Severe urgency if dumped on Lit Market instead of Dark Pool
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

        # Capitulation Check vs Baseline
        try:
            baseline = await self.redis_client.raw.get(f"baseline:volume:{ticker}")
            if baseline and float(baseline) > 0 and volume > float(baseline) * 20:
                tags.append("volume_capitulation")
                anomaly = min(1.0, anomaly * 1.4)
        except Exception as e:
            logger.debug(f"Baseline fetch failed: {e}")

        if anomaly < 0.6:  # Strict floor. Ignore non-anomalous trades.
            return None

        await self._sync_geo_watchlist(ticker, tags)
        await self._update_volume_baseline(ticker, volume)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
        }, key=ticker)

        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.EQUITY_BLOCK,
             # Emits to financial correlation engine
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
                volume_oi_ratio=p.get("vol_oi_ratio") # BUG 4 FIXED: Maps field
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
                event_id=raw.event_id,
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
                    await self.redis_client.raw.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
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
        
        value = float(p.get("transaction_value_usd", 0))
        code = p.get("transaction_code", "J")
        title = (p.get("role") or p.get("title") or "").upper()
        
        # Suppress noise: Ignore standard compensation & tax withholding below $500k
        if code in ("A", "F") and value < 500_000:
            return None
            
        code_label = FORM4_CODES.get(code, "Transaction")
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        
        # Role-based weighting multiplier
        anomaly = min(1.0, value / 10_000_000 * 0.3)
        if "CEO" in title:
            anomaly = min(1.0, anomaly * 1.5)
        elif "DIRECTOR" in title:
            anomaly = min(1.0, anomaly * 1.1)

        # Open market buys are high conviction
        if code == "P":
            anomaly = min(1.0, anomaly * 1.2)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial"}
        }, key=ticker)

        return NormalizedEvent(
            event_id=raw.event_id,
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