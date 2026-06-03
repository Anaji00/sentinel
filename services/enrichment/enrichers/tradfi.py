import logging
from datetime import datetime, timezone
from typing import Optional
from shared.kafka import Topics
from shared.models import NormalizedEvent, EventType, Entity, EntityType, FinancialData
import re 

logger = logging.getLogger("enrichment.tradfi")

GEO_INSTRUMENTS = {
    "LMT": "defense", "RTX": "defense", "USO": "oil", "GLD": "gold",
    "BNO": "oil", "UCO": "oil", "XOP": "oil"
}

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
    def __init__(self, scorer, redis_client, db_writer, graph_writer):
        self.scorer = scorer
        self.redis_client = redis_client
        self.db = db_writer
        self.graph = graph_writer

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
        
        # Send to Anomaly Scorer for ML isolation forest & volume ratio check
        anomaly = await self.scorer.score_financial_trade("tradfi", ticker, notional, volume)
        logger.info(f"🧠 ML INFERENCE | {ticker} | Score: {anomaly:.3f} | Size: ${notional/1e6:.2f}M")
        if anomaly < 0.6:  # Strict floor. Ignore non-anomalous trades.
            return None
        tags = ["tradfi", "equity_block", ticker.lower()]
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
            headline=f"🐋 ML Outlier Block Trade {ticker} ${notional/1e6:.2f}M",
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
        close_p = float(p.get("close", 0))
        notional = float(p.get("notional_usd") or (close_p * volume))

        if open_p == 0 or close_p == 0:
            return None
        
        price_change_pct = abs((close_p - open_p) / open_p)
        volatility_pct   = (high_p - low_p) / open_p

        # ML SCORING: Compare this minute's structure against historical 1-minute structures
        features = [price_change_pct, volatility_pct, notional]
        anomaly = await self.scorer.score_market_candle("tradfi", ticker, features)
        logger.info(f"🧠 ML 1-MINUTE CANDLE INFERENCE | {ticker} Candle | Score: {anomaly:.3f} | Price Change: {price_change_pct*100:.2f}% | Volatility: {volatility_pct*100:.2f}% | Notional: ${notional/1e6:.2f}M")
        if anomaly < 0.6:
            return None
        tags = ["tradfi", "market_structure", "volatile_candle", ticker.lower()]
        await self._sync_geo_watchlist(ticker, tags)
        await self._update_volume_baseline(ticker, volume)

        await self.graph.producer.send(Topics.ONTOLOGY_PROPOSALS, {
            "entity_id": ticker,
            "action": "MERGE_ONTOLOGY_NODE",
            "data": {"label": "Company", "primary_domain": "financial", "confidence": anomaly}
        }, key=ticker)
        
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker)
        direction = "🟢 Bullish" if close_p >= open_p else "🔴 Bearish"

        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.MARKET_ANOMALY,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker, 
                instrument_type="equity",
                trade_type="OHLCV_MINUTE_BAR", 
                premium_usd=notional,
                underlying_price=close_p,
                volume=volume,
                open_price=open_p,
                high_price=high_p,
                low_price=low_p,
                close_price=close_p
            ),
            headline=f"{direction} Structural Anomaly: {ticker} moved {price_change_pct*100:.2f}% on ${notional/1e6:.1f}M vol",
            tags=tags,
            anomaly_score=anomaly,
        )

    async def _sync_geo_watchlist(self, ticker, tags):
        if ticker in GEO_INSTRUMENTS:
            tags.append("geo_linked_asset")
            tags.append(GEO_INSTRUMENTS[ticker])
            try:
                await self.redis_client.sadd("sentinel:watched:equities", ticker)
            except Exception as e:
                logger.error(f"Failed to update geo watchlist for {ticker}: {e}", exc_info=True)
                
    async def _update_volume_baseline(self, ticker: str, volume: float):
        """EMA baselining (α=0.05) to detect accumulation sweeps in thin markets."""
        try:
            key = f"baseline:volume:{ticker}"
            current = await self.redis_client.get(key)
            updated = (0.95 * float(current) + 0.05 * volume) if current else volume
            await self.redis_client.set(key, str(round(updated, 3)), ex=604800)
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