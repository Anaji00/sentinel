import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent
from shared.kafka import Topics
from shared.utils.ollama import SchemaViolationError

logger = logging.getLogger("agent.financial_advisor")

# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

class TradingSignal(BaseModel):
    ticker: str
    action: str  # "BUY", "SELL", "HOLD"
    entry_level: float
    target_price: float
    stop_loss: float
    risk_reward_ratio: float
    kelly_allocation_pct: float  # Kelly Criterion calculation for sizing (0.0% to 100.0%)
    conviction_score: float  # 0.0 to 1.0
    technical_indicators: Dict[str, float]  # RSI, fast_ema, slow_ema, atr, close
    fib_levels: Dict[str, float]  # Fib retracements (0.0, 0.236, 0.382, 0.500, 0.618, 0.786, 1.0)
    quantitative_rationale: str  # High-level Jane Street style quant reasoning

class FinancialAdviceBrief(BaseModel):
    market_regime: str  # "Mean-Reverting", "High-Volatility Expansion", "Trending Up", "Trending Down"
    highest_conviction_plays: List[TradingSignal]
    general_hedging_strategy: str  # tail risk hedge, puts, delta-hedging advice

# ── QUANT LOGIC FOR INDICATORS ────────────────────────────────────────────────

def compute_ta_indicators(closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
    if not closes:
        return {}
    
    current_close = closes[-1]
    
    # 1. RSI (14)
    rsi = 50.0
    if len(closes) >= 15:
        gains = []
        losses = []
        for i in range(len(closes) - 14, len(closes)):
            diff = closes[i] - closes[i - 1]
            gains.append(diff if diff > 0 else 0.0)
            losses.append(abs(diff) if diff < 0 else 0.0)
        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14
        if avg_loss == 0:
            rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            
    # 2. EMAs
    def ema(series, period):
        if len(series) < period:
            return sum(series) / len(series) if series else current_close
        alpha = 2 / (period + 1)
        val = sum(series[:period]) / period
        for price in series[period:]:
            val = price * alpha + val * (1 - alpha)
        return val
        
    ema_12 = ema(closes, 12)
    ema_26 = ema(closes, 26)
    
    # 3. Fibonacci Retracement Levels
    max_h = max(highs) if highs else max(closes)
    min_l = min(lows) if lows else min(closes)
    diff = max_h - min_l
    if diff == 0:
        diff = current_close * 0.05
    
    fib_levels = {
        "0.0": min_l,
        "0.236": min_l + 0.236 * diff,
        "0.382": min_l + 0.382 * diff,
        "0.500": min_l + 0.500 * diff,
        "0.618": min_l + 0.618 * diff,
        "0.786": min_l + 0.786 * diff,
        "1.0": max_h
    }
    
    # 4. ATR (Average True Range)
    ranges = []
    h_len = len(highs)
    l_len = len(lows)
    min_len = min(h_len, l_len)
    for i in range(min_len):
        ranges.append(highs[i] - lows[i])
    atr = sum(ranges[-14:]) / 14 if len(ranges) >= 14 else (sum(ranges) / len(ranges) if ranges else current_close * 0.02)
    
    return {
        "rsi": rsi,
        "ema_12": ema_12,
        "ema_26": ema_26,
        "atr": atr,
        "fib_levels": fib_levels,
        "max_high": max_h,
        "min_low": min_l
    }


class FinancialAdvisorAgent(SentinelAgent):
    """
    Evaluates trading strategies, tail risk, and upside levels for watched instruments.
    Uses programmatically calculated TA metrics (EMA, RSI, ATR) and Fibonacci levels.
    Emits highest-conviction plays to Topics.FINANCIAL_ADVICE.
    """
    
    @property
    def output_topic(self) -> str:
        return Topics.FINANCIAL_ADVICE

    def _extract_tickers_from_event(self, message: Dict[str, Any]) -> List[str]:
        tickers = []
        
        # 1. From IntelBrief / Headline Events
        brief = message.get("brief", {})
        if isinstance(brief, dict):
            for t in brief.get("financial_instruments_affected", []):
                if isinstance(t, str):
                    tickers.append(t.upper())
            for ent in brief.get("entities", []):
                if isinstance(ent, dict) and ent.get("type") in ("instrument", "equity", "crypto"):
                    tickers.append(ent["name"].upper())
                    
        # 2. From Quant Discoveries
        trigger = message.get("trigger", {})
        if isinstance(trigger, dict):
            t = trigger.get("ticker")
            if t:
                tickers.append(t.upper())
                
        discovery = message.get("discovery", {})
        if isinstance(discovery, dict):
            for peer in discovery.get("peer_tickers", []):
                if isinstance(peer, dict) and peer.get("ticker"):
                    tickers.append(peer["ticker"].upper())
            for item in discovery.get("macro_instruments", []):
                if isinstance(item, dict) and item.get("ticker"):
                    tickers.append(item["ticker"].upper())
                    
        # 3. Direct ticker or primary_entity from basic event
        if message.get("ticker"):
            tickers.append(message["ticker"].upper())
            
        prim_ent = message.get("primary_entity", {})
        if isinstance(prim_ent, dict) and prim_ent.get("id"):
            tickers.append(prim_ent["id"].upper())
            
        # Clean and deduplicate
        cleaned = []
        for t in tickers:
            t_clean = t.strip().upper()
            if t_clean and t_clean not in cleaned:
                cleaned.append(t_clean)
        return cleaned

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.logger.info("Received live event trigger from input topic. Parsing tickers...")
        tickers = self._extract_tickers_from_event(message)
        
        if not tickers:
            self.logger.debug("No target tickers found in message payload.")
            return None
            
        self.logger.info(f"Live event triggered evaluation for tickers: {tickers}")
        
        indicators_data = {}
        for ticker in tickers:
            closes, highs, lows = await self._fetch_prices(ticker)
            if len(closes) >= 10:
                indicators = compute_ta_indicators(closes, highs, lows)
                indicators["current_price"] = closes[-1]
                indicators_data[ticker] = indicators
                
                # Track ticker in Redis watched equities
                try:
                    async with self.redis.raw.pipeline(transaction=True) as pipe:
                        pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                        pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
                        await pipe.execute()
                    self.logger.info(f"Added triggered ticker '{ticker}' to sentinel:watched:equities")
                except Exception as ex:
                    self.logger.warning(f"Failed to add {ticker} to watchlist in Redis: {ex}")
            else:
                self.logger.warning(
                    f"Insufficient historical data for {ticker} (only {len(closes)} bars available). "
                    "Skipping live evaluation."
                )
                
        if not indicators_data:
            return None
            
        global_context = await self.fetch_global_context()
        import uuid
        
        user_prompt = f"""
        As a quantitative researcher at Jane Street, formulate an investment advisory brief FOR A LIVE EVENT TRIGGER.
        
        MARKET DATA & TA LEVEL INDICATORS FOR THE TRIGGERED TICKERS:
        {json.dumps(indicators_data, indent=2, default=str)}
        
        GLOBAL CONTEXT:
        {global_context}
        
        Perform a mathematical assessment of entry levels, support clusters, stop-losses, and targets.
        Use the Kelly Criterion to allocate theoretical capital sizes to highest-conviction plays.
        Focus on establishing positive-EV setups with asymmetric R:R (Risk/Reward > 2.0).
        
        Generate the structured JSON advice.
        """

        run_id = f"fin_advisor_live_{int(time.time())}"
        trace_id = message.get("trace_id", str(uuid.uuid4()) if hasattr(uuid, "uuid4") else "trace")
        msg = {"event_id": run_id, "trace_id": trace_id}
        
        try:
            response: FinancialAdviceBrief = await self._execute_with_telemetry(
                message=msg,
                system_prompt=(
                    "You are a Senior Quantitative Advisor at Jane Street. "
                    "Determine the market regime, highest-conviction plays, and hedging strategy for the triggered assets, and return them strictly in JSON."
                ),
                user_prompt=user_prompt,
                schema=FinancialAdviceBrief,
                temperature=0.1
            )
            
            advice_payload = {
                "agent": self.name,
                "agent_run_id": run_id,
                "trace_id": trace_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": response.model_dump() if hasattr(response, "model_dump") else response.dict(),
                "trigger_event": message
            }
            
            # Write advice summary to shared memories
            for play in response.highest_conviction_plays[:3]:
                mem_text = f"FinAdvisor Live Play: {play.action} {play.ticker} (Entry: {play.entry_level:.2f}, Target: {play.target_price:.2f}, R:R: {play.risk_reward_ratio:.2f})"
                asyncio.create_task(self.write_agent_memory(mem_text))
            
            # Emit to Kafka
            await self._producer.send(self.output_topic, advice_payload, key=run_id)
            self.logger.info(f"📊 Live Financial Advice Brief generated successfully. Plays: {[p.ticker for p in response.highest_conviction_plays]}")
            
        except SchemaViolationError as e:
            self.logger.error(f"Live financial brief schema violation: {e}")
        except Exception as e:
            self.logger.error(f"Live financial review failed: {e}", exc_info=True)
            
        return None

    async def run(self):
        review_task = asyncio.create_task(self.run_scheduled_review())
        try:
            await super().run()
        finally:
            review_task.cancel()

    async def _fetch_prices(self, ticker: str) -> Tuple[List[float], List[float], List[float]]:
        """Retrieves closes, highs, and lows for the given ticker, with TimescaleDB fallback."""
        is_crypto = "-" in ticker or ticker.endswith("USD")
        domain = "crypto" if is_crypto else "tradfi"
        
        history_key = f"{domain}:history60m:{ticker}:closes"
        candle_key = f"{domain}:candle60m:{ticker}"
        
        closes = []
        highs = []
        lows = []
        
        try:
            # 1. Fetch from Redis history list
            closes_bytes = await self.redis.raw.lrange(history_key, 0, 30)
            if closes_bytes:
                closes = [float(c.decode("utf-8")) for c in reversed(closes_bytes)]
            
            # Fetch current in-progress candle
            active_json = await self.redis.raw.get(candle_key)
            if active_json:
                active = json.loads(active_json)
                closes.append(float(active.get("close", active.get("c", 0.0))))
                highs.append(float(active.get("high", active.get("h", 0.0))))
                lows.append(float(active.get("low", active.get("l", 0.0))))
        except Exception as e:
            self.logger.warning(f"Failed to fetch prices from Redis for {ticker}: {e}")

        # 2. Fall back to TimescaleDB
        if len(closes) < 10:
            query = """
                SELECT occurred_at, 
                       COALESCE(financial_data->>'close_price', crypto_data->>'close_price')::float as close_price,
                       COALESCE(financial_data->>'high_price', crypto_data->>'high_price')::float as high_price,
                       COALESCE(financial_data->>'low_price', crypto_data->>'low_price')::float as low_price
                FROM events
                WHERE primary_entity_id = :ticker
                  AND (type = 'market_anomaly' OR type = 'crypto_trade' OR type = 'price_anomaly')
                ORDER BY occurred_at DESC
                LIMIT 30
            """
            try:
                records = await self.db.query(query, {"ticker": ticker})
                if records:
                    closes = [float(r["close_price"]) for r in reversed(records) if r.get("close_price") is not None]
                    highs = [float(r["high_price"]) for r in reversed(records) if r.get("high_price") is not None]
                    lows = [float(r["low_price"]) for r in reversed(records) if r.get("low_price") is not None]
            except Exception as e:
                self.logger.error(f"TimescaleDB fallback query failed for {ticker}: {e}")

        return closes, highs, lows

    async def run_scheduled_review(self):
        """Periodically evaluates watched tickers, calculates levels/indicators, and issues advice."""
        # Wait 20 seconds initially for database and networks to warm up
        await asyncio.sleep(20)
        
        while True:
            try:
                self.logger.info("Initiating Quantitative Portfolio & Technical Review...")
                
                # Retrieve current watched equities
                watched_bytes = await self.redis.raw.zrange("sentinel:watched:equities", 0, -1)
                watched_tickers = [t.decode("utf-8") for t in watched_bytes] if watched_bytes else []
                
                # Merge with core target tickers
                targets = list(set(watched_tickers + ["AAPL", "NVDA", "MSFT", "BTC-USD", "ETH-USD"]))
                
                indicators_data = {}
                for ticker in targets:
                    closes, highs, lows = await self._fetch_prices(ticker)
                    if len(closes) >= 10:
                        indicators = compute_ta_indicators(closes, highs, lows)
                        indicators["current_price"] = closes[-1]
                        indicators_data[ticker] = indicators
                    else:
                        self.logger.warning(
                            f"Insufficient historical data for {ticker} (only {len(closes)} bars available). "
                            "Skipping evaluation."
                        )
                
                if not indicators_data:
                    self.logger.warning("No target tickers have sufficient pricing data. Skipping review cycle and waiting.")
                    await asyncio.sleep(300)
                    continue

                # Fetch global macro details to include in advisor prompt
                global_context = await self.fetch_global_context()
                
                user_prompt = f"""
                As a quantitative researcher at Jane Street, formulate an investment advisory brief.
                
                MARKET DATA & TA LEVEL INDICATORS:
                {json.dumps(indicators_data, indent=2, default=str)}
                
                GLOBAL CONTEXT:
                {global_context}
                
                Perform a mathematical assessment of entry levels, support clusters, stop-losses, and targets.
                Use the Kelly Criterion to allocate theoretical capital sizes to highest-conviction plays.
                Focus on establishing positive-EV setups with asymmetric R:R (Risk/Reward > 2.0).
                
                Generate the structured JSON advice.
                """

                run_id = f"fin_advisor_run_{int(time.time())}"
                import uuid
                trace_id = str(uuid.uuid4())
                msg = {"event_id": run_id, "trace_id": trace_id}
                
                response: FinancialAdviceBrief = await self._execute_with_telemetry(
                    message=msg,
                    system_prompt=(
                        "You are a Senior Quantitative Advisor at Jane Street. "
                        "Determine the market regime, highest-conviction plays, and hedging strategy, and return them strictly in JSON."
                    ),
                    user_prompt=user_prompt,
                    schema=FinancialAdviceBrief,
                    temperature=0.1
                )
                
                advice_payload = {
                    "agent": self.name,
                    "agent_run_id": run_id,
                    "trace_id": trace_id,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "brief": response.model_dump() if hasattr(response, "model_dump") else response.dict(),
                }
                
                # Write advice summary to shared memories
                for play in response.highest_conviction_plays[:3]:
                    mem_text = f"FinAdvisor Play: {play.action} {play.ticker} (Entry: {play.entry_level:.2f}, Target: {play.target_price:.2f}, R:R: {play.risk_reward_ratio:.2f})"
                    asyncio.create_task(self.write_agent_memory(mem_text))
                
                # Emit to Kafka
                await self._producer.send(self.output_topic, advice_payload, key=run_id)
                self.logger.info(f"📊 Financial Advice Brief generated successfully. Plays: {[p.ticker for p in response.highest_conviction_plays]}")
                
            except SchemaViolationError as e:
                self.logger.error(f"Financial brief schema violation: {e}")
            except Exception as e:
                self.logger.error(f"Scheduled financial review failed: {e}", exc_info=True)
                
            await asyncio.sleep(300)  # Evaluate every 5 minutes
