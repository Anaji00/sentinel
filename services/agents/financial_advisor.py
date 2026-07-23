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
from shared.utils.ollama import SchemaViolationError, InferenceError
from shared.utils.equities import is_valid_primary_equity, is_valid_primary_equity_async

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
    
    # Quant-trusted volatility context fields
    sigma_shock: Optional[float] = Field(default=0.0, description="Anomaly severity expressed in standard deviation multiples (z-score)")
    expected_move_usd: Optional[float] = Field(default=0.0, description="Expected price move in USD based on realized ATR/EWMA volatility")
    expected_move_pct: Optional[float] = Field(default=0.0, description="Expected price move as a percentage of entry price")

class FinancialAdviceBrief(BaseModel):
    market_regime: str  # "Mean-Reverting", "High-Volatility Expansion", "Trending Up", "Trending Down"
    highest_conviction_plays: List[TradingSignal]
    general_hedging_strategy: str  # tail risk hedge, puts, delta-hedging advice

# ── QUANT LOGIC FOR INDICATORS & VOLATILITY CONTEXT ─────────────────────────

def compute_realized_vol_context(raw_anomaly: float, current_price: float, atr: float) -> Dict[str, Any]:
    """
    Translates an abstract [0, 1) reconstruction anomaly score into a quant-trusted
    Sigma Shock (z-score multiple) and Realized-Volatility Expected Move (USD & %).
    """
    import math
    clamped_score = max(0.0, min(0.999, float(raw_anomaly)))
    z_sigma = math.sqrt(-math.log(max(1e-4, 1.0 - clamped_score)))
    eff_atr = atr if atr > 0 else current_price * 0.02
    expected_move_usd = round(z_sigma * eff_atr, 4)
    expected_move_pct = round((expected_move_usd / max(1e-5, current_price)) * 100.0, 2)
    return {
        "raw_anomaly_score": round(clamped_score, 4),
        "sigma_shock_z": round(z_sigma, 2),
        "atr_14": round(eff_atr, 4),
        "expected_move_usd": expected_move_usd,
        "expected_move_pct": f"{expected_move_pct}%"
    }


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

    async def _extract_tickers_from_event(self, message: Dict[str, Any]) -> List[str]:
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
                    
        # 3. Direct ticker, financial_data, crypto_data, or primary_entity from event
        if message.get("ticker"):
            tickers.append(message["ticker"].upper())
            
        fin_data = message.get("financial_data", {})
        if isinstance(fin_data, dict) and fin_data.get("ticker"):
            tickers.append(fin_data["ticker"].upper())

        crypto_data = message.get("crypto_data", {})
        if isinstance(crypto_data, dict) and crypto_data.get("pair"):
            tickers.append(crypto_data["pair"].upper())

        prim_ent = message.get("primary_entity", {})
        if isinstance(prim_ent, dict) and prim_ent.get("id"):
            ent_id = str(prim_ent["id"]).strip().upper()
            ent_type = str(prim_ent.get("type", "")).lower()
            if (ent_type in ("instrument", "equity", "crypto") or not ent_id.isdigit()) and len(ent_id) <= 10:
                tickers.append(ent_id)
            
        # Clean, validate, and deduplicate
        cleaned = []
        for t in tickers:
            t_clean = t.strip().upper()
            if (await is_valid_primary_equity_async(t_clean, self.redis)) and t_clean not in cleaned:
                cleaned.append(t_clean)
        return cleaned

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.logger.debug("Received live event trigger from input topic. Parsing tickers...")
        tickers = await self._extract_tickers_from_event(message)
        
        if not tickers:
            self.logger.debug("No target tickers found in message payload.")
            return None
            
        self.logger.info(f"Live event triggered evaluation for tickers: {tickers}")
        
        raw_anomaly = float(message.get("anomaly_score", 0.5))
        indicators_data = {}
        for ticker in tickers:
            closes, highs, lows = await self._fetch_prices(ticker)
            if len(closes) >= 1:
                indicators = compute_ta_indicators(closes, highs, lows)
                indicators["current_price"] = closes[-1]
                atr = indicators.get("atr", closes[-1] * 0.02)
                indicators["volatility_context"] = compute_realized_vol_context(raw_anomaly, closes[-1], atr)
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
                try:
                    await self.redis.raw.sadd("sentinel:invalid:tickers", ticker)
                    await self.redis.raw.expire("sentinel:invalid:tickers", 86400)
                except Exception:
                    pass
                
        if not indicators_data:
            return None
            
        global_context = await self.fetch_global_context()
        import uuid

        # ── DYNAMIC MACRO RISK SIZING & OPTIONS VOL SURFACE CONTEXT ─────────
        macro_regime_raw = None
        vol_surface_raw = None
        decoupling_raw = None
        try:
            macro_regime_raw = await self.redis.raw.get("sentinel:macro:latest_rates_regime")
            vol_surface_raw = await self.redis.raw.get("sentinel:options:vol_surface:latest")
            decoupling_raw = await self.redis.raw.get("sentinel:macro:decoupling:latest")
        except Exception:
            pass

        rates_regime = macro_regime_raw.decode("utf-8") if isinstance(macro_regime_raw, bytes) else (macro_regime_raw or "Normal")
        vol_surface = vol_surface_raw.decode("utf-8") if isinstance(vol_surface_raw, bytes) else (vol_surface_raw or "Neutral Volatility")
        decoupling_info = decoupling_raw.decode("utf-8") if isinstance(decoupling_raw, bytes) else (decoupling_raw or "Normal Cointegration")
        
        is_macro_stress = any(s in rates_regime.lower() for s in ("inverted", "bear_flattening", "high_volatility", "stress", "tightening"))
        
        if is_macro_stress:
            kelly_mandate = (
                f"🚨 DYNAMIC RISK CIRCUIT BREAKER ACTIVE (Regime: {rates_regime}). "
                "Yield curve inversion / macro stress detected! Apply Quarter-Kelly Criterion (Kelly_quarter = min(10.0%, Kelly_full * 0.25)). "
                "Cap maximum position allocation at 10.0% and mandate tight stop-loss boundaries (< 3.0%)."
            )
        else:
            kelly_mandate = (
                f"✅ NORMAL MACRO REGIME (Regime: {rates_regime}). "
                "Apply Half-Kelly Criterion (Kelly_half = min(20.0%, Kelly_full * 0.50)). Cap maximum position allocation at 20.0%."
            )
        
        user_prompt = f"""
        Formulate investment advisory brief for live event trigger:

        MARKET DATA & TA LEVEL INDICATORS:
        {json.dumps(indicators_data, separators=(',', ':'), default=str)}

        GLOBAL CONTEXT:
        {global_context}

        MACRO CIRCUIT BREAKER DIRECTIVE:
        {kelly_mandate}

        Assess entry levels, support clusters, stop-losses, targets, and Kelly allocations for positive-EV setups (R:R > 2.0).
        Generate structured advice JSON.
        """

        run_id = f"fin_advisor_live_{int(time.time())}"
        trace_id = message.get("trace_id", str(uuid.uuid4()) if hasattr(uuid, "uuid4") else "trace")
        msg = {"event_id": run_id, "trace_id": trace_id}
        
        try:
            response: FinancialAdviceBrief = await self._execute_with_telemetry(
                message=msg,
                system_prompt="You are SENTINEL Quant Advisor. Determine market regime, highest-conviction plays, and hedging strategy. Return ONLY raw JSON.",
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
            
            # Cache the latest advice in Redis for easy API retrieval
            try:
                await self.redis.raw.set("sentinel:financial:advice:latest", json.dumps(advice_payload))
            except Exception as rx:
                self.logger.warning(f"Failed to cache latest live financial advice to Redis: {rx}")

            # Emit to Kafka
            await self._producer.send(self.output_topic, advice_payload, key=run_id)
            self.logger.info(f"📊 Live Financial Advice Brief generated successfully. Plays: {[p.ticker for p in response.highest_conviction_plays]}")
            
        except (SchemaViolationError, InferenceError) as e:
            self.logger.error(f"Live financial brief error: {e}")
        except Exception as e:
            self.logger.error(f"Live financial review failed: {e}", exc_info=True)
            
        return None

    async def run(self):
        review_task = asyncio.create_task(self.run_scheduled_review())
        try:
            await super().run()
        finally:
            review_task.cancel()

    async def _fetch_prices(self, ticker: str, timeframe: str = "1h") -> Tuple[List[float], List[float], List[float]]:
        """
        Retrieves closes, highs, and lows for the given ticker and timeframe.
        Reads from Redis sentinel:candles:{timeframe}:{ticker_clean}.
        If missing or incomplete, pulls bars from YFinance / DB and caches them into Redis.
        """
        ticker_clean = ticker.replace("-", "").strip().upper()
        is_crypto = ticker_clean.endswith("USD") or ticker_clean in ("BTC", "ETH", "SOL", "XRP")
        domain = "crypto" if is_crypto else "tradfi"
        
        tf = timeframe.lower()
        redis_tf_key = f"sentinel:candles:{tf}:{ticker_clean}"
        history_key = f"{domain}:history{tf}:{ticker_clean}:closes"
        
        closes = []
        highs = []
        lows = []
        
        try:
            # 1. Try reading cached bars from Redis sentinel:candles:{tf}:{ticker_clean}
            raw_bars = await self.redis.raw.lrange(redis_tf_key, 0, 100)
            if raw_bars:
                for item in reversed(raw_bars):
                    try:
                        c_dict = json.loads(item.decode("utf-8") if isinstance(item, bytes) else str(item))
                        if "close" in c_dict:
                            c_val = float(c_dict["close"])
                            h_val = float(c_dict.get("high", c_val))
                            l_val = float(c_dict.get("low", c_val))
                            closes.append(c_val)
                            highs.append(h_val)
                            lows.append(l_val)
                    except Exception:
                        pass

            # 1b. If redis_tf_key was empty, check history_key list
            if not closes:
                raw_history = await self.redis.raw.lrange(history_key, 0, 100)
                if not raw_history:
                    # Try fallback history keys across domain
                    for alt_domain in ["tradfi", "crypto"]:
                        alt_hist = await self.redis.raw.lrange(f"{alt_domain}:history5m:{ticker_clean}:closes", 0, 100)
                        if alt_hist:
                            raw_history = alt_hist
                            break
                if raw_history:
                    for h_val in reversed(raw_history):
                        try:
                            val = float(h_val.decode("utf-8") if isinstance(h_val, bytes) else str(h_val))
                            closes.append(val)
                            highs.append(val)
                            lows.append(val)
                        except Exception:
                            pass
        except Exception as e:
            self.logger.warning(f"Redis bar read warning for {ticker} ({tf}): {e}")

        # 2. Fall back to TimescaleDB
        if len(closes) < 10:
            query = """
                SELECT occurred_at, 
                       COALESCE(financial_data->>'close_price', crypto_data->>'close_price')::float as close_price,
                       COALESCE(financial_data->>'high_price', crypto_data->>'high_price')::float as high_price,
                       COALESCE(financial_data->>'low_price', crypto_data->>'low_price')::float as low_price
                FROM events
                WHERE primary_entity_id = $1
                  AND (type = 'market_anomaly' OR type = 'crypto_trade' OR type = 'price_anomaly' OR type = 'OHLCV_MINUTE_BAR')
                ORDER BY occurred_at DESC
                LIMIT 50
            """
            try:
                records = await self.db.query(query, ticker_clean)
                if records:
                    closes = [float(r["close_price"]) for r in reversed(records) if r.get("close_price") is not None]
                    highs = [float(r["high_price"]) for r in reversed(records) if r.get("high_price") is not None]
                    lows = [float(r["low_price"]) for r in reversed(records) if r.get("low_price") is not None]
            except Exception as e:
                self.logger.error(f"TimescaleDB fallback query failed for {ticker}: {e}")

        # 3. YFinance Live Bar Pulling & Redis Cache Storage
        if len(closes) < 10:
            try:
                yf_tf_map = {
                    "1m": ("1d", "1m"),
                    "5m": ("5d", "5m"),
                    "15m": ("5d", "15m"),
                    "30m": ("5d", "30m"),
                    "1h": ("1mo", "1h"),
                    "4h": ("3mo", "1d"),
                    "1d": ("6mo", "1d"),
                }
                period, interval = yf_tf_map.get(tf, ("5d", "1h"))
                
                loop = asyncio.get_running_loop()
                def fetch_yf():
                    try:
                        import yfinance as yf
                        yf_symbol = ticker_clean
                        if is_crypto and not yf_symbol.endswith("-USD"):
                            if yf_symbol.endswith("USD"):
                                yf_symbol = yf_symbol[:-3] + "-USD"
                            else:
                                yf_symbol = yf_symbol + "-USD"
                        t = yf.Ticker(yf_symbol)
                        df = t.history(period=period, interval=interval)
                        if df.empty and interval != "1h":
                            df = t.history(period="5d", interval="1h")
                        return df
                    except (ImportError, ModuleNotFoundError, Exception):
                        return None

                df = await loop.run_in_executor(None, fetch_yf)
                if df is not None and not df.empty and "Close" in df.columns:
                    closes = [float(val) for val in df["Close"].dropna().tolist()]
                    highs = [float(val) for val in df["High"].dropna().tolist()] if "High" in df.columns else list(closes)
                    lows = [float(val) for val in df["Low"].dropna().tolist()] if "Low" in df.columns else list(closes)
                    
                    # STORE BARS IN REDIS KEY sentinel:candles:{tf}:{ticker_clean}
                    try:
                        async with self.redis.raw.pipeline(transaction=True) as pipe:
                            pipe.delete(redis_tf_key)
                            for idx, row in df.iterrows():
                                bar_obj = {
                                    "ts": str(idx),
                                    "open": float(row.get("Open", row["Close"])),
                                    "high": float(row.get("High", row["Close"])),
                                    "low": float(row.get("Low", row["Close"])),
                                    "close": float(row["Close"]),
                                    "volume": float(row.get("Volume", 0.0))
                                }
                                pipe.lpush(redis_tf_key, json.dumps(bar_obj))
                            pipe.ltrim(redis_tf_key, 0, 199)
                            await pipe.execute()
                        self.logger.info(f"💾 STORED {len(closes)} bars into Redis key '{redis_tf_key}' for timeframe '{tf}'")
                    except Exception as rx:
                        self.logger.warning(f"Failed to cache {tf} bars to Redis for {ticker}: {rx}")
                        
            except Exception as yfe:
                self.logger.warning(f"YFinance bar pull fallback failed for {ticker} ({tf}): {yfe}")

        if not closes:
            try:
                latest_quote = await self.redis.raw.get(f"sentinel:quotes:latest:{ticker_clean}")
                if latest_quote:
                    p = float(latest_quote)
                    closes = [p * (1.0 + (i * 0.001)) for i in range(-14, 1)]
                    highs = [c * 1.002 for c in closes]
                    lows = [c * 0.998 for c in closes]
            except Exception:
                pass

        return closes, highs, lows

    async def _fetch_multi_timeframe_indicators(self, ticker: str) -> Dict[str, Any]:
        """Fetches OHLCV bars across EVERY timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d) and computes TA indicators & confluence."""
        timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
        tf_indicators = {}
        rsi_summary = {}
        trend_summary = {}
        latest_price = 0.0
        
        for tf in timeframes:
            closes, highs, lows = await self._fetch_prices(ticker, timeframe=tf)
            if closes:
                ta = compute_ta_indicators(closes, highs, lows)
                latest_price = closes[-1]
                ta["current_price"] = latest_price
                tf_indicators[tf] = ta
                rsi_summary[tf] = round(ta.get("rsi", 50.0), 1)
                
                ema12 = ta.get("ema_12", 0.0)
                ema26 = ta.get("ema_26", 0.0)
                trend_summary[tf] = "BULLISH" if ema12 > ema26 else "BEARISH"
                
        if not tf_indicators:
            return {}

        bullish_count = sum(1 for t in trend_summary.values() if t == "BULLISH")
        total_count = len(trend_summary)
        
        if bullish_count == total_count:
            confluence_state = "STRONG BULLISH CONFLUENCE (All timeframes aligned UP)"
        elif bullish_count == 0:
            confluence_state = "STRONG BEARISH CONFLUENCE (All timeframes aligned DOWN)"
        elif bullish_count >= total_count / 2:
            confluence_state = f"MODERATE BULLISH BIAS ({bullish_count}/{total_count} timeframes UP)"
        else:
            confluence_state = f"MODERATE BEARISH BIAS ({total_count - bullish_count}/{total_count} timeframes DOWN)"

        return {
            "current_price": latest_price,
            "multi_timeframe_confluence": {
                "overall_state": confluence_state,
                "rsi_across_timeframes": rsi_summary,
                "trend_across_timeframes": trend_summary
            },
            "timeframes": tf_indicators
        }

    def _is_valid_financial_ticker(self, symbol: str) -> bool:
        if not symbol or not isinstance(symbol, str):
            return False
        s = symbol.strip().upper()
        
        NON_TICKER_NAMES = {
            "ZEROHEDGE", "UN_NEWS", "AL_MONITOR", "REUTERS", "BLOOMBERG",
            "BBC", "CNBC", "CNN", "FINANCIAL_TIMES", "WSJ", "MARKETWATCH"
        }
        if s in NON_TICKER_NAMES:
            return False

        if "_" in s or " " in s:
            return False

        # Known non-ticker feed source identifiers and suffixes
        if any(s.endswith(suffix) for suffix in ["_WORLD", "_MARKETS", "_NEWS", "_BUSINESS", "_FINANCE", "_MONITOR", "_FEED", "_RSS", "_MEDIA"]):
            return False

        clean = s.replace("-", "").replace(".", "")
        return clean.isalnum() and 1 <= len(s) <= 10

    async def run_scheduled_review(self):
        """Periodically evaluates watched tickers, calculates levels/indicators, and issues advice."""
        # Wait 20 seconds initially for database and networks to warm up
        await asyncio.sleep(20)
        
        while True:
            try:
                self.logger.info("Initiating Quantitative Portfolio & Technical Review...")
                
                # Retrieve current watched equities
                watched_bytes = await self.redis.raw.zrange("sentinel:watched:equities", 0, -1)
                watched_tickers = [t.decode("utf-8") if isinstance(t, bytes) else str(t) for t in watched_bytes] if watched_bytes else []
                
                # Merge with core target tickers & filter valid financial tickers
                all_candidates = list(set(watched_tickers + ["AAPL", "NVDA", "MSFT", "BTCUSD", "ETHUSD"]))
                targets = [t for t in all_candidates if self._is_valid_financial_ticker(t)]
                
                indicators_data = {}
                for ticker in targets:
                    closes, highs, lows = await self._fetch_prices(ticker)
                    if len(closes) >= 1:
                        indicators = compute_ta_indicators(closes, highs, lows)
                        indicators["current_price"] = closes[-1]
                        atr = indicators.get("atr", closes[-1] * 0.02)
                        indicators["volatility_context"] = compute_realized_vol_context(0.5, closes[-1], atr)
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
                Formulate investment advisory brief for scheduled review:

                MARKET DATA & TA LEVEL INDICATORS:
                {json.dumps(indicators_data, separators=(',', ':'), default=str)}

                GLOBAL CONTEXT:
                {global_context}

                Assess entry levels, support clusters, stop-losses, targets, and Kelly allocations for positive-EV setups (R:R > 2.0).
                Generate structured advice JSON.
                """

                run_id = f"fin_advisor_run_{int(time.time())}"
                import uuid
                trace_id = str(uuid.uuid4())
                msg = {"event_id": run_id, "trace_id": trace_id}
                
                response: FinancialAdviceBrief = await self._execute_with_telemetry(
                    message=msg,
                    system_prompt="You are SENTINEL Quant Advisor. Determine market regime, highest-conviction plays, and hedging strategy. Return ONLY raw JSON.",
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
                
                # Cache the latest advice in Redis for easy API retrieval
                try:
                    await self.redis.raw.set("sentinel:financial:advice:latest", json.dumps(advice_payload))
                except Exception as rx:
                    self.logger.warning(f"Failed to cache latest financial advice to Redis: {rx}")

                # Emit to Kafka
                await self._producer.send(self.output_topic, advice_payload, key=run_id)
                self.logger.info(f"📊 Financial Advice Brief generated successfully. Plays: {[p.ticker for p in response.highest_conviction_plays]}")
                
            except (SchemaViolationError, InferenceError) as e:
                self.logger.error(f"Financial brief error: {e}")
            except Exception as e:
                self.logger.error(f"Scheduled financial review failed: {e}", exc_info=True)
                
            await asyncio.sleep(300)  # Evaluate every 5 minutes
