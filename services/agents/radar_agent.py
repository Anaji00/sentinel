import json
import time
from typing import Any, Dict, Optional, List
from pydantic import BaseModel
from services.agents.base import SentinelAgent
from shared.kafka import Topics
from shared.utils.equities import is_valid_primary_equity

class RadarDecision(BaseModel):
    investigate: bool
    rationale: str

class WatchlistPruneDecision(BaseModel):
    evict_tickers: List[str]
    rationale: str

# An earnings surprise this large is worth a look regardless of the flow behind
# it. Below it, a beat is noise: consensus is set to be beaten by a little.
MIN_EARNINGS_SURPRISE_PCT = 10.0


def _earnings_surprise_pct(message: Dict[str, Any]) -> float:
    """EPS surprise on an event, or 0.0 when it is not an earnings event."""
    for container in (
        message.get("financial_data"),
        message.get("raw_payload"),
        message.get("trigger"),
        message,
    ):
        if not isinstance(container, dict):
            continue
        value = container.get("eps_surprise_pct")
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


class RadarAgent(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cooldown_seconds = 86400
    
    @property
    def output_topic(self) -> str:
        return Topics.RADAR_DECISIONS
    
    def _extract_radar_params(self, message: Dict[str, Any]) -> tuple[Optional[str], float, float]:
        """Extracts ticker, z_score, and notional_usd cleanly from raw, discovery, or enriched payloads."""
        ticker = None
        z_score = 0.0
        notional_usd = 0.0

        def _safe_float(val, default=0.0) -> float:
            if val is None:
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def _notional_from(d: dict) -> float:
            """Dollar flow behind an event, however the producer expressed it.

            Three vocabularies reach this agent for one quantity and none of
            them is present on the events that matter most:

              - collector-radar sent {ticker, volume, close_price, z_score}. It
                computed volume * price, refused to emit below $150k, and then
                dropped the number -- so `notional_usd` was absent and read as
                0.0, and every anomaly it raised died at the $50k floor below.
                Measured: a real CGCP anomaly carried $341,415 of flow and
                arrived here as $0.
              - enriched equity anomalies carry `premium_usd`, an options field,
                fixed at 0.0 for equities, with close_price null.
              - options flow genuinely uses premium_usd.

            So notional is taken from whichever the producer supplied, and
            derived from volume * price when only those exist.
            """
            for key in ("notional_usd", "notional", "premium_usd", "value_usd"):
                value = _safe_float(d.get(key))
                if value > 0:
                    return value
            volume = _safe_float(d.get("volume") or d.get("size") or d.get("shares"))
            price = _safe_float(
                d.get("close_price") or d.get("price") or d.get("last_price")
            )
            return volume * price

        if "raw_payload" in message and isinstance(message["raw_payload"], dict):
            p = message["raw_payload"]
            ticker = p.get("ticker")
            z_score = _safe_float(p.get("z_score"))
            notional_usd = _notional_from(p)
        elif "financial_data" in message and isinstance(message["financial_data"], dict):
            fd = message["financial_data"]
            ticker = fd.get("ticker")
            z_score = _safe_float(message.get("anomaly_score")) * 5.0
            notional_usd = _notional_from(fd)
        elif "trigger" in message and isinstance(message["trigger"], dict):
            trig = message["trigger"]
            ticker = trig.get("ticker")
            z_score = _safe_float(trig.get("anomaly_score"))
            notional_usd = _notional_from(trig)

        if ticker:
            ticker = str(ticker).upper().strip()

        return ticker, z_score, notional_usd

    async def prune_watchlist_if_needed(self):
        """
        Agentic Reasoning Watchlist Pruning:
        Enforces Finnhub WebSocket free-tier limit of 50 tickers.
        When sentinel:watched:equities size >= 45, passes candidate tickers to LLM to reason 
        which lower conviction/stale tickers to remove from surveillance.
        """
        try:
            equities_key = "sentinel:watched:equities"
            total_count = await self.redis.raw.zcard(equities_key)
            if total_count < 45:
                return

            raw_items = await self.redis.raw.zrange(equities_key, 0, -1, withscores=True)
            if not raw_items:
                return

            tickers = [t.decode('utf-8') if isinstance(t, bytes) else str(t) for t, _ in raw_items]
            
            prompt = f"""
            You are a quantitative portfolio manager and surveillance systems engineer.
            The surveillance watchlist currently tracks {total_count} equities, approaching the Finnhub API limit of 50.
            
            Active Tickers in Watchlist:
            {', '.join(tickers)}
            
            Evaluate this watchlist and select 5 to 10 tickers that are lowest-conviction or lowest strategic priority to remove from surveillance.
            Return ONLY valid JSON.
            Schema: {{"evict_tickers": ["TICKER1", "TICKER2"], "rationale": "string"}}
            """

            try:
                decision = await self._execute_with_telemetry(
                    message={"system": "watchlist_prune"},
                    system_prompt="You are a quantitative portfolio manager.",
                    user_prompt=prompt,
                    schema=WatchlistPruneDecision,
                    temperature=0.1,
                    num_predict=256,
                )
                evict_tickers = [t.upper().strip() for t in decision.evict_tickers if t.upper().strip() in tickers]
                if evict_tickers:
                    await self.redis.raw.zrem(equities_key, *evict_tickers)
                    self.logger.info(
                        f"🧠 AGENT REASONING PRUNING: Evicted {len(evict_tickers)} tickers ({', '.join(evict_tickers)}) "
                        f"from sentinel:watched:equities. Rationale: {decision.rationale} (Size: {total_count} -> {total_count - len(evict_tickers)})."
                    )
                    return
            except Exception as e:
                self.logger.warning(f"LLM pruning reasoning unavailable ({e}). Fallback to deterministic oldest score pruning.")

            # Fallback: Evict 5 oldest if LLM reasoning unavailable
            oldest_items = await self.redis.raw.zrange(equities_key, 0, 5, withscores=False)
            fallback_evict = [t.decode('utf-8') if isinstance(t, bytes) else str(t) for t in oldest_items]
            if fallback_evict:
                await self.redis.raw.zrem(equities_key, *fallback_evict)
                self.logger.info(f"⚡ DETERMINISTIC PRUNING: Evicted {fallback_evict} from watchlist.")
        except Exception as e:
            self.logger.warning(f"Error during watchlist pruning: {e}")

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ticker, z_score, notional_usd = self._extract_radar_params(message)

        # 1. PRIMARY EQUITY & DERIVATIVE FILTER (No NVDY, options, derivatives, or crypto)
        if not ticker or not is_valid_primary_equity(ticker):
            return None

        # 2. HEIGHTENED ANOMALY FLOW GATEKEEPER ($50k notional minimum)
        #
        # Flow is not the only way an equity becomes interesting. An earnings
        # surprise has no notional at all -- there is no trade behind it -- so a
        # dollar floor silently excluded the entire category: 501 earnings
        # events over three days, every one with premium_usd null, every one
        # dropped here. Those are judged on the size of the surprise instead,
        # which is the quantity that actually carries the signal.
        surprise_pct = abs(_earnings_surprise_pct(message))
        if surprise_pct < MIN_EARNINGS_SURPRISE_PCT and notional_usd < 50_000:
            return None
        
        # Idempotency: Do not re-evaluate a ticker we already escalated today
        if await self.is_recently_processed(ticker, self.cooldown_seconds):
            self.logger.info(f"Idempotency: Skipped evaluating ticker '{ticker}' (already evaluated recently).")
            return None
        
        # ─── AGENTIC REASONING ───
        # ─── CROSS-DOMAIN QUANT REGIME METRICS ───
        from shared.utils import quant_calc
        import numpy as np

        closes = []
        try:
            raw_candles = await self.redis.raw.lrange(f"sentinel:candles:1h:{ticker}", 0, -1)
            for c in raw_candles:
                item = json.loads(c if isinstance(c, str) else c.decode("utf-8"))
                if isinstance(item, dict):
                    c_val = item.get("close")
                    if c_val is not None:
                        try:
                            closes.append(float(c_val))
                        except (ValueError, TypeError):
                            pass
        except Exception:
            pass

        if len(closes) >= 20:
            returns = quant_calc.simple_returns(closes)
            hurst_val = quant_calc.hurst_exponent(closes)
            garch_vol = quant_calc.garch_volatility(returns, annualize=True)
            regime_str = f"Hurst Exponent: {hurst_val:.3f} ({'Trending' if hurst_val > 0.5 else 'Mean-Reverting'}) | GARCH(1,1) Volatility: {garch_vol:.2%}"
        else:
            regime_str = "Hurst/GARCH Regime: Baseline Initialization"

        self.logger.info(f"🔍 Evaluating anomaly for {ticker} | Z-Score: {z_score:.2f} | Flow: ${notional_usd / 1e6:.2f}M | {regime_str}")
        entity_context = await self.fetch_entity_context(ticker)
        cross_context = await self.get_cross_agent_context(ticker=ticker, limit=2)
        cross_block = f"\nCross-Agent Intelligence:\n{cross_context}\n" if cross_context else ""
        
        prompt = f"""
        You are a quantitative trading systems engineer.
        A background radar has detected an institutional volume anomaly for primary US equity: {ticker}.
        
        Metrics:
        - Z-Score: {z_score:.2f} (standard deviations above the EMA)
        - Notional 1-Minute Flow: ${notional_usd / 1_000_000:.2f} Million
        - Instrument Regime: {regime_str}
        {cross_block}
        {entity_context}
        
        Determine if this ${notional_usd / 1_000_000:.2f}M anomaly warrants active high-frequency tracking. 
        Focus on identifying 'smart money' sweeps.
        Return ONLY valid JSON.
        Schema: {{"investigate": boolean, "rationale": "string"}}
        """

        try:
            decision = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are a quantitative trading systems engineer.",
                user_prompt=prompt,
                schema=RadarDecision,
                temperature=0.1,
                num_predict=256,
            )

            if decision.investigate:
                self.logger.info(f"🧠 AGENT ESCALATION: {ticker} -> Primary Surveillance. Rationale: {decision.rationale}")

                # ─── AGENTIC REASONING WATCHLIST PRUNING BEFORE INJECTION ───
                await self.prune_watchlist_if_needed()

                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -46)
                    await pipe.execute()
                
                await self.mark_processed(ticker, self.cooldown_seconds)

                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.rationale,
                    "z_score_trigger": z_score
                }
            else:
                self.logger.info(f"🧠 AGENT BYPASS: {ticker} not escalated. Rationale: {decision.rationale}")
        except Exception as e:
            self.logger.warning(f"Agent LLM reasoning unavailable for {ticker}: {e}. Engaging Deterministic Quant Rules.")
            if z_score >= 3.5 and notional_usd >= 50_000:
                decision = RadarDecision(
                    investigate=True, 
                    rationale=f"Deterministic Quant Fallback: Volume Z-score {z_score:.2f} >= 3.5 with ${notional_usd/1e6:.2f}M flow."
                )
                self.logger.info(f"⚡ DETERMINISTIC ESCALATION: {ticker} -> Primary Surveillance.")
                
                await self.prune_watchlist_if_needed()

                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -46)
                    await pipe.execute()
                await self.mark_processed(ticker, self.cooldown_seconds)
                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.rationale,
                    "z_score_trigger": z_score
                }
        
        return None
