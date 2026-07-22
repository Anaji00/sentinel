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

        if "raw_payload" in message and isinstance(message["raw_payload"], dict):
            p = message["raw_payload"]
            ticker = p.get("ticker")
            z_score = float(p.get("z_score", 0.0))
            notional_usd = float(p.get("notional_usd", 0.0))
        elif "financial_data" in message and isinstance(message["financial_data"], dict):
            fd = message["financial_data"]
            ticker = fd.get("ticker")
            z_score = float(message.get("anomaly_score", 0.0)) * 5.0
            notional_usd = float(fd.get("premium_usd", 0.0))
        elif "trigger" in message and isinstance(message["trigger"], dict):
            trig = message["trigger"]
            ticker = trig.get("ticker")
            z_score = float(trig.get("anomaly_score", 0.0))
            notional_usd = float(trig.get("notional_usd", 0.0))

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

        # 2. HEIGHTENED ANOMALY FLOW GATEKEEPER ($150k notional minimum)
        if notional_usd < 150_000:
            return None
        
        # Idempotency: Do not re-evaluate a ticker we already escalated today
        if await self.is_recently_processed(ticker, self.cooldown_seconds):
            self.logger.info(f"Idempotency: Skipped evaluating ticker '{ticker}' (already evaluated recently).")
            return None
        
        # ─── AGENTIC REASONING ───
        self.logger.info(f"🔍 Evaluating anomaly for {ticker} | Z-Score: {z_score:.2f} | Flow: ${notional_usd / 1e6:.2f}M")
        entity_context = await self.fetch_entity_context(ticker)
        
        prompt = f"""
        You are a quantitative trading systems engineer.
        A background radar has detected an institutional volume anomaly for primary US equity: {ticker}.
        
        Metrics:
        - Z-Score: {z_score:.2f} (standard deviations above the EMA)
        - Notional 1-Minute Flow: ${notional_usd / 1_000_000:.2f} Million
        
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
            if z_score >= 3.5 and notional_usd >= 150_000:
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
