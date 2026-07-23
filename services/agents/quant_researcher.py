"""
services/agents/quant_researcher.py

QUANT RESEARCHER AGENT
=======================
Triggered by anomalous market events (block trades, ML-flagged candles,
large liquidations). Autonomously researches WHY the instrument is moving
and discovers correlated peers that should be monitored immediately.

Refactored for strict Redis key determinism, asynchronous IO offloading,
and memory safety to prevent Event Loop blocking.
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError, InferenceError
from shared.utils.equities import is_valid_primary_equity, fast_classify_equity
from .prompts import (
    QUANT_PEER_DISCOVERY_SYSTEM,
    build_quant_discovery_prompt,
    QUANT_PEER_DISCOVERY_USER_TEMPLATE,
    QUANT_CRYPTO_BASKET_USER_TEMPLATE,
)

logger = logging.getLogger("agent.quant_researcher")

# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────


class PeerTicker(BaseModel):
    ticker: str
    rationale: str
    relationship_type: str
    expected_direction: str = "uncertain"
    discovery_confidence: float = 0.5
    monitoring_urgency: str = "watchlist"

class MacroInstrument(BaseModel):
    ticker: str
    rationale: str
    expected_direction: str = "uncertain"
    discovery_confidence: float = 0.5

class PeerDiscovery(BaseModel):
    trigger_analysis: str
    is_primary_equity: bool = True
    asset_class: str = "PRIMARY_COMMON_EQUITY"
    equity_validation_reason: Optional[str] = "Verified primary US common equity"
    catalyst_category: str
    peer_tickers: List[PeerTicker] = Field(default_factory=list)
    macro_instruments: List[MacroInstrument] = Field(default_factory=list)
    commodities_affected: List[str] = Field(default_factory=list)
    geopolitical_angle: Optional[str] = None
    risk_to_thesis: Optional[str] = None

# ── THRESHOLDS ────────────────────────────────────────────────────────────────

RESEARCH_TRIGGER_SCORE = 0.65
WATCHLIST_CONFIDENCE_THRESHOLD = 0.70
DEDUP_WINDOW_SECONDS = 1800  # 30 minutes
MAX_WATCHLIST_ADDITIONS = 8

TRIGGER_EVENT_TYPES = {
    "equity_block",
    "market_anomaly",
    "volume_anomaly",
    "crypto_liquidation",
    "crypto_trade",
    "options_flow",
    "insider_trade",
}

class QuantResearcherAgent(SentinelAgent):
    """
    Autonomous quantitative researcher.
    Discovers correlated instruments from market anomalies and
    dynamically expands surveillance coverage.
    """
    @property
    def output_topic(self) -> str:
        return "agents.quant.discoveries"
    
    def _state_key(self, prefix: str, ticker: str) -> str:
        """
        Explicitly defined, strictly typed deterministic key generator.
        """
        clean_prefix = str(prefix).strip().lower()
        clean_ticker = str(ticker).strip().upper()
        return f"sentinel:quant:{clean_prefix}:{clean_ticker}"

    def calculate_garch_volatility(self, returns: List[float], omega: float = 0.000002, alpha: float = 0.08, beta: float = 0.90) -> float:
        """
        Calculates GARCH(1,1) conditional volatility forecast (h_t = omega + alpha*e_{t-1}^2 + beta*h_{t-1}).
        Returns annualized volatility estimate.
        """
        import numpy as np
        if not returns or len(returns) < 5:
            return 0.20  # Default 20% annualized volatility baseline
        
        arr = np.array(returns)
        var = np.var(arr) if np.var(arr) > 0 else 0.0001
        
        for r in arr:
            var = omega + (alpha * (r ** 2)) + (beta * var)
            
        daily_vol = np.sqrt(var)
        annualized_vol = daily_vol * np.sqrt(252)
        return float(annualized_vol)

    def calculate_kelly_position_size(self, win_rate: float, win_loss_ratio: float, kelly_fraction: float = 0.5) -> float:
        """
        Computes fractional Kelly Criterion position sizing: f* = max(0, (p * b - q) / b).
        kelly_fraction: Fractional Kelly scaling (e.g. 0.5 for Half-Kelly risk management).
        """
        if win_loss_ratio <= 0 or win_rate <= 0 or win_rate >= 1:
            return 0.0
            
        p = win_rate
        q = 1.0 - win_rate
        b = win_loss_ratio
        
        full_kelly = (p * b - q) / b
        fractional_kelly = max(0.0, full_kelly * kelly_fraction)
        return round(float(fractional_kelly), 4)

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        
        # ── SCENARIO HANDLING ───────────────────────────────────────────────
        if "scenario_id" in message:
            return await self._handle_scenario(message)
            
        event_type = message.get("type", "")
        if event_type not in TRIGGER_EVENT_TYPES:
            return None

        anomaly_score = float(message.get("anomaly_score", 0.0))
        if anomaly_score == 0.0:
            fd = message.get("financial_data") or message.get("raw_payload") or {}
            if isinstance(fd, dict) and "z_score" in fd:
                anomaly_score = min(1.0, float(fd["z_score"]) / 5.0)

        if anomaly_score < RESEARCH_TRIGGER_SCORE:
            return None
            
        tags = message.get("tags", [])
        is_crypto_candle = event_type == "market_anomaly" and "crypto" in tags
        
        if is_crypto_candle:
            # Buffer the crypto candle
            await self.redis.raw.rpush("sentinel:quant:crypto_candle_buffer", json.dumps(message))
            
            # Check if 5 minutes have passed since last batch
            last_run = await self.redis.raw.get("sentinel:quant:crypto_last_process_time")
            now = time.time()
            if not last_run or (now - float(last_run)) > 300:
                await self.redis.raw.set("sentinel:quant:crypto_last_process_time", str(now))
                
                # Fetch all buffered candles and clear buffer atomically using a transaction pipeline
                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.lrange("sentinel:quant:crypto_candle_buffer", 0, -1)
                    pipe.delete("sentinel:quant:crypto_candle_buffer")
                    results = await pipe.execute()
                
                raw_candles = results[0]
                if raw_candles:
                    candles = [json.loads(c) for c in raw_candles]
                    return await self._handle_crypto_basket(candles)
            return None
        
        ticker = self._extract_ticker(message)
        if not ticker:
            return None

        # ── FAST EQUITY GATEKEEPER: Ensure ticker is NOT leveraged, inverse, or derivative ──
        class_info = fast_classify_equity(ticker)
        if not class_info["is_primary_equity"]:
            logger.warning(
                f"⛔ QUANT RESEARCHER DISCARD: {ticker} rejected because it is a {class_info['asset_class']} "
                f"({class_info['reason']}). Dropping before Redis & LLM."
            )
            # Add to blocked set in Redis so collector watchlists exclude it
            try:
                await self.redis.raw.sadd("sentinel:blocked:tickers", ticker.upper())
            except Exception as rx:
                pass
            return None
        
        dedup_key = self._state_key("seen", f"{ticker}:{event_type}")
        raw_prev_score = await self.redis.raw.get(dedup_key)
        prev_score = float(raw_prev_score) if raw_prev_score else 0.0

        # Ignore if we've seen it recently AND the new event isn't a massive escalation (+0.15 delta)
        if prev_score > 0 and anomaly_score < (prev_score + 0.15):
            await self._accumulate_volume(ticker, message)
            return None
        
        # Lock the ticker with the NEW high score
        await self.redis.raw.set(dedup_key, str(anomaly_score), ex=DEDUP_WINDOW_SECONDS)

        notional = self._extract_notional(message)
        logger.info(
            f"Researching {ticker} | type={event_type} "
            f"notional=${notional/1e6:.1f}M anomaly={anomaly_score:.2f}"
        )

        # ── STEP 1: Gather research context ───────────────────────────────────
        # PERFORMANCE: asyncio.gather runs these network/database requests at the 
        # exact same time (concurrently), rather than waiting for them one by one.
        news_context, graph_context, current_watchlist, macro_context, ontology_context, agent_memories = await asyncio.gather(
            self._fetch_news_context(ticker),
            self._fetch_graph_context(ticker),
            self._get_current_watchlist(),
            self._fetch_macro_context(),
            self._fetch_ontology_context(ticker),
            self.read_agent_memories(limit=8)
        )

        # ── STEP 2: LLM peer discovery ────────────────────────────────────────
        user_prompt = QUANT_PEER_DISCOVERY_USER_TEMPLATE.format(
            ticker=ticker,
            event_type=event_type,
            notional_m=notional / 1e6,
            anomaly_score=anomaly_score,
            direction=self._extract_direction(message),
            ontology_context=json.dumps(ontology_context, separators=(',', ':'), default=str) if ontology_context else "None",
            macro_context=json.dumps(macro_context, separators=(',', ':'), default=str) if macro_context else "None",
            news_context=json.dumps(news_context[:8], separators=(',', ':'), default=str),
            agent_memories=agent_memories,
            graph_context=json.dumps(graph_context[:10], separators=(',', ':'), default=str),
            current_watchlist=json.dumps(list(current_watchlist)[:20], separators=(',', ':')),
        )

        try:
            dynamic_sys = build_quant_discovery_prompt(
                ticker=ticker,
                macro_regime=str(macro_context.get("rates_regime", "Normal") if isinstance(macro_context, dict) else "Normal")
            )
            discovery: PeerDiscovery = await self._execute_with_telemetry(
                message=message,
                system_prompt=dynamic_sys,
                user_prompt=user_prompt,
                schema=PeerDiscovery,
                temperature=0.15,
            )
        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Peer discovery failed: {e}")
            return None
        
        logger.info(
            f"🔬 QUANT THESIS GENERATED | Ticker: {ticker} | Category: '{discovery.catalyst_category}' "
            f"| Correlated Peers: {[p.ticker for p in discovery.peer_tickers[:4]]} "
            f"| Macro Instruments: {[m.ticker for m in discovery.macro_instruments[:3]]}"
        )
        
        unknowns = discovery.peer_tickers + discovery.macro_instruments
        if unknowns:
            unknown_tasks = [
                self._producer.send(
                    "agents.ontology.unknown_entities",
                    {
                        "entity_name": item.ticker,
                        "context": discovery.trigger_analysis[:500],
                        "source_domain": "quant_researcher",
                        "frequency": 1
                    },
                    key=item.ticker
                )
                for item in unknowns
            ]
            await asyncio.gather(*unknown_tasks, return_exceptions=True)
        
        # ── STEP 3: Inject peers (Now Async) ──────────────────────────────────
        added = await self._inject_peers(ticker, discovery, event_type)

        # ── STEP 4: Write discovery to Neo4j ──────────────────────────────────
        asyncio.create_task(self._write_graph_relationships(ticker, discovery))

        # ── STEP 5: Write Episodic Memory ─────────────────────────────────────
        if added or anomaly_score >= 0.8:
            peer_list = [p.ticker for p in discovery.peer_tickers if p.discovery_confidence > 0.6]
            inverse_peers = [p.ticker for p in discovery.peer_tickers if p.relationship_type == "inverse_exposure_to"]
            inverse_str = f" Inverse exposure: {inverse_peers}." if inverse_peers else ""
            mem_text = f"Detected anomalous flow in {ticker} (Score: {anomaly_score:.2f}, ${notional/1e6:.1f}M). Discovered correlated peers: {peer_list}.{inverse_str}"
            asyncio.create_task(self.write_agent_memory(mem_text))

        # ── STEP 6: Update accumulator (Now Async) ────────────────────────────
        await self._accumulate_volume(ticker, message)

        # ── STEP 7: Publish discovery ─────────────────────────────────────────
        # Handle Pydantic v1 vs v2 compatibility gracefully
        discovery_dict = discovery.model_dump() if hasattr(discovery, "model_dump") else discovery.dict()

        return {
            "agent":            self.name,
            "agent_run_id":     f"quant_{ticker}_{event_type}",
            "trigger": {
                "ticker":           ticker,
                "event_type":       event_type,
                "notional_usd":     notional,
                "anomaly_score":    anomaly_score,
                "source_event_id":  message.get("event_id"),
                "trace_id":         message.get("trace_id"),
            },
            "discovery":         discovery_dict,
            "peers_added_to_watchlist": added,
            "created_at":       datetime.now(timezone.utc).isoformat(),
        }

    async def _handle_crypto_basket(self, candles: List[Dict]) -> Optional[Dict]:
        """Analyzes a basket of anomalous crypto candles collected over 5 minutes."""
        if not candles:
            return None
            
        logger.info(f"Researching Crypto Basket with {len(candles)} anomalies")
        
        basket_summary = []
        for c in candles:
            ticker = self._extract_ticker(c)
            score = float(c.get("anomaly_score", 0))
            direction = self._extract_direction(c)
            notional = self._extract_notional(c)
            basket_summary.append(f"- {ticker}: {direction} (Anomaly {score:.2f}, Vol: ${notional/1e6:.1f}M)")
            
        summary_str = "\n".join(basket_summary)
        
        news_context, current_watchlist = await asyncio.gather(
            self._fetch_news_context("crypto"),
            self._get_current_watchlist()
        )
        
        user_prompt = QUANT_CRYPTO_BASKET_USER_TEMPLATE.format(
            basket_summary=summary_str,
            news_context=json.dumps(news_context[:8], default=str),
            current_watchlist=json.dumps(list(current_watchlist)[:20]),
        )
        
        try:
            discovery: PeerDiscovery = await self._execute_with_telemetry(
                message=candles[0], # telemetry tracing
                system_prompt=QUANT_PEER_DISCOVERY_SYSTEM,
                user_prompt=user_prompt,
                schema=PeerDiscovery,
                temperature=0.2,
            )
        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Crypto basket peer discovery failed: {e}")
            return None
            
        added = await self._inject_peers("CRYPTO_BASKET", discovery, "crypto_basket")
        asyncio.create_task(self._write_graph_relationships("CRYPTO_BASKET", discovery))
        
        discovery_dict = discovery.model_dump() if hasattr(discovery, "model_dump") else discovery.dict()

        return {
            "agent":            self.name,
            "agent_run_id":     f"quant_crypto_basket_{int(time.time())}",
            "trigger": {
                "basket_size": len(candles),
            },
            "discovery":         discovery_dict,
            "peers_added_to_watchlist": added,
            "created_at":       datetime.now(timezone.utc).isoformat(),
        }

    # ── CONTEXT FETCHERS ──────────────────────────────────────────────────────

    async def _fetch_news_context(self, ticker: str) -> List[Dict]:
        try:
            rows = await self.db.query("""
                SELECT headline, anomaly_score, occurred_at, named_entities, tags
                FROM events
                WHERE type = 'headline'
                  AND occurred_at > NOW() - INTERVAL '2 hours'
                  AND anomaly_score >= 0.3
                  AND (
                    LOWER(headline) LIKE $1
                    OR $2 = ANY(tags)
                  )
                ORDER BY anomaly_score DESC
                LIMIT 8
            """, f"%{ticker.lower()}%", ticker.lower())
            return [{"headline": r["headline"], "score": r["anomaly_score"]} for r in rows]
        except Exception as e:
            logger.debug(f"News context fetch error: {e}")
            return []

    async def _fetch_graph_context(self, ticker: str) -> List[Dict]:
        loop = asyncio.get_running_loop()
        try:
            rows = await self.neo4j.query("""
                MATCH (n {name: $ticker})-[r]-(m)
                RETURN type(r) as relationship, m.name as connected,
                       labels(m) as labels
                LIMIT 20
            """, {"ticker": ticker})
            return [
                {
                    "relationship": r.get("relationship"),
                    "connected_entity": r.get("connected"),
                    "entity_type": r.get("labels", ["Unknown"])[0],
                }
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"Graph context fetch error: {e}")
            return []

    async def _get_current_watchlist(self) -> set:
        """Asynchronously returns the current equity watchlist from Redis."""
        try:
            # We use the async raw client directly to avoid thread-blocking.
            # Using zrange since equities is a sorted set tracking injection time
            raw = await self.redis.raw.zrange("sentinel:watched:equities", 0, -1)
            return {t.decode("utf-8") if isinstance(t, bytes) else t for t in raw}
        except Exception:
            return set()
            
    async def _fetch_macro_context(self) -> Optional[Dict]:
        """Fetches the latest macro strategist brief from Redis."""
        try:
            raw = await self.redis.raw.get("sentinel:macro:latest_brief")
            if raw:
                return json.loads(raw)
            return None
        except Exception as e:
            logger.debug(f"Macro context fetch error: {e}")
            return None
            
    async def _fetch_ontology_context(self, ticker: str) -> Optional[Dict]:
        """Fetches the pre-computed ontology classification from Redis."""
        try:
            raw = await self.redis.raw.get(f"sentinel:ontology:entity:{ticker.lower()}")
            if raw:
                return json.loads(raw)
            return None
        except Exception as e:
            logger.debug(f"Ontology context fetch error for {ticker}: {e}")
            return None
        
    # ── INJECTION LOGIC ───────────────────────────────────────────────────────
    
    async def _inject_peers(
            self, 
            trigger_ticker: str, 
            discovery: PeerDiscovery, 
            event_type: str
    ) -> List[str]:
        """
        Add verified primary equity trigger and high-confidence peers to the Redis watchlist.
        Returns list of newly added tickers. IO is safely offloaded.
        """
        added = []
        candidates = []

        # 1. Include verified primary equity trigger ticker itself
        if is_valid_primary_equity(trigger_ticker):
            candidates.append((trigger_ticker, "immediate", 0.95))

        for peer in discovery.peer_tickers:
            if (
                peer.discovery_confidence >= WATCHLIST_CONFIDENCE_THRESHOLD
                and is_valid_primary_equity(peer.ticker)
            ):
                candidates.append((peer.ticker, peer.monitoring_urgency, peer.discovery_confidence))

        for macro in discovery.macro_instruments:
            if (
                macro.discovery_confidence >= WATCHLIST_CONFIDENCE_THRESHOLD
                and is_valid_primary_equity(macro.ticker)
            ):
                candidates.append((macro.ticker, "within_4h", macro.discovery_confidence))
                
        # Sort by confidence, cap total additions
        candidates.sort(key=lambda x: x[2], reverse=True)
        candidates = candidates[:MAX_WATCHLIST_ADDITIONS]

        for ticker, urgency, confidence in candidates:
            try:
                # Reasoning Service Double-Check: Confirm valid primary equity / BTC via LLM
                is_verified = await self.verify_ticker_with_reasoning(ticker)
                if not is_verified:
                    continue

                # 1. IO Offloaded Set Addition
                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -51)
                    results = await pipe.execute()
                is_new = results[0] > 0 if results else False
                
                if is_new:
                    added.append(ticker)
                    
                    # 2. Fully Hydrated, Serializable JSON Payload
                    # Truncates LLM output to 200 chars to prevent Redis RAM exhaustion
                    payload = json.dumps({
                        "trigger":    trigger_ticker,
                        "event_type": event_type,
                        "confidence": confidence,
                        "analysis":   discovery.trigger_analysis[:200], 
                        "added_at":   datetime.now(timezone.utc).isoformat(),
                    })
                    
                    # 3. Deterministic key generation and atomic write
                    target_key = self._state_key("discovery", ticker)
                    await self.redis.raw.set(target_key, payload, ex=3600)
                    
                    log_level = logger.warning if urgency == "immediate" else logger.info
                    log_level(
                        f"  🤖 AUTONOMOUS PIVOT: {trigger_ticker} → "
                        f"{ticker} (confidence={confidence:.2f} urgency={urgency})"
                    )
            except Exception as e:
                logger.warning(f"Watchlist injection failed for {ticker}: {e}")

        if added:
            logger.info(f"Watchlist expanded: +{len(added)} tickers from {trigger_ticker} research")

        return added

    async def _write_graph_relationships(self, trigger_ticker: str, discovery: PeerDiscovery):
        """
        Delegates Neo4j writes to the Graph Supervisor via Kafka.
        Prevents concurrent transaction deadlocks.
        """
        for peer in discovery.peer_tickers:
            if peer.discovery_confidence < 0.7:
                continue
            
            rel_type = self._map_relationship_type(peer.relationship_type)
            proposal = {
                "entity_id": trigger_ticker,
                "trace_id": message.get("trace_id"),
                "action": "LINK_ENTITY",
                "data": {
                    "target_id": peer.ticker,
                    "source_label": "Instrument",
                    "target_label": "Instrument",
                    "relation_type": rel_type,
                    "weight": peer.discovery_confidence
                }
            }
            try:
                await self._producer.send("sentinel.ontology.proposals", proposal, key=trigger_ticker)
            except Exception as e:
                logger.error(f"Failed to propose graph relationship {trigger_ticker}→{peer.ticker}: {e}")

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _extract_ticker(self, message: Dict) -> Optional[str]:
        fd = message.get("financial_data") or message.get("raw_payload") or {}
        if isinstance(fd, dict) and fd.get("ticker"):
            return str(fd["ticker"]).upper().strip()

        cd = message.get("crypto_data") or {}
        if isinstance(cd, dict) and cd.get("pair"):
            return str(cd["pair"]).upper().strip()

        entity = message.get("primary_entity") or {}
        if isinstance(entity, dict) and entity.get("id"):
            return str(entity["id"]).upper().strip()

        return None

    def _extract_notional(self, message: Dict) -> float:
        fd = message.get("financial_data") or message.get("raw_payload") or {}
        if isinstance(fd, dict):
            if "notional_usd" in fd:
                return float(fd["notional_usd"])
            if "volume" in fd and "close_price" in fd:
                return float(fd["volume"]) * float(fd["close_price"])
            if "premium_usd" in fd:
                return float(fd["premium_usd"])
        cd = message.get("crypto_data") or {}
        if isinstance(cd, dict):
            return float(cd.get("price", 0)) * float(cd.get("size_tokens", 0))
        return 0.0

    def _extract_direction(self, message: Dict) -> str:
        fd = message.get("financial_data") or {}
        if isinstance(fd, dict):
            return fd.get("side") or "unknown"
        return "unknown"

    async def _accumulate_volume(self, ticker: str, message: Dict):
        """Asynchronously tracks cumulative notional volume per ticker in Redis."""
        notional = self._extract_notional(message)
        if notional <= 0:
            return
            
        key = self._state_key("volume", ticker)
        try:
            # Safely offload IO bound Redis increment commands
            await self.redis.raw.incrbyfloat(key, notional)
            await self.redis.raw.expire(key, 3600)  # Reset hourly
        except Exception:
            pass

    @staticmethod
    def _is_valid_ticker(ticker: str) -> bool:
        return is_valid_primary_equity(ticker)

    @staticmethod
    def _map_relationship_type(raw_type: str) -> str:
        mapping = {
            "supplier":            "SUPPLIES",
            "customer":            "PURCHASES_FROM",
            "competitor":          "COMPETES_WITH",
            "subsidiary":          "SUBSIDIARY_OF",
            "positive_exposure_to": "POSITIVE_EXPOSURE_TO",
            "inverse_exposure_to":  "INVERSE_EXPOSURE_TO",
            "same_sector":         "ADJACENT_TO",
        }
        return mapping.get(raw_type.lower(), "CORRELATED_WITH")

    @staticmethod
    def _estimate_garch_volatility(returns: List[float], omega: float = 0.00001, alpha: float = 0.05, beta: float = 0.90) -> float:
        """
        Estimates conditional variance using a GARCH(1,1) process:
        sigma_t^2 = omega + alpha * eps_{t-1}^2 + beta * sigma_{t-1}^2
        Accounts for volatility clustering to prevent false anomaly triggers.
        """
        if not returns or len(returns) < 2:
            return 0.01

        np_returns = np.array(returns, dtype=np.float64)
        sigma2 = np.var(np_returns)
        if sigma2 == 0:
            sigma2 = 0.0001

        for r in np_returns:
            eps2 = r * r
            sigma2 = omega + alpha * eps2 + beta * sigma2

        return float(np.sqrt(max(1e-6, sigma2)))
        
    async def _evaluate_signal_decay(self, rule_name: str, returns: List[float], risk_free_rate: float = 0.04) -> Dict[str, float]:
        """
        Evaluates signal decay over a 30-day sliding window.
        Calculates Sharpe Ratio (S = (Rp - Rf) / sigma_p) and Information Ratio (IR = (Rp - Rb) / tracking_error).
        Emits pruning recommendations to Topics.RULES_FEEDBACK if Sharpe ratio < 0.50.
        """
        if not returns or len(returns) < 5:
            return {"sharpe_ratio": 1.0, "information_ratio": 1.0, "decay_warning": 0.0}

        np_ret = np.array(returns, dtype=np.float64)
        mean_ret = float(np.mean(np_ret))
        std_ret = float(np.std(np_ret))
        if std_ret == 0:
            std_ret = 0.0001

        # Annualized Sharpe Ratio
        sharpe = round((mean_ret - (risk_free_rate / 252.0)) / std_ret * np.sqrt(252.0), 3)
        info_ratio = round(mean_ret / std_ret * np.sqrt(252.0), 3)
        is_decayed = sharpe < 0.50

        if is_decayed:
            self.logger.warning(f"⚠️ SIGNAL DECAY DETECTED for rule '{rule_name}' | Sharpe: {sharpe:.2f} | IR: {info_ratio:.2f}. Emitting pruning recommendation.")
            try:
                await self._producer.send(
                    Topics.RULES_FEEDBACK,
                    {
                        "action": "PRUNE_RULE_DECAY",
                        "rule_name": rule_name,
                        "sharpe_ratio": sharpe,
                        "information_ratio": info_ratio,
                        "reason": f"Signal alpha decayed below threshold (Sharpe={sharpe:.2f} < 0.50)"
                    },
                    key=rule_name
                )
            except Exception as ex:
                self.logger.error(f"Failed to emit rule pruning feedback for {rule_name}: {ex}")

        return {
            "sharpe_ratio": sharpe,
            "information_ratio": info_ratio,
            "decay_warning": 1.0 if is_decayed else 0.0
        }

    async def _handle_scenario(self, scenario: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.logger.info(f"QuantResearcher processing scenario: {scenario.get('scenario_id')}")
        hypotheses = scenario.get("hypotheses", [])
        for hypothesis in hypotheses:
            self.logger.info(f"QuantResearcher assessing hypothesis: {hypothesis}")
            # Could trigger specific peer research here based on hypothesis text.
        return None