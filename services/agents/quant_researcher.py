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
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .base import SentinelAgent, SchemaViolationError
from .prompts import (
    QUANT_PEER_DISCOVERY_SYSTEM,
    QUANT_PEER_DISCOVERY_USER_TEMPLATE,
)

logger = logging.getLogger("agent.quant_researcher")

# ── OUTPUT SCHEMAS ────────────────────────────────────────────────────────────

# CONCEPT: Pydantic Data Validation
# We define strict structures here. When the AI (Llama3) generates a response,
# Pydantic ensures it perfectly matches this format, preventing downstream crashes.

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
        # BEST PRACTICE: Cache Key Normalization
        # We force lowercase/uppercase and strip spaces to prevent creating
        # multiple duplicate keys like "AAPL ", "aapl", and "AAPL" in Redis.
        clean_prefix = str(prefix).strip().lower()
        clean_ticker = str(ticker).strip().upper()
        return f"sentinel:quant:{clean_prefix}:{clean_ticker}"

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # BEST PRACTICE: Guard Clauses / Early Returns
        # Check for invalid conditions first and exit immediately (`return None`). 
        # This keeps the main logic clean (less indentation) and saves expensive LLM compute.
        event_type = message.get("type", "")
        if event_type not in TRIGGER_EVENT_TYPES:
            return None

        anomaly_score = float(message.get("anomaly_score", 0))
        if anomaly_score < RESEARCH_TRIGGER_SCORE:
            return None
        
        ticker = self._extract_ticker(message)
        if not ticker:
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
        news_context, graph_context, current_watchlist = await asyncio.gather(
            self._fetch_news_context(ticker),
            self._fetch_graph_context(ticker),
            self._get_current_watchlist()
        )

        # ── STEP 2: LLM peer discovery ────────────────────────────────────────
        user_prompt = QUANT_PEER_DISCOVERY_USER_TEMPLATE.format(
            ticker=ticker,
            event_type=event_type,
            notional_m=notional / 1e6,
            anomaly_score=anomaly_score,
            direction=self._extract_direction(message),
            news_context=json.dumps(news_context[:8], default=str),
            graph_context=json.dumps(graph_context[:10], default=str),
            current_watchlist=json.dumps(list(current_watchlist)[:20]),
        )

        try:
            discovery: PeerDiscovery = await self._execute_with_telemetry(
                message=message,
                system_prompt=QUANT_PEER_DISCOVERY_SYSTEM,
                user_prompt=user_prompt,
                schema=PeerDiscovery,
                temperature=0.15,
            )
        except SchemaViolationError as e:
            logger.error(f"Peer discovery failed: {e}")
            return None
        
        unknowns = discovery.peer_tickers + discovery.macro_instruments
        for item in unknowns:
            await self._producer.send(
                "agents.ontology.unknown_entities",
                {
                    "entity_name": item.ticker,
                    "context": discovery.trigger_analysis[:500],
                    "source_domain": "quant_researcher",
                    "frequency": 1
                },
                key=item.ticker
            )
        
        # ── STEP 3: Inject peers (Now Async) ──────────────────────────────────
        added = await self._inject_peers(ticker, discovery, event_type)

        # ── STEP 4: Write discovery to Neo4j ──────────────────────────────────
        asyncio.create_task(self._write_graph_relationships(ticker, discovery))

        # ── STEP 5: Update accumulator (Now Async) ────────────────────────────
        await self._accumulate_volume(ticker, message)

        # ── STEP 6: Publish discovery ─────────────────────────────────────────
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
            },
            "discovery":         discovery_dict,
            "peers_added_to_watchlist": added,
            "created_at":       datetime.now(timezone.utc).isoformat(),
        }

    # ── CONTEXT FETCHERS ──────────────────────────────────────────────────────

    async def _fetch_news_context(self, ticker: str) -> List[Dict]:
        loop = asyncio.get_running_loop()
        try:
            rows = await loop.run_in_executor(
                None,
                lambda: self.db.query("""
                    SELECT headline, anomaly_score, occurred_at, named_entities, tags
                    FROM events
                    WHERE type = 'headline'
                      AND occurred_at > NOW() - INTERVAL '2 hours'
                      AND anomaly_score >= 0.3
                      AND (
                        LOWER(headline) LIKE %s
                        OR %s = ANY(tags)
                      )
                    ORDER BY anomaly_score DESC
                    LIMIT 8
                """, (f"%{ticker.lower()}%", ticker.lower())),
            )
            return [{"headline": r["headline"], "score": r["anomaly_score"]} for r in rows]
        except Exception as e:
            logger.debug(f"News context fetch error: {e}")
            return []

    async def _fetch_graph_context(self, ticker: str) -> List[Dict]:
        loop = asyncio.get_running_loop()
        try:
            rows = await loop.run_in_executor(
                None,
                lambda: self.neo4j.query("""
                    MATCH (n {name: $ticker})-[r]-(m)
                    RETURN type(r) as relationship, m.name as connected,
                           labels(m) as labels
                    LIMIT 20
                """, {"ticker": ticker}),
            )
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
            # CONCEPT: asyncio.to_thread
            # Redis commands like `smembers` are synchronous (they block the thread).
            # `to_thread` safely pushes this work to a background thread so the 
            # main asyncio loop doesn't freeze up while waiting for the network.
            raw = await asyncio.to_thread(self.redis.smembers, "sentinel:watched:equities")
            return {t.decode("utf-8") if isinstance(t, bytes) else t for t in raw}
        except Exception:
            return set()
        
    # ── INJECTION LOGIC ───────────────────────────────────────────────────────
    
    async def _inject_peers(
            self, 
            trigger_ticker: str, 
            discovery: PeerDiscovery, 
            event_type: str
    ) -> List[str]:
        """
        Add high-confidence peers to the Redis watchlist.
        Returns list of newly added tickers. IO is safely offloaded.
        """
        added = []
        candidates = []

        for peer in discovery.peer_tickers:
            if (
                peer.discovery_confidence >= WATCHLIST_CONFIDENCE_THRESHOLD
                and peer.ticker != trigger_ticker
                and self._is_valid_ticker(peer.ticker)
            ):
                candidates.append((peer.ticker, peer.monitoring_urgency, peer.discovery_confidence))

        for macro in discovery.macro_instruments:
            if (
                macro.discovery_confidence >= WATCHLIST_CONFIDENCE_THRESHOLD
                and macro.ticker != trigger_ticker
                and self._is_valid_ticker(macro.ticker)
            ):
                candidates.append((macro.ticker, "within_4h", macro.discovery_confidence))
                
        # Sort by confidence, cap total additions
        candidates.sort(key=lambda x: x[2], reverse=True)
        candidates = candidates[:MAX_WATCHLIST_ADDITIONS]

        for ticker, urgency, confidence in candidates:
            try:
                # 1. IO Offloaded Set Addition
                is_new = await asyncio.to_thread(self.redis.raw.sadd, "sentinel:watched:equities", ticker)
                
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
                    await asyncio.to_thread(self.redis.set, target_key, payload, ex=3600)
                    
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
        fd = message.get("financial_data") or {}
        if isinstance(fd, dict) and fd.get("ticker"):
            return fd["ticker"].upper()

        cd = message.get("crypto_data") or {}
        if isinstance(cd, dict) and cd.get("pair"):
            return cd["pair"].upper()

        entity = message.get("primary_entity") or {}
        if isinstance(entity, dict) and entity.get("id"):
            return entity["id"].upper()

        return None

    def _extract_notional(self, message: Dict) -> float:
        fd = message.get("financial_data") or {}
        if isinstance(fd, dict):
            return float(fd.get("premium_usd") or 0)
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
            await asyncio.to_thread(self.redis.incrbyfloat, key, notional)
            await asyncio.to_thread(self.redis.expire, key, 3600)  # Reset hourly
        except Exception:
            pass

    @staticmethod
    def _is_valid_ticker(ticker: str) -> bool:
        return (
            ticker
            and ticker.isalpha()
            and ticker.isupper()
            and 1 <= len(ticker) <= 5
        )

    @staticmethod
    def _map_relationship_type(relationship_type: str) -> str:
        mapping = {
            "supplier":          "SUPPLIES",
            "customer":          "PURCHASES_FROM",
            "competitor":        "COMPETES_WITH",
            "sector_peer":       "CORRELATED_WITH",
            "commodity_linked":  "COMMODITY_EXPOSURE",
            "macro_correlated":  "MACRO_CORRELATED",
        }
        return mapping.get(relationship_type, "CORRELATED_WITH")