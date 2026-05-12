"""
services/agents/quant_researcher.py

QUANT RESEARCHER AGENT
=======================
Triggered by anomalous market events (block trades, ML-flagged candles,
large liquidations). Autonomously researches WHY the instrument is moving
and discovers correlated peers that should be monitored immediately.

The core feedback loop:
  Anomalous NVDA block trade
    → "Why is NVDA moving?"
    → Query recent news + graph context
    → Llama3: "AI data center demand surge. Watch SMCI, ARM, AVGO, TSM"
    → Inject SMCI, ARM, AVGO, TSM into sentinel:watched:equities
    → Finnhub collector immediately subscribes to those tickers
    → Future anomalies on those tickers are now captured

This is the primary mechanism by which SENTINEL expands its market surveillance
coverage autonomously without human intervention.

Deduplication strategy:
  We deduplicate on (ticker, event_type) with a 30-minute window.
  A single block trade triggers one research cycle. Subsequent trades
  on the same ticker within 30 minutes are grouped (volume accumulation)
  rather than triggering repeated LLM calls.
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

# CONCEPT: Structured LLM Outputs via Pydantic
# We use Pydantic models to strictly define the JSON shape we want the LLM to return.
# This transitions the LLM from a "text generator" to a "reliable data extractor".
# If the LLM misses a field or provides the wrong type, Pydantic throws an error.
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

# Minimum anomaly score to trigger LLM research
RESEARCH_TRIGGER_SCORE = 0.65

# Only add peers with confidence above this to the live watchlist
WATCHLIST_CONFIDENCE_THRESHOLD = 0.70

TRIGGER_EVENT_TYPES = {
    "equity_block",
    "market_anomaly",
    "crypto_liquidation",
    "crypto_trade",
    "options_flow",
    "insider_trade",
}

# Deduplication window per ticker (seconds)
DEDUP_WINDOW_SECONDS = 1800  # 30 minutes

# Maximum peers to add to watchlist per research cycle
MAX_WATCHLIST_ADDITIONS = 8


class QuantResearcherAgent(SentinelAgent):
    """
    Autonomous quantitative researcher.
    Discovers correlated instruments from market anomalies and
    dynamically expands surveillance coverage.
    """
    @property
    def output_topic(self) -> str:
        return "agents.quant.discoveries"
    
    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        
        # BEST PRACTICE: Guard Clauses (Early Returns)
        # We check failure conditions immediately and return `None`. This prevents deeply nested
        # `if` statements and saves expensive LLM compute time if the event is irrelevant.
        event_type = message.get("type", "")
        if event_type not in TRIGGER_EVENT_TYPES:
            return None

        anomaly_score = float(message.get("anomaly_score", 0))
        if anomaly_score < RESEARCH_TRIGGER_SCORE:
            return None
        
        ticker = self._extract_ticker(message)
        if not ticker:
            return None
        
        # CONCEPT: Deduplication & Idempotency
        # Market data streams often have highly repetitive events (e.g., 5 trades in 1 second). 
        # We use a Redis key with a 30-minute expiration (TTL) to ensure we 
        # only research a specific ticker's anomaly once per half hour.
        dedup_key = f"{ticker}:{event_type}"
        if self.is_recently_processed(dedup_key, DEDUP_WINDOW_SECONDS):
            # Even if we skip the LLM call, we still track the volume for analytics!
            self._accumulate_volume(ticker, message)
            return None
        
        self.mark_processed(dedup_key, DEDUP_WINDOW_SECONDS)

        notional = self._extract_notional(message)
        logger.info(
            f"Researching {ticker} | type={event_type} "
            f"notional=${notional/1e6:.1f}M anomaly={anomaly_score:.2f}"
        )


        # ── STEP 1: Gather research context ───────────────────────────────────
        news_context    = await self._fetch_news_context(ticker)
        graph_context   = await self._fetch_graph_context(ticker)
        current_watchlist = self._get_current_watchlist()

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
            discovery: PeerDiscovery = await self._llm.infer(
                system_prompt=QUANT_PEER_DISCOVERY_SYSTEM,
                user_prompt=user_prompt,
                schema=PeerDiscovery,
                temperature=0.15,
            )
        except SchemaViolationError as e:
            logger.error(f"Peer discovery failed: {e}")
            return None
        
        added = self._inject_peers(ticker, discovery, event_type)

        # ── STEP 4: Write discovery to Neo4j ──────────────────────────────────
        asyncio.create_task(self._write_graph_relationships(ticker, discovery))

        # ── STEP 5: Update accumulator ────────────────────────────────────────
        self._accumulate_volume(ticker, message)

        # ── STEP 6: Publish discovery ─────────────────────────────────────────

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
            "discovery":         discovery.dict(),
            "peers_added_to_watchlist": added,
            "created_at":       datetime.now(timezone.utc).isoformat(),
        }
    # ── CONTEXT FETCHERS ──────────────────────────────────────────────────────

    async def _fetch_news_context(self, ticker: str) -> List[Dict]:
        """
        Fetch recent news headlines related to this ticker and its sector.
        Searches both the ticker symbol directly and common synonyms.
        """
        loop = asyncio.get_running_loop()
        # UNDER THE HOOD: Non-blocking DB Queries (Thread Pools)
        # The Postgres database library (`psycopg2`) is synchronous. If we just ran `self.db.query()`,
        # it would freeze the entire asyncio event loop while waiting for the network.
        # `run_in_executor` pushes this blocking network call into a background thread.
        # We wrap it in a `lambda:` so the function execution is deferred until the thread runs it.
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
            return [
                {"headline": r["headline"], "score": r["anomaly_score"]}
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"News context fetch error: {e}")
            return []

    async def _fetch_graph_context(self, ticker: str) -> List[Dict]:
        """
        Pull known relationships for this ticker from Neo4j.
        Returns supplier/customer/competitor relationships for the LLM.
        """
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

    def _get_current_watchlist(self) -> set:
        """Return the current equity watchlist from Redis."""
        try:
            raw = self.redis.raw.smembers("sentinel:watched:equities")
            return {
                t.decode("utf-8") if isinstance(t, bytes) else t
                for t in raw
            }
        except Exception:
            return set()
        
    # ── INJECTION LOGIC ───────────────────────────────────────────────────────

    def _inject_peers(
            self, 
            trigger_ticker: str, 
            discovery: PeerDiscovery, 
            event_type: str
    ) -> List[str]:
        """
        Add high-confidence peers to the Redis watchlist.
        Returns list of newly added tickers.
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
                # UNDER THE HOOD: Redis Sets (SADD)
                # `sadd` adds an item to a mathematical "Set" in Redis. Sets automatically 
                # prevent duplicates. If the ticker is already being watched, `is_new` 
                # will be False, and we won't accidentally log a duplicate pivot alert.
                is_new = self.redis.raw.sadd("sentinel:watched:equities", ticker)
                if is_new:
                    added.append(ticker)
                    log_level = logger.warning if urgency == "immediate" else logger.info
                    log_level(
                        f"  🤖 AUTONOMOUS PIVOT: {trigger_ticker} → "
                        f"{ticker} (confidence={confidence:.2f} urgency={urgency})"
                    )

                    self.redis.raw.set(
                        self.state_key(discovery, ticker)

                    )
                    # Store the discovery rationale in Redis for transparency
                    self.redis.set(
                        self.state_key("discovery", ticker),
                        json.dumps({
                            "trigger":    trigger_ticker,
                            "event_type": event_type,
                            "confidence": confidence,
                            "analysis":   discovery.trigger_analysis[:200],
                            "added_at":   datetime.now(timezone.utc).isoformat(),
                        }),
                        ttl=86400,  # 24h audit trail
                    )
            except Exception as e:
                logger.warning(f"Watchlist injection failed for {ticker}: {e}")

        if added:
            logger.info(f"Watchlist expanded: +{len(added)} tickers from {trigger_ticker} research")

        return added

    async def _write_graph_relationships(
        self, trigger_ticker: str, discovery: PeerDiscovery
    ):
        """
        Write peer relationships discovered to Neo4j.
        This enriches the knowledge graph for future correlation.
        Non-fatal: failures do not block result publication.
        """
        loop = asyncio.get_running_loop()
        for peer in discovery.peer_tickers:
            if peer.discovery_confidence < 0.7:
                continue
            try:
                rel_type = self._map_relationship_type(peer.relationship_type)
                await loop.run_in_executor(
                    None,
                    # CONCEPT: Graph Database Idempotency (Cypher MERGE)
                    # In Neo4j, `MERGE` behaves like an "upsert" (Insert or Update).
                    # It ensures that even if we discover this peer relationship multiple times,
                    # we only ever create a single connection between these two nodes,
                    # simply updating the `updated_at` timestamp.
                    lambda: self.neo4j.execute("""
                        MERGE (a:Instrument {name: $source})
                        MERGE (b:Instrument {name: $target})
                        MERGE (a)-[r:CORRELATED_WITH]->(b)
                        SET r.relationship_type = $rel_type,
                            r.confidence        = $confidence,
                            r.discovered_by     = 'quant_agent',
                            r.updated_at        = datetime()
                    """, {
                        "source":     trigger_ticker,
                        "target":     peer.ticker,
                        "rel_type":   rel_type,
                        "confidence": peer.discovery_confidence,
                    }),
                )
            except Exception as e:
                logger.debug(f"Graph write failed {trigger_ticker}→{peer.ticker}: {e}")

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _extract_ticker(self, message: Dict) -> Optional[str]:
        """Extract ticker symbol from various event payload formats."""
        # Direct financial_data path
        fd = message.get("financial_data") or {}
        if isinstance(fd, dict) and fd.get("ticker"):
            return fd["ticker"].upper()

        # Crypto path
        cd = message.get("crypto_data") or {}
        if isinstance(cd, dict) and cd.get("pair"):
            return cd["pair"].upper()

        # Entity path (fallback)
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

    def _accumulate_volume(self, ticker: str, message: Dict):
        """
        Track cumulative notional volume per ticker in Redis.
        Used for volume surge detection across multiple events.
        """
        notional = self._extract_notional(message)
        if notional <= 0:
            return
        key = self.state_key("volume", ticker)
        try:
            self.redis.raw.incrbyfloat(key, notional)
            self.redis.raw.expire(key, 3600)  # Reset hourly
        except Exception:
            pass

    @staticmethod
    def _is_valid_ticker(ticker: str) -> bool:
        """Basic validation: real US equity tickers are 1-5 uppercase letters."""
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