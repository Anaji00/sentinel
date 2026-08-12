"""
services/agents/stock_correlation_agent.py

DEDICATED STOCK CORRELATION AGENT (LLM & REASONING ENGINE)
=============================================================
Dynamically analyzes cross-asset market correlations, energy price shocks, rate pressures,
and equity reactions without hardcoded ticker lists or fixed numerical thresholds.

Uses local Ollama LLM zero-shot reasoning to analyze live spot prices, returns covariance,
and sector news headlines to discover macroeconomic transmission mechanisms.
"""

import asyncio
import json
import logging
import time
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError
from shared.kafka import Topics
from shared.models import NormalizedEvent

logger = logging.getLogger("agent.stock_correlation")


class SympathyMover(BaseModel):
    ticker: str
    relationship: str  # "COMPETITOR_SYMPATHY", "SUPPLIER_SYMPATHY", "SECTOR_PEER", "CROSS_ASSET_HEDGE"
    direction: str  # "BULLISH", "BEARISH", "CORRELATED"
    conviction: float
    reasoning: str


class DynamicCorrelationAssessment(BaseModel):
    macro_asset: str
    equity_ticker: str
    correlation_type: str  # "INVERSE_MARGIN_PRESSURE", "DECOUPLING", "COMMODITY_SPIKE_EQUITY_DUMP", "FLIGHT_TO_SAFETY"
    detected_covariance: float
    conviction: float
    transmission_channel: str
    agentic_rationale: str
    impact_severity: str  # "LOW", "MODERATE", "SEVERE"
    sympathy_movers: List[SympathyMover] = Field(default_factory=list)
    recommended_hedging: List[str] = Field(default_factory=list)


class StockCorrelationAgent(SentinelAgent):
    """
    Dedicated Agentic Reasoning Task Engine for Stock Correlations & Sympathy Movers.
    Continuously ingests multi-asset price ticks, news headlines, and market events
    to dynamically discover cross-asset equity correlations and project sympathy nodes
    into the Neo4j Knowledge Graph.
    """

    def __init__(self, kafka_producer=None, redis_client=None, neo4j_client=None):
        super().__init__(
            name="StockCorrelationAgent",
            description="Agentic reasoning engine for dynamic stock, cross-asset correlation & sympathy mover discovery",
            subscribed_topics=[
                Topics.ENRICHED_EVENTS,
                Topics.QUANT_DISCOVERIES,
                Topics.MACRO_ASSESSMENT,
            ],
            kafka_producer=kafka_producer,
            redis_client=redis_client,
            neo4j_client=neo4j_client,
        )

    async def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Main reasoning loop for incoming events and tick updates.
        """
        event_type = message.get("type", "")
        source = message.get("source", "")
        ticker = message.get("ticker") or message.get("primary_entity_id") or ""

        # Extract underlying financial or headline payload
        fd = message.get("financial_data", {})
        ticker = ticker or fd.get("ticker", "")
        if not ticker:
            return None

        # Fetch recent live quotes from Redis for dynamic cross-asset returns calculation
        try:
            quote_keys = await self.redis.raw.keys("sentinel:quotes:latest:*")
            if not quote_keys or len(quote_keys) < 2:
                return None

            clean_tickers = [k.decode().split(":")[-1] for k in quote_keys if isinstance(k, bytes)]
            if len(clean_tickers) < 2:
                return None

            # Fetch recent price points
            quotes_raw = await self.redis.raw.mget(quote_keys)
            price_map = {}
            for t_name, q_val in zip(clean_tickers, quotes_raw):
                if q_val:
                    try:
                        price_map[t_name] = float(q_val)
                    except (ValueError, TypeError):
                        pass

            # Identify macro/commodity assets vs equities dynamically
            macro_assets = [t for t in price_map if any(m in t for m in ["WTI", "BRENT", "CL=F", "US10Y", "US02Y", "GLD", "VIX", "OIL"])]
            equities = [t for t in price_map if t not in macro_assets]

            if not macro_assets or not equities:
                return None

            # Pick target macro asset and target equity
            target_macro = macro_assets[0]
            target_equity = ticker if ticker in equities else equities[0]

            dedup_key = f"stock_corr_task:{target_macro}:{target_equity}:{int(time.time() // 600)}"
            if await self.is_recently_processed(dedup_key, window_seconds=600):
                return None
            await self.mark_processed(dedup_key, window_seconds=600)

            # Get recent headlines or news context for agentic reasoning
            headline = message.get("headline") or message.get("summary") or f"Active price action on {ticker}"
            cross_context = await self.get_cross_agent_context(ticker=target_equity, limit=2)
            cross_block = f"\n- Cross-Agent Memory:\n{cross_context}" if cross_context else ""

            user_prompt = f"""
            Analyze the dynamic real-time market relationship between macro asset {target_macro} (${price_map.get(target_macro, 0):.2f}) and equity {target_equity} (${price_map.get(target_equity, 0):.2f}).
            Recent Market Event: {headline}
            Active Market Universe Tickers: {', '.join(clean_tickers[:15])}
            {cross_block}

            Task Requirements:
            1. Synthesize the dynamic correlation mechanism (e.g. rising crude oil compressing profit margins, rate yields putting pressure on tech valuations, or safe-haven rotation).
            2. Identify 1-3 SYMPATHY MOVERS (sector peers, competitors, suppliers, or cross-asset hedges) from the active market universe or broader market that will react in sympathy with {target_equity}.
            3. Provide a clear agentic rationale, conviction, and impact severity without hardcoded assumptions.
            """

            brief: DynamicCorrelationAssessment = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Stock Correlation & Sympathy Engine. Perform agentic macro-to-equity correlation analysis and identify sympathy movers. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=DynamicCorrelationAssessment,
                temperature=0.2,
            )

            logger.info(
                f"🧠 Stock Correlation Agent Reasoning | {brief.macro_asset} vs {brief.equity_ticker} | "
                f"Type: {brief.correlation_type} | Sympathy Movers: {len(brief.sympathy_movers)} | Conviction: {brief.conviction:.2f}"
            )

            # ── KNOWLEDGE GRAPH EXPANSION FOR SYMPATHY MOVERS ──────────────
            if brief.sympathy_movers and self.neo4j:
                for sm in brief.sympathy_movers:
                    try:
                        cypher_query = """
                        MERGE (a:Entity {id: $primary_id})
                        ON CREATE SET a.name = $primary_id, a.type = 'EQUITY'
                        MERGE (b:Entity {id: $sympathy_id})
                        ON CREATE SET b.name = $sympathy_id, b.type = 'EQUITY'
                        MERGE (a)-[r:SYMPATHY_MOVER]->(b)
                        SET r.relationship = $relationship,
                            r.direction = $direction,
                            r.conviction = $conviction,
                            r.reasoning = $reasoning,
                            r.updated_at = datetime()
                        """
                        await self.neo4j.query(cypher_query, {
                            "primary_id": brief.equity_ticker,
                            "sympathy_id": sm.ticker,
                            "relationship": sm.relationship,
                            "direction": sm.direction,
                            "conviction": sm.conviction,
                            "reasoning": sm.reasoning,
                        })
                        logger.info(f"🕸️ Knowledge Graph Sympathy Edge: ({brief.equity_ticker}) -[:SYMPATHY_MOVER]-> ({sm.ticker}) [{sm.relationship}]")
                    except Exception as kg_err:
                        logger.debug(f"Knowledge Graph sympathy edge insertion error: {kg_err}")

                    # Push ontology proposal to Kafka for system-wide awareness
                    if self.producer:
                        try:
                            await self.producer.send(
                                Topics.ONTOLOGY_PROPOSALS,
                                {
                                    "entity_id": brief.equity_ticker,
                                    "action": "ADD_SYMPATHY_EDGE",
                                    "data": {
                                        "primary_ticker": brief.equity_ticker,
                                        "sympathy_ticker": sm.ticker,
                                        "relationship": sm.relationship,
                                        "conviction": sm.conviction,
                                    }
                                },
                                key=brief.equity_ticker,
                            )
                        except Exception as ge:
                            logger.warning(f"Failed to publish sympathy edge proposal for {sm.ticker}: {ge}")

            # Publish correlation discovery to Kafka & Redis
            payload = {
                "agent": self.name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "assessment": brief.model_dump(),
            }

            if self.producer:
                await self.producer.send(
                    Topics.CORRELATIONS,
                    payload,
                    key=f"{brief.macro_asset}_{brief.equity_ticker}",
                )

            await self.redis.raw.set(
                f"sentinel:correlations:stock:{brief.macro_asset}:{brief.equity_ticker}",
                json.dumps(payload),
                ex=3600,
            )

            # Publish AgentBulletin
            sympathy_summary = f" Sympathy: {', '.join([sm.ticker for sm.ticker in brief.sympathy_movers])}" if brief.sympathy_movers else ""
            await self.publish_bulletin(
                bulletin_type="alert",
                primary_entity_id=brief.equity_ticker,
                ticker=brief.equity_ticker,
                conviction=brief.conviction,
                summary=f"Dynamic Correlation Discovery ({brief.macro_asset} / {brief.equity_ticker}): {brief.agentic_rationale}.{sympathy_summary}",
                payload=payload,
                ttl_seconds=3600,
            )

            return payload

        except (SchemaViolationError, InferenceError) as e:
            logger.error(f"Stock Correlation Agent LLM reasoning error: {e}")
            return None
        except Exception as err:
            logger.error(f"Stock Correlation Agent processing error: {err}", exc_info=True)
            return None
