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
import re
import logging
import time
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError
from shared.utils.equities import split_macro_and_equities
from shared.kafka import Topics
from shared.models import NormalizedEvent

logger = logging.getLogger("agent.stock_correlation")


# The price was arriving inside the identifier.
#
# The prompt renders the pair as "equity CPB ($21.53)" so the model has the
# quote in front of it, and the model echoed the whole string back into
# equity_ticker. Live: ticker='CPB ($21.53)' and primary_entity_id='CPB
# ($21.53)', with the summary reading "Dynamic Correlation Discovery (TLT
# ($82.29) / CPB ($21.53))".
#
# That identifier is the join key for three separate systems. The consensus
# engine fuses bulletins by entity, so a bulletin keyed this way can never be
# corroborated or contradicted by another agent looking at the same company --
# it is a different entity to every consumer, and a new one every time the price
# moves. It is also written to Neo4j as a node id and used as a Kafka partition
# key.
#
# Stripped here rather than only in the prompt, because a prompt is a request
# and a validator is a guarantee: the same model will find another way to
# decorate a symbol, and every consumer downstream assumes this field is one.
_TICKER_DECORATION = re.compile(r"\s*[\(\[].*$")


def _clean_symbol(value: str) -> str:
    """The symbol out of whatever decoration arrived with it.

    "CPB ($21.53)" -> "CPB"; "TLT [82.29]" -> "TLT"; "AAPL" -> "AAPL".
    Deliberately conservative: it removes a trailing parenthetical and
    normalises case and whitespace, and does not attempt to rescue a string
    that has no symbol in it at all.
    """
    if not isinstance(value, str):
        return value
    cleaned = _TICKER_DECORATION.sub("", value).strip().upper()
    # A symbol is one token. If stripping the parenthetical still leaves
    # several, the leading one is the symbol and the rest is prose.
    if " " in cleaned:
        cleaned = cleaned.split()[0]
    return cleaned or value


class SympathyMover(BaseModel):
    ticker: str

    relationship: str  # "COMPETITOR_SYMPATHY", "SUPPLIER_SYMPATHY", "SECTOR_PEER", "CROSS_ASSET_HEDGE"
    direction: str  # "BULLISH", "BEARISH", "CORRELATED"
    conviction: float
    reasoning: str

    @field_validator("ticker", mode="before")
    @classmethod
    def _strip_ticker(cls, v):
        return _clean_symbol(v)


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

    @field_validator("macro_asset", "equity_ticker", mode="before")
    @classmethod
    def _strip_symbols(cls, v):
        return _clean_symbol(v)


class StockCorrelationAgent(SentinelAgent):
    """
    Dedicated Agentic Reasoning Task Engine for Stock Correlations & Sympathy Movers.
    Continuously ingests multi-asset price ticks, news headlines, and market events
    to dynamically discover cross-asset equity correlations and project sympathy nodes
    into the Neo4j Knowledge Graph.
    """

    def __init__(
        self,
        agent_name: str = "stock_correlation_agent",
        input_topics: Optional[List[str]] = None,
        redis_client=None,
        db_client=None,
        neo4j_client=None,
        producer=None,
        consumer=None,
        dlq=None,
        model: str = "qwen2.5:1.5b",
        fallback_model: Optional[str] = "gemma3:1b",
    ):
        super().__init__(
            agent_name=agent_name,
            input_topics=input_topics or [
                Topics.RAW_NEWS,
                Topics.ENRICHED_EVENTS,
                Topics.QUANT_DISCOVERIES,
                Topics.MACRO_ASSESSMENT,
                Topics.CORRELATIONS,
            ],
            redis_client=redis_client,
            db_client=db_client,
            neo4j_client=neo4j_client,
            producer=producer,
            consumer=consumer,
            dlq=dlq,
            model=model,
            fallback_model=fallback_model,
        )

    @property
    def output_topic(self) -> str:
        # handle() returns None on every path, so the base class never publishes
        # here; the agent's one send is explicit and goes to MACRO_DECOUPLING.
        # Declared consistently with it so that if handle() ever does return a
        # payload, it lands on the topic shaped for it rather than on the
        # cluster contract.
        return Topics.MACRO_DECOUPLING

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return await self.handle_message(message)

    async def handle_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Main reasoning loop for incoming events and tick updates.
        """
        # Admission is checked before the context is built, not after.
        #
        # The wargamer, the knowledge graph and the reasoning service already
        # guard here. These three did not: they queried TimescaleDB, read Redis
        # and assembled a prompt, and only then reached the budget check inside
        # _execute_with_telemetry, which sheds the request. Nothing bypassed
        # admission, so this is wasted preparation rather than unfairness -- but
        # on a host that affords about thirty-five inferences an hour, the
        # preparation is most of what a shed request costs.
        if not await self._inference_budget.is_available():
            return None
        if not isinstance(message, dict):
            return None

        event_type = message.get("type", "")
        source = message.get("source", "")

        # Safely extract primary entity or ticker
        primary_entity = message.get("primary_entity")
        primary_entity_id = ""
        if isinstance(primary_entity, dict):
            primary_entity_id = primary_entity.get("id", "") or primary_entity.get("name", "")
        elif isinstance(primary_entity, str):
            primary_entity_id = primary_entity

        ticker = message.get("ticker") or message.get("primary_entity_id") or primary_entity_id or ""

        # Extract underlying financial or headline payload safely
        fd = message.get("financial_data")
        if isinstance(fd, dict):
            ticker = ticker or fd.get("ticker", "")

        if not ticker:
            return None

        # Fetch recent live quotes from Redis for dynamic cross-asset returns calculation
        try:
            quote_keys = await self.redis.raw.keys("sentinel:quotes:latest:*")
            if not quote_keys or len(quote_keys) < 2:
                return None

            clean_tickers = [k.decode().split(":")[-1] if isinstance(k, bytes) else str(k).split(":")[-1] for k in quote_keys]
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

            # Identify macro/commodity assets vs equities.
            #
            # Was a substring match against names like "BRENT", "GLD" and "VIX"
            # while the quote keys hold ticker symbols, so BZ=F (Brent), GC=F
            # (gold), SI=F (silver), NG=F (gas), ZC=F, ZW=F, ES=F, NQ=F, VXX and
            # TIP were all classified as equities. On the live quote set that
            # was 2 macro assets recognised out of 14, with the other 12 sitting
            # on the equity side of a cross-asset comparison.
            macro_assets, equities = split_macro_and_equities(list(price_map))

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

            # ── QUERY REAL EMPIRICAL CORRELATIONS FROM NEO4J GRAPH (§7.2) ──
            empirical_links = []
            if self.neo4j:
                try:
                    neo_res = await self.neo4j.query("""
                        MATCH (e) WHERE toUpper(e.name) = $ticker OR toUpper(e.id) = $ticker
                        MATCH (e)-[r:STATISTICALLY_CORRELATED_WITH|GRANGER_CAUSES|SYMPATHY_MOVER|MEMBER_OF|CUSTOMER_OF|SUPPLIER_TO|COMPETES_WITH]-(p)
                        RETURN coalesce(p.name, p.id) AS peer_id,
                               type(r) AS rel,
                               coalesce(r.coefficient, r.weight) AS coef,
                               r.p_value AS p_val,
                               r.lag AS lag,
                               r.f_stat AS f_stat
                        LIMIT 10
                    """, {"ticker": target_equity.upper()})
                    # No coalesce onto plausible numbers.
                    #
                    # A missing coefficient defaulted to 1.0 and a missing
                    # p-value to 0.05, so an edge that had never been measured
                    # reached the model as "Coef: 1.0, p-val: 0.05" -- a perfect
                    # correlation, significant at the five percent level, both
                    # invented by the query that fetched it.
                    for nr in (neo_res or []):
                        parts = [f"Relationship: {nr['rel']}"]
                        coef = nr.get("coef") if hasattr(nr, "get") else nr["coef"]
                        parts.append(f"Coef: {float(coef):.3f}" if coef is not None else "Coef: unmeasured")
                        p_val = nr.get("p_val") if hasattr(nr, "get") else nr["p_val"]
                        if p_val is not None:
                            parts.append(f"p-val: {float(p_val):.4f}")
                        lag = nr.get("lag") if hasattr(nr, "get") else nr["lag"]
                        if lag is not None:
                            parts.append(f"lag: {lag}")
                        empirical_links.append(f"{nr['peer_id']} [{', '.join(parts)}]")
                except Exception as ne:
                    logger.debug(f"Neo4j empirical correlation lookup bypass: {ne}")

            _nl = chr(10)
            # Says what is there, including when nothing is. The empty case
            # read "Live continuous correlation matrix active" -- asserting
            # statistics existed, with none supplied, directly above a task
            # telling the model to ground its answer in them.
            if empirical_links:
                empirical_block = (
                    _nl + "- Grounded Empirical Statistics & Graph Links "
                    "(Computed Deterministically):" + _nl + "  - "
                    + (_nl + "  - ").join(empirical_links)
                )
                mechanism_task = (
                    f"Synthesize the causal economic transmission mechanism explaining WHY "
                    f"the measured relationship between {target_macro} and {target_equity} "
                    f"exists (e.g. input cost pressure, discount rate repricing, margin "
                    f"squeeze, or FX translation). Treat any coefficient marked unmeasured "
                    f"as unknown strength rather than strong."
                )
                grounding_task = (
                    "Ground your explanation directly in the empirical statistics provided "
                    "above, maintaining high analytical precision."
                )
            else:
                empirical_block = (
                    _nl + "- Grounded Empirical Statistics: NONE. No measured "
                    "relationship between these two instruments exists in the graph."
                )
                mechanism_task = (
                    f"State plainly that no correlation between {target_macro} and "
                    f"{target_equity} has been measured. Describe at most one mechanism by "
                    f"which one COULD affect the other, labelled explicitly as a hypothesis "
                    f"rather than an observation. Do not assert that a correlation exists."
                )
                grounding_task = (
                    "There are no empirical statistics for this pair. Say so rather than "
                    "describing a relationship as established."
                )

            user_prompt = f"""
            Analyze the real-time market relationship between macro asset {target_macro} and equity {target_equity}.
            Last prices, for context only: {target_macro} ${price_map.get(target_macro, 0):.2f}, {target_equity} ${price_map.get(target_equity, 0):.2f}.
            Report macro_asset as exactly "{target_macro}" and equity_ticker as exactly "{target_equity}" -- the bare symbol, with no price, no currency and no parentheses.
            Recent Market Event: {headline}
            Active Market Universe Tickers: {', '.join(clean_tickers[:15])}
            {empirical_block}
            {cross_block}

            Task Requirements:
            1. {mechanism_task}
            2. For each SYMPATHY MOVER, explain the business causality and structural transmission link (supply chain dependence, shared customer base, sector multiple contagion, or competitor displacement). If no such link exists, omit the mover entirely -- do not name a mechanism from that list to satisfy the format. Returning an empty sympathy_movers list is the correct answer when there is no structural relationship.
            3. {grounding_task}
            """

            brief: Optional[DynamicCorrelationAssessment] = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Stock Correlation & Sympathy Engine. Explain the causal economic mechanisms behind real, computed statistical correlations and identify sympathy transmission paths. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=DynamicCorrelationAssessment,
                temperature=0.2,
            )

            if not brief:
                return None

            logger.info(
                f"🧠 Stock Correlation Agent Reasoning | {brief.macro_asset} vs {brief.equity_ticker} | "
                f"Type: {brief.correlation_type} | Sympathy Movers: {len(brief.sympathy_movers)} | Conviction: {brief.conviction:.2f}"
            )

            # ── KNOWLEDGE GRAPH GOVERNED PROPOSALS FOR SYMPATHY MOVERS (§3.3) ──
            #
            # Gated on there being a measured relationship to reason from.
            #
            # A SYMPATHY_MOVER edge is not a transient opinion: it is written
            # into Neo4j, and from there it is read back by this agent's own
            # empirical lookup above, by statistical_discovery, and by the
            # correlation service's centrality multiplier -- each of which
            # treats it as established structure. An edge asserted by a model
            # that was explicitly told no correlation had been measured would
            # return, one cycle later, as evidence.
            if brief.sympathy_movers and self.producer and not empirical_links:
                logger.info(
                    "Withheld %d sympathy edge proposal(s) for %s: no measured "
                    "relationship exists for this pair, so these are hypotheses "
                    "and do not belong in the graph.",
                    len(brief.sympathy_movers), brief.equity_ticker,
                )
            if brief.sympathy_movers and self.producer and empirical_links:
                for sm in brief.sympathy_movers:
                    try:
                        await self.producer.send(
                            Topics.ONTOLOGY_PROPOSALS,
                            {
                                "entity_id": brief.equity_ticker,
                                "action": "LINK_ENTITY",
                                "data": {
                                    "target_id": sm.ticker,
                                    "source_label": "Company",
                                    "target_label": "Company",
                                    "relation_type": "SYMPATHY_MOVER",
                                    "weight": float(sm.conviction or 1.0),
                                    "confidence": float(sm.conviction or 1.0),
                                    "relationship": sm.relationship,
                                    "direction": sm.direction,
                                    "properties": {
                                        "relationship": sm.relationship,
                                        "direction": sm.direction,
                                        "conviction": sm.conviction,
                                        "reasoning": sm.reasoning,
                                    }
                                }
                            },
                            key=brief.equity_ticker,
                        )
                        logger.info(f"🕸️ Emitted governed sympathy edge proposal: ({brief.equity_ticker}) -[:SYMPATHY_MOVER]-> ({sm.ticker}) [{sm.relationship}]")
                    except Exception as ge:
                        logger.warning(f"Failed to publish sympathy edge proposal for {sm.ticker}: {ge}")

            # Publish correlation discovery to Kafka & Redis
            # `detected_covariance` is a field the model fills, and it was
            # published under a name that reads as a measurement.
            #
            # Live: TLT/CPB went out with detected_covariance 0.0 -- literally
            # "no covariance detected" -- alongside a SYMPATHY_MOVER edge and a
            # supply-chain narrative between a Treasury bond ETF and a soup
            # company. Nothing downstream could tell that number from one the
            # statistical discovery job had actually computed, because the field
            # name is the same either way.
            #
            # The flag travels with the assessment so a reader, a consumer and
            # the graph writer all see the same thing: whether any measured
            # relationship existed at all.
            assessment = brief.model_dump()
            assessment["covariance_measured"] = bool(empirical_links)
            if not empirical_links:
                assessment["detected_covariance"] = None

            payload = {
                "agent": self.name,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "assessment": assessment,
            }

            if self.producer:
                # Not CORRELATIONS. That topic's contract is CorrelationCluster
                # -- the correlation engine and the quant engine both send
                # `cluster.model_dump()` -- and this payload is an agent
                # assessment: {agent, created_at, assessment}. It has none of
                # rule_id, rule_name or alert_tier, so every consumer that
                # constructs a cluster from it raises. The alert manager was
                # doing exactly that, five validation errors at a time, four
                # messages an hour discarded with a full traceback each.
                #
                # Eight consumer groups read sentinel.correlations and all of
                # them are entitled to assume the contract.
                #
                # MACRO_DECOUPLING is where this belongs on both counts: it is
                # an agents.* topic, matching the convention for agent output,
                # and it carries macro-versus-equity relationship findings,
                # which is what this assessment is. It also has two live
                # consumers, so this is a redirection to readers rather than
                # the quieter fix of deleting a publish nothing could parse.
                await self.producer.send(
                    Topics.MACRO_DECOUPLING,
                    payload,
                    key=f"{brief.macro_asset}_{brief.equity_ticker}",
                )

            await self.redis.raw.set(
                f"sentinel:correlations:stock:{brief.macro_asset}:{brief.equity_ticker}",
                json.dumps(payload),
                ex=3600,
            )

            # Publish AgentBulletin
            # `for sm in`, not `for sm.ticker in`.
            #
            # The comprehension target was an attribute, so each SympathyMover
            # was assigned into the `.ticker` field of the `sm` still bound by
            # the loop above, and the comprehension then yielded those objects
            # rather than their tickers:
            #
            #   Stock Correlation Agent processing error:
            #   sequence item 0: expected str instance, SympathyMover found
            #
            # It raised on every brief that had sympathy movers, so this agent's
            # bulletin never reached the consensus engine -- and on the way past
            # it overwrote a real mover's ticker with a SympathyMover object.
            sympathy_summary = (
                f" Sympathy: {', '.join(sm.ticker for sm in brief.sympathy_movers)}"
                if brief.sympathy_movers else ""
            )
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
