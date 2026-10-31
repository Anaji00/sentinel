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
import os
import time
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field, field_validator

from services.agents.base import (BULLETIN_PREDICTION_HORIZON_HOURS, InferenceBatcher, SentinelAgent,
                                 SchemaViolationError, InferenceError)
from shared.utils.equities import asset_class, is_macro_asset, is_valid_primary_equity_async, split_macro_and_equities
from shared.utils import fact_check
from shared.utils import sympathy
from shared.utils.market_cap import is_large_cap
from shared.kafka import Topics
from shared.utils.focus import prioritise
from shared.utils.quote_cache import quote_key


def _as_unit_conviction(value) -> float:
    """A model-supplied conviction, normalised to [0, 1], preserving zero.

    `x or 1.0` reads 0.0 -- the model's least confident answer -- as its most
    confident one. A percentage is divided down, matching what
    `_as_probability_value` does for AgentBulletin and AgentPrediction, so one
    model habit does not mean two different things depending on which field it
    lands in.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if 1.0 < number <= 100.0:
        number /= 100.0
    return min(1.0, max(0.0, number))
from shared.models import NormalizedEvent
from shared.utils.quiet_failures import dropped, swallowed

logger = logging.getLogger("agent.stock_correlation")

# Keys per SCAN round trip, and the ceiling on how many quote keys one
# correlation pass will consider.
SCAN_BATCH = 500
MAX_QUOTE_KEYS = 2000


# Which macro asset this pass compares the equity against.
#
# This was `macro_assets[0]`, and the list is a Redis SCAN of
# `sentinel:quotes:latest:*` split by asset class -- deterministic in practice.
# Measured 2026-09-19, three consecutive runs returned the identical ordering
# and therefore the identical choice:
#
#     18 macro assets available, ZW=F selected every time
#     never examined: VXX HYG US10Y NQ=F BZ=F NG=F CL=F SI=F TIP
#                     LQD US2Y ZC=F TLT GC=F US30Y ES=F XLE
#
# Seventeen of eighteen had never been looked at once. Volatility, credit, the
# whole yield curve, oil, gold and the index futures -- the assets that
# actually transmit to equities -- were unreachable, while every correlation
# the platform had ever published was wheat against something.
#
# Least-recently-examined rather than round-robin over an index: the list's
# length and order both change as quotes expire, so a counter modulo its
# length silently skips entries. A symbol absent from the hash sorts first,
# which makes a newly collected asset the next one looked at rather than the
# last.
#
# Deliberately not routed through `prioritise`. Focus exists so two agents
# converge on one subject and consensus can form, and the consensus engine
# keys on the equity side -- which still uses focus. The macro asset is the
# axis of the comparison, not its subject, so coverage is what it owes.
MACRO_ROTATION_KEY = "sentinel:correlations:macro_rotation"


async def _next_macro_asset(redis_client, macro_assets: List[str]) -> str:
    """The macro asset examined longest ago, or one never examined at all.

    Falls back to the caller's existing behaviour if Redis cannot answer: a
    rotation that fails closed would stop the agent, and picking the head of
    the list is exactly what it did before.
    """
    if not macro_assets:
        return ""
    raw = getattr(redis_client, "raw", None) if redis_client is not None else None
    if raw is None:
        return macro_assets[0]
    try:
        seen = await raw.hgetall(MACRO_ROTATION_KEY) or {}

        def _last(symbol: str) -> float:
            value = seen.get(symbol) or seen.get(symbol.encode())
            if value is None:
                return 0.0
            try:
                return float(value.decode() if isinstance(value, bytes) else value)
            except (TypeError, ValueError):
                return 0.0

        # Ties broken by the incoming order, so behaviour is stable on a cold
        # hash rather than dependent on dict iteration.
        chosen = min(macro_assets, key=lambda s: (_last(s), macro_assets.index(s)))
        await raw.hset(MACRO_ROTATION_KEY, chosen, str(time.time()))
        return chosen
    except Exception as _exc:
        swallowed("agents.stock_correlation_agent.macro_rotation", _exc, logger)
        return macro_assets[0]



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


def _message_equity(message: Dict[str, Any]) -> Optional[str]:
    """The US equity a message is about, or None when it names none.

    Read from the same fields this agent always read, in the same order. A
    multi-word value is a name, not a symbol, and is not truncated to its first
    word the way a model's output is: "Bank of America" is not BANK. Crypto,
    futures, rates and funds are refused here because this agent's comparison
    is macro-versus-equity, and putting any of them on the equity side is how
    136 of its assessments became crypto pairs.
    """
    primary_entity = message.get("primary_entity")
    primary_entity_id = ""
    if isinstance(primary_entity, dict):
        primary_entity_id = primary_entity.get("id", "") or primary_entity.get("name", "")
    elif isinstance(primary_entity, str):
        primary_entity_id = primary_entity

    ticker = message.get("ticker") or message.get("primary_entity_id") or primary_entity_id or ""
    fd = message.get("financial_data")
    if isinstance(fd, dict):
        ticker = ticker or fd.get("ticker", "")
    if not isinstance(ticker, str):
        return None
    undecorated = _TICKER_DECORATION.sub("", ticker).strip()
    if not undecorated or " " in undecorated:
        return None
    symbol = _clean_symbol(undecorated)
    if asset_class(symbol) != "equity":
        return None
    return symbol


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

    # The argument before the verdicts: fields are generated in this order.
    transmission_channel: str
    agentic_rationale: str
    correlation_type: str  # "INVERSE_MARGIN_PRESSURE", "DECOUPLING", "COMMODITY_SPIKE_EQUITY_DUMP", "FLIGHT_TO_SAFETY"
    detected_covariance: float
    conviction: float
    impact_severity: str  # "LOW", "MODERATE", "SEVERE"
    sympathy_movers: List[SympathyMover] = Field(default_factory=list)
    recommended_hedging: List[str] = Field(default_factory=list)
    # The claim that can be graded. Without it the bulletin carried no
    # direction, so this agent -- the largest local model consumer on 28 Oct,
    # 23 calls at 224 s -- recorded no forecast and could never earn or lose
    # its budget. "neutral" is no claim and is not recorded.
    expected_direction: str = Field(default="neutral",
                                    description="up, down or neutral: the equity's move against the market next session")

    @field_validator("expected_direction", mode="before")
    @classmethod
    def _direction(cls, v: object) -> str:
        s = str(v or "").strip().lower()
        return s if s in ("up", "down") else "neutral"

    @field_validator("macro_asset", "equity_ticker", mode="before")
    @classmethod
    def _strip_symbols(cls, v):
        return _clean_symbol(v)


class DynamicCorrelationBatch(BaseModel):
    """One assessment per pair in a batched prompt."""
    assessments: List[DynamicCorrelationAssessment] = Field(default_factory=list)


STOCK_CORR_BATCH_SIZE = int(os.getenv("STOCK_CORR_BATCH_SIZE", "6"))
STOCK_CORR_BATCH_WAIT_SEC = float(os.getenv("STOCK_CORR_BATCH_WAIT_SEC", "30"))
STOCK_CORR_SYSTEM = ("You assess how a stock's measured exposure to one macro instrument bears on the stock's next "
                     "session against the market, and which of its measured sympathy peers would move with it. Use "
                     "only the data given; a coefficient marked unmeasured is of unknown strength. Write the "
                     "mechanism and rationale first, then the direction. Most sessions a macro move does not move a "
                     "stock against the market: answer neutral unless the macro move and the sign of the exposure "
                     "together give a reason. Name sympathy movers only from the peers listed. Return ONLY raw JSON.")
_MEASURED_EXPOSURES_SQL = "SELECT macro, beta, t, partial_corr, n FROM macro_exposures WHERE ticker = $1"
SYMPATHY_PEERS_SHOWN = 5
_PEERS_SQL = f"SELECT peer, corr, n FROM sympathy_peers WHERE ticker = $1 ORDER BY corr DESC LIMIT {SYMPATHY_PEERS_SHOWN}"
# The macro instrument's last two closes, from one source: macro_daily_bars
# (futures, yields, dollar, VIX), else the venue's daily bars (ETFs such as
# TLT). Mixing sources could set a session against itself.
_MACRO_CLOSES_SQL = """
    WITH m AS (SELECT day, close FROM macro_daily_bars WHERE symbol = $1 ORDER BY day DESC LIMIT 2),
         d AS (SELECT day, close FROM daily_bars WHERE ticker = $1 ORDER BY day DESC LIMIT 2)
    SELECT close FROM (
        SELECT day, close FROM m
        UNION ALL
        SELECT day, close FROM d WHERE NOT EXISTS (SELECT 1 FROM m)
    ) b ORDER BY day DESC LIMIT 2
"""



def macro_exposures(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Exposure rows whose target is a macro instrument, strongest |weight|
    first; an edge to anything else (a feed name, a ranking) is dropped."""
    out = []
    for r in rows:
        m = str(r.get("macro") or "").strip().upper()
        if not m or not is_macro_asset(m):
            continue
        try:
            w = float(r.get("weight")) if r.get("weight") is not None else None
        except (TypeError, ValueError):
            w = None
        out.append({**r, "macro": m, "weight": w})
    return sorted(out, key=lambda e: -abs(e["weight"] or 0.0))


def exposure_line(e: Dict[str, Any]) -> str:
    """One exposure as the prompt states it; nothing unmeasured is filled in."""
    sign = "positive" if e.get("rel") == "POSITIVE_EXPOSURE_TO" else \
        "inverse" if e.get("rel") == "INVERSE_EXPOSURE_TO" else "commodity"
    parts = [f"{sign} exposure to {e['macro']}"]
    parts.append(f"weight {e['weight']:.3f}" if e.get("weight") is not None else "weight unmeasured")
    if e.get("method"):
        parts.append(f"method {e['method']}")
    if e.get("window"):
        parts.append(f"window {e['window']}")
    return ", ".join(parts)


def pair_key(macro: str, equity: str) -> str:
    """The batch key for a macro asset and an equity, case-insensitive."""
    return f"{str(macro).strip().upper()}|{str(equity).strip().upper()}"


def batch_prompt(sections: List[str]) -> str:
    """Several pairs' own prompts as one, each assessed independently."""
    parts = [f"PAIR {k + 1}\n{s.strip()}" for k, s in enumerate(sections)]
    return ("Assess each pair below on its own data only; a finding for one pair says nothing about another.\n\n"
            + "\n\n".join(parts)
            + f'\n\nReturn ONLY JSON: {{"assessments": [one assessment per pair, {len(sections)} in all, each with '
              "macro_asset and equity_ticker exactly as its pair names them]}")


class StockCorrelationAgent(SentinelAgent):
    """
    Dedicated Agentic Reasoning Task Engine for Stock Correlations & Sympathy Movers.
    Continuously ingests multi-asset price ticks, news headlines, and market events
    to dynamically discover cross-asset equity correlations and project sympathy nodes
    into the Neo4j Knowledge Graph.
    """
    FOCUS_DOMAIN = "tradfi"
    FOCUS_DOMAINS = ("tradfi", "market", "equity", "macro")

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
        model: str = "qwen2.5:3b",
        fallback_model: Optional[str] = "qwen2.5:3b",
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
        # One stock/macro pair per call was 794 calls on 29 Oct, 68% of the
        # day's hosted calls. Pairs arriving together now share one.
        self._batcher = InferenceBatcher(
            name="stock_correlation",
            flush_fn=self._assess_batch,
            max_items=STOCK_CORR_BATCH_SIZE,
            max_wait_sec=STOCK_CORR_BATCH_WAIT_SEC,
            logger=logger,
            max_waiters=self.dispatch_concurrency,
        )

    async def _macro_exposures(self, ticker: str) -> List[Dict[str, Any]]:
        """The stock's measured macro exposures (macro_exposures table, from
        shared/utils/macro_exposure.py), strongest first; [] without them.

        Read from the graph, these were sector priors -- CCJ to Brent, WTI and
        natural gas at weight 1.0 "via sector Energy" -- and ranked first as if
        measured. A stock with no measured exposure is not assessed."""
        if not self.db:
            return []
        try:
            rows = await self.db.query(_MEASURED_EXPOSURES_SQL, ticker.upper()) or []
        except Exception as e:
            swallowed("agents.stock_correlation_agent.exposures", e, logger)
            return []
        return macro_exposures([{
            "macro": r["macro"], "rel": "POSITIVE_EXPOSURE_TO" if float(r["beta"]) > 0 else "INVERSE_EXPOSURE_TO",
            "weight": float(r["partial_corr"]), "method": f"daily OLS with the market, t {float(r['t']):+.1f}",
            "window": f"{int(r['n'])} sessions",
        } for r in rows])

    async def _sympathy_peers(self, ticker: str) -> Dict[str, Dict[str, Any]]:
        """The stock's measured sympathy peers (sympathy_job), strongest first; {} without them."""
        if not self.db:
            return {}
        try:
            rows = await self.db.query(_PEERS_SQL, ticker.upper()) or []
        except Exception as e:
            swallowed("agents.stock_correlation_agent.sympathy_peers", e, logger)
            return {}
        return {str(r["peer"]).upper(): dict(r) for r in rows}

    async def _macro_move(self, macro: str) -> Optional[float]:
        """The macro instrument's last session, close to close; None when no bars are stored."""
        if not self.db:
            return None
        try:
            rows = await self.db.query(_MACRO_CLOSES_SQL, macro) or []
        except Exception as e:
            swallowed("agents.stock_correlation_agent.macro_move", e, logger)
            return None
        closes = [float(r["close"]) for r in rows if r["close"]]
        return closes[0] / closes[1] - 1 if len(closes) >= 2 else None

    async def _assess_batch(self, items: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, DynamicCorrelationAssessment]:
        """Assesses every queued pair in one inference; {pair_key: assessment}.

        A pair the model does not answer for is absent, and the batcher
        resolves it to None: no assessment rather than an invented one. One
        pair alone is asked exactly as before."""
        if not items:
            return {}
        message = items[0][1]["message"]
        if len(items) == 1:
            key, item = items[0]
            got = await self._execute_with_telemetry(
                message=message, system_prompt=STOCK_CORR_SYSTEM, user_prompt=item["section"],
                schema=DynamicCorrelationAssessment, temperature=0.2,
            )
            return {key: got} if got else {}
        batch = await self._execute_with_telemetry(
            message=message, system_prompt=STOCK_CORR_SYSTEM,
            user_prompt=batch_prompt([item["section"] for _, item in items]),
            schema=DynamicCorrelationBatch, temperature=0.2,
            num_predict=min(4096, 160 + 420 * len(items)),
        )
        wanted = {k for k, _ in items}
        out: Dict[str, DynamicCorrelationAssessment] = {}
        for a in (batch.assessments if batch else []):
            k = pair_key(a.macro_asset, a.equity_ticker)
            if k in wanted and k not in out:
                out[k] = a
        return out

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
        if not isinstance(message, dict):
            return None

        # The message must name a quoted US equity, and that is checked before
        # admission so that a message which cannot be worked never occupies the
        # candidate buffer.
        #
        # A message without one used to fall back to `equities[0]` -- the
        # first, focus-boosted entry of the quote scan -- with the headline
        # still pasted into the prompt as the "Recent Market Event". Measured
        # over 20-21 September, this agent held more inference slots than any
        # other (282) and spent them on 19 instruments: GOOGL 78 times, and
        # AVAXUSDT, LINKUSDT, ETHUSDT, BTCUSDT, SOLUSDT and XRPUSDT 136 times
        # between them, because the macro/equity split puts crypto on the
        # equity side. Its buffer was full of al-Jazeera and BBC World
        # headlines, each one winning a slot to produce another GOOGL-versus-
        # wheat assessment the headline had nothing to do with.
        ticker = _message_equity(message)
        if ticker is None:
            dropped(
                "agents.stock_correlation_agent.no_equity",
                "message names no US equity; an assessment of some other "
                "instrument would not be about this event",
                logger,
                variant=str(message.get("source") or "unknown")[:40],
            )
            return None
        if not await self.redis.raw.exists(quote_key(ticker)):
            dropped(
                "agents.stock_correlation_agent.unquoted",
                "equity has no live quote to anchor the comparison",
                logger,
                variant="unquoted",
            )
            return None

        if not await self.capacity_or_defer(message):
            return None

        event_type = message.get("type", "")
        source = message.get("source", "")

        # Fetch recent live quotes from Redis for dynamic cross-asset returns calculation
        try:
            # SCAN, not KEYS.
            #
            # KEYS walks the entire keyspace in one blocking pass, and Redis is
            # single threaded -- so on an instance holding ~144,000 keys this
            # stalled every other client for the duration, during ingestion, on
            # an agent loop that runs continuously. scan_iter does the same work
            # in bounded increments the server can interleave.
            #
            # Bounded as well as incremental: an unbounded scan feeding an MGET
            # would fetch every quote on the platform to compute a correlation
            # over a handful of them.
            quote_keys = []
            async for k in self.redis.raw.scan_iter(
                match="sentinel:quotes:latest:*", count=SCAN_BATCH
            ):
                quote_keys.append(k)
                if len(quote_keys) >= MAX_QUOTE_KEYS:
                    break
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
                    except (ValueError, TypeError) as _exc:
                        swallowed("agents.stock_correlation_agent.handle_message", _exc, logger)

            # Identify macro/commodity assets vs equities.
            #
            # Was a substring match against names like "BRENT", "GLD" and "VIX"
            # while the quote keys hold ticker symbols, so BZ=F (Brent), GC=F
            # (gold), SI=F (silver), NG=F (gas), ZC=F, ZW=F, ES=F, NQ=F, VXX and
            # TIP were all classified as equities. On the live quote set that
            # was 2 macro assets recognised out of 14, with the other 12 sitting
            # on the equity side of a cross-asset comparison.
            macro_assets, equities = split_macro_and_equities(list(price_map))

            if not macro_assets:
                return None

            # Pick target macro asset and target equity, preferring whatever
            # the rest of the swarm is already looking at.
            #
            # `publish_bulletin` offers every subject to
            # `sentinel:focus:entities` and nine agents feed it; for months only
            # the quant engine read it back, so nothing ever directed a second
            # agent to what a first had found. Measured over 24 hours, the
            # consensus engine's own drift check reported 277 agent pairs at
            # Jaccard 0.00 and two bulletins existed in Redis, from one agent --
            # so a fusion engine that combines opinions by entity had one
            # opinion per entity, always.
            #
            # `prioritise` is additive: every candidate stays, in its original
            # relative order, and a focused one rises. This agent takes the
            # first of each list, so the ordering is exactly what decides
            # whether a second opinion is ever formed.
            #
            # The equity side is now the message's own ticker, so only the
            # macro side is ordered here.
            try:
                macro_assets = await prioritise(self.redis, macro_assets, domains=self.FOCUS_DOMAINS)
            except Exception as _exc:
                swallowed("agents.stock_correlation_agent.focus", _exc, logger)

            # The macro asset this stock is measured against, not the next in
            # a rotation. The rotation paired each stock with whichever macro
            # symbol had waited longest -- GOOGL against wheat -- and the
            # "measured relationship" gate below was then satisfied by the
            # stock's competitor and peer edges, so the model was asked to
            # explain a macro link nobody had measured (29 Oct 2026 review).
            # Calls only on companies of $1bn and up, checked before any inference.
            if not await is_large_cap(self.redis, ticker):
                dropped("agents.stock_correlation_agent.below_min_market_cap",
                        "company under $1bn: no assessment", logger, variant="below_min_market_cap")
                return None
            exposures = await self._macro_exposures(ticker)
            exposures = [e for e in exposures if e["macro"] in price_map
                         or await self.redis.raw.exists(quote_key(e["macro"]))]
            if not exposures:
                dropped(
                    "agents.stock_correlation_agent.no_macro_exposure",
                    "no measured macro exposure for the stock, so no inference",
                    logger,
                    variant="no_macro_exposure",
                )
                return None
            target_macro = exposures[0]["macro"]
            target_equity = ticker
            if target_equity not in price_map:
                # The scan is bounded, so a quoted equity can sit outside it.
                try:
                    price_map[target_equity] = float(
                        await self.redis.raw.get(quote_key(target_equity))
                    )
                except (TypeError, ValueError) as _exc:
                    swallowed("agents.stock_correlation_agent.target_quote", _exc, logger)
                    return None

            dedup_key = f"stock_corr_task:{target_macro}:{target_equity}:{int(time.time() // 600)}"
            if await self.is_recently_processed(dedup_key, window_seconds=600):
                return None
            await self.mark_processed(dedup_key, window_seconds=600)

            # Get recent headlines or news context for agentic reasoning
            headline = message.get("headline") or message.get("summary") or f"Active price action on {ticker}"
            cross_context = await self.get_cross_agent_context(ticker=target_equity, limit=2)
            cross_block = f"\n- Cross-Agent Memory:\n{cross_context}" if cross_context else ""

            # ── THE MEASURED MACRO EXPOSURE, AND ONLY THAT (§7.2) ──
            #
            # Stock-to-stock edges (competitors, peers, Granger leads) are not
            # evidence about a macro relationship, and this agent's scope is
            # macro; peer structure is the quant engine's and the graph's.
            empirical_links = [exposure_line(e) for e in exposures if e["macro"] == target_macro]

            # No measured relationship, no inference. The prompt for this case
            # told the model to say that no correlation was measured, and every
            # sympathy edge it then proposed was withheld below as a hypothesis
            # -- a whole slot, on a host that affords about twenty-five an hour,
            # spent restating the absence of data.
            if not empirical_links:
                dropped(
                    "agents.stock_correlation_agent.unmeasured_pair",
                    "no measured relationship for the pair, so no inference",
                    logger,
                    variant="unmeasured_pair",
                )
                return None
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

            # Flush-left: the indentation was ~40 spaces a line of hosted tokens.
            # The universe tickers are gone (sympathy movers are out of scope),
            # and the macro's own last move is in: a direction cannot be judged
            # from an exposure without knowing which way the instrument went.
            macro_move = await self._macro_move(target_macro)
            move_line = (f"{target_macro} moved {macro_move * 100:+.2f}% over its last session"
                         if macro_move is not None else f"{target_macro}'s last-session move: not available")
            # Sympathy movers come from measurement (sympathy_job); the model
            # only says which of these this channel would carry, and how.
            peers = await self._sympathy_peers(target_equity)
            peer_block = (f"{_nl}- Measured sympathy peers of {target_equity} (the only names allowed as movers):"
                          f"{_nl}  - " + f"{_nl}  - ".join(sympathy.peer_lines(list(peers.values())))
                          if peers else f"{_nl}- Measured sympathy peers: none stored")
            user_prompt = (
                f"Pair: macro {target_macro}, equity {target_equity}. Report macro_asset as exactly "
                f'"{target_macro}" and equity_ticker as exactly "{target_equity}".{_nl}'
                f"- {move_line}; last prices {target_macro} {price_map.get(target_macro, 0):.2f}, "
                f"{target_equity} {price_map.get(target_equity, 0):.2f}{_nl}"
                f"- Recent event: {headline}"
                f"{empirical_block}{peer_block}{cross_block}{_nl}{_nl}"
                f"Task:{_nl}1. {mechanism_task}{_nl}2. {grounding_task}{_nl}"
                f"3. expected_direction: up, down or neutral for {target_equity} against the market next session, "
                f"from the move above and the sign of the exposure; neutral when either is missing or small.{_nl}"
                "4. sympathy_movers: the listed peers this macro channel would also move, each with direction "
                "BULLISH or BEARISH and one sentence of reasoning; an empty list when none are listed or none "
                "share the channel."
            )

            # Batched: several pairs share one inference (_assess_batch). Each
            # pair keeps its own prompt section, which is also what its answer
            # is fact-checked against below.
            brief: Optional[DynamicCorrelationAssessment] = await self._batcher.submit(
                pair_key(target_macro, target_equity),
                {"message": message, "section": user_prompt},
            )

            if not brief:
                return None
            # Movers only from the measured peers. Named freely, each became a
            # SYMPATHY_MOVER graph edge (842 on 29 Oct, XOM's including MAR and
            # ABNB) that the graph then served back as structure.
            off_list = [sm.ticker for sm in brief.sympathy_movers if sm.ticker not in peers]
            if off_list:
                dropped("agents.stock_correlation_agent.unmeasured_mover", "sympathy mover not among the measured peers",
                        logger, detail=", ".join(off_list)[:200])
            brief.sympathy_movers = [sm for sm in brief.sympathy_movers if sm.ticker in peers]
            # Each mover with a direction is a forecast too, graded apart from
            # the agent's own calls (prediction_scoring.SIDE_STRATEGIES), so
            # its graph edge earns a record and not only a correlation.
            for sm in brief.sympathy_movers:
                side = {"BULLISH": "up", "BEARISH": "down"}.get(str(sm.direction).upper())
                if not side:
                    continue
                try:
                    entry = await self._latest_price(sm.ticker)
                    if isinstance(entry, (int, float)) and entry > 0:
                        await self.record_prediction(
                            ticker=sm.ticker, direction=side, conviction=_as_unit_conviction(sm.conviction),
                            entry_price=float(entry), time_horizon_hours=BULLETIN_PREDICTION_HORIZON_HOURS,
                            strategy=f"sympathy:{target_equity}")
                except Exception as e:
                    swallowed("agents.stock_correlation_agent.sympathy_forecast", e, logger)
            # Checked against what it was given (shared/utils/fact_check.py):
            # sympathy movers the prompt never named and that are not listed
            # equities are dropped; when most numbers in the rationale are not
            # in the prompt, the direction is withheld, so an unsupported
            # story is never recorded as a forecast.
            movers = [sm.ticker for sm in brief.sympathy_movers]
            listed = {t for t in movers if await is_valid_primary_equity_async(t, redis_client=self.redis)}
            fc = fact_check.check(f"{brief.agentic_rationale} {brief.transmission_channel}",
                                  user_prompt, movers, known=listed)
            if fc.unknown_tickers:
                brief.sympathy_movers = [sm for sm in brief.sympathy_movers if sm.ticker not in fc.unknown_tickers]
                dropped("agents.stock_correlation_agent.unknown_mover", "sympathy mover neither in the data nor listed",
                        logger, detail=", ".join(fc.unknown_tickers)[:200])
            if not fc.numbers_ok and brief.expected_direction != "neutral":
                brief.expected_direction = "neutral"
                dropped("agents.stock_correlation_agent.unsupported_numbers",
                        "rationale's numbers not in the data; direction withheld", logger, detail=fc.summary())

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
                                    # Normalised, and 0.0 preserved.
                                    #
                                    # `float(sm.conviction or 1.0)` turned a
                                    # model saying "no confidence" into
                                    # certainty, and nothing clamped the value
                                    # at all -- so the percentage problem the
                                    # AgentBulletin and AgentPrediction
                                    # validators exist to stop was still open on
                                    # this path. Live: 17 RELATED_TO and 5
                                    # SYMPATHY_MOVER edges carry a weight above
                                    # 1.0, the largest 95.0, and
                                    # `coalesce(r.coefficient, r.weight)` hands
                                    # that to a model as a correlation
                                    # coefficient of 95.
                                    # The measured correlation (sympathy_peers),
                                    # not the model's conviction: the peer is
                                    # measured, the model only chose it.
                                    "weight": _as_unit_conviction(float(peers[sm.ticker]["corr"])),
                                    "confidence": _as_unit_conviction(float(peers[sm.ticker]["corr"])),
                                    "confidence_basis": "measured_corr",
                                    "method": f"excess_return_corr_{sympathy.WINDOW}d",
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
                expected_direction=brief.expected_direction,
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
