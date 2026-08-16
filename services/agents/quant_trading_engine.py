"""
services/agents/quant_trading_engine.py

MASTER QUANT & SWE CONSOLIDATED QUANT TRADING ENGINE
===================================================
Consolidates 3 quantitative agents into a single high-performance engine:
  - QuantResearcherAgent (Peer discovery, catalyst analysis, Granger causality)
  - FinancialAdvisorAgent (TA indicators, Fib levels, VaR/CVaR, Half-Kelly signals)
  - InsiderClusteringAgent (SEC Form 4 C-suite accumulation & insider flow)

Preserves 100% of existing Kafka topics, Redis keys, and output schemas.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from services.agents.base import SentinelAgent, SchemaViolationError, InferenceError
from shared.kafka import Topics
from shared.utils import quant_calc
from shared.utils.tasks import safe_create_task
import numpy as np
from shared.utils.equities import is_valid_primary_equity_async
from shared.db import get_neo4j

logger = logging.getLogger("agent.quant_trading")


# ── FORM 4 INSIDER MODELS ─────────────────────────────────────────────────────

class InsiderClusterBrief(BaseModel):
    ticker: str
    insider_sentiment: str  # "Bullish Accumulation", "Bearish Distribution", "Neutral"
    total_net_notional_usd: float
    c_suite_involvement: bool
    summary: str


# ── PEER DISCOVERY & RESEARCH MODELS ──────────────────────────────────────────

class PeerTicker(BaseModel):
    ticker: str
    relation: str
    discovery_confidence: float

class MacroInstrument(BaseModel):
    symbol: str
    instrument_type: str  # "treasury", "forex", "commodity", "volatility"
    correlation_reasoning: str

class PeerDiscovery(BaseModel):
    primary_ticker: str
    peer_tickers: List[PeerTicker] = Field(default_factory=list)
    macro_instruments: List[MacroInstrument] = Field(default_factory=list)
    catalyst_category: str
    structural_decoupling: bool


# ── FINANCIAL ADVISORY & RISK MODELS ──────────────────────────────────────────

class BlackLittermanAllocation(BaseModel):
    ticker: str
    target_weight_pct: float
    expected_return_pct: float
    equilibrium_weight_pct: float

class GarchVolatilityCone(BaseModel):
    cond_volatility_pct: float
    tp1_sigma_1_0: float
    tp2_sigma_2_0: float
    tp3_sigma_3_0: float
    sl_sigma_1_5: float

class SmartMoneyConvergence(BaseModel):
    is_aligned: bool = False
    insider_buyer_role: Optional[str] = None
    insider_notional_usd: Optional[float] = None
    option_sweep_premium_usd: Optional[float] = None
    conviction_boost: float = 0.0

class PortfolioMetrics(BaseModel):
    var_95_pct: Optional[float] = None
    cvar_99_pct: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    recommended_cash_pct: Optional[float] = None
    hawkes_risk_factor: Optional[float] = None
    metrics_source: str = "insufficient_history"

class TradingSignal(BaseModel):
    ticker: str
    action: str  # "BUY", "SELL", "HOLD"
    trade_type: str = Field(default="Long/Buy", description="Type of trade recommendation e.g. 'Long/Buy', 'Short/Sell', 'Scalp/Buy', 'Swing/Long'")
    entry_level: float
    target_price: float
    stop_loss: float
    risk_reward_ratio: float
    kelly_allocation_pct: float
    conviction_score: float
    sigma_shock: Optional[float] = None
    expected_move_pct: Optional[float] = None
    order_type: Optional[str] = "Limit"
    slippage_est_bps: Optional[float] = 3.5
    microstructure_stop_multiplier: float = 1.5
    volatility_cone: Optional[GarchVolatilityCone] = None
    smart_money: Optional[SmartMoneyConvergence] = None
    technical_indicators: Dict[str, Any] = Field(default_factory=dict)
    fib_levels: Dict[str, float] = Field(default_factory=dict)
    quantitative_rationale: str

class FinancialAdviceBrief(BaseModel):
    market_regime: str
    portfolio_metrics: PortfolioMetrics = Field(default_factory=PortfolioMetrics)
    black_litterman_allocations: List[BlackLittermanAllocation] = Field(default_factory=list)
    highest_conviction_plays: List[TradingSignal] = Field(default_factory=list)
    covered_call_overlays: List[Dict[str, Any]] = Field(default_factory=list)
    general_hedging_strategy: str


def compute_ta_indicators(closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
    """Helper function computing RSI, EMA, ATR, Fib levels, and 20d/50d/200d SMAs."""
    curr = closes[-1] if closes else 0.0
    max_h = max(highs) if highs else curr
    min_l = min(lows) if lows else curr
    diff = max_h - min_l if max_h != min_l else curr * 0.05

    fibs = {
        "0.0": min_l,
        "0.382": min_l + 0.382 * diff,
        "0.500": min_l + 0.500 * diff,
        "0.618": min_l + 0.618 * diff,
        "1.0": max_h,
    }

    gains, losses = [], []
    for i in range(max(1, len(closes) - 14), len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(change if change > 0 else 0)
        losses.append(-change if change < 0 else 0)

    avg_gain = sum(gains) / max(1, len(gains))
    avg_loss = sum(losses) / max(1, len(losses))

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    def _ema(span: int) -> float:
        k = 2.0 / (span + 1)
        res = closes[0] if closes else 0.0
        for val in closes[1:]:
            res = (val * k) + (res * (1.0 - k))
        return res

    tr_list = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        tr_list.append(tr)
    atr = sum(tr_list[-14:]) / max(1, len(tr_list[-14:])) if tr_list else (curr * 0.02)

    ma_res = quant_calc.moving_average_distances(closes)

    return {
        "rsi": round(rsi, 2),
        "ema_12": round(_ema(12), 4),
        "ema_26": round(_ema(26), 4),
        "atr": round(atr, 4),
        "sma_20": ma_res.get("sma_20"),
        "dist_sma_20_pct": ma_res.get("dist_sma_20_pct"),
        "sma_50": ma_res.get("sma_50"),
        "dist_sma_50_pct": ma_res.get("dist_sma_50_pct"),
        "sma_200": ma_res.get("sma_200"),
        "dist_sma_200_pct": ma_res.get("dist_sma_200_pct"),
        "ma_alignment": ma_res.get("ma_alignment", "NEUTRAL"),
        "fib_levels": {k: round(v, 4) for k, v in fibs.items()},
    }


# ── CONSOLIDATED QUANT TRADING ENGINE ──────────────────────────────────────────

class QuantTradingEngine(SentinelAgent):
    """
    Unified Quant & Trading Engine.
    Combines peer discovery, SEC Form 4 insider flow tracking, quantitative risk modeling
    (VaR/CVaR, Half-Kelly), and technical trading signal generation in a single pass.
    """

    def __init__(
        self,
        agent_name: str = "quant_trading_engine",
        input_topics: Optional[List[str]] = None,
        redis_client=None,
        db_client=None,
        neo4j_client=None,
        producer=None,
        consumer=None,
        dlq=None,
        model: str = "llama3",
        fallback_model: Optional[str] = None,
    ):
        super().__init__(
            agent_name=agent_name,
            input_topics=input_topics or [Topics.ENRICHED_EVENTS, Topics.CORRELATIONS, Topics.RAW_TRADFI],
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
        return Topics.FINANCIAL_ADVICE

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = message.get("source", "")
        event_type = message.get("type", "")
        raw = message.get("raw_payload", message)

        # ── 1. SEC FORM 4 INSIDER TRADE HANDLER ────────────────────────────────
        if source == "sec_form4" or "insider" in event_type:
            return await self._process_insider_form4(message, raw)

        trig = message.get("trigger") or {}
        pe = message.get("primary_entity") or {}
        raw_sec = message.get("security_data") or raw.get("security_data") or {}

        # Extract entity ID array if CorrelationCluster
        ent_ids = message.get("entity_ids") or raw.get("entity_ids") or []
        first_ent_id = ent_ids[0] if isinstance(ent_ids, list) and ent_ids else None

        ticker = str(
            raw.get("ticker") or
            trig.get("ticker") or
            pe.get("id") or
            first_ent_id or
            ""
        ).upper().strip()

        if not ticker or ticker == "UNKNOWN":
            return None

        # ── CVE TO EQUITY MAPPER ───────────────────────────────────────────────
        if ticker.startswith("CVE-"):
            vendor_name = (
                raw.get("vendor") or
                raw.get("vendor_name") or
                raw.get("vendorProject") or
                raw.get("affected_org") or
                trig.get("vendor") or
                trig.get("vendor_name") or
                raw_sec.get("affected_org") or
                (pe.get("name") if pe and pe.get("id") != ticker else None) or
                None
            )
            description = (
                raw.get("description") or
                raw.get("headline") or
                raw.get("summary") or
                message.get("headline") or
                ""
            )
            from shared.utils.cyber_mapper import map_cve_to_equity
            mapped_ticker, _ = map_cve_to_equity(ticker, vendor_name=vendor_name, description=description)
            if mapped_ticker:
                ticker = mapped_ticker

        # ── PRIMARY EQUITY VALIDATOR GATE ──────────────────────────────────────
        if not await is_valid_primary_equity_async(ticker, redis_client=self.redis):
            return None

        anomaly_score = float(raw.get("anomaly_score") or trig.get("anomaly_score") or message.get("anomaly_score", 0.5))
        if anomaly_score < 0.60:
            return None

        # Run Peer Discovery & Trading Advisory concurrently via asyncio.gather
        discovery_task = self._process_peer_discovery(message, ticker, anomaly_score)
        advisory_task = self._process_trading_advisory(message, ticker, anomaly_score)

        discovery_res, advisory_res = await asyncio.gather(
            discovery_task, advisory_task, return_exceptions=True
        )

        if isinstance(discovery_res, dict):
            await self._producer.send(Topics.QUANT_DISCOVERIES, discovery_res, key=ticker)

        if isinstance(advisory_res, dict):
            await self._producer.send(Topics.FINANCIAL_ADVICE, advisory_res, key=ticker)

        return advisory_res if isinstance(advisory_res, dict) else (discovery_res if isinstance(discovery_res, dict) else None)

    # ── SUB-ENGINE 1: SEC FORM 4 INSIDER CLUSTERING ───────────────────────────

    async def _process_insider_form4(self, message: Dict[str, Any], raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ticker = (raw.get("ticker") or "").upper()
        if not ticker:
            title = raw.get("title", "")
            match = re.search(r'\(\s*([A-Za-z]+)\s*\)', title)
            if match:
                ticker = match.group(1).upper()

        if not ticker:
            return None

        code = str(raw.get("transaction_code", "P")).upper()
        notional = float(raw.get("notional_usd", 0.0))
        is_buyer = code in ("P", "M", "X")
        signed_notional = notional if is_buyer else -notional

        # Aggregate 7-day insider flow in Redis
        async with self.redis.raw.pipeline(transaction=True) as pipe:
            pipe.rpush(f"sentinel:insider:flow:{ticker}", json.dumps({"code": code, "notional": signed_notional, "ts": time.time()}))
            pipe.ltrim(f"sentinel:insider:flow:{ticker}", -50, -1)
            pipe.expire(f"sentinel:insider:flow:{ticker}", 604800)
            await pipe.execute()

        raw_history = await self.redis.raw.lrange(f"sentinel:insider:flow:{ticker}", 0, -1)
        total_net = sum(json.loads(h)["notional"] for h in raw_history if h)

        if abs(total_net) < 100_000:
            return None

        sentiment = "Bullish Accumulation" if total_net > 0 else "Bearish Distribution"
        c_suite = any(kw in str(raw).lower() for kw in ("ceo", "cfo", "director", "president", "officer"))

        brief = InsiderClusterBrief(
            ticker=ticker,
            insider_sentiment=sentiment,
            total_net_notional_usd=total_net,
            c_suite_involvement=c_suite,
            summary=f"C-Suite {sentiment} in {ticker}: net 7-day flow ${total_net/1e6:+.2f}M across {len(raw_history)} transactions.",
        )

        res_payload = {
            "agent": self.name,
            "agent_run_id": f"insider_{ticker}_{int(time.time())}",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "brief": brief.model_dump(),
        }

        await self.redis.raw.set(f"sentinel:insider:brief:{ticker}", json.dumps(res_payload["brief"]), ex=86400)
        await self._producer.send(Topics.INSIDER_CLUSTERS, res_payload, key=ticker)

        # Publish structured AgentBulletin
        safe_create_task(
            self.publish_bulletin(
                bulletin_type="signal",
                summary=brief.summary,
                ticker=ticker,
                conviction=0.75 if c_suite else 0.55,
                expected_direction="up" if total_net > 0 else "down",
                payload={"net_notional": total_net, "c_suite": c_suite},
                ttl_seconds=86400,
            ),
            name=f"insider-bulletin-{ticker}"
        )

        return res_payload

    # ── SUB-ENGINE 2: QUANT PEER DISCOVERY ───────────────────────────────────

    async def _process_peer_discovery(self, message: Dict[str, Any], ticker: str, anomaly_score: float) -> Optional[Dict[str, Any]]:
        dedup_key = f"quant_discovery:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(dedup_key, window_seconds=1800):
            return None
        await self.mark_processed(dedup_key, window_seconds=1800)

        # Concurrent context hydration
        news_context, graph_context, global_context, cross_context, earnings_ctx, funding_ctx = await asyncio.gather(
            self._fetch_news_context(ticker),
            self._fetch_graph_context(ticker),
            self.fetch_global_context(),
            self.get_cross_agent_context(ticker=ticker, limit=3),
            self._fetch_earnings_context(ticker),
            self._fetch_funding_context(ticker),
        )
        cross_block = f"\n- Cross-Agent Intelligence:\n{cross_context}" if cross_context else ""
        earnings_block = f"\n- Earnings Context: {earnings_ctx}" if earnings_ctx else ""
        funding_block = f"\n- Derivatives Context: {funding_ctx}" if funding_ctx else ""

        user_prompt = f"""=== ANOMALOUS INSTRUMENT RESEARCH ===
Target Symbol: {ticker} | Anomaly Score: {anomaly_score:.2f}
Global Context: {global_context}
{cross_block}{earnings_block}{funding_block}
Recent News: {json.dumps(news_context[:3], default=str)}
Entity Graph: {json.dumps(graph_context[:3], default=str)}

INSTRUCTIONS:
Discover correlated equity/macro peers, macro instruments, and structural catalysts. Return raw JSON matching schema:"""

        try:
            discovery: PeerDiscovery = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Quant Researcher. Discover correlated equity/macro peers and structural catalysts. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=PeerDiscovery,
                temperature=0.15,
            )

            # Test Granger causality on discovered peers if historical prices exist
            verified_peers = []
            x_prices, _, _ = await self._fetch_prices(ticker)
            for peer in discovery.peer_tickers:
                y_prices, _, _ = await self._fetch_prices(peer.ticker)
                if len(x_prices) >= 20 and len(y_prices) >= 20:
                    causality = quant_calc.granger_causality(x_prices, y_prices, max_lag=3)
                    if causality.get("x_granger_causes_y"):
                        peer.discovery_confidence = min(1.0, peer.discovery_confidence + 0.15)
                verified_peers.append(peer)
            discovery.peer_tickers = verified_peers

            # Inject top verified primary equity peers into watched equities ZSET
            for p in discovery.peer_tickers[:4]:
                if p.discovery_confidence >= 0.65 and await is_valid_primary_equity_async(p.ticker):
                    await self.redis.raw.zadd("sentinel:watched:equities", mapping={p.ticker: time.time()})

            closes, _, _ = await self._fetch_prices(ticker)
            if len(closes) >= 10:
                returns = list(np.diff(closes) / closes[:-1])
                sr = quant_calc.sharpe_ratio(returns)
                mdd, _, _ = quant_calc.max_drawdown(closes)
            else:
                sr, mdd = 0.0, 0.0

            discovery_dict = discovery.model_dump()
            res_payload = {
                "agent": self.name,
                "agent_run_id": f"quant_{ticker}_{int(time.time())}",
                "trigger": {"ticker": ticker, "anomaly_score": anomaly_score},
                "discovery": discovery_dict,
                "quality_metrics": {
                    "sharpe_ratio": sr,
                    "max_drawdown": mdd,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }

            # Register discovered high-confidence peers in ontology/graph pipelines and emit CorrelationCluster
            high_conf_peers = [p for p in discovery.peer_tickers if p.discovery_confidence >= 0.65]
            for peer in high_conf_peers:
                await self._producer.send(
                    Topics.ONTOLOGY_PROPOSALS,
                    {
                        "source_entity": ticker,
                        "target_entity": peer.ticker,
                        "relationship": "CORRELATED_PEER",
                        "confidence": peer.discovery_confidence,
                        "rationale": peer.rationale,
                    },
                    key=peer.ticker,
                )

            if high_conf_peers:
                peer_ids = [p.ticker for p in high_conf_peers]
                corr_cluster = CorrelationCluster(
                    rule_id="QUANT_PEER_DECOUPLING",
                    rule_name="Quantitative Equity Peer Cointegration & Decoupling",
                    alert_tier=AlertTier.INTELLIGENCE,
                    primary_domain="financial",
                    confidence_score=float(high_conf_peers[0].discovery_confidence),
                    summary_headline=f"📈 Peer Decoupling Detected: {ticker} vs {', '.join(peer_ids[:3])}",
                    supporting_headlines=[f"{p.ticker}: {p.rationale}" for p in high_conf_peers[:3]],
                    metrics_summary={
                        "catalyst_category": discovery.catalyst_category,
                        "sharpe_ratio": sr,
                        "max_drawdown": mdd,
                        "peer_count": len(peer_ids),
                    },
                    trigger_event_id=f"quant_{ticker}_{int(time.time())}",
                    supporting_event_ids=[],
                    entity_ids=[ticker] + peer_ids,
                    entity_names=[ticker] + peer_ids,
                    description=f"QuantTradingEngine discovered cointegrated peer correlation for {ticker} with {', '.join(peer_ids)}. Catalyst: {discovery.catalyst_category}",
                    tags=["quant_peer_discovery", f"ticker:{ticker}"] + [f"peer:{p}" for p in peer_ids],
                )
                await self._producer.send(Topics.CORRELATIONS, corr_cluster.model_dump(), key=ticker)

            # Publish structured AgentBulletin
            peer_names = [p.ticker for p in discovery.peer_tickers[:4]]
            safe_create_task(
                self.publish_bulletin(
                    bulletin_type="thesis",
                    summary=f"Quant Discovery {ticker}: {discovery.catalyst_category}. Peers: {peer_names}",
                    ticker=ticker,
                    conviction=min(1.0, anomaly_score * 0.9),
                    expected_direction="uncertain",
                    payload={"peers": peer_names, "catalyst": discovery.catalyst_category},
                    ttl_seconds=3600,
                ),
                name=f"quant-discovery-bulletin-{ticker}"
            )

            return res_payload

        except (SchemaViolationError, InferenceError, Exception) as e:
            logger.error(f"Quant peer discovery failed for {ticker}: {e}")
            return None

    # ── SUB-ENGINE 3: FINANCIAL ADVISORY & RISK SIGNALS ─────────────────────

    async def _process_trading_advisory(self, message: Dict[str, Any], ticker: str, anomaly_score: float) -> Optional[Dict[str, Any]]:
        closes, highs, lows = await self._fetch_prices(ticker)
        if len(closes) < 5:
            return None

        # Calculate TA indicators
        indicators = self._compute_ta(closes, highs, lows)
        current_price = closes[-1]
        atr = indicators.get("atr", current_price * 0.02)

        # Risk calculations: EWMA Volatility, VaR, CVaR, Empirical Half-Kelly
        returns = list(np.diff(closes) / closes[:-1]) if len(closes) > 1 else [0.0]
        has_history = len(returns) >= 10
        ewma_vol = quant_calc.ewma_volatility(returns, annualize=True)
        var_95 = quant_calc.var_historical(returns, confidence=0.95, position_value=10_000)
        cvar_95 = quant_calc.cvar_historical(returns, confidence=0.95, position_value=10_000)
        cvar_99 = quant_calc.cvar_historical(returns, confidence=0.99, position_value=10_000)
        var_95_pct = round(quant_calc.var_historical(returns, confidence=0.95, position_value=1.0) * 100.0, 2)
        cvar_99_pct = round(quant_calc.cvar_historical(returns, confidence=0.99, position_value=1.0) * 100.0, 2)
        sharpe_ratio_val = round(float(quant_calc.sharpe_ratio(returns, annualize=True)), 2)

        # Empirical Win Probability (W) & Payoff Ratio (R) from scorecard
        card = await self.get_scorecard()
        if card.predictions_made >= 5:
            win_prob = min(0.85, max(0.35, card.predictions_correct / max(1, card.predictions_made)))
        else:
            win_prob = 0.55  # Default baseline prior

        win_loss_ratio = 2.0  # 2:1 reward/risk payoff target
        kelly_pct = quant_calc.kelly_criterion(win_prob, win_loss_ratio, half_kelly=True)

        # Macro risk sizing check & circuit breaker
        macro_regime_raw = await self.redis.raw.get("sentinel:macro:latest_rates_regime")
        rates_regime = macro_regime_raw.decode("utf-8") if isinstance(macro_regime_raw, bytes) else (macro_regime_raw if isinstance(macro_regime_raw, str) else "Normal")
        is_stress = any(s in rates_regime.lower() for s in ("inverted", "bear_flattening", "stress"))

        if is_stress:
            kelly_pct = min(0.05, kelly_pct * 0.5)

        indicators_data = {
            ticker: {
                "current_price": current_price,
                "rsi": indicators["rsi"],
                "ema_12": indicators["ema_12"],
                "ema_26": indicators["ema_26"],
                "atr": atr,
                "sma_20": indicators.get("sma_20"),
                "dist_sma_20_pct": indicators.get("dist_sma_20_pct"),
                "sma_50": indicators.get("sma_50"),
                "dist_sma_50_pct": indicators.get("dist_sma_50_pct"),
                "sma_200": indicators.get("sma_200"),
                "dist_sma_200_pct": indicators.get("dist_sma_200_pct"),
                "ma_alignment": indicators.get("ma_alignment", "NEUTRAL"),
                "ewma_volatility_annualized": ewma_vol,
                "var_95_per_10k": var_95,
                "cvar_95_per_10k": cvar_95,
                "half_kelly_allocation_pct": round(kelly_pct * 100.0, 2),
                "fib_levels": indicators["fib_levels"],
            }
        }

        # ── 1-HOP GRAPH CONTEXT PRE-CHECKS (§7.3) ──
        graph_correlations = []
        graph_context = {
            "sector": None,
            "industry": None,
            "index_membership": [],
            "supply_chain": [],
            "competitors": [],
            "correlated_entities": [],
        }
        try:
            neo4j_client = self.neo4j or await get_neo4j()
            if neo4j_client:
                # 1. 1-hop structural relationships (Sector, Index, Supply Chain, Competitors)
                struct_query = """
                MATCH (inst) WHERE toUpper(inst.name) = $ticker OR toUpper(inst.id) = $ticker
                OPTIONAL MATCH (inst)-[:OPERATES_IN]->(s:Sector)
                OPTIONAL MATCH (inst)-[:MEMBER_OF]->(idx:Index)
                OPTIONAL MATCH (inst)-[:SUPPLIER_TO|CUSTOMER_OF|SUPPLIES]-(sc:Entity)
                OPTIONAL MATCH (inst)-[:COMPETES_WITH]-(comp:Entity)
                RETURN coalesce(s.name, s.id) AS sector,
                       collect(DISTINCT coalesce(idx.name, idx.id)) AS indices,
                       collect(DISTINCT coalesce(sc.name, sc.id)) AS supply_chain,
                       collect(DISTINCT coalesce(comp.name, comp.id)) AS competitors
                """
                struct_rows = await neo4j_client.query(struct_query, {"ticker": ticker.upper()})
                if struct_rows and struct_rows[0]:
                    sr = struct_rows[0]
                    if sr.get("sector"):
                        graph_context["sector"] = sr["sector"]
                    if sr.get("indices"):
                        graph_context["index_membership"] = [x for x in sr["indices"] if x]
                    if sr.get("supply_chain"):
                        graph_context["supply_chain"] = [x for x in sr["supply_chain"] if x]
                    if sr.get("competitors"):
                        graph_context["competitors"] = [x for x in sr["competitors"] if x]

                # 2. Exposure & statistical correlations
                graph_query = """
                MATCH (e:Entity)-[r:COMMODITY_EXPOSURE|SUPPLIES|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO|STATISTICALLY_CORRELATED_WITH|GRANGER_CAUSES*1..2]-(inst:Entity)
                WHERE toUpper(inst.name) = $ticker OR toUpper(inst.id) = $ticker
                RETURN DISTINCT coalesce(e.name, e.id) AS correlated_entity, type(r[0]) AS predicate, coalesce(r[0].confidence, 0.6) AS confidence
                LIMIT 5
                """
                rows = await neo4j_client.query(graph_query, {"ticker": ticker.upper()})
                if rows:
                    graph_correlations = [
                        f"{r['correlated_entity']} -[{r['predicate']}]-> {ticker} (confidence: {r['confidence']:.2f})"
                        for r in rows if r.get("correlated_entity")
                    ]
                    graph_context["correlated_entities"] = [r['correlated_entity'] for r in rows if r.get("correlated_entity")]
        except Exception as e:
            logger.debug(f"Graph context lookup for {ticker} bypass: {e}")

        # Ground prompt with 1-hop topology
        graph_topo_lines = []
        if graph_context["sector"]:
            graph_topo_lines.append(f"Sector: {graph_context['sector']}")
        if graph_context["index_membership"]:
            graph_topo_lines.append(f"Indices: {', '.join(graph_context['index_membership'])}")
        if graph_context["supply_chain"]:
            graph_topo_lines.append(f"Supply Chain Linkages: {', '.join(graph_context['supply_chain'])}")
        if graph_context["competitors"]:
            graph_topo_lines.append(f"Key Competitors: {', '.join(graph_context['competitors'])}")

        graph_topo_block = ("\n        GRAPH TOPOLOGY & EXPOSURES:\n        - " + "\n        - ".join(graph_topo_lines) + "\n") if graph_topo_lines else ""
        graph_block = f"\n        VALIDATED GRAPH CORRELATIONS:\n        - " + "\n        - ".join(graph_correlations) + "\n" if graph_correlations else ""

        cross_context = await self.get_cross_agent_context(ticker=ticker, limit=3)
        cross_block = f"\n        CROSS-AGENT INTELLIGENCE:\n        {cross_context}\n" if cross_context else ""

        # Earnings & derivatives context injection (Phase 4)
        earnings_ctx = await self._fetch_earnings_context(ticker)
        funding_ctx = await self._fetch_funding_context(ticker)
        earnings_block = f"\n        EARNINGS CONTEXT: {earnings_ctx}\n" if earnings_ctx else ""
        funding_block = f"\n        DERIVATIVES CONTEXT: {funding_ctx}\n" if funding_ctx else ""

        user_prompt = f"""=== FINANCIAL ADVISORY & RISK EVALUATION ===
Target Instrument: {ticker} | Macro Regime: {rates_regime}
Risk Indicators: {json.dumps(indicators_data, separators=(',', ':'), default=str)}
{graph_topo_block}{graph_block}{cross_block}{earnings_block}{funding_block}
HARD RISK CONSTRAINTS (MANDATORY):
- Empirical Win Probability (W): {win_prob:.1%} | Payoff Ratio (R): {win_loss_ratio:.1f}
- Max Half-Kelly Allocation: {kelly_pct * 100:.1f}% (Set kelly_allocation_pct <= {kelly_pct * 100:.1f}%)
- Specify action (BUY/SELL/HOLD), trade_type, entry_level, target_price, and stop_loss targets.
Return raw JSON matching schema:"""

        try:
            brief: FinancialAdviceBrief = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Chief Quant Risk Strategist. Formulate high-conviction quantitative trade recommendations with strict risk limits. Return ONLY raw JSON.",
                user_prompt=user_prompt,
                schema=FinancialAdviceBrief,
                temperature=0.1,
            )

            # Post-hoc deterministic trade signal construction and risk limits enforcement
            max_kelly_pct = round(kelly_pct * 100.0, 2)
            for play in brief.highest_conviction_plays:
                play.entry_level = round(current_price, 2)
                stop_distance = atr * 1.5
                
                # Conviction-tiered Risk-Reward ratio selection
                if play.conviction_score < 0.6:
                    rr = 1.5
                elif play.conviction_score < 0.8:
                    rr = 2.0
                else:
                    rr = 3.0
                play.risk_reward_ratio = rr

                if play.action == "BUY":
                    play.stop_loss = round(play.entry_level - stop_distance, 2)
                    play.target_price = round(play.entry_level + (stop_distance * rr), 2)
                elif play.action == "SELL":
                    play.stop_loss = round(play.entry_level + stop_distance, 2)
                    play.target_price = round(play.entry_level - (stop_distance * rr), 2)
                else:
                    play.stop_loss = round(play.entry_level - stop_distance, 2)
                    play.target_price = play.entry_level

                # Hard clamp Kelly allocation to server-calculated half-Kelly limit
                play.kelly_allocation_pct = round(min(max(0.0, float(play.kelly_allocation_pct)), max_kelly_pct), 2)

            # Evaluate closed-form Covered Call recommendation if CAGG Z-score >= +2.5 (§2.3, §2.6)
            z_score = 0.0
            try:
                from shared.db import get_timescale
                db = self.db or await get_timescale()
                if db:
                    row = await db.query_one(
                        """
                        SELECT z_score FROM tradfi_bars_5m_zscore 
                        WHERE ticker = $1 
                        ORDER BY bucket_time DESC 
                        LIMIT 1;
                        """,
                        ticker.upper()
                    )
                    if row and row.get("z_score") is not None:
                        z_score = float(row["z_score"])
            except Exception as z_err:
                logger.debug(f"CAGG Z-score view lookup for {ticker} fallback: {z_err}")
                # Fallback to in-memory preceding window during cold start or offline db
                if len(returns) >= 20:
                    mean_ret = float(np.mean(returns[-21:-1])) if len(returns) > 20 else float(np.mean(returns[:-1]))
                    std_ret = float(np.std(returns[-21:-1])) if len(returns) > 20 else float(np.std(returns[:-1]))
                    if std_ret > 1e-6:
                        z_score = (returns[-1] - mean_ret) / std_ret

            live_iv = None
            raw_iv = await self.redis.raw.get(f"sentinel:options:iv:{ticker}")
            if raw_iv:
                try:
                    live_iv = float(raw_iv)
                except Exception:
                    pass

            raw_watchlist = await self.redis.raw.zrange("sentinel:watchlist:equities", 0, -1)
            watched_set = {s.decode() if isinstance(s, bytes) else str(s) for s in raw_watchlist} if raw_watchlist else None

            cc_rec = quant_calc.generate_covered_call_recommendation(
                ticker=ticker,
                current_price=current_price,
                z_score=z_score,
                target_delta=0.30,
                dte_days=30,
                live_iv=live_iv,
                realized_volatility=ewma_vol,
                watched_equities=watched_set
            )
            if cc_rec:
                brief.covered_call_overlays.append(cc_rec)

            # Force-write PortfolioMetrics from real computed values (§1.6, §1.7, §1.8)
            if not brief.portfolio_metrics:
                brief.portfolio_metrics = PortfolioMetrics()

            if has_history:
                brief.portfolio_metrics.var_95_pct = var_95_pct
                brief.portfolio_metrics.cvar_99_pct = cvar_99_pct
                brief.portfolio_metrics.sharpe_ratio = sharpe_ratio_val
                brief.portfolio_metrics.metrics_source = "computed"
            else:
                brief.portfolio_metrics.var_95_pct = None
                brief.portfolio_metrics.cvar_99_pct = None
                brief.portfolio_metrics.sharpe_ratio = None
                brief.portfolio_metrics.metrics_source = "insufficient_history"

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"fin_{ticker}_{int(time.time())}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
                "graph_context": graph_context,
            }

            await self.redis.raw.set("sentinel:financial:advice:latest", json.dumps(res_payload), ex=86400)

            # Record predictions & publish bulletins for top plays
            for play in brief.highest_conviction_plays[:2]:
                direction = "up" if play.action == "BUY" else "down" if play.action == "SELL" else "neutral"

                safe_create_task(
                    self.record_prediction(
                        ticker=play.ticker,
                        direction=direction,
                        conviction=play.conviction_score,
                        entry_price=play.entry_level,
                        target_price=play.target_price,
                        time_horizon_hours=24,
                    ),
                    name=f"record-prediction-{play.ticker}"
                )

                safe_create_task(
                    self.publish_bulletin(
                        bulletin_type="signal",
                        summary=f"{play.action} {play.ticker} @ ${play.entry_level:.2f} -> ${play.target_price:.2f} (Kelly {play.kelly_allocation_pct:.1f}%)",
                        ticker=play.ticker,
                        conviction=play.conviction_score,
                        expected_direction=direction,
                        payload={"action": play.action, "entry": play.entry_level, "target": play.target_price, "var_95": var_95},
                        ttl_seconds=3600,
                    ),
                    name=f"quant-advisory-bulletin-{play.ticker}"
                )

            return res_payload

        except (SchemaViolationError, InferenceError, Exception) as e:
            logger.error(f"Trading advisory LLM error for {ticker}: {e}")
            return None

    async def run_scheduled_review(self) -> Optional[Dict[str, Any]]:
        """
        Scheduled review sweep across watched equities.
        Brings scheduled execution path to 100% parity with live trigger path.
        """
        try:
            raw_watched = await self.redis.raw.zrange("sentinel:watched:equities", 0, -1)
            tickers = [t.decode("utf-8") if isinstance(t, bytes) else str(t) for t in raw_watched]
            if not tickers:
                tickers = ["AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD"]

            results = []
            for ticker in tickers[:5]:  # Sweep top 5 watched tickers
                res = await self._process_trading_advisory({"ticker": ticker, "anomaly_score": 0.75}, ticker, 0.75)
                if res:
                    results.append(res)
            return {"scheduled_review": results} if results else None
        except Exception as e:
            logger.error(f"Error during quant scheduled review: {e}")
            return None

    # ── HELPER METHODS ────────────────────────────────────────────────────────

    async def _fetch_prices(self, ticker: str) -> Tuple[List[float], List[float], List[float]]:
        key = f"sentinel:candles:1h:{ticker.upper()}"
        raw = await self.redis.raw.lrange(key, 0, -1)
        closes, highs, lows = [], [], []
        for item in raw:
            try:
                bar = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
                closes.append(float(bar.get("close", 0)))
                highs.append(float(bar.get("high", bar.get("close", 0))))
                lows.append(float(bar.get("low", bar.get("close", 0))))
            except Exception:
                pass
        return closes, highs, lows

    def _compute_ta(self, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
        return compute_ta_indicators(closes, highs, lows)

    async def _fetch_news_context(self, ticker: str) -> List[Dict]:
        try:
            rows = await self.db.query("""
                SELECT headline, anomaly_score, occurred_at, named_entities, tags
                FROM events
                WHERE type = 'headline'
                  AND occurred_at > NOW() - INTERVAL '4 hours'
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
            logger.debug(f"News context fetch error for {ticker}: {e}")
            return []

    async def _fetch_graph_context(self, ticker: str) -> List[Dict]:
        try:
            neo4j_client = self.neo4j or await get_neo4j()
            if not neo4j_client:
                return []
            q = "MATCH (e:Entity)-[r]-(t:Entity {id: $ticker}) RETURN e.id AS related_id, type(r) AS rel_type, coalesce(r.confidence, 0.5) AS confidence LIMIT 10"
            rows = await neo4j_client.query(q, {"ticker": ticker.upper()})
            return rows or []
        except Exception as e:
            logger.debug(f"Graph context fetch error for {ticker}: {e}")
            return []

    async def _fetch_earnings_context(self, ticker: str) -> str:
        """Fetch cached earnings context from Redis (populated by Finnhub earnings poller)."""
        try:
            raw = await self.redis.raw.get(f"sentinel:earnings:{ticker}")
            if raw:
                data = json.loads(raw)
                report_date = data.get("report_date", "")
                session = data.get("session", "")
                eps_est = data.get("eps_estimate")
                eps_act = data.get("eps_actual")
                surprise = data.get("eps_surprise_pct")
                rev_est = data.get("revenue_estimate")
                rev_act = data.get("revenue_actual")
                trade_type = data.get("trade_type", "")

                if trade_type == "EARNINGS_SURPRISE" and eps_act is not None:
                    return (
                        f"📊 EARNINGS REPORTED: {ticker} | Date: {report_date} ({session}) | "
                        f"EPS: {eps_act} vs Est {eps_est} (Surprise: {surprise:+.1f}%) | "
                        f"Revenue: {rev_act} vs Est {rev_est}"
                    )
                else:
                    session_label = {"bmo": "Pre-Market", "amc": "After-Close", "dmh": "During Hours"}.get(
                        session, session.upper() if session else "TBD"
                    )
                    return (
                        f"📅 EARNINGS UPCOMING: {ticker} | Date: {report_date} ({session_label}) | "
                        f"Est EPS: {eps_est} | Est Revenue: {rev_est}"
                    )
        except Exception:
            pass
        return ""

    async def _fetch_funding_context(self, symbol: str) -> str:
        """Fetch cached crypto funding rate context from Redis (populated by markPrice stream)."""
        try:
            for candidate in [symbol.upper(), symbol.upper() + "USDT"]:
                raw = await self.redis.raw.get(f"sentinel:crypto:funding:{candidate}")
                if raw:
                    data = json.loads(raw)
                    fr = data.get("funding_rate", 0)
                    basis = data.get("basis_bps", 0)
                    mark = data.get("mark_price", 0)
                    idx = data.get("index_price", 0)
                    annualized = abs(fr) * 3 * 365 * 100
                    return (
                        f"⚡ PERP FUNDING: {candidate} | Rate: {fr:.6f} ({annualized:.1f}% annualized) | "
                        f"Basis: {basis:.2f}bps | Mark: {mark:.2f} | Index: {idx:.2f}"
                    )
        except Exception:
            pass
        return ""
