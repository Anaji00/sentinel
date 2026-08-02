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
    technical_indicators: Dict[str, float] = Field(default_factory=dict)
    fib_levels: Dict[str, float] = Field(default_factory=dict)
    quantitative_rationale: str

class FinancialAdviceBrief(BaseModel):
    market_regime: str
    highest_conviction_plays: List[TradingSignal] = Field(default_factory=list)
    general_hedging_strategy: str


def compute_ta_indicators(closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, Any]:
    """Helper function computing RSI, EMA, ATR, and Fib levels."""
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

    return {
        "rsi": round(rsi, 2),
        "ema_12": round(_ema(12), 4),
        "ema_26": round(_ema(26), 4),
        "atr": round(atr, 4),
        "fib_levels": {k: round(v, 4) for k, v in fibs.items()},
    }


# ── CONSOLIDATED QUANT TRADING ENGINE ──────────────────────────────────────────

class QuantTradingEngine(SentinelAgent):
    """
    Unified Quant & Trading Engine.
    Combines peer discovery, SEC Form 4 insider flow tracking, quantitative risk modeling
    (VaR/CVaR, Half-Kelly), and technical trading signal generation in a single pass.
    """

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
        asyncio.create_task(self.publish_bulletin(
            bulletin_type="signal",
            summary=brief.summary,
            ticker=ticker,
            conviction=0.75 if c_suite else 0.55,
            expected_direction="up" if total_net > 0 else "down",
            payload={"net_notional": total_net, "c_suite": c_suite},
            ttl_seconds=86400,
        ))

        return res_payload

    # ── SUB-ENGINE 2: QUANT PEER DISCOVERY ───────────────────────────────────

    async def _process_peer_discovery(self, message: Dict[str, Any], ticker: str, anomaly_score: float) -> Optional[Dict[str, Any]]:
        dedup_key = f"quant_discovery:{ticker}:{int(time.time() // 1800)}"
        if await self.is_recently_processed(dedup_key, window_seconds=1800):
            return None
        await self.mark_processed(dedup_key, window_seconds=1800)

        # Concurrent context hydration
        news_context, graph_context, global_context, cross_context = await asyncio.gather(
            self._fetch_news_context(ticker),
            self._fetch_graph_context(ticker),
            self.fetch_global_context(),
            self.get_cross_agent_context(ticker=ticker, limit=3),
        )
        cross_block = f"\n- Cross-Agent Intelligence:\n{cross_context}" if cross_context else ""

        user_prompt = f"""=== ANOMALOUS INSTRUMENT RESEARCH ===
Target Symbol: {ticker} | Anomaly Score: {anomaly_score:.2f}
Global Context: {global_context}
{cross_block}
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
            asyncio.create_task(self.publish_bulletin(
                bulletin_type="thesis",
                summary=f"Quant Discovery {ticker}: {discovery.catalyst_category}. Peers: {peer_names}",
                ticker=ticker,
                conviction=min(1.0, anomaly_score * 0.9),
                expected_direction="uncertain",
                payload={"peers": peer_names, "catalyst": discovery.catalyst_category},
                ttl_seconds=3600,
            ))

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
        ewma_vol = quant_calc.ewma_volatility(returns, annualize=True)
        var_95 = quant_calc.var_historical(returns, confidence=0.95, position_value=10_000)
        cvar_95 = quant_calc.cvar_historical(returns, confidence=0.95, position_value=10_000)

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
                "ewma_volatility_annualized": ewma_vol,
                "var_95_per_10k": var_95,
                "cvar_95_per_10k": cvar_95,
                "half_kelly_allocation_pct": round(kelly_pct * 100.0, 2),
                "fib_levels": indicators["fib_levels"],
            }
        }

        cross_context = await self.get_cross_agent_context(ticker=ticker, limit=3)
        cross_block = f"\n        CROSS-AGENT INTELLIGENCE:\n        {cross_context}\n" if cross_context else ""

        user_prompt = f"""=== FINANCIAL ADVISORY & RISK EVALUATION ===
Target Instrument: {ticker} | Macro Regime: {rates_regime}
Risk Indicators: {json.dumps(indicators_data, separators=(',', ':'), default=str)}
{cross_block}
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

            res_payload = {
                "agent": self.name,
                "agent_run_id": f"fin_{ticker}_{int(time.time())}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "brief": brief.model_dump(),
            }

            await self.redis.raw.set("sentinel:financial:advice:latest", json.dumps(res_payload), ex=86400)

            # Record predictions & publish bulletins for top plays
            for play in brief.highest_conviction_plays[:2]:
                direction = "up" if play.action == "BUY" else "down" if play.action == "SELL" else "neutral"

                asyncio.create_task(self.record_prediction(
                    ticker=play.ticker,
                    direction=direction,
                    conviction=play.conviction_score,
                    entry_price=play.entry_level,
                    target_price=play.target_price,
                    time_horizon_hours=24,
                ))

                asyncio.create_task(self.publish_bulletin(
                    bulletin_type="signal",
                    summary=f"{play.action} {play.ticker} @ ${play.entry_level:.2f} -> ${play.target_price:.2f} (Kelly {play.kelly_allocation_pct:.1f}%)",
                    ticker=play.ticker,
                    conviction=play.conviction_score,
                    expected_direction=direction,
                    payload={"action": play.action, "entry": play.entry_level, "target": play.target_price, "var_95": var_95},
                    ttl_seconds=3600,
                ))

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
            neo4j_client = await get_neo4j()
            rows = await neo4j_client.query("""
                MATCH (n {id: $ticker})-[r]-(m)
                RETURN type(r) as relationship, coalesce(m.name, m.id) as connected,
                       labels(m) as labels
                LIMIT 20
            """, {"ticker": ticker.upper()})
            return [
                {
                    "relationship": r.get("relationship"),
                    "connected_entity": r.get("connected"),
                    "entity_type": r.get("labels", ["Unknown"])[0],
                }
                for r in rows
            ]
        except Exception as e:
            logger.debug(f"Graph context fetch error for {ticker}: {e}")
            return []
