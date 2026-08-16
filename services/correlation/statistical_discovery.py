"""
services/correlation/statistical_discovery.py

STATISTICAL CORRELATION DISCOVERY ENGINE (§4.1 - §4.4)
======================================================
Systematic, scheduled statistical discovery engine:
1. Prunes candidate asset pairs using Tier-3 graph topology & core watchlists (§4.1).
2. Reads historical price series from Tier-2 TimescaleDB Continuous Aggregates (CAGGs).
3. Computes rolling Pearson/Spearman correlations, bidirectional Granger causality, and cointegration (§4.2).
4. Fits intra-tradfi sector excitation point processes with IntraTradFiHawkesCorrelator (§4.3).
5. Calibrates significance cutoffs using ThresholdCalibrationHarness (§4.4).
6. Writes results strictly as governed graph proposals via Kafka Topics.ONTOLOGY_PROPOSALS.
"""

import asyncio
import json
import logging
import math
import time
from typing import Dict, List, Tuple, Set, Optional, Any
from datetime import datetime, timezone

import numpy as np

from shared.kafka import SentinelProducer, Topics
from shared.models.ontology import is_valid_predicate
from shared.utils import quant_calc
from services.correlation.sector_hawkes import IntraTradFiHawkesCorrelator, GICS_SECTORS
from services.reasoning.calibration_harness import ThresholdCalibrationHarness

logger = logging.getLogger("correlation.statistical_discovery")

# Core candidate seed tickers (Tech / Semi / Defense / Energy / Macro Proxies)
DEFAULT_WATCHLIST_TICKERS = [
    "NVDA", "TSM", "AAPL", "MSFT", "AMD", "AVGO", "QQQ", "SPY",
    "GOOGL", "AMZN", "META", "INTC", "ASML", "MU",
    "XOM", "CVX", "OXY", "LMT", "RTX", "GD",
    "CL=F", "GC=F", "TNX", "DXY", "EURUSD", "BTC-USD", "ETH-USD"
]

# Sector ETF mappings for Intra-TradFi Hawkes Contagion
SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Consumer Cyclical": "XLY",
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Basic Materials": "XLB",
}


class StatisticalDiscoveryEngine:
    """
    Continuous statistical correlation discovery engine.
    Reads durable TimescaleDB CAGGs, runs mathematical causal/correlation tests,
    and updates the Neo4j Knowledge Graph via governed proposals.
    """

    def __init__(
        self,
        db_client=None,
        redis_client=None,
        neo4j_client=None,
        producer: Optional[SentinelProducer] = None,
        candidate_tickers: Optional[List[str]] = None,
    ):
        self.db = db_client
        self.redis = redis_client
        self.neo4j = neo4j_client
        self.producer = producer
        self.candidate_tickers = candidate_tickers or DEFAULT_WATCHLIST_TICKERS
        self.sector_hawkes = IntraTradFiHawkesCorrelator()
        self.calibrator = ThresholdCalibrationHarness(db_client=self.db, redis_client=self.redis)

    # ── 1. CANDIDATE PAIR GENERATION (§4.1) ────────────────────────────────────

    async def build_candidate_pairs(self) -> List[Tuple[str, str]]:
        """
        Builds candidate pairs pruned by Tier-3 Knowledge Graph topology
        and core cross-asset proxies, preventing combinatorial O(N^2) noise.
        """
        pairs: Set[Tuple[str, str]] = set()

        # 1. Structural graph relationships from Neo4j
        if self.neo4j:
            try:
                query = """
                MATCH (a:Entity)-[r:MEMBER_OF|CUSTOMER_OF|SUPPLIER_TO|COMPETES_WITH|SYMPATHY_MOVER|COMMODITY_EXPOSURE|FX_EXPOSURE_TO|OPERATES_IN|RELATED_TO]->(b:Entity)
                RETURN toUpper(coalesce(a.name, a.id)) AS src, toUpper(coalesce(b.name, b.id)) AS tgt
                LIMIT 200
                """
                records = await self.neo4j.query(query)
                for r in (records or []):
                    src, tgt = r.get("src"), r.get("tgt")
                    if src and tgt and src != tgt and len(src) <= 10 and len(tgt) <= 10:
                        pair = (src, tgt) if src < tgt else (tgt, src)
                        pairs.add(pair)
            except Exception as e:
                logger.debug(f"Neo4j topology pair extraction fallback: {e}")

        # 2. Add high-conviction core semi/tech peer pairs & macro releases (§5.2)
        core_peers = [
            ("NVDA", "TSM"), ("NVDA", "AMD"), ("NVDA", "AVGO"), ("NVDA", "QQQ"),
            ("AAPL", "MSFT"), ("AAPL", "TSM"), ("AMD", "INTC"), ("ASML", "TSM"),
            ("XOM", "CVX"), ("XOM", "CL=F"), ("CVX", "CL=F"), ("OXY", "CL=F"),
            ("LMT", "RTX"), ("LMT", "GD"), ("SPY", "QQQ"), ("SPY", "TNX"),
            ("GC=F", "DXY"), ("CL=F", "DXY"), ("EURUSD", "DXY"), ("BTC-USD", "QQQ"),
            # Macro release surprise cross-asset reactions (§5.2)
            ("MACRO:CPI", "MU"), ("MACRO:CPI", "NVDA"), ("MACRO:CPI", "TLT"),
            ("MACRO:NFP", "SPY"), ("MACRO:FOMC", "QQQ"), ("MACRO:PMI", "XLI"),
            ("MACRO:PCE", "AAPL"), ("MACRO:GDP", "SPY"),
        ]
        for src, tgt in core_peers:
            pair = (src, tgt) if src < tgt else (tgt, src)
            pairs.add(pair)

        return list(pairs)

    # ── 2. PRICE & SURPRISE SERIES RETRIEVAL FROM TIMESCALEDB (§4.1, §4.2, §5.2) ──

    async def fetch_price_series(self, ticker: str, limit: int = 100) -> List[float]:
        """
        Fetches historical closing price series or macro release surprise series from
        TimescaleDB Continuous Aggregates (CAGGs) / Redis, falling back to tradfi_bars hypertable.
        """
        # Handle macro surprise series (§5.2)
        if ticker.startswith("MACRO:"):
            if self.redis:
                try:
                    raw_items = await self.redis.raw.lrange(f"sentinel:macro_surprises:{ticker}", -limit, -1)
                    if raw_items:
                        return [float(x) for x in raw_items if x]
                except Exception as e:
                    logger.debug(f"Redis macro surprise fetch fallback: {e}")
            # If no cached history, check timescale or return baseline
            return []

        if not self.db:
            return []

        try:
            # Query 5-minute continuous aggregate first
            query_cagg = """
            SELECT close FROM tradfi_bars_5m
            WHERE ticker = $1
            ORDER BY bucket_time DESC
            LIMIT $2
            """
            rows = await self.db.query(query_cagg, ticker.upper(), limit)
            if not rows:
                # Fallback to 1-minute tradfi_bars hypertable
                query_bars = """
                SELECT close FROM tradfi_bars
                WHERE ticker = $1
                ORDER BY time DESC
                LIMIT $2
                """
                rows = await self.db.query(query_bars, ticker.upper(), limit)

            if rows:
                closes = [float(r["close"]) for r in reversed(rows) if r.get("close") is not None]
                return closes
        except Exception as e:
            logger.debug(f"TimescaleDB price series fetch error for {ticker}: {e}")

        return []

    # ── 3. SIGNIFICANCE THRESHOLD RETRIEVAL (§4.4) ─────────────────────────────

    async def get_calibrated_thresholds(self) -> Dict[str, float]:
        """Retrieves dynamically calibrated significance thresholds from Redis or defaults."""
        defaults = {
            "min_correlation_coef": 0.65,
            "max_p_value": 0.05,
            "min_granger_f_stat": 3.84,
            "min_hawkes_branching_ratio": 0.25,
            "min_cointegration_p_value": 0.05,
        }
        if self.redis:
            try:
                raw = await self.redis.raw.get("sentinel:calibration:correlation_thresholds")
                if raw:
                    cached = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                    for k, v in defaults.items():
                        defaults[k] = float(cached.get(k, v))
            except Exception as e:
                logger.debug(f"Redis calibration threshold fetch fallback: {e}")
        return defaults

    # ── 4. PAIRWISE STATISTICAL DISCOVERY (§4.2) ───────────────────────────────

    async def discover_pairwise_correlations(self) -> List[Dict[str, Any]]:
        """
        Computes Pearson, Spearman, Granger Causality, and Cointegration across candidate pairs.
        Emits governed graph proposals for statistically significant discoveries.
        """
        pairs = await self.build_candidate_pairs()
        thresholds = await self.get_calibrated_thresholds()
        discoveries: List[Dict[str, Any]] = []

        for ticker_a, ticker_b in pairs:
            try:
                prices_a = await self.fetch_price_series(ticker_a, limit=120)
                prices_b = await self.fetch_price_series(ticker_b, limit=120)

                min_len = min(len(prices_a), len(prices_b))
                if min_len < 20:
                    continue

                x = np.array(prices_a[-min_len:], dtype=np.float64)
                y = np.array(prices_b[-min_len:], dtype=np.float64)

                # Returns series
                ret_x = np.diff(x) / (x[:-1] + 1e-8)
                ret_y = np.diff(y) / (y[:-1] + 1e-8)

                if len(ret_x) < 15 or np.std(ret_x) < 1e-6 or np.std(ret_y) < 1e-6:
                    continue

                # 1. Pearson Correlation
                r_pearson = float(np.corrcoef(ret_x, ret_y)[0, 1])
                # t-statistic for Pearson p-value
                n = len(ret_x)
                df = n - 2
                t_stat = r_pearson * math.sqrt(df / max(1e-6, 1.0 - r_pearson ** 2)) if abs(r_pearson) < 0.9999 else 999.0
                p_pearson = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(t_stat) / math.sqrt(2.0)))) # Normal approx

                # 2. Granger Causality (A -> B and B -> A)
                gc_ab = quant_calc.granger_causality(list(x), list(y), max_lag=3)
                gc_ba = quant_calc.granger_causality(list(y), list(x), max_lag=3)

                # 3. Engle-Granger Cointegration
                coint = quant_calc.engle_granger_cointegration(list(x), list(y))

                discovery = {
                    "pair": f"{ticker_a}:{ticker_b}",
                    "ticker_a": ticker_a,
                    "ticker_b": ticker_b,
                    "pearson_r": round(r_pearson, 4),
                    "pearson_p": round(p_pearson, 4),
                    "granger_a_causes_b": gc_ab.get("x_granger_causes_y", False),
                    "granger_b_causes_a": gc_ba.get("x_granger_causes_y", False),
                    "cointegrated": coint.get("is_cointegrated", False),
                    "hedge_ratio": coint.get("hedge_ratio", 1.0),
                    "half_life_bars": coint.get("half_life_bars", 0.0),
                }
                discoveries.append(discovery)

                # ── GOVERNED GRAPH PROPOSAL EMISSION (§4.2, §5.2) ─────────────
                if self.producer:
                    label_a = "MacroFactor" if ticker_a.startswith("MACRO:") else "Company"
                    label_b = "MacroFactor" if ticker_b.startswith("MACRO:") else "Company"

                    # A. Statistical Correlation Edge
                    if abs(r_pearson) >= thresholds["min_correlation_coef"] and p_pearson <= thresholds["max_p_value"]:
                        rel_type = "STATISTICALLY_CORRELATED_WITH"
                        await self.producer.send(
                            Topics.ONTOLOGY_PROPOSALS,
                            {
                                "entity_id": ticker_a,
                                "action": "LINK_ENTITY",
                                "data": {
                                    "target_id": ticker_b,
                                    "source_label": label_a,
                                    "target_label": label_b,
                                    "relation_type": rel_type,
                                    "weight": round(abs(r_pearson), 4),
                                    "confidence": round(max(0.5, 1.0 - p_pearson), 4),
                                    "properties": {
                                        "coefficient": round(r_pearson, 4),
                                        "p_value": round(p_pearson, 4),
                                        "method": "pearson",
                                        "window": f"{min_len}_bars",
                                    }
                                }
                            },
                            key=ticker_a,
                        )
                        logger.info(f"📊 Statistical Correlation Link: {ticker_a} <-> {ticker_b} (r={r_pearson:.3f}, p={p_pearson:.4f})")
                        
                        # Cache active correlation IDs in Redis for event enrichment linking (§8.2)
                        if self.redis:
                            try:
                                corr_id = f"corr:stat:{ticker_a}:{ticker_b}"
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_a}", corr_id)
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_b}", corr_id)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_a}", 86400)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_b}", 86400)
                            except Exception as re:
                                logger.debug(f"Failed to cache correlation ID in Redis: {re}")

                    # B. Directional Granger Causality Edge (A -> B)
                    if gc_ab.get("x_granger_causes_y"):
                        f_stat = float(gc_ab.get("f_statistic", 0.0))
                        p_val = float(gc_ab.get("p_value", 0.01))
                        lag = int(gc_ab.get("optimal_lag", 1))
                        await self.producer.send(
                            Topics.ONTOLOGY_PROPOSALS,
                            {
                                "entity_id": ticker_a,
                                "action": "LINK_ENTITY",
                                "data": {
                                    "target_id": ticker_b,
                                    "source_label": label_a,
                                    "target_label": label_b,
                                    "relation_type": "GRANGER_CAUSES",
                                    "weight": min(1.0, round(f_stat / 10.0, 4)),
                                    "confidence": round(max(0.5, 1.0 - p_val), 4),
                                    "properties": {
                                        "lag": lag,
                                        "f_stat": round(f_stat, 4),
                                        "p_value": round(p_val, 4),
                                    }
                                }
                            },
                            key=ticker_a,
                        )
                        logger.info(f"🎯 Granger Causality Link: {ticker_a} -[GRANGER_CAUSES (lag={lag})]-> {ticker_b}")
                        
                        if self.redis:
                            try:
                                granger_id = f"granger:{ticker_a}:{ticker_b}:lag{lag}"
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_a}", granger_id)
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_b}", granger_id)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_a}", 86400)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_b}", 86400)
                            except Exception as re:
                                logger.debug(f"Failed to cache Granger ID in Redis: {re}")

                    # C. Directional Granger Causality Edge (B -> A)
                    if gc_ba.get("x_granger_causes_y"):
                        f_stat = float(gc_ba.get("f_statistic", 0.0))
                        p_val = float(gc_ba.get("p_value", 0.01))
                        lag = int(gc_ba.get("optimal_lag", 1))
                        await self.producer.send(
                            Topics.ONTOLOGY_PROPOSALS,
                            {
                                "entity_id": ticker_b,
                                "action": "LINK_ENTITY",
                                "data": {
                                    "target_id": ticker_a,
                                    "source_label": label_b,
                                    "target_label": label_a,
                                    "relation_type": "GRANGER_CAUSES",
                                    "weight": min(1.0, round(f_stat / 10.0, 4)),
                                    "confidence": round(max(0.5, 1.0 - p_val), 4),
                                    "properties": {
                                        "lag": lag,
                                        "f_stat": round(f_stat, 4),
                                        "p_value": round(p_val, 4),
                                    }
                                }
                            },
                            key=ticker_b,
                        )
                        logger.info(f"🎯 Granger Causality Link: {ticker_b} -[GRANGER_CAUSES (lag={lag})]-> {ticker_a}")

                        if self.redis:
                            try:
                                granger_id = f"granger:{ticker_b}:{ticker_a}:lag{lag}"
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_a}", granger_id)
                                await self.redis.raw.sadd(f"sentinel:correlation:active_ids:{ticker_b}", granger_id)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_a}", 86400)
                                await self.redis.raw.expire(f"sentinel:correlation:active_ids:{ticker_b}", 86400)
                            except Exception as re:
                                logger.debug(f"Failed to cache Granger ID in Redis: {re}")

            except Exception as e:
                logger.debug(f"Pairwise statistical discovery error for {ticker_a}:{ticker_b}: {e}")

        return discoveries

    # ── 5. INTRA-TRADFI SECTOR HAWKES CONTAGION (§4.3) ─────────────────────────

    async def discover_sector_hawkes_contagion(self) -> Dict[str, Any]:
        """
        Gathers sector price movement / anomaly timestamps across the 11 GICS sectors,
        fits the IntraTradFiHawkesCorrelator, and writes HAWKES_EXCITES graph edges.
        """
        now = time.time()
        sector_event_streams: Dict[str, List[float]] = {s: [] for s in GICS_SECTORS}

        # Gather sector ETF price volatility timestamps
        for sector, etf in SECTOR_ETF_MAP.items():
            closes = await self.fetch_price_series(etf, limit=60)
            if len(closes) >= 5:
                returns = np.diff(closes) / (np.array(closes[:-1]) + 1e-8)
                std_ret = np.std(returns) if len(returns) > 0 else 0.01
                # Mark high-volatility prints as point-process arrival events
                for idx, ret in enumerate(returns):
                    if abs(ret) >= 1.5 * max(1e-4, std_ret):
                        event_ts = now - (len(returns) - idx) * 300.0
                        sector_event_streams[sector].append(event_ts)

        # Ensure sorted timestamps and normalize relative to t0
        all_timestamps = [ts for s in sector_event_streams.values() for ts in s]
        if not all_timestamps:
            return {"fit_summary": {}, "significant_excitations": []}

        t0 = min(all_timestamps)
        duration = max(1.0, max(all_timestamps) - t0)
        normalized_streams = {
            s: sorted([ts - t0 for ts in timestamps])
            for s, timestamps in sector_event_streams.items()
        }

        # Fit IntraTradFiHawkesCorrelator
        try:
            fit_result = self.sector_hawkes.fit(normalized_streams, T=duration)
            thresholds = await self.get_calibrated_thresholds()
            min_ratio = thresholds["min_hawkes_branching_ratio"]

            # Discover significant cross-sector excitations
            significant_excitations = []
            for src_sector in GICS_SECTORS:
                for tgt_sector in GICS_SECTORS:
                    if src_sector == tgt_sector:
                        continue
                    ratio = self.sector_hawkes.get_branching_ratio(src_sector, tgt_sector)
                    if ratio >= min_ratio:
                        significant_excitations.append({
                            "source_sector": src_sector,
                            "target_sector": tgt_sector,
                            "branching_ratio": round(ratio, 4),
                        })

                        # Emit Governed HAWKES_EXCITES Proposal (§4.3)
                        if self.producer:
                            half_life = math.log(2.0) / max(1e-4, self.sector_hawkes.mle.beta)
                            await self.producer.send(
                                Topics.ONTOLOGY_PROPOSALS,
                                {
                                    "entity_id": src_sector.upper(),
                                    "action": "LINK_ENTITY",
                                    "data": {
                                        "target_id": tgt_sector.upper(),
                                        "source_label": "Sector",
                                        "target_label": "Sector",
                                        "relation_type": "HAWKES_EXCITES",
                                        "weight": round(min(1.0, ratio), 4),
                                        "confidence": round(min(1.0, ratio), 4),
                                        "properties": {
                                            "branching_ratio": round(ratio, 4),
                                            "half_life": round(half_life, 2),
                                            "scope": "sector_contagion",
                                        }
                                    }
                                },
                                key=src_sector.upper(),
                            )
                            logger.info(f"⚡ Intra-TradFi Hawkes Contagion: ({src_sector}) -[HAWKES_EXCITES (η={ratio:.2f})]-> ({tgt_sector})")

            return {
                "fit_summary": fit_result,
                "significant_excitations": significant_excitations,
            }
        except Exception as e:
            logger.warning(f"Sector Hawkes fitting failed: {e}")
            return {}

    # ── 6. THRESHOLD CALIBRATION EXECUTION (§4.4) ──────────────────────────────

    async def run_threshold_calibration(self) -> Dict[str, Any]:
        """Runs the ThresholdCalibrationHarness and updates Redis significance configuration."""
        try:
            return await self.calibrator.calibrate_optimal_thresholds()
        except Exception as e:
            logger.error(f"Threshold calibration execution error: {e}")
            return {}
