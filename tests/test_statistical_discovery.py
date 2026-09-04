"""
tests/test_statistical_discovery.py

Unit & Integration Test Suite for Tier 4:
- StatisticalDiscoveryEngine (§4.1, §4.2)
- IntraTradFiHawkesCorrelator Wiring (§4.3)
- ThresholdCalibrationHarness Wiring (§4.4)
- Governed Ontology Proposals for Statistical Graph Edges
"""

import asyncio
import json
import math
import time
import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock, patch

from services.correlation.statistical_discovery import StatisticalDiscoveryEngine, GICS_SECTORS
from services.correlation.sector_hawkes import IntraTradFiHawkesCorrelator
from services.reasoning.calibration_harness import ThresholdCalibrationHarness
from shared.kafka import Topics


def test_build_candidate_pairs_prunes_via_graph_topology():
    async def _test():
        mock_neo4j = AsyncMock()
        mock_neo4j.query = AsyncMock(return_value=[
            {"src": "NVDA", "tgt": "TSM"},
            {"src": "AAPL", "tgt": "FOXCONN"},
            {"src": "XOM", "tgt": "OXY"},
        ])

        engine = StatisticalDiscoveryEngine(neo4j_client=mock_neo4j)
        pairs = await engine.build_candidate_pairs()
        
        # Verify topology pairs included
        pair_tuples = set(pairs)
        assert ("NVDA", "TSM") in pair_tuples or ("TSM", "NVDA") in pair_tuples
        assert ("XOM", "OXY") in pair_tuples or ("OXY", "XOM") in pair_tuples
        # Verify core watchlist pairs included
        assert ("NVDA", "AMD") in pair_tuples or ("AMD", "NVDA") in pair_tuples

    asyncio.run(_test())


def test_threshold_calibration_harness_and_redis_persistence():
    async def _test():
        mock_redis = AsyncMock()
        mock_db = AsyncMock()

        # Mock historical scenario outcomes
        outcomes_data = [
            json.dumps({"status": "CONFIRMED", "confidence": 85.0, "payload": {"anomaly_score": 0.8}}),
            json.dumps({"status": "CONFIRMED", "confidence": 78.0, "payload": {"anomaly_score": 0.65}}),
            json.dumps({"status": "DENIED", "confidence": 40.0, "payload": {"anomaly_score": 0.3}}),
        ]
        mock_redis.raw.lrange = AsyncMock(return_value=outcomes_data)

        engine = StatisticalDiscoveryEngine(db_client=mock_db, redis_client=mock_redis)
        calibrated = await engine.run_threshold_calibration()

        assert "min_correlation_coef" in calibrated
        assert "min_granger_f_stat" in calibrated
        assert "min_hawkes_branching_ratio" in calibrated
        assert calibrated["min_correlation_coef"] >= 0.55
        # One write, not two. The harness wrote the same payload to both
        # sentinel:calibration:latest and :correlation_thresholds, and only
        # the latter has a reader (statistical_discovery reads it back);
        # the duplicate was state nothing consumed.
        assert mock_redis.raw.set.call_count >= 1
        written_keys = [c.args[0] for c in mock_redis.raw.set.call_args_list if c.args]
        assert "sentinel:calibration:correlation_thresholds" in written_keys, written_keys

        # Test threshold retrieval reads from Redis
        mock_redis.raw.get = AsyncMock(return_value=json.dumps(calibrated))
        thresholds = await engine.get_calibrated_thresholds()
        assert thresholds["min_correlation_coef"] == calibrated["min_correlation_coef"]

    asyncio.run(_test())


def test_discover_pairwise_correlations_emits_governed_proposals():
    async def _test():
        mock_db = AsyncMock()
        mock_redis = AsyncMock()
        mock_redis.raw.get = AsyncMock(return_value=json.dumps({
            "min_correlation_coef": 0.60,
            "max_p_value": 0.05,
            "min_granger_f_stat": 2.50,
            "min_hawkes_branching_ratio": 0.20,
        }))
        mock_neo4j = AsyncMock()
        mock_neo4j.query = AsyncMock(return_value=[
            {"src": "NVDA", "tgt": "TSM"}
        ])
        mock_producer = AsyncMock()

        engine = StatisticalDiscoveryEngine(
            db_client=mock_db,
            redis_client=mock_redis,
            neo4j_client=mock_neo4j,
            producer=mock_producer,
        )

        # Synthetic correlated series (NVDA leading and correlating with TSM)
        np.random.seed(42)
        n_points = 80
        base_innovations = np.random.normal(0, 0.02, n_points)
        x_prices = [100.0]
        y_prices = [50.0]
        for i in range(1, n_points):
            # X price follows random walk
            x_prices.append(x_prices[-1] * (1.0 + base_innovations[i]))
            # Y price has strong contemporaneous correlation and Granger lag from X
            y_ret = 0.70 * base_innovations[i] + 0.25 * base_innovations[i - 1] + 0.05 * np.random.normal(0, 0.005)
            y_prices.append(y_prices[-1] * (1.0 + y_ret))

        async def mock_fetch_price(ticker, limit=120):
            if ticker == "NVDA":
                return x_prices
            elif ticker == "TSM":
                return y_prices
            return []

        engine.fetch_price_series = mock_fetch_price
        engine.build_candidate_pairs = AsyncMock(return_value=[("NVDA", "TSM")])

        discoveries = await engine.discover_pairwise_correlations()
        assert len(discoveries) == 1
        disc = discoveries[0]
        assert disc["pair"] == "NVDA:TSM"
        assert abs(disc["pearson_r"]) > 0.50

        # Verify governed proposal emissions
        assert mock_producer.send.call_count >= 1
        calls = [c[0] for c in mock_producer.send.call_args_list]
        topic_names = [c[0] for c in calls]
        assert all(t == Topics.ONTOLOGY_PROPOSALS for t in topic_names)

        actions = [c[1]["action"] for c in calls]
        assert "LINK_ENTITY" in actions

    asyncio.run(_test())


def test_intra_tradfi_hawkes_correlator_wiring():
    async def _test():
        mock_db = AsyncMock()
        mock_redis = AsyncMock()
        mock_redis.raw.get = AsyncMock(return_value=json.dumps({
            "min_hawkes_branching_ratio": 0.15,
        }))
        mock_producer = AsyncMock()

        engine = StatisticalDiscoveryEngine(
            db_client=mock_db,
            redis_client=mock_redis,
            producer=mock_producer,
        )

        now = time.time()
        # Mock price series for sector ETFs
        def mock_prices(ticker, limit=60):
            # Create volatile burst for Technology (XLK) and Energy (XLE)
            np.random.seed(123)
            prices = [100.0]
            for i in range(50):
                ret = np.random.normal(0, 0.03) if i in (10, 11, 20, 21, 35) else np.random.normal(0, 0.005)
                prices.append(prices[-1] * (1.0 + ret))
            return prices

        engine.fetch_price_series = AsyncMock(side_effect=lambda ticker, limit=60: mock_prices(ticker, limit))

        res = await engine.discover_sector_hawkes_contagion()
        assert "fit_summary" in res
        # Check that IntraTradFiHawkesCorrelator was fitted
        assert engine.sector_hawkes.mle is not None

    asyncio.run(_test())


def test_api_gateway_surfaces_statistical_edge_properties():
    from fastapi.testclient import TestClient
    from services.api_gateway.routes.main import app
    from services.api_gateway.dependencies import get_graph, get_db, create_jwt_token

    client = TestClient(app)
    client.cookies.set("sentinel_session", create_jwt_token({"sub": "admin@sentinel.io"}))

    now_ts = time.time()
    mock_graph = MagicMock()
    mock_graph.query = AsyncMock(return_value=[
        {
            "source_name": "NVDA", "source_type": "Company", "relationship": "STATISTICALLY_CORRELATED_WITH",
            "target_id": "TSM", "target_name": "Taiwan Semiconductor", "target_type": "Company",
            "weight": 0.88, "confidence": 0.95, "last_updated": (now_ts - 100) * 1000,
            "coefficient": 0.88, "p_value": 0.0001, "lag": 1, "f_stat": 8.45,
            "branching_ratio": None, "method": "pearson", "window": "60_bars"
        }
    ])
    mock_db = MagicMock()

    app.dependency_overrides[get_graph] = lambda: mock_graph
    app.dependency_overrides[get_db] = lambda: mock_db

    try:
        res = client.get("/api/v1/graph/entity/NVDA")
        assert res.status_code == 200
        data = res.json()
        conns = data["connections"]
        assert len(conns) == 1
        conn = conns[0]
        assert conn["relationship"] == "STATISTICALLY_CORRELATED_WITH"
        assert conn["coefficient"] == 0.88
        assert conn["p_value"] == 0.0001
        assert conn["f_stat"] == 8.45
        assert conn["method"] == "pearson"
        assert conn["window"] == "60_bars"
    finally:
        app.dependency_overrides.pop(get_graph, None)
        app.dependency_overrides.pop(get_db, None)
