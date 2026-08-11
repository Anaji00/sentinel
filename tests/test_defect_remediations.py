"""
tests/test_defect_remediations.py

Comprehensive unit test suite for systemic reasoning & data-handling defect remediations.
Validates:
1. ThresholdCalibrationHarness execution (missing self, timescale query method, lowercase status)
2. ScenarioTracker SQL join table name (correlations vs correlation_clusters)
3. ContextBuilder entity deduplication ordering before slicing
4. PatternLibrary tag overlap threshold query logic
5. CyberEnricher BGP anomaly score floor-capping (0.05 min score)
6. safe_create_task reference tracking and exception logging
"""

import asyncio
import inspect
import logging
import pytest
from unittest.mock import AsyncMock, MagicMock

from services.reasoning.calibration_harness import ThresholdCalibrationHarness, DynamicBayesianCalibrator
from services.reasoning.scenario_tracker import ScenarioTracker
from services.reasoning.context_builder import ContextBuilder
from services.reasoning.pattern_library import PatternLibrary, SIMILARITY_TAGS_THRESHOLD
from services.enrichment.enrichers.cyber import CyberEnricher, CyberThreatScorer
from shared.models.events import RawEvent, EventType
from shared.utils.tasks import safe_create_task


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_threshold_calibration_harness_execution():
    mock_db = AsyncMock()
    mock_db.query.return_value = [
        {"scenario_id": "11111111-1111-1111-1111-111111111111", "status": "confirmed", "confidence_overall": 85.0, "payload": '{"anomaly_score": 0.80}'},
        {"scenario_id": "22222222-2222-2222-2222-222222222222", "status": "denied", "confidence_overall": 20.0, "payload": '{"anomaly_score": 0.15}'},
    ]
    mock_redis = AsyncMock()
    mock_redis.raw.lrange.return_value = []
    mock_redis.raw.set.return_value = True

    harness = ThresholdCalibrationHarness(db_client=mock_db, redis_client=mock_redis)
    result = await harness.calibrate_optimal_thresholds()

    assert "z_score_threshold" in result
    assert "similarity_threshold" in result
    assert result["sample_count"] == 2
    mock_db.query.assert_called_once()
    assert "fetch_all" not in str(mock_db.mock_calls)


@pytest.mark.anyio
async def test_scenario_tracker_denied_sql_join_table_name():
    mock_db = AsyncMock()
    mock_db.query.return_value = [{"rule_id": "rule_test_123"}]
    mock_producer = AsyncMock()

    tracker = ScenarioTracker(db=mock_db, producer=mock_producer)
    tracker._check_scenario = AsyncMock()
    tracker._fetch_active_scenarios = AsyncMock(return_value=[])

    await tracker.check_all()

    # Verify denied scenario query inside scenario_tracker uses 'JOIN correlations c'
    source = inspect.getsource(ScenarioTracker._check_scenario)
    assert "JOIN correlations c ON s.correlation_id = c.correlation_id" in source
    assert "JOIN correlation_clusters c" not in source


@pytest.mark.anyio
async def test_context_builder_entity_deduplication_ordering(monkeypatch):
    builder = ContextBuilder(db_client=AsyncMock())
    
    # Input list with duplicate entities near start
    entity_ids = ["AAPL", "AAPL", "MSFT", "MSFT", "NVDA", "GOOG", "AMZN"]
    
    mock_neo4j = AsyncMock()
    mock_neo4j.query.return_value = []
    monkeypatch.setattr("services.reasoning.context_builder.get_neo4j", AsyncMock(return_value=mock_neo4j))
    
    await builder._fetch_entity_graph(entity_ids)
    
    # Extract query parameters passed to Neo4j
    assert mock_neo4j.query.called
    call_args = mock_neo4j.query.call_args[0][1]
    ids_passed = call_args["ids"]
    
    # Deduplication must preserve unique order before truncating to top 5
    assert ids_passed == ["AAPL", "MSFT", "NVDA", "GOOG", "AMZN"]
    assert len(ids_passed) == 5


@pytest.mark.anyio
async def test_pattern_library_tag_overlap_threshold_query():
    mock_db = AsyncMock()
    mock_db.query.return_value = []
    library = PatternLibrary(db_client=mock_db)

    tags = ["hormuz", "tanker", "sanctions"]
    await library.find_similar(tags=tags, rule_id="rule_test_1")

    # Verify query includes tag overlap threshold logic
    call_args = mock_db.query.call_args_list
    assert len(call_args) >= 1
    fuzzy_call = call_args[-1]
    sql_str = fuzzy_call[0][0]
    
    assert "unnest(c.tags)" in sql_str
    min_overlap_passed = fuzzy_call[0][4] if len(fuzzy_call[0]) > 4 else None
    assert min_overlap_passed == min(SIMILARITY_TAGS_THRESHOLD, len(tags))


@pytest.mark.anyio
async def test_cyber_enricher_bgp_anomaly_floor_capping():
    mock_scorer = AsyncMock()
    mock_scorer.score_bgp_event.return_value = {"score": 0.0, "path_novelty": 0.0}
    mock_redis = AsyncMock()
    mock_graph = AsyncMock()

    enricher = CyberEnricher(scorer=mock_scorer, redis_client=mock_redis, graph_writer=mock_graph)

    raw_bgp = RawEvent(
        source="bgp_monitor",
        raw_payload={
            "prefix": "192.0.2.0/24",
            "origin_as": "65001",
            "is_hijack": False,
            "as_name": "Power Terminal Grid AS65001",
        }
    )

    normalized = await enricher.enrich(raw_bgp)
    assert normalized is not None
    assert normalized.anomaly_score == 0.05  # Floor-capped score instead of None drop


@pytest.mark.anyio
async def test_safe_create_task_exception_logging(caplog):
    async def failing_coroutine():
        await asyncio.sleep(0.01)
        raise ValueError("Task execution error")

    with caplog.at_level(logging.ERROR):
        task = safe_create_task(failing_coroutine(), name="test-failing-task")
        await asyncio.sleep(0.05)
        
    assert task.done()
    assert "test-failing-task" in caplog.text
    assert "ValueError: Task execution error" in caplog.text
