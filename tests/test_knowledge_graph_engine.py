"""
tests/test_knowledge_graph_engine.py

Unit tests for KnowledgeGraphEngine:
  1. Off-whitelist predicate ("Source") rejection in _merge_graph_triples.
  2. Case-insensitive self-loop (subject == object) triple rejection in _merge_graph_triples.
  3. Valid predicate ("LOCATED_IN") and distinct subject/object pass-through to Neo4j query().
  4. IntelBrief({}) instantiation raising error instead of returning placeholder headline "Intelligence Brief".
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from services.agents.knowledge_graph_engine import (
    GraphTriple,
    IntelBrief,
    KnowledgeGraphEngine,
    VALID_PREDICATES,
)


def test_intel_brief_empty_dict_raises():
    """Verify that constructing IntelBrief from an empty dict raises an exception instead of returning a placeholder brief."""
    with pytest.raises((ValueError, ValidationError)):
        IntelBrief(**{})

    with pytest.raises((ValueError, ValidationError)):
        IntelBrief.model_validate({})


def test_merge_graph_triples_rejects_off_whitelist_predicate():
    """Verify _merge_graph_triples rejects triples with predicate not in VALID_PREDICATES (e.g., 'Source')."""
    mock_neo4j = AsyncMock()
    mock_neo4j.query = AsyncMock()

    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    invalid_triple = GraphTriple(
        subject="Report Alpha",
        predicate="Source",
        object="Reuters",
        confidence=0.9,
    )

    asyncio.run(engine._merge_graph_triples([invalid_triple]))

    mock_neo4j.query.assert_not_called()


def test_merge_graph_triples_rejects_self_loop():
    """Verify _merge_graph_triples rejects triples where subject == object (case-insensitive)."""
    mock_neo4j = AsyncMock()
    mock_neo4j.query = AsyncMock()

    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    self_loop_triple = GraphTriple(
        subject="US Navy",
        predicate="OPERATES_IN",
        object="us navy",
        confidence=0.9,
    )

    asyncio.run(engine._merge_graph_triples([self_loop_triple]))

    mock_neo4j.query.assert_not_called()


def test_merge_graph_triples_accepts_valid_triple():
    """Verify _merge_graph_triples executes Neo4j MERGE query for valid triples."""
    mock_neo4j = AsyncMock()
    mock_neo4j.query = AsyncMock()

    engine = KnowledgeGraphEngine.__new__(KnowledgeGraphEngine)
    engine.neo4j = mock_neo4j

    valid_triple = GraphTriple(
        subject="US Navy",
        predicate="LOCATED_IN",
        object="Red Sea",
        confidence=0.85,
    )

    asyncio.run(engine._merge_graph_triples([valid_triple]))

    mock_neo4j.query.assert_called_once()
    args, kwargs = mock_neo4j.query.call_args
    cypher_query = args[0]
    params = args[1]

    assert "MERGE (a:Entity {id: $subj})" in cypher_query
    assert "LOCATED_IN" in cypher_query
    assert params["subj"] == "US NAVY"
    assert params["obj"] == "RED SEA"
    assert params["conf"] == 0.85
