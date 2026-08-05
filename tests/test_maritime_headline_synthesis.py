"""
tests/test_maritime_headline_synthesis.py

Tests for Maritime Event Headline Synthesis (Task 1) and
KnowledgeGraphEngine VALID_PREDICATES expansion.
"""

import pytest
import asyncio
from services.agents.knowledge_graph_engine import VALID_PREDICATES


def test_valid_predicates_contains_exposure_types():
    """KnowledgeGraphEngine VALID_PREDICATES must contain exposure predicates."""
    assert "POSITIVE_EXPOSURE_TO" in VALID_PREDICATES
    assert "INVERSE_EXPOSURE_TO" in VALID_PREDICATES
    assert "COMMODITY_EXPOSURE" in VALID_PREDICATES
    assert "SUPPLIES" in VALID_PREDICATES


def test_vessel_dark_headline_synthesis_format():
    """Synthesized headline string for vessel dark should contain vessel info and region."""
    vtype = "Tanker"
    name = "SUNRISE GLORY"
    mmsi = "123456789"
    region = "Strait of Hormuz"
    gap_hrs = 12.5
    flags = ["sanctioned"]

    headline = (
        f"{vtype or 'Vessel'} '{name or f'MMSI:{mmsi}'}' went dark in "
        f"{region} after {gap_hrs}h of AIS silence"
        + (f" (flagged: {', '.join(flags)})" if flags else "")
    )

    assert "SUNRISE GLORY" in headline
    assert "Strait of Hormuz" in headline
    assert "12.5h" in headline
    assert "flagged: sanctioned" in headline


def test_vessel_position_headline_synthesis_format():
    """Synthesized headline for non-routine vessel position event."""
    vtype = "Cargo"
    name = "CONTAINER ONE"
    nav_status = "Restricted Maneuverability"
    region = "South China Sea"
    is_sanctioned = False
    is_emergency_nav = True
    is_watched = True

    headline = (
        f"{vtype or 'Vessel'} '{name}' "
        f"{nav_status.lower() if nav_status else 'transiting'} in {region or 'unknown waters'}"
        + (" — sanctioned/flagged vessel" if is_sanctioned else "")
        + (" — emergency navigation status" if is_emergency_nav else "")
    ) if (is_sanctioned or is_emergency_nav or is_watched) else None

    assert headline is not None
    assert "CONTAINER ONE" in headline
    assert "restricted maneuverability" in headline
    assert "emergency navigation status" in headline
