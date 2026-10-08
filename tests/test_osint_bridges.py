"""
tests/test_osint_bridges.py

A vessel, an aircraft or a place reaches an instrument.

13,539 vessels and 24,778 aircraft had edges only to a region and a flag, and no
region had an edge to anything that trades. These mappings tag OSINT events
with what they expose, and clusters carry those tags into reasoning.
"""

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.ontology import is_valid_predicate
from shared.utils.instrument_reach import exposure_tags, listed_instruments
from shared.utils.osint_bridges import (
    region_exposure_edges, region_exposure_tags, vessel_exposure_tags,
)


def test_a_tanker_at_a_chokepoint_exposes_crude():
    assert vessel_exposure_tags("Strait of Hormuz", "Tanker") == ["exposes:BZ=F", "exposes:CL=F"]
    assert "exposes:CL=F" in vessel_exposure_tags("Singapore Approach", "Tanker")


def test_cargo_in_the_black_sea_exposes_grain():
    assert vessel_exposure_tags("Black Sea", "CargoVessel") == ["exposes:ZC=F", "exposes:ZW=F"]


def test_a_fishing_boat_in_open_water_exposes_nothing():
    assert vessel_exposure_tags("Singapore Approach", "Fishing") == []
    assert vessel_exposure_tags(None, "Tanker") == []


def test_taiwan_exposes_semiconductors_whatever_is_observed():
    assert region_exposure_tags("Taiwan Strait") == ["exposes:SOXX", "exposes:TSM"]
    assert vessel_exposure_tags("Taiwan Territorial", "Fishing") == ["exposes:SOXX", "exposes:TSM"]


def test_cluster_tags_carry_their_events_exposures():
    merged = exposure_tags(["tanker", "exposes:cl=f"], ["exposes:CL=F", "exposes:BZ=F"], None)
    assert merged == ["exposes:BZ=F", "exposes:CL=F"]
    cluster = SimpleNamespace(tags=merged, entity_ids=[], entity_names=[],
                              primary_entity_id=None, primary_entity_name=None)
    assert listed_instruments(cluster) == {"CL=F", "BZ=F"}


def test_region_edges_use_canonical_predicates_and_existing_symbols():
    edges = region_exposure_edges()
    assert ("Strait of Hormuz", "CL=F", "Commodity") in edges
    assert ("Taiwan Strait", "TSM", "Company") in edges
    assert is_valid_predicate("COMMODITY_EXPOSURE") and is_valid_predicate("HAS_EXPOSURE_IN")


def test_every_cluster_site_propagates_exposures():
    cascade = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    main = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert '"exposes": exposure_tags(' in cascade
    assert main.count("exposure_tags(") >= 3


def test_the_enrichers_attach_the_bridges():
    for rel, call in (
        ("services/enrichment/enrichers/maritime.py", "vessel_exposure_tags(region, vtype)"),
        ("services/enrichment/gap_detector.py", "vessel_exposure_tags(reg, vtype)"),
    ):
        assert call in (ROOT / rel).read_text(encoding="utf-8"), rel


def test_aircraft_reach_a_stock_through_their_operator_not_the_region():
    """Region exposure on aircraft made 109 single-flight surges a day look like
    crude and grain signals: an airliner over the Gulf says nothing about oil."""
    for rel in ("services/enrichment/enrichers/aviation.py",):
        src = (ROOT / rel).read_text(encoding="utf-8")
        assert "region_exposure_tags(" not in src, rel
        assert "operator_ticker(" in src, rel
