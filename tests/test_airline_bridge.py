"""
tests/test_airline_bridge.py

An aircraft reaches a stock through its operator.

24,778 aircraft in the graph had edges only to a region and a flag. The ICAO
designator that opens an airline callsign names the operator, so DAL1234 reaches
DAL and FDX5701 reaches FDX -- with no new data source.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.ontology import is_valid_predicate
from shared.utils.airlines import AIRLINE_TICKERS, operator_designator, operator_ticker


def test_mainline_cargo_and_regional_callsigns_map_to_their_listed_operator():
    assert operator_ticker("DAL1234") == "DAL"
    assert operator_ticker("SWA2345") == "LUV"
    assert operator_ticker("FDX5701") == "FDX"
    assert operator_ticker("EDV4821") == "DAL"
    assert operator_ticker(" ual9 ") == "UAL"


def test_private_and_unlisted_flights_map_to_nothing():
    for cs in ("N123AB", "GABCD", "", None, "DAL", "XYZ123", "BAW9X"):
        assert operator_ticker(cs) is None, cs


def test_a_registration_is_not_a_designator():
    assert operator_designator("N123AB") is None
    assert operator_designator("GABCD") is None
    assert operator_designator("BAW9X") == "BAW"


def test_every_mapped_ticker_is_ticker_shaped():
    for code, ticker in AIRLINE_TICKERS.items():
        assert len(code) == 3 and code.isalpha(), code
        assert 1 <= len(ticker) <= 5 and ticker.isalpha(), ticker


def test_the_link_predicate_is_canonical():
    assert is_valid_predicate("OPERATED_BY")


def test_the_enricher_tags_and_links_the_operator():
    av = (ROOT / "services" / "enrichment" / "enrichers" / "aviation.py").read_text(encoding="utf-8")
    assert 'f"exposes:{operator}"' in av and '"operator_ticker": operator' in av
    gw = (ROOT / "services" / "enrichment" / "graph_writer.py").read_text(encoding="utf-8")
    assert 'icao24, "OPERATED_BY", operator' in gw
