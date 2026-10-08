"""A hull that sails out of AIS coverage has not gone dark.

AIS here is the AISStream subscription boxes and nothing else. On 4 October,
336 of 656 vessels marked dark with a known speed were making 5 knots or more at
their last report, most in the Singapore and Taiwan boxes -- they had left the
boxes, not switched their transponders off.
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.maritime_behaviour import (  # noqa: E402
    AIS_COVERAGE_BOXES, could_have_left_coverage, nm_to_coverage_edge,
)


def test_distance_to_the_edge_of_the_malacca_box():
    # Singapore anchorage, 1.27N: the box's southern edge is 1.0N, 16 nm away.
    d = nm_to_coverage_edge(1.27, 103.88)
    assert d is not None and 15.5 < d < 16.5


def test_outside_every_box_is_none():
    assert nm_to_coverage_edge(50.0, -30.0) is None


def test_a_ship_under_way_near_an_edge_has_left_not_gone_dark():
    assert could_have_left_coverage(1.27, 103.88, 12.0, 2.0)


def test_a_stopped_ship_that_falls_silent_is_still_dark():
    assert not could_have_left_coverage(1.27, 103.88, 0.2, 6.0)


def test_a_slow_ship_deep_inside_coverage_is_still_dark():
    # Mid Black Sea, about 3 degrees from every edge; 3 knots for 2 hours is 6 nm.
    assert not could_have_left_coverage(43.5, 34.5, 3.0, 2.0)


def test_unknown_position_is_given_the_benefit_of_the_doubt():
    assert could_have_left_coverage(None, None, 0.0, 10.0)


def test_the_collector_subscribes_to_the_same_boxes():
    src = (ROOT / "services/collector-ais/main.py").read_text(encoding="utf-8")
    assert "WATCH_ZONES = AIS_COVERAGE_BOXES" in src
    assert len(AIS_COVERAGE_BOXES) == 9


def test_the_detector_applies_the_test_before_alerting():
    src = (ROOT / "services/enrichment/gap_detector.py").read_text(encoding="utf-8")
    gate = src.index("if could_have_left_coverage(")
    assert gate < src.index("anomalous_mmsis.append(mmsi)")
