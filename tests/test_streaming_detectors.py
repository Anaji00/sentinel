"""
tests/test_streaming_detectors.py

Unit tests for shared/utils/streaming_detectors.py.
Validates RRCF, KalmanResidualFilter, HawkesIntensityTracker,
FirstStoryDetector, BGPGraphFeatureExtractor, and STS zone multipliers.
"""

import math
import pytest
import numpy as np
from unittest.mock import AsyncMock, MagicMock

from shared.utils.streaming_detectors import (
    RRCFDetector,
    KalmanResidualFilter,
    HawkesIntensityTracker,
    FirstStoryDetector,
    BGPGraphFeatureExtractor,
    sts_zone_risk_multiplier,
    STS_TRANSFER_ZONES,
)


def test_rrcf_detector_basic():
    detector = RRCFDetector(num_trees=10, window_size=50)

    # Insert normal points
    normal_points = [np.array([1.0, 2.0, 3.0]) + np.random.normal(0, 0.1, 3) for _ in range(30)]
    scores = detector.insert_batch(normal_points)
    assert len(scores) == 30
    assert all(0.0 <= s <= 1.0 for s in scores)

    # Insert an extreme outlier
    outlier = np.array([100.0, 200.0, 300.0])
    outlier_score = detector.insert(outlier)
    assert 0.0 <= outlier_score <= 1.0


def test_rrcf_fallback_without_rrcf():
    import shared.utils.streaming_detectors as sd

    orig_has_rrcf = sd.HAS_RRCF
    sd.HAS_RRCF = False
    try:
        detector = RRCFDetector(num_trees=5, window_size=20)
        # Test fallback insert
        s1 = detector.insert(np.array([1.0, 2.0]))
        s2 = detector.insert(np.array([1.1, 2.1]))
        s3 = detector.insert(np.array([50.0, 50.0]))
        assert 0.0 <= s1 <= 1.0
        assert 0.0 <= s2 <= 1.0
        assert s3 > s2  # Outlier should score higher than normal
    finally:
        sd.HAS_RRCF = orig_has_rrcf


def test_kalman_residual_filter():
    kf = KalmanResidualFilter()

    # Step 1: Initial position
    res1 = kf.predict_and_update(
        lat=25.0, lon=55.0, speed=15.0, heading=90, timestamp=1000.0
    )
    assert res1["residual_distance"] == 0.0
    assert res1["prediction_confidence"] == 1.0

    # Step 2: Normal 15-min movement heading East (90 deg) at 15 kts
    # In 15 min (0.25h), vessel travels 3.75 nm East (~0.06 deg lon)
    res2 = kf.predict_and_update(
        lat=25.0, lon=55.0625, speed=15.0, heading=90, timestamp=1900.0
    )
    assert res2["residual_distance"] < 1.0  # Low residual for expected movement

    # Step 3: Impossible jump 50nm North in 1 minute (AIS spoofing scenario)
    res3 = kf.predict_and_update(
        lat=25.83, lon=55.0625, speed=15.0, heading=90, timestamp=1960.0
    )
    assert res3["residual_distance"] > 40.0  # Large residual distance
    assert res3["prediction_confidence"] < 0.2  # Low confidence


def test_hawkes_intensity_tracker():
    tracker = HawkesIntensityTracker()

    # Initial baseline
    init_ratio = tracker.get_excitation_ratio("tradfi", timestamp=100.0)
    assert abs(init_ratio - 1.0) < 1e-4

    # Record rapid crypto liquidations at t=100.0, 101.0, 102.0
    tracker.record_event("crypto", timestamp=100.0)
    tracker.record_event("crypto", timestamp=101.0)
    state = tracker.record_event("crypto", timestamp=102.0)

    assert state["excitation_ratio"] > 1.0

    # Check cross-domain excitation on tradfi
    tradfi_ratio = tracker.get_excitation_ratio("tradfi", timestamp=102.5)
    assert tradfi_ratio > 1.0  # TradFi intensity excited by crypto liquidations!


def test_first_story_detector():
    fsd = FirstStoryDetector(window_size=100)

    # Seed with initial background stories to populate window (>= 3 stories)
    fsd.score_novelty("Tech companies announce earnings report dates for Q3")
    fsd.score_novelty("Global shipping rates stabilize after Red Sea disruption")
    fsd.score_novelty("European Central Bank maintains key interest rates unchanged")

    # Story 1: Completely novel story
    s1 = "OPEC announces unexpected crude oil production cut"
    n1 = fsd.score_novelty(s1)
    assert n1 >= 0.6  # High novelty for first story on OPEC cut

    # Story 2: Slightly different wording, same topic
    s2 = "OPEC crude oil production cuts announced today in Vienna"
    n2 = fsd.score_novelty(s2)
    assert n2 < n1  # Lower novelty because OPEC cuts story was just seen

    # Story 3: Completely different domain/topic
    s3 = "Federal Reserve signals potential rate cut at next FOMC meeting"
    n3 = fsd.score_novelty(s3)
    assert n3 > n2  # Higher novelty for new topic


def test_bgp_graph_feature_extractor():
    extractor = BGPGraphFeatureExtractor(neo4j_client=None)

    # Specificity check
    assert extractor._prefix_specificity("1.2.3.0/24") == 1.0
    assert extractor._prefix_specificity("10.0.0.0/8") == 0.3333333333333333

    # Feature extraction with no Neo4j client (fallback defaults)
    async def run_test():
        feats = await extractor.extract_features("AS64500", "1.2.3.0/24")
        assert feats["prefix_specificity"] == 1.0
        assert feats["path_novelty"] == 1.0

    import asyncio
    asyncio.run(run_test())


def test_sts_zone_risk_multiplier():
    assert sts_zone_risk_multiplier("Strait of Hormuz") == 3.0
    assert sts_zone_risk_multiplier("Lamu Anchorage") == 2.5
    assert sts_zone_risk_multiplier("Mid Atlantic") == 1.0
    assert sts_zone_risk_multiplier(None) == 1.0
