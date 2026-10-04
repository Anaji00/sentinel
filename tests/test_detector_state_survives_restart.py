"""
tests/test_detector_state_survives_restart.py

Every deploy started the anomaly detectors cold.

They live in memory, so a restart emptied the forest, the shingle buffer and the
percentile history. Measured on 1 October: twenty minutes after a restart a
$1.31M QQQ block trade scored 0.000, marked unmeasured, during a session that
saw three deploys. The detectors now keep their recent raw points; the
enrichment service saves them every five minutes and on shutdown, and replays
them before it starts consuming.
"""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.streaming_detectors import RRCFDetector


def _stream(n, seed=7):
    rng = np.random.default_rng(seed)
    return [rng.normal(0, 1, 4) for _ in range(n)]


def test_a_cold_detector_cannot_score():
    """The defect: a fresh shingled detector returns 0.0 with no coverage."""
    det = RRCFDetector(num_trees=5, window_size=32, shingle_size=3, name="tradfi")
    assert det.insert(np.ones(4)) == 0.0
    assert det.coverage()["fraction"] == 0.0


def test_a_warmed_detector_scores_its_first_live_point():
    original = RRCFDetector(num_trees=5, window_size=32, shingle_size=3, name="tradfi")
    for p in _stream(60):
        original.insert(p)

    restarted = RRCFDetector(num_trees=5, window_size=32, shingle_size=3, name="tradfi")
    replayed = restarted.warm(json.loads(json.dumps(original.export_points())))

    assert replayed == 32 + 3
    spike = np.array([25.0, -25.0, 25.0, -25.0])
    assert restarted.insert(spike) > 0.0
    assert restarted.coverage()["fraction"] > 0.0


def test_only_the_last_window_is_kept():
    det = RRCFDetector(num_trees=3, window_size=16, shingle_size=2, name="x")
    for p in _stream(100):
        det.insert(p)
    assert len(det.export_points()) == 18


def test_a_point_of_a_different_width_stops_the_replay():
    """A feature set changed between deploys must not corrupt the forest."""
    det = RRCFDetector(num_trees=3, window_size=16, name="x")
    points = [[1.0, 2.0, 3.0]] * 5 + [[1.0, 2.0]] + [[1.0, 2.0, 3.0]] * 5
    assert det.warm(points) == 5


def test_non_finite_and_malformed_points_are_skipped():
    det = RRCFDetector(num_trees=3, window_size=16, name="x")
    assert det.warm([[1.0, float("nan")], [[1.0]], [1.0, 2.0], None and [], [3.0, 4.0]]) == 2
    assert det.warm(None) == 0


def _scorer_with_redis(stored=None):
    from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
    scorer = DynamicAnomalyScorer.__new__(DynamicAnomalyScorer)
    scorer._rrcf_detectors = {
        "tradfi": RRCFDetector(num_trees=3, window_size=16, shingle_size=3, name="tradfi"),
    }
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=pipe)
    ctx.__aexit__ = AsyncMock(return_value=False)
    redis = MagicMock()
    redis.raw.pipeline = MagicMock(return_value=ctx)
    redis.raw.hgetall = AsyncMock(return_value=stored or {})
    scorer.redis = redis
    scorer._detector_for = MagicMock(return_value=None)
    return scorer, pipe


def test_the_scorer_saves_every_detector_that_has_seen_data():
    scorer, pipe = _scorer_with_redis()
    for p in _stream(10):
        scorer._rrcf_detectors["tradfi"].insert(p)
    assert asyncio.run(scorer.save_detector_state()) == 1
    mapping = pipe.hset.call_args.kwargs["mapping"]
    assert len(json.loads(mapping["tradfi"])) == 10
    pipe.expire.assert_called_once()


def test_the_scorer_restores_by_name_and_skips_what_it_cannot_map():
    points = json.dumps([list(p) for p in _stream(20)])
    scorer, _ = _scorer_with_redis({b"tradfi": points.encode(), b"gone_type": points.encode()})
    restored = asyncio.run(scorer.restore_detector_state())
    assert restored == {"tradfi": 20}
    assert "gone_type" not in restored


def test_restore_runs_before_the_consumer_starts():
    src = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert src.index("await scorer.restore_detector_state()") < src.index("await consumer.start()")
    assert "await scorer.save_detector_state()" in src.split("finally:")[-1]
