"""Anomaly scores fitted to what followed them."""

import pathlib
import random
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import score_calibration as scal  # noqa: E402


def test_pav_returns_the_closest_non_decreasing_fit() -> None:
    assert scal.pav([0.1, 0.3, 0.2, 0.4], [1, 1, 1, 1]) == [0.1, 0.25, 0.25, 0.4]
    assert scal.pav([0.5, 0.1], [3, 1]) == [0.4, 0.4]


def test_an_informative_score_lifts_the_big_move_rate_and_a_flat_one_does_not() -> None:
    rng = random.Random(4)
    good = [(s, 3.0 if rng.random() < 0.05 + 0.4 * s else 0.8) for s in (rng.random() for _ in range(2000))]
    flat = [(s, 3.0 if rng.random() < 0.15 else 0.8) for s in (rng.random() for _ in range(2000))]
    g, f = scal.fit(good), scal.fit(flat)
    assert g["informative"] and g["lift_top"] > 1.4 and g["p"] == sorted(g["p"])
    assert not f["informative"]
    p_lo, _ = scal.calibrate(g, 0.05)
    p_hi, lift_hi = scal.calibrate(g, 0.95)
    assert p_hi > p_lo and lift_hi > 1.5
    assert scal.capped(0.9, f) == scal.UNINFORMATIVE_CAP and scal.capped(0.9, g) == 0.9
    assert scal.fit(good[:50]) is None


def test_enrichment_writes_the_probability_and_caps_an_uninformative_producer() -> None:
    from services.enrichment import main as em
    from shared.models import NormalizedEvent

    flat = scal.fit([(i / 1000, 3.0 if i % 7 == 0 else 0.8) for i in range(1000)])
    ev = NormalizedEvent.model_validate({"type": "equity_block", "source": "finnhub_equities",
                                         "primary_entity": {"id": "AAPL", "type": "instrument", "name": "AAPL"},
                                         "occurred_at": datetime(2026, 10, 15, tzinfo=timezone.utc),
                                         "anomaly_score": 0.9})
    em._apply_calibration(ev, {"equity_block/finnhub_equities": flat})
    assert ev.anomaly_score == scal.UNINFORMATIVE_CAP
    assert ev.anomaly_breakdown.calibrated_p_big_move is not None
    assert ev.score_adjustments[-1].reason == "uninformative_producer_cap"
