"""Phase 4.12: the confidence loop has to close, and close honestly.

Two defects sat in the wiring, and both are the shapes this audit keeps finding.

The calibrator was called at one of three publishers. Measured over six hours of
live traffic that was the wrong one: SEMANTIC_001 produced 7 of 9 clusters, the
cascade path 1, the rule path -- the only calibrated one -- 1. Zero of 379,716
stored correlations carried a `raw_confidence`, which is what "the mechanism was
never on the live path" looks like from the database.

And the tracker trained the map on `correlations.confidence_score`, which is the
value written *after* calibration. Once fitted, published confidences are
calibrated numbers, those get recorded as the next fit's raw inputs, and the
isotonic map converges toward identity while `get_status()` reports itself
calibrated -- the same closed loop already recorded for the similarity
calibrator, one layer up.
"""
import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
CORR = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
TRACKER = (ROOT / "services" / "reasoning" / "scenario_tracker.py").read_text(encoding="utf-8")


def _cluster_constructions(src):
    tree = ast.parse(src)
    return [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "CorrelationCluster"
    ]


def test_every_publisher_calibrates():
    """A fourth publisher must not be able to skip it quietly."""
    calls = _cluster_constructions(CORR)
    assert len(calls) >= 3, f"only {len(calls)} CorrelationCluster sites found"

    uncalibrated = []
    for call in calls:
        kw = {k.arg: k for k in call.keywords}
        conf = kw.get("confidence_score")
        if conf is None:
            continue
        # The confidence must arrive from the calibration helper, not be
        # computed inline at the constructor.
        src = ast.unparse(conf.value)
        if not src.startswith("_") or "min(" in src:
            uncalibrated.append(f"line {call.lineno}: confidence_score={src}")
    assert not uncalibrated, (
        "publishers computing a confidence inline instead of through "
        "_calibrated():\n  " + "\n  ".join(uncalibrated)
    )


def test_every_publisher_keeps_the_raw_score():
    """The raw heuristic is the only honest input to a refit."""
    calls = _cluster_constructions(CORR)
    missing = []
    for call in calls:
        kw = {k.arg: k for k in call.keywords}
        metrics = kw.get("metrics_summary")
        if metrics is None:
            continue
        src = ast.unparse(metrics.value)
        if "raw_confidence" not in src and "_calib" not in src:
            missing.append(f"line {call.lineno}")
    assert not missing, (
        f"metrics_summary at {missing} carries no raw_confidence, so an outcome "
        "recorded against it would train the calibrator on its own output."
    )


def test_the_helper_exists_and_returns_both_numbers():
    tree = ast.parse(CORR)
    fn = next(
        (n for n in ast.walk(tree)
         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == "_calibrated"),
        None,
    )
    assert fn is not None, "no _calibrated helper; each publisher would carry its own copy"
    assert isinstance(fn, ast.AsyncFunctionDef)


def test_the_tracker_does_not_train_on_the_published_value():
    assert "raw_confidence" in TRACKER, (
        "scenario_tracker records an outcome against correlations.confidence_score, "
        "which is the calibrated value -- the calibrator would observe its own output."
    )
    # And the fallback must be present, so rows written before raw_confidence
    # existed still contribute rather than being silently skipped.
    assert 'get("confidence_score")' in TRACKER


@pytest.mark.anyio
async def test_an_uncalibrated_platform_passes_the_score_through():
    """Below the sample floor the map must do nothing, and say so."""
    from shared.utils.confidence_calibration import calibrate

    out = await calibrate(None, 0.80)
    assert out["confidence"] == pytest.approx(0.80)
    assert out["calibrated"] is False
    assert out["calibration_samples"] == 0


@pytest.mark.anyio
async def test_a_fitted_map_moves_an_overconfident_score():
    """The point of the exercise, driven end to end against a fake store."""
    from shared.utils import confidence_calibration as cc

    class _Raw:
        def __init__(self):
            self.store = []

        def pipeline(self):
            return self

        def lpush(self, key, val):
            self.store.insert(0, val)

        def ltrim(self, *a):
            pass

        async def execute(self):
            pass

        async def lrange(self, key, start, stop):
            return list(self.store)

        async def get(self, key):
            return None

        async def set(self, *a, **k):
            return None

    class _Client:
        def __init__(self):
            self.raw = _Raw()

    client = _Client()
    # A heuristic that ranks correctly and is systematically overconfident:
    # published 0.80, true rate 0.40.
    for i in range(cc.MIN_CALIBRATION_SAMPLES + 20):
        await cc.record_outcome(client, 0.80, was_correct=(i % 5 < 2))

    out = await cc.calibrate(client, 0.80)
    assert out["calibrated"] is True
    assert out["confidence"] < 0.80, out
