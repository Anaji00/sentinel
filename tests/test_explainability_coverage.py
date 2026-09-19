"""
tests/test_explainability_coverage.py

The explainability column, filled by one enricher of eight.

Measured over an hour on the live deployment:

    crypto   57,234 events   0 with a breakdown
    vessel    7,370          0
    flight    2,152          0
    market      660          0
    options     421          0
    filing       62          0
    equity      198        198

Migration 0024 added `anomaly_breakdown` precisely so /explain/event/{id} could
show how a score was arrived at instead of printing an invented waterfall. One
enricher filled it, so for 99.7% of the platform the endpoint still had nothing
to read -- the state that migration was written to end.

It stayed that way because a breakdown was treated as five sub-scores each
domain had to invent. Most of it is not: the composite score, whether it cleared
the bar, which domain scored it and what backed the number come from the shared
scorer for every domain already. They were computed and dropped.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_an_unmeasured_dimension_is_absent_not_zero():
    """A bar labelled "Spatial Dispersion 0.0" on an equity trade is a claim.

    /explain builds its waterfall from whichever dimensions are not None, so a
    0.0 default made every one of them look measured. An economic-calendar
    event sets only volatility and rendered four more dimensions at zero
    beside it.
    """
    from shared.models.events import AnomalyBreakdown

    b = AnomalyBreakdown(composite_score=0.8, volatility_z_score=1.2, domain="macro")
    assert b.volatility_z_score == 1.2
    assert b.spatial_score is None
    assert b.temporal_score is None
    assert b.volume_z_score is None
    assert b.cross_domain_correlation_score is None


def test_the_builder_fills_only_what_was_measured():
    from services.enrichment.anomaly_scorer import breakdown_from_score

    b = breakdown_from_score(
        {"score": 0.72, "is_significant": True, "domain": "crypto",
         "coverage": {"fraction": 0.9, "basis": "percentile"}},
        "crypto",
    )
    assert b.composite_score == 0.72
    assert b.is_significant is True
    assert b.domain == "crypto"
    assert b.coverage_fraction == 0.9
    assert b.coverage_basis == "percentile"
    # Nothing invented to fill a column.
    assert b.spatial_score is None
    assert b.volume_z_score is None


def test_a_kinematic_residual_becomes_a_spatial_score():
    """The one sub-score maritime and aviation genuinely have.

    How far the hull or airframe is from where its own Kalman filter predicted
    is a spatial anomaly in the sense the field means.
    """
    from services.enrichment.anomaly_scorer import breakdown_from_score

    near = breakdown_from_score({"score": 0.5, "residual_distance": 0.1}, "maritime")
    far = breakdown_from_score({"score": 0.5, "residual_distance": 20.0}, "maritime")
    assert near.spatial_score is not None and far.spatial_score is not None
    assert far.spatial_score > near.spatial_score
    assert 0.0 <= near.spatial_score <= 1.0 and 0.0 <= far.spatial_score <= 1.0


def test_a_scorer_that_returned_nothing_yields_no_breakdown():
    """Absent is absent. An empty breakdown would assert a measurement."""
    from services.enrichment.anomaly_scorer import breakdown_from_score

    assert breakdown_from_score(None, "crypto") is None
    assert breakdown_from_score("not a dict", "crypto") is None


def test_the_crypto_batch_scorer_keeps_what_it_computed():
    """It returned `[r["score"] for r in res]` on the busiest path here.

    The coverage, the significance flag and the domain existed for the length
    of that return statement and were discarded, which is why 57,234 events an
    hour reached the store with no breakdown.
    """
    src = (ROOT / "services" / "enrichment" / "anomaly_scorer.py").read_text(encoding="utf-8")
    fn = src.index("async def score_crypto_trade_batch")
    body = src[fn : fn + 2200]
    # Asserted on the code rather than the absence of a string: the comment
    # explaining this repair quotes the line it replaced, so "the old text is
    # gone" would fail on the explanation of why it is gone.
    lines = [l.strip() for l in body.splitlines() if l.strip().startswith("return ")]
    assert lines, "no return statement found in score_crypto_trade_batch"
    assert lines[-1] == "return await self.score_event_batch(\"crypto_trade\", entities, features_list)", (
        f"expected the full scorer result to be returned, found: {lines[-1]}"
    )


def test_the_high_volume_enrichers_attach_a_breakdown():
    base = ROOT / "services" / "enrichment" / "enrichers"
    for name, domain in (("crypto", "crypto"), ("maritime", "maritime"), ("aviation", "aviation")):
        src = (base / f"{name}.py").read_text(encoding="utf-8")
        assert "breakdown_from_score" in src, f"{name} attaches no breakdown"
        assert f'"{domain}"' in src


def test_explain_renders_only_measured_dimensions():
    """The endpoint already skipped None. The model is what defaulted to 0.0."""
    src = (ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(encoding="utf-8")
    assert "if value is None:" in src
    assert "continue" in src


# -- the derivation, for paths that have one but no detector -----------------


def test_a_lift_records_what_it_actually_moved():
    """The weight asked for and the amount applied are not the same number.

    `lift_score` raises a score by a share of the *headroom* above it, within a
    budget, so a second lift of the same weight moves less than the first. The
    delta is the honest quantity: it is what changed.
    """
    from services.enrichment.anomaly_scorer import lift_and_record

    trail = []
    a = lift_and_record(0.5, 0.15, 0.0, "suspect_counterparty", trail)
    b = lift_and_record(a, 0.15, 0.15, "watchlisted_wallet", trail)
    assert len(trail) == 2
    assert trail[0].reason == "suspect_counterparty"
    assert trail[1].delta < trail[0].delta, (
        "the second lift shares one headroom budget with the first"
    )
    assert b > a > 0.5


def test_a_step_that_moved_nothing_is_not_recorded():
    """A waterfall of zeroes describes the code, not the event."""
    from services.enrichment.anomaly_scorer import lift_and_record

    trail = []
    out = lift_and_record(0.5, 0.0, 0.0, "transfer_frequency", trail)
    assert out == 0.5
    assert trail == []


def test_a_ceiling_is_recorded_as_the_negative_step_it_is():
    """The cap is the step a reader most wants to see.

    "Why did a $354m transfer score 0.95 and not 1.0" is a question about
    exactly this line.
    """
    from services.enrichment.anomaly_scorer import record_cap

    trail = []
    out = record_cap(0.98, 0.95, "notional_score_ceiling", trail)
    assert out == 0.95
    assert len(trail) == 1
    assert trail[0].delta < 0


def test_the_cap_records_nothing_when_it_does_not_bite():
    from services.enrichment.anomaly_scorer import record_cap

    trail = []
    assert record_cap(0.40, 0.95, "notional_score_ceiling", trail) == 0.40
    assert trail == []


def test_the_transfer_trail_exists_on_both_branches():
    """The event is built after the if/else, so a trail defined in one arm
    is a NameError on the other -- and the arm it would have been missing from
    is the baseline path, which is most transfers.
    """
    src = (
        ROOT / "services" / "enrichment" / "enrichers" / "crypto.py"
    ).read_text(encoding="utf-8")
    init = src.index('trail = [ScoreAdjustment(reason="notional_size_score"')
    branch = src.index("if not is_whale and not (is_suspect and is_alertable):")
    assert init < branch, "the trail must be initialised before the branch"
    assert src.count("trail = [ScoreAdjustment(") == 1, (
        "one initialisation, not one per arm"
    )
    assert "score_adjustments=trail" in src
