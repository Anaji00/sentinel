"""Covers the OSINT primitive the platform was missing.

Source reliability was already tracked historically (SourceScorecard, Brier
scored against confirmed scenarios). What nothing asked was whether a given
claim is independently corroborated *now* -- the first question an analyst asks.

The distinction that makes this worth having: four outlets running byte-identical
wire copy are one source repeated, not four confirmations. Counting mentions
without testing independence would make a single source look like consensus,
which is worse than not measuring at all.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.corroboration import (  # noqa: E402
    CorroborationTracker,
    SYNDICATION_THRESHOLD,
    similarity,
    tokenize,
)

CLAIM = "Semiconductor export controls tightened on advanced chips to China"
SAME_STORY_REWORDED = "China hit with tighter export controls covering advanced semiconductor chips"
UNRELATED = "Container vessel grounded in the Suez Canal after engine failure"


def test_a_first_report_is_single_sourced():
    t = CorroborationTracker()
    a = t.observe(CLAIM, source="Reuters", reliability=0.9, now=1000)
    assert a.independent_sources == 1
    assert a.is_single_sourced is True
    assert a.corroboration_score == 0.0, "one source is a lead, not a confirmation"


def test_a_second_independent_source_corroborates():
    t = CorroborationTracker()
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=1000)
    a = t.observe(SAME_STORY_REWORDED, source="Bloomberg", reliability=0.9, now=1600)
    assert a.independent_sources == 2
    assert a.is_single_sourced is False
    assert a.corroboration_score > 0.0


def test_syndicated_copy_is_not_corroboration():
    """The failure mode that makes mention-counting misleading."""
    t = CorroborationTracker()
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=1000)
    for outlet in ("YahooFinance", "MSN", "Nasdaq.com"):
        a = t.observe(CLAIM, source=outlet, reliability=0.5, now=1100)
    assert a.independent_sources == 1, "identical wire copy counted as independent"
    assert a.is_single_sourced is True
    assert a.is_syndicated is True


def test_the_same_source_repeating_itself_adds_nothing():
    t = CorroborationTracker()
    for i in range(5):
        a = t.observe(CLAIM, source="Reuters", reliability=0.9, now=1000 + i)
    assert a.independent_sources == 1
    assert a.total_reports == 5


def test_unrelated_claims_do_not_corroborate_each_other():
    t = CorroborationTracker()
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=1000)
    a = t.observe(UNRELATED, source="Bloomberg", reliability=0.9, now=1010)
    assert a.independent_sources == 1, "two different stories were merged"


def test_reliable_corroborators_count_for_more():
    """Confirmation by outlets with a poor record should move the needle less."""
    strong = CorroborationTracker()
    strong.observe(CLAIM, source="Reuters", reliability=0.95, now=0)
    s = strong.observe(SAME_STORY_REWORDED, source="Bloomberg", reliability=0.95, now=60)

    weak = CorroborationTracker()
    weak.observe(CLAIM, source="BlogA", reliability=0.15, now=0)
    w = weak.observe(SAME_STORY_REWORDED, source="BlogB", reliability=0.15, now=60)

    assert s.corroboration_score > w.corroboration_score


def test_time_to_corroboration_is_reported():
    t = CorroborationTracker()
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=0)
    a = t.observe(SAME_STORY_REWORDED, source="Bloomberg", reliability=0.9, now=1800)
    assert a.minutes_to_corroboration == pytest.approx(30.0, abs=0.1)


def test_claims_expire_from_the_window():
    """A later story reusing the same words is not retrospective confirmation."""
    t = CorroborationTracker(window_sec=3600)
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=0)
    a = t.observe(SAME_STORY_REWORDED, source="Bloomberg", reliability=0.9, now=7200)
    assert a.independent_sources == 1


def test_tracked_claims_stay_bounded():
    t = CorroborationTracker(max_claims=50)
    for i in range(500):
        t.observe(f"Distinct claim number {i} about entity {i} and topic {i}",
                  source=f"src{i}", now=1000 + i)
    assert t.tracked_claims <= 50


def test_empty_text_is_handled_without_inventing_support():
    t = CorroborationTracker()
    a = t.observe("", source="Reuters", now=0)
    assert a.independent_sources == 0 and a.corroboration_score == 0.0


def test_similarity_bounds():
    a, b = tokenize(CLAIM), tokenize(CLAIM)
    assert similarity(a, b) == 1.0
    assert similarity(a, tokenize(UNRELATED)) < 0.2
    assert similarity(frozenset(), a) == 0.0


def test_syndication_threshold_is_stricter_than_claim_matching():
    """Otherwise every same-claim report would be dismissed as syndication."""
    from shared.utils.corroboration import SAME_CLAIM_THRESHOLD
    assert SYNDICATION_THRESHOLD > SAME_CLAIM_THRESHOLD


def test_the_assessment_serialises_for_persistence():
    t = CorroborationTracker()
    t.observe(CLAIM, source="Reuters", reliability=0.9, now=0)
    d = t.observe(SAME_STORY_REWORDED, source="Bloomberg", reliability=0.9, now=60).to_dict()
    assert set(d) == {
        "independent_sources", "total_reports", "corroboration_score",
        "is_single_sourced", "is_syndicated", "minutes_to_corroboration",
        "contributing_sources",
    }
    assert isinstance(d["corroboration_score"], float)


# ── how the signal is used downstream ────────────────────────────────────────

def _event(corroboration=None):
    from datetime import datetime, timezone
    from shared.models.events import Entity, EntityType, EventType, NormalizedEvent
    return NormalizedEvent(
        event_id="e1", trace_id="t1", type=EventType.HEADLINE,
        occurred_at=datetime.now(timezone.utc), source="Reuters",
        primary_entity=Entity(id="NVDA", type=EntityType.COMPANY, name="NVIDIA"),
        headline="Export controls tightened", corroboration=corroboration,
    )


def test_the_event_model_carries_the_assessment():
    e = _event({"is_single_sourced": False, "corroboration_score": 0.8})
    assert e.corroboration["corroboration_score"] == 0.8


def test_a_single_sourced_correlation_is_discounted_not_discarded():
    """A lead is still worth surfacing; it just must not read as confirmed."""
    from services.correlation.main import _corroboration_weight
    w = _corroboration_weight(_event({"is_single_sourced": True, "corroboration_score": 0.0}))
    assert 0.0 < w < 1.0


def test_a_corroborated_correlation_is_weighted_up():
    from services.correlation.main import _corroboration_weight
    assert _corroboration_weight(
        _event({"is_single_sourced": False, "corroboration_score": 0.9})
    ) > 1.0


def test_events_that_cannot_be_corroborated_are_left_alone():
    """Market ticks and position fixes have no notion of a second source and
    must not be penalised for lacking a field that does not apply."""
    from services.correlation.main import _corroboration_weight
    assert _corroboration_weight(_event(None)) == 1.0


def test_a_malformed_assessment_does_not_alter_confidence():
    from services.correlation.main import _corroboration_weight

    assert _corroboration_weight(_event({"corroboration_score": "high"})) == 1.0

    class NotAnAssessment:
        corroboration = "single-sourced, probably"

    assert _corroboration_weight(NotAnAssessment()) == 1.0


def test_the_assessment_survives_the_database_round_trip():
    """It is written as JSON, so it must serialise from the model as JSON."""
    import json
    from services.enrichment.db_writer import DBWriter
    row = DBWriter._extract_tuple(DBWriter.__new__(DBWriter),
                                  _event({"is_single_sourced": True, "corroboration_score": 0.0}))
    payload = next(
        (v for v in row if isinstance(v, dict) and "is_single_sourced" in v), None
    )
    assert payload is not None, "corroboration is not included in the persisted row"
    assert payload["is_single_sourced"] is True
