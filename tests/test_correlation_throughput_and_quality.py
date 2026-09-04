"""
tests/test_correlation_throughput_and_quality.py

The correlation engine was a day and a half behind, and saying so confidently.

Two measured problems, one cause between them -- nothing bounded how late an
observation could be and still be treated as news:

  * Throughput. SentinelConsumer defaults to 15 records a poll and the loop
    spends about a second a cycle, so ~15 events/second was a hard ceiling. It
    consumed 12.75/s against 12/s of production: a net drain of 0.75/s against a
    357,000-message backlog, which is five and a half days. Every alert it fired
    during the audit described market state from the previous day.

  * Evidence. A container ship "semantically converged" with a flight alert
    because both headlines named the Strait of Malacca -- the encoder matching a
    place name in a templated sentence. The alert then reported "matched 10
    highly similar cross-domain events" while storing and scoring three, and
    scored every such match at exactly 1.00 confidence.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402


def _source() -> str:
    return (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")


def _is_stale():
    """Loads the guard without importing the module's service dependencies."""
    import importlib.util

    os.environ.setdefault("SENTINEL_ENV", "test")
    spec = importlib.util.spec_from_file_location(
        "_correlation_main", ROOT / "services" / "correlation" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._is_stale


class _Event:
    def __init__(self, occurred_at):
        self.occurred_at = occurred_at


# -- staleness ----------------------------------------------------------------

def test_a_current_event_is_correlated():
    assert _is_stale()(_Event(datetime.now(timezone.utc))) is False


def test_an_event_from_yesterday_is_not_correlated():
    """The measured failure: alerts describing state 32 hours old."""
    stale = datetime.now(timezone.utc) - timedelta(hours=32)
    assert _is_stale()(_Event(stale)) is True


def test_a_naive_timestamp_is_read_as_utc():
    """Producers are inconsistent about tzinfo; a missing offset is not licence
    to treat a day-old event as current."""
    stale = datetime.utcnow() - timedelta(hours=32)
    assert _is_stale()(_Event(stale)) is True


def test_an_event_with_no_timestamp_is_treated_as_current():
    """Fail toward correlating.

    Refusing an event because its clock is missing would silently drop a whole
    producer's output, which is a worse failure than correlating one event that
    turns out to be old.
    """
    assert _is_stale()(_Event(None)) is False


def test_a_malformed_timestamp_does_not_raise():
    for junk in ("yesterday", 0, [], {}):
        assert _is_stale()(_Event(junk)) is False


def test_the_guard_skips_rather_than_seeks():
    """Non-destructive by construction.

    The event is still consumed, still committed and still counted -- it just
    produces no correlation. Seeking past the backlog instead would move the
    offset over messages nothing had examined.
    """
    source = _source()
    assert "if _is_stale(event):" in source
    # Actual seek calls, not the word in prose -- the comment above the guard
    # explains why it does not seek, and an earlier version of this test failed
    # on its own explanation.
    for call in ("consumer.seek", ".seek_to_end", ".seek(", "seek_to_beginning"):
        assert call not in source, f"the engine moves offsets with {call}"


# -- throughput ---------------------------------------------------------------

def test_the_consumer_raises_its_poll_ceiling():
    """15 records a poll is ~15 events/second, below production."""
    source = _source()
    start = source.index('group_id="correlation-engine"')
    # Bounded by the next statement rather than the next ")", which lands inside
    # a comment in the same block.
    block = source[start : source.index("processed = 0", start)]
    assert "max_poll_records=" in block, "the correlation consumer is on the default of 15"


def test_the_poll_ceiling_is_configurable():
    assert "CORRELATION_MAX_POLL_RECORDS" in _source()


# -- evidence honesty ---------------------------------------------------------

def test_the_headline_counts_what_was_kept_not_what_was_considered():
    """"matched 10 highly similar cross-domain events" while storing three.

    A reader who went looking for the other seven would not find them, because
    they were never recorded.
    """
    source = _source()
    assert "across {len(similar_events)} cross-domain events" not in source
    # The headline must count what was kept. It now also states the domain
    # count as its own number rather than folding "cross-domain" into the
    # phrase, which is a stronger version of this test's point: the earlier
    # wording asserted cross-domain-ness of the subjects, where the code now
    # reports how many domains there actually were.
    assert "{distinct_subjects} subject(s) in " in source
    assert "{semantic_domain_count} domain(s)" in source


def test_the_stored_description_states_kept_and_considered_separately():
    """The description is the field a reader gets back from the table.

    Fixing only summary_headline left the claim intact in the one place anyone
    would actually read it.
    """
    source = _source()
    assert "matched {len(similar_events)} highly similar" not in source
    assert "{len(supporting_ids)} retained event(s)" in source
    assert "candidate(s) considered" in source


def test_the_description_does_not_call_resemblance_a_relationship():
    """A vessel and an aircraft naming the same strait score highly and mean
    nothing by it."""
    source = _source()
    assert "Textual similarity" in source
    assert "Anomalous semantic convergence detected." not in source


def test_candidates_and_kept_evidence_are_reported_separately():
    source = _source()
    assert '"supporting_event_count": len(supporting_ids)' in source
    assert '"candidates_considered": len(similar_events)' in source


def test_subjects_are_counted_not_headlines():
    """Three headlines about one vessel are one observation seen three times."""
    source = _source()
    assert "distinct_subjects = len({" in source


# -- confidence ---------------------------------------------------------------

@pytest.mark.parametrize(
    "subjects,expected",
    [(1, 0.50), (2, 0.65), (3, 0.80)],
)
def test_confidence_separates_one_match_from_three(subjects, expected):
    """The old formula was 0.70 + 0.1 * n with n capped at 3 -- so every
    semantic correlation that fired scored exactly 1.00 and ranked nothing."""
    assert round(min(0.95, 0.35 + 0.15 * subjects), 2) == expected


def test_a_semantic_match_never_reads_as_certainty():
    """It is an embedding's opinion that two sentences resemble each other."""
    for subjects in range(1, 20):
        assert min(0.95, 0.35 + 0.15 * subjects) <= 0.95


def test_the_old_saturating_formula_is_gone():
    assert "0.70 + (0.1 * len(supporting_ids))" not in _source()
