"""
tests/test_freshness_guard.py

Every stage that answers "what is happening now" must refuse history.

The correlation engine got this guard first, because that is where the symptom
was measured: 357,000 messages and 32 hours behind, firing cascades about the
previous day. The fix was applied in one place and the others were left, which
an overnight laptop suspend then exposed -- containers stopped, the wall clock
did not, and on resume the reasoning service held 26,405 correlations and the
radar orchestrator roughly 44,000 events, all of them from before the gap.

An interruption is not exceptional. A deploy, a crash, a consumer-group
rebalance and a sleeping laptop all produce the same thing: a consumer resuming
at its committed offset with a backlog in front of it. Without a bound on age,
each stage works forward through that backlog spending its scarcest resource --
an inference slot, minutes long -- to describe a world that has already moved.

The rule is shared so the three stages cannot drift apart, and it skips rather
than seeks: the message is still consumed, still committed and still counted,
so the lag figure stays honest and the backlog drains at parse speed.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.utils.freshness import is_stale, occurred_at_of  # noqa: E402


def _ago(**kw):
    return datetime.now(timezone.utc) - timedelta(**kw)


# -- the rule ------------------------------------------------------------------

def test_a_current_item_is_processed():
    assert is_stale({"occurred_at": datetime.now(timezone.utc)}) is False


def test_an_item_from_before_an_overnight_gap_is_skipped():
    """The measured case: containers suspended, wall clock kept moving."""
    assert is_stale({"occurred_at": _ago(hours=8)}) is True


def test_the_window_is_caller_supplied():
    """Correlation wants 900s; reasoning is minutes-per-answer and gets 3600s."""
    item = {"occurred_at": _ago(minutes=30)}
    assert is_stale(item, max_age_sec=900) is True
    assert is_stale(item, max_age_sec=3600) is False


# -- timestamps arrive in several shapes --------------------------------------

def test_an_iso_string_is_understood():
    assert is_stale({"occurred_at": _ago(hours=8).isoformat()}) is True


def test_a_trailing_z_is_understood():
    stamp = _ago(hours=8).isoformat().replace("+00:00", "Z")
    assert is_stale({"occurred_at": stamp}) is True


def test_a_finding_carries_detected_at_not_occurred_at():
    """Correlations are findings; events are observations. Both must be bounded."""
    assert is_stale({"detected_at": _ago(hours=8)}) is True


def test_an_object_is_read_the_same_as_a_dict():
    class _Cluster:
        detected_at = _ago(hours=8)

    assert is_stale(_Cluster()) is True


def test_a_naive_timestamp_is_read_as_utc():
    """Reading a naive stamp as local time would make a day-old item look
    current for the length of the UTC offset."""
    assert is_stale({"occurred_at": datetime.utcnow() - timedelta(hours=8)}) is True


# -- failure direction ---------------------------------------------------------

def test_a_missing_timestamp_is_treated_as_current():
    """Fail toward processing.

    Refusing an item because a field is missing would silently drop a whole
    producer's output -- a worse failure than analysing one item that turns out
    to be old.
    """
    assert is_stale({"headline": "no timestamp here"}) is False


@pytest.mark.parametrize("junk", ["yesterday", "", 0, [], {}, None, object()])
def test_malformed_timestamps_never_raise(junk):
    assert is_stale({"occurred_at": junk}) is False


def test_occurred_at_of_returns_none_when_there_is_nothing_to_read():
    assert occurred_at_of({"headline": "x"}) is None


# -- every stage is covered ----------------------------------------------------

@pytest.mark.parametrize(
    "path,marker",
    [
        ("services/correlation/main.py", "_shared_is_stale"),
        ("services/reasoning/main.py", "is_stale(cluster"),
        ("services/agents/base.py", "is_stale(raw"),
    ],
)
def test_each_stage_bounds_the_age_of_what_it_processes(path, marker):
    """The gap this closes: the guard existed only in correlation."""
    assert marker in (ROOT / path).read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "path",
    ["services/correlation/main.py", "services/reasoning/main.py", "services/agents/base.py"],
)
def test_no_stage_seeks_past_its_backlog(path):
    """Skipping keeps the offset record honest; seeking moves it over messages
    nothing examined."""
    source = (ROOT / path).read_text(encoding="utf-8")
    for call in (".seek(", ".seek_to_end", "seek_to_beginning"):
        assert call not in source, f"{path} moves offsets with {call}"


def test_one_definition_governs_all_three():
    """Three copies of this rule would drift, and the drift would only show up
    after the next outage."""
    correlation = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    reasoning = (ROOT / "services/reasoning/main.py").read_text(encoding="utf-8")
    agents = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    for source in (correlation, reasoning, agents):
        assert "from shared.utils.freshness import" in source


def test_the_agent_window_exceeds_steady_state_lag():
    """The guard must not silently become a total filter.

    Agents consume the whole enriched.events firehose while caring about a
    small slice, so they run steadily behind -- radar_agent sat ~15,600
    messages back. With a 900s window everything it read was stale and every
    event was dropped, including a $479,668 GOOGL block that passed every other
    gate. A guard meant to stop reasoning about yesterday was stopping it
    reasoning at all.
    """
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert 'os.getenv("AGENT_MAX_EVENT_AGE_SEC", "3600")' in source


def test_correlation_keeps_the_tighter_window():
    """Fifteen minutes is correlation's question, not a general freshness rule."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert 'os.getenv("CORRELATION_MAX_EVENT_AGE_SEC", "900")' in source
