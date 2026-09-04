"""
tests/test_chokepoint_traffic.py

The platform watched nine straits and had no measure of traffic through any of
them. It could say a particular vessel went dark; it could not say whether the
Strait of Hormuz was quieter this morning than it had been all month -- which is
the question a blockade, a closure or a fleet standing off actually poses.

Two things make the measure awkward, and both are the point of this module:

  Sources differ    AIS delivers a vessel count; Sentinel-1 delivers a share of
                    water returning like metal. Neither converts into the other
                    and averaging them would mean nothing. Each is scored
                    against its own history, and only the z-score is comparable.

  Coverage differs  Four of nine chokepoints have never returned an AIS message,
                    so an AIS count of zero there describes receiver coverage,
                    not traffic. A chokepoint with no history must be refused
                    rather than reported as quiet -- otherwise the blindest
                    straits look like the calmest ones.

The output is a deviation, not a volume. "Forty vessels" means nothing without
knowing the usual figure is two hundred; "three sigma below normal" is the same
sentence in every strait and from either instrument.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.chokepoints import (  # noqa: E402
    MIN_BASELINE_OBSERVATIONS, NOTABLE_SIGMA, TrafficReading, assess, baseline_key,
)

BUSY = [200, 210, 195, 205, 198, 202, 207, 199, 203, 201, 196, 204, 208]


def _read(value, chokepoint="Strait of Hormuz", source="ais"):
    return TrafficReading(chokepoint, source, value, "2026-09-02T00:00:00Z")


def test_a_strait_emptying_is_detected():
    """The event this exists for."""
    a = assess(_read(120), BUSY)
    assert a.direction == "quieter_than_usual"
    assert a.is_notable


def test_a_strait_filling_is_a_different_event():
    a = assess(_read(260), BUSY)
    assert a.direction == "busier_than_usual"
    assert a.is_notable


def test_ordinary_traffic_is_not_an_event():
    a = assess(_read(203), BUSY)
    assert a.direction == "normal"
    assert not a.is_notable


def test_a_chokepoint_with_no_history_is_refused_not_called_quiet():
    """Four straits have never returned an AIS message. Scoring their zero as
    'quiet' would rank the blindest as the calmest."""
    assert assess(_read(0, "Bab-el-Mandeb"), [1, 2, 3]) is None


def test_the_baseline_bar_is_high_enough_to_mean_something():
    assert MIN_BASELINE_OBSERVATIONS >= 10
    assert assess(_read(120), BUSY[:MIN_BASELINE_OBSERVATIONS - 1]) is None


def test_an_unvarying_baseline_cannot_rank_anything():
    """Repetition looks like certainty. This is the frozen-quote shape: a
    series that never moves would give every new reading an infinite z-score."""
    assert assess(_read(500), [100.0] * 40) is None


def test_malformed_history_entries_are_skipped_not_fatal():
    history = BUSY + ["abc", None, float("nan")]
    assert assess(_read(203), history) is not None


def test_sources_keep_separate_baselines():
    """A radar density and a vessel count share no units. Mixing them into one
    baseline would make both meaningless."""
    assert baseline_key("ais", "Strait of Hormuz") != baseline_key("sar", "Strait of Hormuz")


def test_chokepoints_keep_separate_baselines():
    assert baseline_key("ais", "Strait of Hormuz") != baseline_key("ais", "Suez Canal")


def test_the_key_is_stable_across_spelling():
    assert baseline_key("AIS", "Strait Of Hormuz") == baseline_key("ais", "strait of hormuz")


def test_the_payload_states_what_it_measured():
    """A z-score with no baseline attached is unauditable."""
    payload = assess(_read(120), BUSY).as_payload()
    for field in ("baseline_mean", "baseline_std", "observations", "z_score", "source"):
        assert field in payload


def test_the_reading_is_scored_before_it_joins_the_baseline():
    """Scoring against a baseline containing the reading pulls the mean toward
    the observation being judged -- the defect that made a first-ever earnings
    surprise look unremarkable."""
    source = (ROOT / "shared" / "utils" / "chokepoints.py").read_text(encoding="utf-8")
    block = source.split("async def record_and_assess")[1]
    assert block.index("assess(reading, history)") < block.index("lpush")


def test_notable_means_the_same_thing_in_both_directions():
    assert NOTABLE_SIGMA > 0
    quiet = assess(_read(200 - 5 * NOTABLE_SIGMA * 4), BUSY)
    busy = assess(_read(200 + 5 * NOTABLE_SIGMA * 4), BUSY)
    assert quiet.is_notable and busy.is_notable
