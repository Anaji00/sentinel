"""The macro review read a scale its input does not use.

Measured on the running deployment 2026-09-20:

    macro_intelligence_engine  processed=77,880 at 12.75/s
                               inferences in 24 hours: 0

It is the highest message-rate agent in the fleet and produced nothing. Of 434
consecutive live enriched.events:

    carries severity             0   (0%)
    carries computed_severity    0   (0%)
    carries anomaly_score      434   (100%)

`severity` is the 1-5 integer an IntelBrief carries, and this agent does not
subscribe to INTEL_BRIEFS; `computed_severity` is written onto that same topic
by the knowledge graph engine. Both names were read on a stream carrying
neither, so the comparison was against 0 every time and the high-severity
trigger could not fire. 4,635 events cleared anomaly_score >= 0.8 in the same
day and none of them reached it.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.macro_intelligence_engine import (  # noqa: E402
    _MACRO_REVIEW_ANOMALY,
    _MACRO_REVIEW_SEVERITY,
    _is_macro_review_worthy,
)


def test_the_anomaly_scale_the_events_actually_carry_is_read():
    assert _is_macro_review_worthy({"anomaly_score": 0.85}) is True
    assert _is_macro_review_worthy({"anomaly_score": 0.50}) is False


def test_the_severity_scale_an_intel_brief_carries_still_works():
    """Accepting the new scale must not drop the old one."""
    assert _is_macro_review_worthy({"severity": 5}) is True
    assert _is_macro_review_worthy({"computed_severity": 4}) is True
    assert _is_macro_review_worthy({"severity": 2}) is False


def test_the_bar_is_the_same_height_on_both_scales():
    """0.8 is 4 out of 5. The threshold did not move; the reading did."""
    assert _MACRO_REVIEW_ANOMALY == _MACRO_REVIEW_SEVERITY / 5.0


def test_an_explicit_severity_wins_over_an_anomaly_score():
    """A brief that states its severity has said what it means."""
    assert _is_macro_review_worthy({"severity": 1, "anomaly_score": 0.99}) is False


def test_a_message_stating_neither_is_not_worthy():
    assert _is_macro_review_worthy({}) is False
    assert _is_macro_review_worthy({"headline": "something happened"}) is False


def test_an_unparseable_value_does_not_raise():
    assert _is_macro_review_worthy({"severity": "high"}) is False
    assert _is_macro_review_worthy({"anomaly_score": None, "severity": None}) is False


def test_the_old_unreachable_comparison_is_gone():
    src = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    assert 'message.get("computed_severity") or message.get("severity") or 0' not in code
    assert "_is_macro_review_worthy(message)" in code
