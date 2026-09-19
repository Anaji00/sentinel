"""
tests/test_rule_lifecycle.py

A rule deleted by its first verdict can never be evaluated.

Measured over this deployment's life:

    rules synthesised                                     145
    that ever produced a correlation reaching a scenario    3
    of those, confirmed                                     0
                denied                                      3
    alive now                                              15

All three died on their only verdict, because `rule_failure` went straight to
`hdel` on the dynamic-rule hash plus a tombstone. Platform-wide, 536 of 818
resolved scenarios are denied, so a rule with a genuine 40% confirmation rate
had a 60% chance of being deleted the first time it was judged.

The threshold to judge one was one import away, and the analyst branch in the
same `handle()` already used it.

Two further gaps in the evidence reaching that judgement:

  only denials   282 confirmations produced no message at all, so a rule could
                 be penalised and never credited.

  a prefix       `startswith("syn_") or startswith("rule_")` excluded
                 SEMANTIC_001 and HAWKES_EXCITATION, which are 81% of the
                 correlation stream and 203 of the 536 denials -- 37.9% of all
                 the negative evidence there is.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

AGENT = ROOT / "services" / "agents" / "rule_agent.py"
TRACKER = ROOT / "services" / "reasoning" / "scenario_tracker.py"


# -- reviewing and deleting are different judgements --------------------------


def test_deprecation_is_stricter_than_review():
    """needs_review fires below this platform's own base rate.

    536 of 818 resolved scenarios are denied -- 65.5% across every rule there
    is -- so a rule at the 0.6 review threshold is beating the stream it belongs
    to. Deleting at that bar removes rule_financial_block_volume_spike, which
    confirms at 37.3% over 77 scenarios.
    """
    from shared.utils.rule_feedback import (
        NEGATIVE_SHARE_FOR_DEPRECATION,
        NEGATIVE_SHARE_FOR_REVIEW,
        MIN_VERDICTS_FOR_DEPRECATION,
        MIN_FEEDBACK_FOR_REVIEW,
    )

    assert NEGATIVE_SHARE_FOR_DEPRECATION > NEGATIVE_SHARE_FOR_REVIEW
    assert MIN_VERDICTS_FOR_DEPRECATION >= MIN_FEEDBACK_FOR_REVIEW
    assert NEGATIVE_SHARE_FOR_DEPRECATION > 0.655, (
        "the bar must sit above the platform's own denial base rate"
    )


def test_one_verdict_never_deletes_a_rule():
    from shared.utils.rule_feedback import should_deprecate

    assert should_deprecate(1, 1) is False
    assert should_deprecate(4, 4) is False, "four of four is still not enough evidence"


def test_the_rules_this_platform_actually_has_survive():
    """Run the real records through the real threshold."""
    from shared.utils.rule_feedback import should_deprecate

    # (confirmed, denied) as stored.
    records = {
        "rule_financial_block_volume_spike": (28, 47),   # 37.3% confirmed
        "rule_crypto_equity_contagion": (8, 7),          # best performer
        "SEMANTIC_001": (60, 199),                       # worst high-volume rule
    }
    for rule, (confirmed, denied) in records.items():
        assert not should_deprecate(confirmed + denied, denied), (
            f"{rule} confirms often enough that deleting it would be a loss"
        )


def test_a_rule_denied_almost_every_time_still_goes():
    """The threshold must not be so lax that nothing is ever pruned."""
    from shared.utils.rule_feedback import should_deprecate

    assert should_deprecate(10, 10) is True
    assert should_deprecate(8, 7) is True


# -- the evidence that reaches it ---------------------------------------------


def test_the_agent_records_before_it_judges():
    """The verdict is written and consulted before anything is removed."""
    code = AGENT.read_text(encoding="utf-8")
    assert "record_rule_verdict(" in code
    body = code.split('if message.get("type") in (')[1].split("summary = ")[0]
    assert body.index("record_rule_verdict(") < body.index("should_deprecate("), (
        "the verdict must be recorded before the record is judged"
    )
    assert "_deprecate_rule(" in body, "and removal still happens when it is earned"
    # Removal lives in its own method, so the handler has one exit and does not
    # need a `dropped()` marker for a path that acted on the message.
    assert "hdel" not in body
    assert "async def _deprecate_rule" in code
    assert 'hdel("sentinel:correlation:dynamic_rules"' in code


def test_both_verdicts_are_published():
    """A loop that only reports failure cannot reinforce anything."""
    code = TRACKER.read_text(encoding="utf-8")
    assert "rule_success" in code
    assert "ScenarioStatus.DENIED, ScenarioStatus.CONFIRMED" in code


def test_no_rule_is_excluded_from_feedback_by_its_name():
    """SEMANTIC_001 and HAWKES_EXCITATION are 81% of the stream."""
    code = TRACKER.read_text(encoding="utf-8")
    body = code.split("if status_change in (ScenarioStatus.DENIED")[1].split("async def")[0]
    assert 'startswith("syn_")' not in body, (
        "the prefix filter dropped 203 of 536 denials before they were sent"
    )
    assert 'startswith("rule_")' not in body
