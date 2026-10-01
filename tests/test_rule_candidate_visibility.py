"""The line written to tell a quiet loop from a dead one was itself silent.

`_rule_candidate_loop` proposes rules for co-occurrences the rule set does not
cover -- what services/agents/main.py calls "the signal that makes this agent
able to learn anything". Its else branch carried the comment "A quiet pass and
a dead loop look identical without this" above a `logger.debug`, and this
deployment does not emit DEBUG. So they still looked identical.

Measured 2026-09-20 against the running broker:

    agents.rules.candidates      24 messages, lifetime
    rule_synthesizer inferences   0 in 24 hours

and eight hours of correlation-service logs carried nothing from this loop at
all -- no way to tell whether it was running and finding nothing, or had
stopped.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
LOOP = SRC[SRC.index("async def _rule_candidate_loop"):]
LOOP = LOOP[: LOOP.index("async def _vector_retention_loop")]


def test_a_quiet_pass_is_logged_where_it_can_be_read():
    assert "logger.debug(" not in LOOP, (
        "the quiet-pass line is back at a level this deployment does not emit"
    )
    assert "logger.info(" in LOOP


def test_the_quiet_log_is_rate_limited_not_every_pass():
    """Audible without a line every thirty minutes forever."""
    assert "_candidate_passes[0] % 10 == 0" in LOOP
    assert "_candidate_passes[0] == 1" in LOOP


def test_the_counter_is_initialised_outside_the_loop():
    assert "_candidate_passes = [0]" in SRC
    assert SRC.index("_candidate_passes = [0]") < SRC.index("async def _rule_candidate_loop")


def test_the_pass_reports_what_it_compared_against():
    """"Nothing found" is only meaningful beside how many rules it checked."""
    assert "len(rules)" in LOOP
