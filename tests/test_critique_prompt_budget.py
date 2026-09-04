"""The Pass 2 critique prompt was the only prompt on the reasoning path that was
never sized against the model's window.

It interpolated the entire scenario draft -- three hypotheses with mechanisms,
beneficiaries, watch and deny signals, monitoring and rationale -- plus 1,500
characters of the original context, with no budget check at all. The live
truncation log showed prompts of 8,608, 10,748 and 11,173 characters arriving at
a 7,664 ceiling.

That truncation cuts the middle of the prompt, a rule chosen so the task at the
tail survives. On this prompt the middle *is* the draft, so the reviewer was
being handed a scenario with its centre removed and asked to review it
ruthlessly. It cannot report that it only saw part; it corrects what it can see
and returns something shaped exactly like a real critique.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "reasoning"))

from services.reasoning.scenario_generator import (  # noqa: E402
    _CRITIQUE_FIXED_CHARS,
    _CRITIQUE_TEMPLATE,
    _CritiqueNotAffordable,
    _critique_context_room,
    SCENARIO_TOKEN_BUDGET,
)
from shared.utils.ollama import deliverable_prompt_chars  # noqa: E402

MODEL = "qwen2.5:1.5b"
SYSTEM = "You are SENTINEL Red Team / Devil's Advocate." + " x" * 60


def _ceiling() -> int:
    return int(deliverable_prompt_chars(MODEL, SCENARIO_TOKEN_BUDGET))


def test_a_normal_draft_produces_a_prompt_inside_the_window():
    """The whole point: what gets sent must fit without truncation."""
    draft = '{"headline": "' + "a" * 1200 + '"}'
    room = _critique_context_room(draft, SYSTEM, MODEL)
    prompt = _CRITIQUE_TEMPLATE.format(draft=draft, context="c" * room)
    assert len(prompt) + len(SYSTEM) <= _ceiling(), (
        f"critique prompt is {len(prompt) + len(SYSTEM)} chars "
        f"against a {_ceiling()} ceiling"
    )


def test_the_context_absorbs_what_the_draft_leaves():
    """A longer draft takes its room from the context, not from the ceiling.

    The context is corroboration on this prompt; the draft is the subject. When
    they compete the context is what gives way.
    """
    short = '{"headline": "' + "a" * 400 + '"}'
    long = '{"headline": "' + "a" * 1400 + '"}'
    assert _critique_context_room(long, SYSTEM, MODEL) < _critique_context_room(short, SYSTEM, MODEL)


def test_an_oversized_draft_is_skipped_rather_than_truncated():
    """The case that produced the 11,173-character prompt.

    Skipping returns the Pass 1 draft unchanged. That is a worse outcome than a
    real critique and a better one than a fabricated critique of a fragment.
    """
    draft = '{"headline": "' + "a" * 20000 + '"}'
    with pytest.raises(_CritiqueNotAffordable):
        _critique_context_room(draft, SYSTEM, MODEL)


def test_the_skip_says_both_numbers():
    """A skip that does not say how far over it was cannot be tuned."""
    draft = "z" * 20000
    with pytest.raises(_CritiqueNotAffordable) as e:
        _critique_context_room(draft, SYSTEM, MODEL)
    assert "20000" in str(e.value), "the skip does not report the draft size"
    assert any(ch.isdigit() for ch in str(e.value).split("against")[-1]), (
        "the skip does not report the room it had"
    )


def test_the_fixed_cost_is_measured_from_the_template():
    """It was hardcoded at 420 first, and the real figure is 299.

    A hardcoded constant goes stale the moment the instruction is reworded, and
    it goes stale silently -- the prompt just starts overflowing again.
    """
    assert _CRITIQUE_FIXED_CHARS == len(_CRITIQUE_TEMPLATE.format(draft="", context=""))


def test_the_template_still_carries_the_draft_and_the_instruction():
    """Sizing must not have cost the prompt its content."""
    filled = _CRITIQUE_TEMPLATE.format(draft="DRAFT_HERE", context="CTX_HERE")
    assert "DRAFT_HERE" in filled and "CTX_HERE" in filled
    assert "ORIGINAL SCENARIO DRAFT" in filled
    assert "sum to 100" in filled, "the probability instruction was lost"
