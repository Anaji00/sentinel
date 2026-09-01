"""
tests/test_prompt_truncation_preserves_task.py

The truncation was cutting off the question.

Every prompt in this system is assembled with the task last -- the rules, the
output schema, "produce exactly 3 hypotheses". A live scenario prompt of 49,115
characters reached the model as 7,664, an 84% cut taken entirely from the end.
The model was being handed evidence and no question.

    Prompt truncated for qwen2.5:1.5b: 49115 chars -> 7664 (41451 discarded,
    84%). The tail is cut, so whatever the caller placed last is what is lost.

The warning had been describing the defect accurately for some time. It reads as
a note about ordering; it was a report that the instructions were being deleted.

This is the likeliest explanation for instructions that appeared to be ignored.
Scenario headlines carried a concrete identifier 10 times in 83 while the prompt
asked for a named subject throughout -- it was asking into a region of the
prompt that never arrived. The prompt builder's own docstring budgets ~2,500
tokens; the assembled prompt was five times that, because its caps are on the
count of items and not their size.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ollama import _truncate_middle  # noqa: E402

TASK = "=== TASK === Produce exactly 3 hypotheses whose probabilities sum to 100."
HEAD = "=== SUBJECT === Wallet 0x28c6 moved 4.2M USDT."


def _long_prompt(bulk_repeats: int = 400) -> str:
    return f"{HEAD}\n" + ("bulk evidence line. " * bulk_repeats) + f"\n{TASK}"


def test_the_task_survives_truncation():
    """The defect. Without this the model receives evidence and no question."""
    out = _truncate_middle(_long_prompt(), 600)
    assert out.rstrip().endswith("probabilities sum to 100.")


def test_the_subject_survives_truncation():
    """The head carries what the cluster is about, which is what grounds the
    headline the generator is asked to write."""
    out = _truncate_middle(_long_prompt(), 600)
    assert out.startswith("=== SUBJECT ===")


def test_the_budget_is_respected():
    for budget in (200, 600, 4000):
        assert len(_truncate_middle(_long_prompt(), budget)) <= budget


def test_a_prompt_within_budget_is_untouched():
    """Truncation must not be a tax on prompts that already fit."""
    short = f"{HEAD}\n{TASK}"
    assert _truncate_middle(short, 10_000) == short


def test_the_cut_is_announced_inside_the_prompt():
    """The model should be able to tell that it is reading a gap rather than
    a non-sequitur."""
    out = _truncate_middle(_long_prompt(), 600)
    assert "middle of prompt dropped" in out


def test_the_task_wins_when_there_is_room_for_only_one_end():
    """A reply is impossible without the question; it is merely less grounded
    without the subject."""
    out = _truncate_middle(_long_prompt(), 40)
    assert out.rstrip().endswith("sum to 100.")


def test_a_zero_budget_does_not_raise():
    assert _truncate_middle(_long_prompt(), 0) == _long_prompt()


def test_the_middle_is_what_goes():
    """Bulk evidence is the part the budget cannot afford, and the part any
    single line of which matters least."""
    out = _truncate_middle(_long_prompt(), 600)
    assert out.count("bulk evidence line.") < 400
