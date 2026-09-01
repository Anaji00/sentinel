"""
tests/test_prompt_section_budget.py

The prompt builder assembled five times what could be sent.

Its own docstring budgets ~2,500 tokens. A live scenario prompt reached 49,115
characters against a send budget of 7,664, and 84% was discarded -- by position,
because the send path cuts text, not sections. The builder's caps count items
(five events, ten relationships, three patterns) and never measure them, so five
events carrying full JSON payloads exhaust the window on their own, and the
consensus block had no cap at all.

Truncating in the middle rather than the tail already stopped the instructions
being deleted. This is the other half: choosing what fits by what it is for,
rather than by where it happened to sit.

Nothing about the window changes here. deliverable_prompt_chars() is the same
arithmetic the send path already applied, made reachable before the prompt is
built instead of after.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reasoning.scenario_generator import (  # noqa: E402
    _DYNAMIC_BUDGET_SHARE, _SECTION_SHARES, _clip,
)
from shared.utils.ollama import deliverable_prompt_chars  # noqa: E402


def test_a_section_within_its_budget_is_untouched():
    assert _clip("short", 500, "events") == "short"


def test_an_oversized_section_is_cut_to_budget():
    assert len(_clip("x" * 5000, 300, "events")) <= 300


def test_the_cut_is_announced_inside_the_section():
    """A model handed a JSON object that stops mid-key cannot tell truncation
    from malformed input, and will reason about the fragment as the whole."""
    assert "truncated to fit" in _clip("x" * 5000, 300, "signal data")
    assert "signal data" in _clip("x" * 5000, 300, "signal data")


def test_clipping_survives_a_budget_smaller_than_its_own_marker():
    assert isinstance(_clip("x" * 500, 5, "events"), str)


def test_empty_and_none_sections_are_safe():
    assert _clip("", 100, "events") == ""
    assert _clip(None, 100, "events") == ""


# -- the allocation ------------------------------------------------------------

def test_the_shares_do_not_exceed_the_dynamic_budget():
    assert abs(sum(_SECTION_SHARES.values()) - 1.0) < 1e-6


def test_evidence_gets_the_largest_share():
    """The signal data is what the judgment rests on. Everything else informs
    or corroborates it."""
    assert _SECTION_SHARES["events"] == max(_SECTION_SHARES.values())


def test_the_static_half_is_left_room():
    """Rules, the JSON schema and the task are not negotiable, and deleting
    them is the failure this whole area already had once."""
    assert 0.0 < _DYNAMIC_BUDGET_SHARE < 1.0


def test_every_section_gets_something():
    assert all(share > 0 for share in _SECTION_SHARES.values())


def test_the_consensus_block_now_has_a_cap():
    """It had none at all, which is how one section could exhaust the window."""
    assert "consensus" in _SECTION_SHARES


# -- the budget itself ---------------------------------------------------------

def test_the_budget_helper_agrees_with_the_send_path():
    """One source of truth: callers cannot size to a window they have to guess."""
    assert deliverable_prompt_chars("qwen2.5:1.5b", 1800) > 0
    assert deliverable_prompt_chars("llama3:8b", 1800) > deliverable_prompt_chars("qwen2.5:1.5b", 1800)


def test_a_smaller_model_gets_a_smaller_prompt():
    small = deliverable_prompt_chars("qwen2.5:1.5b")
    large = deliverable_prompt_chars("llama3:8b")
    assert small < large


def test_the_builder_sizes_before_it_renders():
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    assert source.index("sized = _apply(dynamic)") < source.index("prompt = _render(sized)")


def test_the_builder_re_sizes_when_the_first_pass_still_overshoots():
    """A fixed split cannot know how large the static half is. Measuring the
    rendered prompt and re-sizing against what the template actually left is
    the only way to be sure, and it costs two string builds, not an inference."""
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    block = source.split("prompt = _render(sized)")[1][:900]
    assert "if len(prompt) > budget:" in block
    assert "_render(_apply(remaining))" in block


def test_the_second_pass_measures_the_static_half_rather_than_assuming_it():
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    block = source.split("prompt = _render(sized)")[1][:900]
    assert "static_chars = len(prompt) - sum(" in block


def test_a_budget_failure_does_not_stop_the_prompt_being_built():
    """Losing the ability to size sections must not lose the scenario."""
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    block = source.split("budget = int(deliverable_prompt_chars(")[1][:300]
    assert "except Exception" in block


# ── The schema was being sent twice ──────────────────────────────────────────
#
# On the first attempt the schema is passed to Ollama as `format`, where it
# stops being a request and becomes a decoding constraint. It was *also* being
# restated as prose in the prompt -- 2,715 characters of a 7,664-character
# window, 35% of everything this host can be told, spent restating what the
# decoder already enforces structurally.
#
# The client's own note establishes that this model reads the prose and ignores
# it. Retries keep it, because they fall back to format="json", which compels
# valid JSON but says nothing about shape.

def test_the_grammar_attempt_does_not_also_send_prose():
    source = (ROOT / "shared" / "utils" / "ollama.py").read_text(encoding="utf-8")
    assert 'attempt_schema = "" if (attempt == 0 and schema_dict) else schema_instruction' in source


def test_the_prompt_uses_the_per_attempt_schema():
    source = (ROOT / "shared" / "utils" / "ollama.py").read_text(encoding="utf-8")
    assert "{user_prompt}{attempt_schema}{correction}" in source
    assert "{user_prompt}{schema_instruction}{correction}" not in source


def test_retries_still_state_the_schema():
    """format="json" compels valid JSON and says nothing about shape, so on a
    retry the prose is the only statement of the schema the model gets."""
    source = (ROOT / "shared" / "utils" / "ollama.py").read_text(encoding="utf-8")
    line = [l for l in source.splitlines() if "attempt_schema =" in l][0]
    assert "else schema_instruction" in line


def test_the_reclaimed_window_goes_to_the_user_prompt():
    """900 characters for the subject, the evidence, the graph, precedents and
    the task was the arithmetic before this."""
    from services.reasoning.scenario_generator import (
        SCENARIO_SYSTEM_PROMPT, _SCHEMA_RESERVE_CHARS,
    )

    assert _SCHEMA_RESERVE_CHARS == 0
    assert 7664 - len(SCENARIO_SYSTEM_PROMPT) - _SCHEMA_RESERVE_CHARS > 3000
