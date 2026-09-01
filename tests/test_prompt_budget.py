"""
tests/test_prompt_budget.py

The model was reasoning about events it had never been shown.

Traced end to end on a live scenario call, the prompt assembled for
qwen2.5:1.5b was 12,115 characters:

    system prompt   4,049
    signal data     5,349     the cluster, the events, the entity graph
    JSON schema     2,715

The client truncated it to a flat 3,500 characters. What reached the model:

    system prompt   3,500 of 4,049   (86%)
    signal data         0 of 5,349   (0%)
    JSON schema         0 of 2,715   (0%)

ending mid-word inside a template slot: `"observable": "<must di`. Every
scenario this deployment produced was generated from the instructions alone,
with none of the data it was supposed to be reasoning about. That explains a
string of downstream symptoms treated separately for weeks -- hypotheses named
"Baseline / Alternative / High-impact Maritime Traffic", watch signals naming
plausible-but-generic entities, and the existence of a guard whose whole job is
catching drafts that echo the template.

Two things made it invisible. The cap was a magic number unrelated to the
context window the same request asks for -- 3,500 characters is ~875 tokens
against num_ctx=3072, so it discarded 72% of the space the model had. And the
truncation was silent: the "[truncated for speed]" marker is appended to the
prompt, not written to a log, so grepping the logs for it returns nothing and
reads as "truncation never fires".

The budget is now derived from the window: num_ctx covers prompt and completion
together, so the prompt gets what is left after num_predict and a margin.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import ollama as mod  # noqa: E402


def _budget(context_tokens: int, num_predict: int) -> int:
    reserved = num_predict + mod.PROMPT_BUDGET_MARGIN_TOKENS
    return max(1024, (context_tokens - reserved) * mod.CHARS_PER_TOKEN)


def test_the_budget_comes_from_the_context_window():
    """Not a constant. 3,500 was unrelated to num_ctx and to num_predict, so it
    was wrong in both directions depending on the caller."""
    source = (ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8")
    assert "max_prompt_chars = 3500 if is_small_model else 6000" not in source
    assert "context_tokens - reserved" in source


def test_a_small_model_gets_far_more_than_it_did():
    """The measured scenario prompt was 12,115 chars against a 3,500 cap."""
    assert _budget(3072, 900) > 3500 * 2


def test_the_budget_leaves_room_for_the_answer():
    """num_ctx covers prompt AND completion. A budget that spends the whole
    window on the prompt truncates the response server-side, where there is no
    warning at all."""
    for num_predict in (384, 512, 900, 2048):
        prompt_tokens = _budget(3072, num_predict) / mod.CHARS_PER_TOKEN
        assert prompt_tokens + num_predict <= 3072


def test_a_large_request_still_leaves_a_usable_prompt():
    """A caller asking for the maximum output must not be left with nothing to
    reason from."""
    assert _budget(3072, 2048) >= 1024


def test_truncation_is_reported():
    """The marker goes into the prompt, not the log. Searching the logs for it
    returns nothing, which reads as 'this never happens' -- and that is exactly
    the conclusion it produced."""
    import ast

    tree = ast.parse((ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8"))
    call = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_call_ollama"
    )
    warns = [
        n for n in ast.walk(call)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
        and n.func.attr in {"warning", "error"}
        and isinstance(n.func.value, ast.Name) and n.func.value.id == "logger"
    ]
    assert warns, "truncation is still silent"


def test_the_ratio_and_margin_are_named_not_inlined():
    source = (ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8")
    assert "CHARS_PER_TOKEN" in source
    assert "PROMPT_BUDGET_MARGIN_TOKENS" in source


@pytest.mark.parametrize("num_predict", [128, 384, 900, 1800])
def test_the_budget_never_goes_negative(num_predict):
    assert _budget(3072, num_predict) >= 1024


def test_the_budget_reserves_what_generation_can_actually_produce():
    """The requested num_predict is not the delivered one.

    A scenario asks for 1800 tokens; _bounded_num_predict caps this host at 900
    because generation is linear and 1800 tokens cannot finish inside the
    timeout. Reserving the *request* set aside twice the tokens generation could
    ever use, and the prompt paid for it: 4,064 characters of budget where 7,664
    was available, on a 12,546-character prompt.
    """
    import aiohttp

    from shared.utils.ollama import _bounded_num_predict

    # The cap derives from OLLAMA_TIMEOUT, which is 600s in the container and
    # defaults to 1200s in a bare test process -- so the deployment's cap is
    # stated here rather than inherited from whatever ambient env the suite
    # happens to run under.
    original = mod.OLLAMA_TIMEOUT
    try:
        mod.OLLAMA_TIMEOUT = aiohttp.ClientTimeout(total=600.0)
        requested = 1800
        capped = _bounded_num_predict(requested, is_small_model=True)
    finally:
        mod.OLLAMA_TIMEOUT = original

    assert capped < requested, "the deployment's timeout no longer caps this request"

    on_request = (3072 - (requested + mod.PROMPT_BUDGET_MARGIN_TOKENS)) * mod.CHARS_PER_TOKEN
    on_capped = (3072 - (capped + mod.PROMPT_BUDGET_MARGIN_TOKENS)) * mod.CHARS_PER_TOKEN
    assert on_capped > on_request * 1.5


def test_one_value_is_used_for_both_the_budget_and_the_request():
    """Two derivations of the output size drift, and the drift is invisible --
    the prompt silently shrinks while generation is unaffected."""
    source = (ROOT / "shared/utils/ollama.py").read_text(encoding="utf-8")
    assert source.count("effective_predict") >= 2
    assert '"num_predict": _bounded_num_predict(' not in source
