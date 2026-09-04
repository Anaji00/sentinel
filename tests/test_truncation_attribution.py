"""A truncation warning that names only the model cannot be acted on.

Every truncation in the log read `Prompt truncated for qwen2.5:1.5b: 8608 chars
-> 7664`. Seventeen call sites build prompts for that model, so the line
narrowed the cause to one in seventeen. Finding the unbudgeted one -- the Pass 2
critique -- meant reading all of them.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.ollama import _calling_site  # noqa: E402


def test_the_label_names_the_caller_not_the_helper():
    """The frame wanted is the one that built the prompt."""
    site = _calling_site()
    assert site.startswith("test_truncation_attribution.py:"), site
    assert "ollama.py" not in site, "the label points at the truncator, not the caller"


def test_it_names_the_function():
    def a_prompt_builder() -> str:
        return _calling_site()

    assert a_prompt_builder().endswith(":a_prompt_builder")


def test_it_never_raises_through_a_broken_stack():
    """A diagnostic that can break the call it diagnoses is worse than none.

    The truncation branch is already the unhappy path; an exception raised from
    the log line would turn a degraded inference into a failed one.
    """
    import shared.utils.ollama as mod

    real = mod.traceback if hasattr(mod, "traceback") else None
    import builtins

    real_import = builtins.__import__

    def exploding(name, *a, **k):
        if name == "traceback":
            raise RuntimeError("stack unavailable")
        return real_import(name, *a, **k)

    builtins.__import__ = exploding
    try:
        assert _calling_site() == "unknown"
    finally:
        builtins.__import__ = real_import
        if real is not None:
            mod.traceback = real
