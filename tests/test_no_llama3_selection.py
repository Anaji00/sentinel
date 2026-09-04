"""llama3 must not be selectable.

The string "llama3" was the default at five call sites. Every deployed service
sets AGENT_MODEL, so the default never fired and looked harmless -- but
llama3:latest is installed on the host, so a service that ever missed the
variable would resolve to it and run, with nothing reporting the substitution.

It is not a neutral substitution. The prompt budget is computed from the
requested model: llama3 advertises 11,760 characters where qwen2.5:1.5b
advertises 7,664. A service defaulting to llama3 builds prompts half again
larger than the model executing them can accept, and the send path cuts the
difference out of the middle -- which is the evidence.

It was also first in MODEL_TIER_HEAVY and last in MODEL_TIER_LIGHTWEIGHT, so it
was reachable as a fallback from either tier regardless of any default.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.ollama import (  # noqa: E402
    DEFAULT_MODEL, MODEL_TIER_HEAVY, MODEL_TIER_LIGHTWEIGHT,
)

SCANNED = [ROOT / "services", ROOT / "shared"]


def _python_files():
    for root in SCANNED:
        for path in root.rglob("*.py"):
            yield path


def test_the_default_model_is_not_llama():
    assert "llama" not in DEFAULT_MODEL.lower(), DEFAULT_MODEL


def test_no_fallback_tier_can_reach_llama():
    """A tier entry is a fallback, so it runs without anyone selecting it."""
    for tier_name, tier in (("lightweight", MODEL_TIER_LIGHTWEIGHT), ("heavy", MODEL_TIER_HEAVY)):
        for model in tier:
            assert "llama" not in model.lower(), f"{tier_name} tier can fall back to {model}"


def test_the_tiers_are_not_empty():
    """Removing entries must not have emptied a tier.

    An empty tier has no llama in it and would satisfy the test above while
    silently disabling fallback altogether.
    """
    assert len(MODEL_TIER_LIGHTWEIGHT) >= 2
    assert len(MODEL_TIER_HEAVY) >= 2


def test_no_llama_string_survives_as_a_value_anywhere():
    """Any llama string in an executable position, found by parsing rather than
    grepping.

    A regex over the source would also match the word in prose, and this file
    would then be reporting on comments. Only string *values* count: defaults,
    assignments, call arguments, list entries.
    """
    import re

    # "llama" also occurs inside "ollama", which is the server's name and must
    # not be flagged. A preceding letter rules that out.
    name = re.compile(r"(?<![a-z])llama")

    offenders = []
    visited = 0
    for path in _python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        visited += 1

        # Docstrings are Constants too, and this file is about what the code
        # *selects*, not what its prose mentions.
        docstrings = set()
        for holder in ast.walk(tree):
            if isinstance(holder, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                body = getattr(holder, "body", None) or []
                if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                    docstrings.add(id(body[0].value))

        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                continue
            if id(node) in docstrings:
                continue
            if name.search(node.value.lower()):
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno} -> {node.value!r}")

    # Without this the scan could visit nothing -- a bad path, a rename -- and
    # report a clean result forever.
    assert visited > 20, f"the scan only parsed {visited} files; it is not looking where it should"
    assert not offenders, "llama is still a value at:\n  " + "\n  ".join(offenders)
