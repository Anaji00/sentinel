"""
tests/test_macro_iv_resolution.py

The macro intelligence engine failed on every options message without an
implied-volatility field, which is most of them:

    [agent.macro_intelligence_engine] Transient or unhandled dispatch error:
    float() argument must be a string or a real number, not 'NoneType'   x516

Found by reading a day of logs rather than by a test failing. The dispatcher
catches the exception and moves on, so the agent stayed up, kept consuming, and
produced nothing -- visible only as a count in a log nobody was totalling.

The read path was:

    float(vals[2].decode(...) if isinstance(vals[2], bytes)
          else (vals[2] or (measured_iv if option_type == "CALL" else 0.25)))

For a CALL with nothing cached, that resolves to `None or measured_iv`, and
measured_iv is None whenever the payload omits the field. Nesting is what hid
it; written out, the innermost fallback being allowed to be None is obvious.

The named helper that replaced it, _as_iv, ended its chain in a stated default
of 0.25. That stopped the crash and started a quieter problem: the default and
the put skew beside it were multiplied into a 25-delta IV skew of 375.0 bps and
published as a measurement on every surface whose IVs were not cached. The
helper is now _measured_iv and returns None instead, so an unobserved surface
says so. These tests follow the behaviour to its replacement.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.macro_intelligence_engine import _measured_iv  # noqa: E402


def test_the_crash_case_no_longer_raises():
    """The original defect: None reaching float(). It must return, not throw."""
    assert _measured_iv(None, None) is None


def test_a_cached_value_wins():
    assert _measured_iv(b"0.42", 0.33) == 0.42


def test_bytes_from_redis_are_decoded():
    assert _measured_iv(b"0.42", None) == 0.42


def test_a_measured_value_is_used_when_nothing_is_cached():
    assert _measured_iv(None, 0.33) == 0.33


def test_there_is_no_assumed_last_resort():
    """It used to return 0.25 here, which became a 375.0 bps published skew."""
    assert _measured_iv(None, None) is None


def test_unparseable_values_fall_through_rather_than_raise():
    assert _measured_iv("abc", None) is None
    assert _measured_iv([], {}) is None


def test_a_non_positive_iv_is_not_accepted():
    assert _measured_iv(0.0, None) is None
    assert _measured_iv(-1.0, None) is None


def test_the_nested_form_is_gone():
    """The inline expression is what allowed None to reach float()."""
    source = (ROOT / "services" / "agents" / "macro_intelligence_engine.py").read_text(encoding="utf-8")
    assert 'float(vals[2].decode' not in source
    assert "_measured_iv(vals[2]" in source


def _engine_names_used():
    """Identifiers the engine actually references, parsed rather than grepped.

    A text search matches the word inside the comment explaining why the
    constant was removed, which is exactly the prose worth keeping.
    """
    import ast
    tree = ast.parse((ROOT / "services" / "agents" / "macro_intelligence_engine.py").read_text(encoding="utf-8"))
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}


def test_the_assumed_constants_are_gone_from_the_engine():
    """They existed only to feed the fallback, and their product was published
    as a measured 375.0 bps skew."""
    used = _engine_names_used()
    assert "DEFAULT_CALL_IV" not in used
    assert "PUT_IV_SKEW" not in used
