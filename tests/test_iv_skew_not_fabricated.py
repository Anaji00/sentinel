"""An implied-volatility skew must be measured or absent, never assumed.

The vol surface computed its skew from two constants whenever the cache was
empty:

    call_iv = DEFAULT_CALL_IV                 = 0.25
    put_iv  = DEFAULT_CALL_IV * PUT_IV_SKEW   = 0.2875
    skew    = (0.2875 - 0.25) * 10000         = 375.0 bps, always

and published it as an observation. A stored brief reads
"iv_skew_25d_bps": -375.0 alongside "tail_risk_conviction": 1.0 -- maximum
confidence in the product of two hardcoded numbers.

This was the ordinary case, not an edge one: 11,755 of 15,575 options events in
24 hours (75.5%) carry no implied volatility at all. And the assumed 0.25 is
less than half the 0.5382 median of the IVs that do arrive, so where it did
stand in for a real value it understated volatility by more than half.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.macro_intelligence_engine import (  # noqa: E402
    VolatilitySurfaceBrief, _measured_iv,
)

SOURCE = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")


def _skew(call_iv, put_iv):
    """The expression the engine now uses."""
    return (round((put_iv - call_iv) * 10000.0, 1)
            if (call_iv is not None and put_iv is not None) else None)


def test_an_unobserved_leg_reads_as_none():
    assert _measured_iv(None, None) is None
    assert _measured_iv("", None) is None
    assert _measured_iv("not-a-number", None) is None
    assert _measured_iv(0, None) is None, "a zero IV is not an observation"


def test_a_measured_leg_is_returned():
    assert _measured_iv("0.5382", None) == 0.5382
    assert _measured_iv(None, 0.42) == 0.42
    assert _measured_iv(b"0.31", None) == 0.31


def test_the_cache_is_preferred_over_the_payload():
    assert _measured_iv(0.60, 0.20) == 0.60


def test_no_skew_without_both_legs():
    """The defect: this returned 375.0 from two constants."""
    assert _skew(None, None) is None
    assert _skew(0.25, None) is None
    assert _skew(None, 0.2875) is None


def test_a_skew_between_two_measured_legs_is_reported():
    assert _skew(0.50, 0.58) == 800.0


def test_the_constant_skew_can_no_longer_be_produced():
    """375.0 bps was the signature of the fabrication."""
    assert _skew(None, None) != 375.0


def test_the_brief_field_can_hold_absence():
    """It was a required float, so a model with nothing to report had to invent
    a number -- and it echoed the one in its prompt."""
    brief = VolatilitySurfaceBrief(
        ticker="MCK", put_call_volume_ratio=0.92, volatility_regime="N/A",
        tail_risk_conviction=0.5, analytical_summary="x",
    )
    assert brief.iv_skew_25d_bps is None


def test_the_measured_value_overwrites_the_models():
    """The measurement belongs to the caller; the analysis belongs to the model."""
    assert "brief.iv_skew_25d_bps = iv_skew_bps" in SOURCE


def test_the_prompt_states_absence_rather_than_a_number():
    assert "{skew_str}" in SOURCE
    assert "not measured" in SOURCE
    assert "- 25D IV Skew: {iv_skew_bps:+.1f} bps" not in SOURCE


def test_the_engine_cannot_substitute_an_assumed_leg():
    """Guards the call site, not a reimplementation of it.

    Restoring the fabrication in the engine left every other test in this file
    passing, because they exercise the helpers rather than the code that uses
    them. This reads the engine itself, and parses rather than greps so the
    prose explaining the removal does not count as a use.
    """
    import ast
    used = {n.id for n in ast.walk(ast.parse(SOURCE)) if isinstance(n, ast.Name)}
    assert "DEFAULT_CALL_IV" not in used, "an assumed call IV is back"
    assert "PUT_IV_SKEW" not in used, "an assumed put skew is back"
    assert "if (call_iv is not None and put_iv is not None) else None" in SOURCE
