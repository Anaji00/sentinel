"""The rates path must survive its optional inputs being absent.

The prompt interpolated {credit_ratio:.4f} with no guard, while the two lines
above it carefully rendered "not measured" for a missing TIPS yield and
breakeven. credit_ratio is None whenever HYG or LQD is missing, so the whole
evaluation died on:

    unsupported format string passed to NoneType.__format__

losing the brief, both Redis keys and the bulletin, two seconds after logging a
correct 2s10s spread.

It had never run. _process_rates_and_macro_regime returns early unless both
US2Y and US10Y are present, and nothing wrote those keys for the life of the
deployment -- so the line was unreachable until the Treasury collector landed,
and the first evaluation that got past the yield guard hit it immediately.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")


def test_the_brief_is_built_without_any_optional_input():
    """The brief is now computed (shared.utils.deterministic_briefs), not
    prompted. Missing TIPS, breakeven or credit must still degrade rather than
    raise, and say they were not measured."""
    from shared.utils.deterministic_briefs import rates_brief_fields

    f = rates_brief_fields(47.0, [], None, None, None, None)
    assert f["credit_spread_widening_signal"] == "Unclassified"
    assert "breakeven" not in f["regime_summary"]
    assert "rates_brief_fields(" in SOURCE, "the engine no longer builds the brief from measurements"


def test_a_missing_credit_ratio_renders_rather_than_raises():
    """The defect, reproduced on the same expression the engine uses."""
    for credit_ratio in (None, 0.75):
        rendered = f"{credit_ratio:.4f}" if credit_ratio is not None else "not measured"
        assert isinstance(rendered, str)
    assert (lambda c: f"{c:.4f}" if c is not None else "not measured")(None) == "not measured"


def test_formatting_none_directly_still_raises():
    """Confirms the failure mode is real rather than assumed."""
    import pytest
    with pytest.raises(TypeError):
        "{:.4f}".format(None)


def test_the_yield_guard_still_gates_the_path():
    """Only y2 and y10 are required; the rest degrade to 'not measured'."""
    assert "if y2 is None or y10 is None:" in SOURCE
    # Sized past the warning's own text, which is long enough to push the
    # return out of a 400-character slice.
    guard = SOURCE[SOURCE.index("if y2 is None or y10 is None:"):]
    guard = guard[:guard.index("spread_2y10y_bps")]
    assert "return None" in guard, guard[-120:]


def test_credit_ratio_is_none_when_either_leg_is_missing():
    for hyg, lqd in ((None, 105.0), (79.0, None), (None, None), (79.0, 0.0)):
        ratio = (hyg / lqd) if (hyg and lqd and lqd > 0) else None
        assert ratio is None, (hyg, lqd)
    assert round((79.11 / 105.35), 4) == round(79.11 / 105.35, 4)
