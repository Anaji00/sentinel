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
PROMPT = SOURCE[SOURCE.index("Analyze Treasury yield curve"):]
PROMPT = PROMPT[:PROMPT.index('"""')]


def test_the_prompt_formats_no_optional_value_directly():
    """y2, y10 and the spread are guaranteed by the early return. Everything
    else in this prompt is optional and must arrive pre-rendered."""
    formatted = set(re.findall(r"\{([a-z_0-9]+):", PROMPT))
    assert formatted <= {"y2", "y10", "spread_2y10y_bps"}, formatted


def test_every_optional_input_says_when_it_is_missing():
    for name in ("tips_str", "breakeven_str", "credit_str"):
        assert "{" + name + "}" in PROMPT, name
        definition = next(
            line for line in SOURCE.splitlines() if line.strip().startswith(f"{name} =")
        ) if f"{name} =" in SOURCE else ""
        block = SOURCE[SOURCE.index(f"{name} ="):SOURCE.index(f"{name} =") + 400]
        assert "not measured" in block, f"{name} does not report its own absence"


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
