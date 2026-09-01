"""
tests/test_macro_rates_integrity.py

An inverted yield curve, reported hourly, computed from two typed constants.

    📊 Macro Rates Evaluation | 2Y: 4.25% | 10Y: 4.15% | Spread: -10.0 bps

Identical every hour, because sentinel:quotes:latest:US2Y and US10Y have never
been populated and the reader defaulted them:

    y2  = float(vals[0] or 4.25)
    y10 = float(vals[1] or 4.15)

A 2s10s spread of -10bp is an inversion, which is a recession signal. This one
was arithmetic on two numbers somebody typed, and it went into the model's
prompt stated as measurement -- "2Y Yield: 4.250% | 10Y Yield: 4.150%".

The TIPS path was worse in an interesting way. vals[3] is the TIP *ETF price*,
106.82, and the guard `tips_val < 15.0` quietly caught it and substituted 1.85
-- so a breakeven of 230bp was published from a price mistaken for a yield and
then replaced by a constant when it failed a sanity check. The check noticed
something was wrong and then hid it.

The rule is the one has_excitation_path() already applies to the Hawkes model:
a missing input produces no reading, not a plausible one.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODULE = ROOT / "services" / "agents" / "macro_intelligence_engine.py"


def _source() -> str:
    return MODULE.read_text(encoding="utf-8")


def _code() -> str:
    """Source with comment lines stripped, so prose quoting the old constants
    does not read as the constants surviving."""
    return "\n".join(
        line for line in _source().splitlines() if not line.strip().startswith("#")
    )


@pytest.mark.parametrize("default", ["4.25", "4.15", "77.5", "108.2"])
def test_no_yield_or_credit_default_survives(default):
    """`or 4.25` is indistinguishable from a measurement once it is printed to
    two decimal places in a log line."""
    assert f"or {default}" not in _code()


def test_a_missing_curve_produces_no_reading():
    """Both legs or nothing: half a curve is not a spread."""
    code = _code()
    assert "if y2 is None or y10 is None:" in code
    assert "return None" in code


def test_the_refusal_is_announced():
    """Silently skipping and silently defaulting look the same from outside."""
    code = _code()
    start = code.index("if y2 is None or y10 is None:")
    assert "logger.warning" in code[start:start + 500]


def test_tips_is_validated_as_a_yield_not_swapped_for_a_constant():
    """The old guard caught the price and substituted 1.85, which turned a
    detected error into a fabricated number."""
    code = _code()
    assert "tips_val < 15.0 else 1.85" not in code
    assert "-5.0 < tips_yield < 15.0" in code


def test_an_unmeasured_field_says_so_in_the_prompt():
    """The model cannot discount a number it is told is a measurement."""
    code = _code()
    assert '"not measured"' in code
    assert "tips_str" in code and "breakeven_str" in code


def test_the_credit_ratio_needs_both_legs():
    """hyg / max(1.0, lqd) turns a missing denominator into a ratio of hyg,
    which is not a credit spread, and is not flagged as anything."""
    code = _code()
    assert "hyg / max(1.0, lqd)" not in code
    assert "if (hyg and lqd and lqd > 0) else None" in code


def test_the_spread_is_still_computed_when_the_data_is_there():
    """The fix must not disable the engine, only stop it inventing."""
    code = _code()
    assert "spread_2y10y_bps = (y10 - y2) * 100.0" in code
