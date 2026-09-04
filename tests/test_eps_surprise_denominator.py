"""A percentage surprise divided by a near-zero estimate measures the estimate.

The only guard was `eps_estimate != 0`, so an estimate of $0.0051 passed it.
Live rows from the events table:

    SECZ  actual -2.37   estimate 0.0051  ->  -46,570.6%
    RRGB  actual  0.12   estimate 0.0034  ->   +3,429.4%
    MLCI  actual -0.37   estimate 0.0102  ->   -3,727.5%

None of those describe the size of a miss; they describe how close a consensus
sat to zero. 70 of 101 earnings surprises cleared the radar agent's 10% gate on
this arithmetic, so the gate was selecting for small-cap names with near-zero
consensus rather than for large surprises.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "tradfi_collector", ROOT / "services" / "collector-tradfi" / "main.py"
)


def _floor():
    if "tradfi_collector" in sys.modules:
        return sys.modules["tradfi_collector"].MIN_EPS_SURPRISE_DENOMINATOR
    m = importlib.util.module_from_spec(_spec)
    sys.modules["tradfi_collector"] = m
    _spec.loader.exec_module(m)
    return m.MIN_EPS_SURPRISE_DENOMINATOR


def _surprise(actual, estimate):
    """The expression the collector now uses."""
    floor = _floor()
    return ((actual - estimate) / max(abs(estimate), floor)) * 100.0


def test_the_floor_is_a_meaningful_eps_magnitude():
    assert _floor() == 0.05


def test_a_normal_beat_is_unchanged():
    """Estimate well above the floor: the arithmetic is the same as before."""
    assert abs(_surprise(1.10, 1.00) - 10.0) < 0.001


def test_a_normal_miss_is_unchanged():
    assert abs(_surprise(0.90, 1.00) + 10.0) < 0.001


def test_the_secz_row_is_no_longer_a_five_figure_percentage():
    """-46,570.6% became a four-digit number describing a real miss."""
    assert abs(_surprise(-2.37, 0.0051)) < 5000
    old = ((-2.37 - 0.0051) / abs(0.0051)) * 100.0
    assert abs(old) > 46000, "the original arithmetic is what this replaces"


def test_the_rrgb_row_is_bounded():
    assert abs(_surprise(0.12, 0.0034)) < 500
    assert _surprise(0.12, 0.0034) > 0, "a beat is still positive"


def test_a_zero_estimate_no_longer_yields_nothing():
    """`eps_estimate != 0` returned None; the floor makes it computable."""
    assert _surprise(0.10, 0.0) == 200.0


def test_the_sign_survives_the_floor():
    assert _surprise(-0.10, 0.0) < 0
    assert _surprise(0.10, 0.0) > 0


def test_ordering_is_preserved_below_the_floor():
    """A bigger absolute miss still reads as a bigger surprise."""
    assert abs(_surprise(-2.00, 0.001)) > abs(_surprise(-0.50, 0.001))


def test_the_absolute_difference_is_published():
    """Magnitude a consumer can judge without inferring it from a ratio."""
    source = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert '"eps_surprise_abs": eps_surprise_abs,' in source
    assert source.count('"eps_surprise_abs"') >= 2, "both publish sites must carry it"


def test_the_unguarded_division_is_gone():
    """Guards the collector itself, not the helper above."""
    source = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    executable = chr(10).join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert "/ abs(float(eps_estimate))) * 100.0" not in executable
    # The division must use the floored denominator, not merely mention the
    # constant somewhere in the file.
    assert "denominator = max(abs(estimate_f), MIN_EPS_SURPRISE_DENOMINATOR)" in executable
    assert "/ denominator) * 100.0" in executable
