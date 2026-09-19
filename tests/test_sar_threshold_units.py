"""
tests/test_sar_threshold_units.py

A decibel threshold was being compared against linear power.

openEO's SENTINEL1_GRD applies the sigma0-ellipsoid coefficient and returns
LINEAR backscatter. Measured over Bab-el-Mandeb, max over an eight-day window:

    min   0.000102   (-39.9 dB)
    mean  0.0549     (-12.6 dB)
    max   122.59     (+20.9 dB)

Sigma0 is a power ratio and is therefore always positive, so `value > 0.0`
admitted every valid pixel. The share of water "returning like metal" came back
as 1.0 -- and that is exactly what every baseline this platform had collected
contained:

    bab-el-mandeb      1.0  1.0  1.0  0.99999995 ... 0.99999979
    strait_of_hormuz   0.99999999 ... 1.0  0.99996635
    suez_canal         0.99999992 ... 1.0

Nine to fifteen observations per chokepoint, all of them 1.0 to eight decimal
places. Every z-score computed from that series, every traffic assessment
published from it, and every SUPPLY_CHAIN_METRIC that reached
`rule_physical_disruption_repricing` described the difference between 0.99999979
and 1.0.

Measured at the corrected threshold, the same chokepoint:

    > 0.0 linear   1.0          (everything -- the defect)
    > 0.1          0.0655
    > 0.5          0.00407
    > 1.0  (0 dB)  0.00113      <- selective, and free to move either way
    > 2.0          0.00028
    > 5.0          0.000034
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DETECTION = ROOT / "services" / "collector-sar" / "sar_detection.py"
COLLECTOR = ROOT / "services" / "collector-sar" / "main.py"


def _detection():
    spec = importlib.util.spec_from_file_location("sar_detection_for_test", DETECTION)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_zero_db_is_one_in_linear():
    m = _detection()
    assert m._linear(0.0) == 1.0
    assert round(m._linear(-10.0), 6) == 0.1
    assert round(m._linear(10.0), 6) == 10.0


def test_the_threshold_used_against_pixels_is_linear():
    m = _detection()
    assert m.VV_TARGET_THRESHOLD_LINEAR == m._linear(m.VV_TARGET_THRESHOLD_DB)


def test_the_measured_distribution_is_no_longer_saturated():
    """The real values from Bab-el-Mandeb, through the real counter."""
    m = _detection()
    measured = [0.000102, 0.0549, 0.9, 1.1, 122.59]
    targets, water = m.count_targets(measured)
    assert water == 5, "every finite sample is water"
    assert targets == 2, (
        "only the two above 0 dB are targets; before the fix all five were"
    )


def test_the_collector_compares_in_linear():
    """The server-side mask is where the defect actually lived."""
    code = COLLECTOR.read_text(encoding="utf-8")
    assert "> VV_TARGET_THRESHOLD_LINEAR" in code
    assert "> VV_TARGET_THRESHOLD_DB" not in code, (
        "comparing the dB constant against sigma0 pixels is the defect itself"
    )


def test_the_method_string_states_both_units():
    """A reader must be able to tell which number was applied."""
    code = DETECTION.read_text(encoding="utf-8")
    assert "VV_TARGET_THRESHOLD_LINEAR:.4g" in code
    assert "linear" in code


# -- and the baseline must not be compared across calibrations ----------------


def test_a_baseline_is_keyed_on_its_calibration():
    """Fixing the threshold moves the density from ~1.0 to ~0.001.

    Scored against the old history that is roughly a thousand sigma low, so the
    repair itself would have announced that every chokepoint had emptied at
    once. A reading is only comparable to readings taken the same way.
    """
    from shared.utils.chokepoints import baseline_key

    old = baseline_key("sar", "Strait of Hormuz")
    new = baseline_key("sar", "Strait of Hormuz", "vv0db")
    assert old != new, "a new calibration must start a new series"
    assert "@vv0db" in new
    # A source that states no calibration keeps the key it was already using.
    assert old == "sentinel:chokepoint:baseline:sar:strait_of_hormuz"


def test_the_collector_states_its_calibration():
    code = COLLECTOR.read_text(encoding="utf-8")
    assert "calibration=f\"vv{VV_TARGET_THRESHOLD_DB:g}db\"" in code, (
        "the SAR reading must name the threshold that produced it"
    )
