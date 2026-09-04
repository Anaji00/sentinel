"""The SAR process graph must be one the backend will actually run.

The collector asked aggregate_spatial for "array_element", intending to pull raw
pixels back and threshold them locally. With CDSE credentials finally present,
the server answered:

  [500] Internal: Unexpected error during 'aggregate_spatial'
  java.lang.IllegalArgumentException: Unsupported reducer for
  aggregate_spatial: array_element

which is precisely the failure the audit had listed as unverifiable without
credentials -- a process graph the client accepts and the server rejects.
aggregate_spatial exists to collapse pixels, so asking it to hand them back was
the wrong shape of request. Thresholding now happens server-side and the mask is
summed and counted.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "services" / "collector-sar"))

_spec = importlib.util.spec_from_file_location(
    "sar_main", ROOT / "services" / "collector-sar" / "main.py"
)


def _mod():
    if "sar_main" in sys.modules:
        return sys.modules["sar_main"]
    mod = importlib.util.module_from_spec(_spec)
    sys.modules["sar_main"] = mod
    _spec.loader.exec_module(mod)
    return mod


def test_the_rejected_reducer_is_gone():
    source = (ROOT / "services/collector-sar/main.py").read_text(encoding="utf-8")
    executable = "\n".join(
        line for line in source.splitlines() if not line.strip().startswith("#")
    )
    assert 'reducer="array_element"' not in executable


def test_the_reducers_used_are_ones_aggregate_spatial_supports():
    source = (ROOT / "services/collector-sar/main.py").read_text(encoding="utf-8")
    import re
    used = set(re.findall(r'aggregate_spatial\([^)]*reducer="([a-z_]+)"', source))
    assert used, "no aggregate_spatial reducer found"
    assert used <= {"sum", "count", "mean", "max", "min", "median", "sd"}, used


def test_a_bare_number_is_read():
    assert _mod()._as_counts(42) == 42
    assert _mod()._as_counts(42.6) == 43


def test_a_nested_list_is_unwrapped():
    """Backends wrap per geometry, and sometimes again per band."""
    m = _mod()
    assert m._as_counts([17]) == 17
    assert m._as_counts([[17]]) == 17
    assert m._as_counts([[[17.0]]]) == 17


def test_nothing_measurable_reads_as_zero_not_as_a_guess():
    """Zero water pixels is how the caller recognises no acquisition. A wrong
    guess here would report an empty strait instead of a missing image."""
    m = _mod()
    for value in (None, [], [[]], [None], "n/a", float("nan")):
        assert m._as_counts(value) == 0, value


def test_the_threshold_is_the_one_the_detector_defines():
    """The comparison moved server-side; it must still be the same number."""
    source = (ROOT / "services/collector-sar/main.py").read_text(encoding="utf-8")
    assert "VV_TARGET_THRESHOLD_DB" in source
    from sar_detection import VV_TARGET_THRESHOLD_DB
    assert isinstance(VV_TARGET_THRESHOLD_DB, (int, float))
