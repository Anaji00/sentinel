"""
tests/test_window_measurement.py

Three measurements in this audit were wrong for the same class of reason, and
this tool exists so the fourth is not.

  * A window opened for forty minutes closed twelve hours later. The laptop had
    suspended from 09:52 to 21:23 with no inference at all, and a first-and-last
    reading divided real work by twelve hours of wall clock.

  * Two builds were compared across a period when the model server was wedged.
    Nothing could run under either configuration; the difference was attributed
    to the code.

  * A latency mean was computed over HTTP 500s, because the parser took the
    duration column and ignored the status column. Failed requests are fast --
    35s and 41s, against a 157s baseline -- and reported as a 22% improvement.

The shape they share: an instrument that cannot see returns the same answer as
an instrument that sees nothing there.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.measure_window import _seconds, _DURATION, _STATUS  # noqa: E402


@pytest.mark.parametrize(
    "token,expected",
    [
        ("4m59s", 299.0),
        ("157.4s", 157.4),
        ("35.291132406s", 35.291132406),
        ("2.96983ms", 0.00296983),
        ("60.183µs", 6.0183e-05),
        ("nonsense", None),
    ],
)
def test_every_duration_format_ollama_emits(token, expected):
    """GIN switches unit by magnitude, so a parser that handles only seconds
    silently drops the sub-second and multi-minute rows -- which are exactly the
    healthchecks and the pathological requests."""
    got = _seconds(token)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_a_failed_request_is_not_counted():
    """The 22% gain that was four errors."""
    failed = '[GIN] 2026/08/31 - 07:15:26 | 500 | 35.291132406s | 172.18.0.27 | POST "/api/generate"'
    assert _STATUS.search(failed).group(1) == "500"


def test_a_successful_request_is():
    ok = '[GIN] 2026/08/31 - 07:14:48 | 200 |         1m57s | 172.18.0.26 | POST "/api/generate"'
    assert _STATUS.search(ok).group(1) == "200"
    assert _seconds(_DURATION.search(ok).group(1)) == pytest.approx(117.0)


def test_a_suspend_is_subtracted_not_averaged():
    """11.5 hours of a sleeping laptop divided into 37 minutes of work is not a
    throughput figure."""
    source = (ROOT / "scripts/measure_window.py").read_text(encoding="utf-8")
    assert "SUSPEND_FACTOR" in source
    assert "active time" in source
    assert "/ (active / 60)" in source, "throughput is still divided by wall clock"


def test_attribution_comes_from_the_services_not_the_access_log():
    """Container IPs are reassigned on restart, so attributing a window that
    spans a deploy mixes services together."""
    source = (ROOT / "scripts/measure_window.py").read_text(encoding="utf-8")
    assert "sentinel:metrics:" in source
    assert "ollama_calls_total" in source


def test_the_tool_explains_the_measurements_it_replaces():
    """A tool with no rationale gets replaced by the obvious version that has
    all three bugs again."""
    source = (ROOT / "scripts/measure_window.py").read_text(encoding="utf-8")
    for reason in ("suspend", "wedged", "status column"):
        assert reason in source
