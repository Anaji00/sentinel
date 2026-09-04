"""
tests/test_no_undefined_names.py

Nine undefined names were live in the tree, each a NameError waiting on
whichever code path reached it:

    base.py:997,1005          logger              (prediction dedupe)
    base.py:1872              is_valid_primary_equity
    quant_trading_engine:459  CorrelationCluster
    quant_trading_engine:462  AlertTier
    collector-ais:201,203     metrics
    tradfi.py:1447            FilingData
    tradfi.py:1492            ThirteenFData

None of them stopped a service. `FilingData` and `ThirteenFData` exist in the
models and were simply never imported, so the SEC filing and 13F paths raised on
every execution. The AIS one sat in the hot path directly above a broad
`except Exception`, so messages still reached Kafka while every single one took
the failure path and no ingest metric was ever recorded. Two were introduced by
this audit's own prediction-dedupe change, in a class that uses `self.logger`.

The suite passed with all nine present, because a test only catches a NameError
on a line it executes, and these lay on error paths, seldom-taken branches, and
code nothing calls yet.

pyflakes reads every line without running any of them, which is exactly the
coverage a test suite cannot offer.
"""

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _undefined_names():
    result = subprocess.run(
        [sys.executable, "-m", "pyflakes", "services", "shared"],
        cwd=ROOT, capture_output=True, text=True,
    )
    return [ln for ln in result.stdout.splitlines() if "undefined name" in ln]


def test_no_undefined_names_anywhere_in_the_tree():
    pytest.importorskip("pyflakes")
    findings = _undefined_names()
    assert not findings, (
        "undefined names will raise NameError when their path executes:\n  "
        + "\n  ".join(findings)
    )


def test_the_scan_actually_runs():
    """A scanner that errors out prints nothing and would pass vacuously --
    the same failure this audit hit twice with regex-based guards."""
    pytest.importorskip("pyflakes")
    result = subprocess.run(
        [sys.executable, "-m", "pyflakes", "services", "shared"],
        cwd=ROOT, capture_output=True, text=True,
    )
    assert result.returncode in (0, 1), f"pyflakes failed to run: {result.stderr[:200]}"
    assert (ROOT / "services").is_dir()
