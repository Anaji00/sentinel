"""A collector that measures itself has to tell the measurement what it saw.

`CollectorMetrics` exists for one failure: a collector that connects
successfully and then receives nothing. Its own docstring says so -- "no
exception, no restart, a healthy heartbeat, and an empty panel that looks
identical to a quiet market". It offers `.ingested()` to record arrivals and
`.watch_for_starvation()` to shout when they stop.

Measured on the running deployment, eleven collectors construct it and ten never
called `.ingested()`. Two consequences, both live:

  - `collector_ingested_total` and `collector_last_success_epoch` sat at 0 for
    ten of eleven collectors while they demonstrably produced --
    collector-crypto had published 11,548 messages and reported both as zero.
    A freshness reader taking `now - last_success_epoch` gets 56 years.

  - collector-radar started the starvation watcher without ever reporting an
    arrival, so `_ingested` stayed 0 and the watcher took its first branch
    permanently:

        collector-radar has received NOTHING from alpaca in 360s despite a
        successful connection. The credential may be inactive...

    logged at ERROR, while the same process was evaluating 11,672 symbols a
    poll. A starvation alarm that is always on is worse than no alarm: it is
    the loudest line the platform emits, it blames the operator's credentials,
    and it is the one that would be ignored on the day a feed actually died.

This is the contract that keeps the two halves together. The allowlist is the
debt, written down and named rather than remembered.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COLLECTORS = ROOT / "services"

# Collectors that construct CollectorMetrics and do not yet report arrivals.
#
# Each needs the metrics object threaded to wherever its data actually lands,
# which is a different place in each one -- that is why collector-radar's was
# never wired: `metrics` was local to main() and the data arrives in
# poll_alpaca_snapshots. Shrink this list; do not grow it.
NOT_YET_REPORTING = {
    # Retired. The cyber domain was withdrawn and its container has not run
    # since; wiring telemetry into a collector nobody starts would be work
    # that can never be observed to be right. It stays named here rather than
    # removed so the exemption is visible if the domain ever comes back.
    "collector-cyber",
}


def _collector_mains():
    return sorted(p / "main.py" for p in COLLECTORS.glob("collector-*") if (p / "main.py").exists())


def _uses(path: Path):
    src = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return {}, src
    called = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            called.add(node.func.attr)
    return called, src


def test_the_scan_can_see_its_subject():
    mains = _collector_mains()
    assert len(mains) >= 8, f"only {len(mains)} collector entrypoints found"
    # collector-ais is the reference implementation: it reports arrivals and
    # watches for their absence. If this stops being true the scan is broken,
    # not the collector.
    ais = COLLECTORS / "collector-ais" / "main.py"
    called, src = _uses(ais)
    assert "ingested" in called, "the reference collector no longer reports arrivals"
    assert "watch_for_starvation" in src, "the reference collector no longer watches for starvation"


@pytest.mark.parametrize("path", _collector_mains(), ids=lambda p: p.parent.name)
def test_a_collector_that_watches_for_starvation_reports_its_arrivals(path: Path):
    """Starting the watcher without feeding it produces a permanent false alarm."""
    called, src = _uses(path)
    if "watch_for_starvation" not in src:
        pytest.skip("does not run the starvation watcher")
    assert "ingested" in called, (
        f"{path.parent.name} starts watch_for_starvation() and never calls "
        "metrics.ingested(), so `_ingested` stays 0 and the watcher reports "
        "'has received NOTHING ... the credential may be inactive' for the life "
        "of the process, whatever is actually arriving."
    )


@pytest.mark.parametrize("path", _collector_mains(), ids=lambda p: p.parent.name)
def test_a_collector_reports_what_it_ingests_or_is_named_as_owing_it(path: Path):
    name = path.parent.name
    called, src = _uses(path)
    if "CollectorMetrics" not in src:
        pytest.skip("does not use CollectorMetrics")
    if "ingested" in called:
        assert name not in NOT_YET_REPORTING, (
            f"{name} now reports arrivals -- remove it from NOT_YET_REPORTING "
            "so the list keeps meaning what it says."
        )
        return
    assert name in NOT_YET_REPORTING, (
        f"{name} constructs CollectorMetrics and never calls .ingested(), so "
        "collector_ingested_total and collector_last_success_epoch stay at 0 "
        "while it produces. Wire it, or add it to NOT_YET_REPORTING."
    )


def test_the_allowlist_names_real_collectors():
    """A stale exemption is a permission nobody reviews."""
    present = {p.parent.name for p in _collector_mains()}
    stale = sorted(NOT_YET_REPORTING - present)
    assert not stale, f"NOT_YET_REPORTING names collectors that no longer exist: {stale}"
