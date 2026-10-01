"""Counting a failure into a dict nothing reads is not observability.

Measured across the tree 2026-09-20:

    files calling swallowed/dropped       73
    files importing heartbeat_line         2   base.py, enrichment/main.py

    services that count:                  16
    services with any surface for it:      2

So fourteen services -- api_gateway, correlation, telemetry-worker, reasoning
and every collector -- record quiet failures into a process-local dictionary
that nothing ever reads. The 23 files under `shared/` are the sharpest part:
shared utilities called from inside all of them record failures that are
invisible in fourteen of the sixteen.

This is the defect the module was written to end, reproduced at fourteen times
the scale, and `quiet_failures.heartbeat_line` names it in its own docstring:
"`snapshot()` existed from the day this module was written and nothing ever
called it."

The fix rides the universal heartbeat for the same stated reason
`liveness_flush` does: every service already runs that loop, and a surface each
service has to remember to wire is one more mechanism nobody wires.
"""

import asyncio
import json
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import quiet_failures as qf  # noqa: E402
from shared.utils.heartbeat import (  # noqa: E402
    HEARTBEAT_SUPPRESSED_LIMIT,
    _suppressed_digest,
    get_all_heartbeats_status,
    touch_heartbeat,
)


@pytest.fixture(autouse=True)
def _clean():
    qf.reset()
    yield
    qf.reset()


class _Raw:
    def __init__(self):
        self.store = {}

    async def set(self, key, value, ex=None):
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)


# ── the digest ───────────────────────────────────────────────────────────────


def test_a_clean_process_adds_nothing_to_its_heartbeat():
    assert _suppressed_digest() == {}


def test_the_digest_names_each_site_and_its_count():
    qf.dropped("site.alpha", "x")
    qf.dropped("site.alpha", "x")
    qf.swallowed("site.beta", ValueError("y"))
    d = _suppressed_digest()
    assert d["counts"] == {"site.alpha": 2, "site.beta": 1}
    assert d["sites"] == 2 and d["total"] == 3


def test_the_digest_is_bounded():
    """It is a Redis value read on every health poll."""
    for i in range(HEARTBEAT_SUPPRESSED_LIMIT + 10):
        qf.dropped(f"site.{i}", "x")
    assert len(_suppressed_digest()["counts"]) == HEARTBEAT_SUPPRESSED_LIMIT


def test_truncation_is_visible_rather_than_silent():
    """`sites` is the true total, so a reader can tell the list was cut."""
    for i in range(HEARTBEAT_SUPPRESSED_LIMIT + 10):
        qf.dropped(f"site.{i}", "x")
    d = _suppressed_digest()
    assert d["sites"] == HEARTBEAT_SUPPRESSED_LIMIT + 10
    assert d["sites"] > len(d["counts"])


def test_the_loudest_sites_are_the_ones_kept():
    qf.dropped("quiet", "x")
    for _ in range(50):
        qf.dropped("loud", "x")
    for i in range(HEARTBEAT_SUPPRESSED_LIMIT):
        qf.dropped(f"filler.{i}", "x")
    assert "loud" in _suppressed_digest()["counts"]


def test_a_broken_counter_never_takes_the_heartbeat_down(monkeypatch):
    monkeypatch.setattr(
        "shared.utils.heartbeat.quiet_snapshot",
        lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert _suppressed_digest() == {}


# ── it reaches the payload, and the reader ───────────────────────────────────


def test_the_heartbeat_payload_carries_the_counters():
    qf.dropped("site.alpha", "x")
    raw = _Raw()
    asyncio.run(touch_heartbeat(raw, "probe"))
    payload = json.loads(raw.store["sentinel:heartbeat:probe"])
    assert payload["suppressed"]["counts"] == {"site.alpha": 1}


def test_the_counters_sit_beside_metadata_not_inside_it():
    """A caller publishing its own "suppressed" key would clobber one of them."""
    qf.dropped("site.alpha", "x")
    raw = _Raw()
    asyncio.run(touch_heartbeat(raw, "probe", metadata={"suppressed": "mine"}))
    payload = json.loads(raw.store["sentinel:heartbeat:probe"])
    assert payload["metadata"]["suppressed"] == "mine"
    assert payload["suppressed"]["counts"] == {"site.alpha": 1}


def test_the_status_reader_forwards_them():
    """Forwarding `metadata` alone would drop a sibling key."""
    qf.dropped("site.alpha", "x")
    raw = _Raw()
    asyncio.run(touch_heartbeat(raw, "probe"))
    out = asyncio.run(get_all_heartbeats_status(raw, custom_components=["probe"]))
    row = out["components"]["probe"] if "components" in out else out["probe"]
    assert row["suppressed"]["counts"] == {"site.alpha": 1}


# ── the measurement that motivated it ────────────────────────────────────────


def test_the_recorders_are_used_far_more_widely_than_any_surface():
    """If this ever stops being true, the heartbeat carrier is redundant."""
    counting, surfaced = set(), set()
    for folder in ("services", "shared"):
        for path in (ROOT / folder).rglob("*.py"):
            if "__pycache__" in path.parts or "test" in path.name:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            if "from shared.utils.quiet_failures import" in text:
                counting.add(path)
            if re.search(r"\bheartbeat_line\b", text):
                surfaced.add(path)
    assert len(counting) > 10 * len(surfaced), (
        f"{len(counting)} files count, {len(surfaced)} have a local surface"
    )
