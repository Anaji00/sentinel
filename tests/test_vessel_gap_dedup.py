"""
tests/test_vessel_gap_dedup.py

The maritime dark-vessel detector had the same in-memory deduplication defect as
its aviation twin, and fixing it uncovered two more of the shape that had made
the aviation one so hard to see.

  1. `self._seen_gaps = set()` lives in the process. Every restart empties it and
     the next scan re-reports every vessel already dark as though it had just
     gone silent. In aviation that produced 200 and 262 events in the two hours
     of a deploy against a steady rate of a few, and those records are
     indistinguishable in the database from real ones.

  2. The seen-marker was written in a *different loop* from the one that decides
     to emit. `dedup_key` is bound while scanning; the emission loop iterates
     `zip(anomalous_mmsis, info_results)`. Reading it there records the last
     vessel scanned rather than the one being emitted -- so every alert marks
     the wrong ship, and the right ones re-fire forever. Nothing raises.

  3. The two loops derive the region differently. The emission side applies a
     `or "unknown waters"` fallback for the headline that the scanning side does
     not, so keying on it writes "123:unknown waters" against a lookup for
     "123:None" -- a silent miss on exactly the vessels with no region.

Each of these is invisible at runtime. The first inflates counts, the second and
third disable the deduplication entirely for some or all vessels, and none of
them produces an error.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = (ROOT / "services" / "enrichment" / "gap_detector.py").read_text(encoding="utf-8")


def test_dedup_is_not_process_memory():
    """An in-memory set is empty after every deploy."""
    assert "_seen_gaps" not in SOURCE
    assert 'self._seen_key = "sentinel:maritime:seen_gaps"' in SOURCE


def test_the_marker_survives_a_restart():
    assert "hexists(self._seen_key" in SOURCE
    assert "hset(" in SOURCE


def test_returning_to_normal_clears_the_record():
    """Otherwise a vessel reported once could never be reported again."""
    assert "hdel(self._seen_key" in SOURCE


def test_the_record_expires():
    from services.enrichment.gap_detector import SEEN_GAP_TTL_SEC

    assert SEEN_GAP_TTL_SEC >= 86400, "must outlive a deploy"
    assert SEEN_GAP_TTL_SEC <= 30 * 86400, "must not suppress forever"


def test_the_emission_loop_builds_its_own_key():
    """Reading dedup_key across loops records the last vessel scanned, not the
    one being emitted. It does not raise; it just marks the wrong ship."""
    emit = SOURCE.split("events_to_write.append(event)")[0].split("for mmsi, info_raw in zip(")[-1]
    executable = [
        ln for ln in emit.splitlines()
        if "dedup_key" in ln and not ln.lstrip().startswith("#")
    ]
    assert not executable, f"emission must not reuse the scan loop's key: {executable}"
    assert re.search(r'f"\{mmsi\}:\{data\.get\(.region.\)\}"', SOURCE)


def test_both_loops_derive_the_region_the_same_way():
    """The headline's 'unknown waters' fallback must not leak into the key, or
    the lookup and the write disagree for every region-less vessel."""
    assert 'f"{mmsi}:{reg}"' not in SOURCE, "reg carries a display fallback"
    scan = re.search(r'dedup_key = f"\{mmsi\}:\{(\w+)\}"', SOURCE)
    assert scan and scan.group(1) == "region"


def test_a_store_failure_does_not_suppress_a_real_alert():
    """If Redis is unreachable, reporting a duplicate is the safer error."""
    block = SOURCE.split("hexists(self._seen_key")[1][:300]
    assert "except Exception" in block


def test_the_aviation_twin_has_the_same_shape():
    """These two detectors are copies of one another. A fix applied to one and
    not the other is how this defect survived in the first place."""
    aviation = (ROOT / "services" / "enrichment" / "aviation_gap_detector.py").read_text(encoding="utf-8")
    assert "_seen_gaps" not in aviation
    assert "seen_gaps" in aviation  # the redis key, not the set
