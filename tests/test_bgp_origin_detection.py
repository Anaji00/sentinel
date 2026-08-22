"""Guards the BGP path that had never produced a single row.

The collector emitted a RawEvent for every announcement with `is_hijack` hardcoded
False and `as_name` empty. The enricher drops anything that is neither a hijack
nor a high-value org, so 100% of BGP events were discarded -- no `ripe_ris` row
has ever existed in the events table -- while the unfiltered firehose ran
continuously. Detection now happens at the collector: only an origin-AS change
for an already-known prefix is emitted.
"""
import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_collector():
    spec = importlib.util.spec_from_file_location(
        "cyber_collector", ROOT / "services/collector-cyber/main.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cyber_collector"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def collector():
    m = _load_collector()
    m._BGP_ORIGIN_BASELINE.clear()
    return m


def test_first_sight_is_learning_not_a_hijack(collector):
    assert collector._bgp_origin_change("1.2.3.0/24", "64500") is None


def test_unchanged_origin_emits_nothing(collector):
    collector._bgp_origin_change("1.2.3.0/24", "64500")
    for _ in range(100):
        assert collector._bgp_origin_change("1.2.3.0/24", "64500") is None


def test_origin_change_returns_previous_origin(collector):
    collector._bgp_origin_change("1.2.3.0/24", "64500")
    prev = collector._bgp_origin_change("1.2.3.0/24", "64999")
    assert prev == "64500", "an origin change must surface what it replaced"


def test_baseline_updates_so_a_change_fires_once(collector):
    collector._bgp_origin_change("1.2.3.0/24", "64500")
    assert collector._bgp_origin_change("1.2.3.0/24", "64999") == "64500"
    # The new origin is now known; the same announcement is no longer news.
    assert collector._bgp_origin_change("1.2.3.0/24", "64999") is None


def test_multi_origin_prefix_alerts_once_per_origin_not_per_flap(collector):
    """A prefix announced by several ASes is normal (anycast, multihoming).

    Alerting on every change flagged that churn as an attack: against live RIS
    data it produced 2,882 events from 120 prefixes in twenty minutes, one
    prefix alone accounting for 571 as two ASes oscillated.
    """
    p = "203.0.113.0/24"
    assert collector._bgp_origin_change(p, "64500") is None      # learning
    assert collector._bgp_origin_change(p, "64999") == "64500"   # genuinely new
    # Oscillating between two now-known origins must stay silent.
    for _ in range(50):
        assert collector._bgp_origin_change(p, "64500") is None
        assert collector._bgp_origin_change(p, "64999") is None


def test_a_third_unseen_origin_still_alerts(collector):
    """Suppressing MOAS churn must not suppress a genuinely novel origin."""
    p = "203.0.113.0/24"
    collector._bgp_origin_change(p, "64500")
    collector._bgp_origin_change(p, "64999")
    assert collector._bgp_origin_change(p, "64777") is not None


def test_remembered_origins_per_prefix_are_capped(collector):
    """Bounded per prefix as well as overall, or a churning prefix leaks memory."""
    p = "203.0.113.0/24"
    for i in range(50):
        collector._bgp_origin_change(p, f"as{i}")
    assert len(collector._BGP_ORIGIN_BASELINE[p]) <= collector._MAX_ORIGINS_PER_PREFIX


def test_prefixes_are_tracked_independently(collector):
    collector._bgp_origin_change("10.0.0.0/8", "1")
    collector._bgp_origin_change("20.0.0.0/8", "2")
    assert collector._bgp_origin_change("10.0.0.0/8", "999") == "1"
    assert collector._bgp_origin_change("20.0.0.0/8", "2") is None


def test_baseline_memory_is_bounded(collector):
    """The global table is ~1M prefixes; this runs under a 384M ceiling."""
    cap = collector._BGP_BASELINE_MAX
    for i in range(cap + 5_000):
        collector._bgp_origin_change(f"10.{i // 65536}.{(i // 256) % 256}.{i % 256}/32", "1")
    assert len(collector._BGP_ORIGIN_BASELINE) <= cap, "baseline grew without bound"
