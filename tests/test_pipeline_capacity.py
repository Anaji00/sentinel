"""Holds the pipeline's intake within what enrichment can actually consume.

Aviation was sweeping every 30 seconds and producing 1,108 events/min on its
own -- more than the entire enrichment tier could process (~720/min) -- so
routine position fixes queued ahead of news, filings and market data, which are
the domains this platform exists to reason about. The enricher already caps
those fixes at 0.15 anomaly and treats them as uninteresting.

Enrichment is also the narrowest point in the pipeline and must be able to scale
horizontally, which Kafka only permits when a topic has partitions to spread and
Docker only permits when a service has no fixed container name.
"""
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
ADSB = (ROOT / "services/collector-adsb/main.py").read_text(encoding="utf-8")


def test_aviation_sweep_is_slow_enough_not_to_starve_other_domains():
    """At 30s this one collector outproduced the whole enrichment tier."""
    m = re.search(r'ADSB_POLL_INTERVAL_SEC["\']?,\s*["\'](\d+)["\']', ADSB)
    assert m, "the ADS-B poll interval is no longer configurable"
    assert int(m.group(1)) >= 120, (
        f"a {m.group(1)}s sweep reproduces the backlog that starved news and filings"
    )


def test_the_sweep_interval_stays_tunable():
    """The right cadence depends on the deployment; it must not be a literal."""
    assert "os.getenv(\"ADSB_POLL_INTERVAL_SEC\"" in ADSB


def test_enrichment_can_be_scaled_horizontally():
    """Docker refuses --scale for a service with a fixed container_name."""
    enrichment = COMPOSE["services"]["enrichment"]
    assert "container_name" not in enrichment, (
        "enrichment cannot be scaled while it has a fixed container name"
    )


def test_enrichment_has_headroom_above_a_single_core():
    """It is CPU-bound: scoring, sanctions matching and payload construction."""
    limits = ((enr := COMPOSE["services"]["enrichment"]).get("deploy") or {}).get("resources", {}).get("limits", {})
    cpus = float(limits.get("cpus", 0))
    assert cpus >= 2.0, f"enrichment capped at {cpus} CPUs remains the pipeline bottleneck"
    assert enr.get("restart"), "enrichment must restart on failure"


def test_dark_flight_detection_survives_the_slower_sweep():
    """Gap detection reads a Redis key held for 24h, so a 5-minute sweep does
    not blind it -- worth pinning, because shortening that TTL would."""
    assert "aircraft:last_seen" in (ROOT / "services/enrichment/enrichers/aviation.py").read_text(encoding="utf-8")
    assert "ex=86400" in (ROOT / "services/enrichment/enrichers/aviation.py").read_text(encoding="utf-8")
