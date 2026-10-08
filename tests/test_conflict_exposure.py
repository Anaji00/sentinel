"""Observations tagged by conflict zone, and instruments ranked by zone exposure."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.conflict_zones import REGION_ZONES, ZONES, region_zone_tags  # noqa: E402


def test_every_zone_region_maps_back_to_its_zone():
    for z in ZONES:
        for r in z.regions:
            assert z.zone_id in REGION_ZONES[r]


def test_region_tags():
    assert region_zone_tags("Persian Gulf") == ["zone:gulf_hormuz"]
    assert region_zone_tags("Black Sea") == ["zone:ukraine"]
    assert region_zone_tags("North Sea") == []
    assert region_zone_tags(None) == []


def test_enrichment_tags_every_written_event_by_region():
    src = (ROOT / "services/enrichment/main.py").read_text(encoding="utf-8")
    loop = src[src.index("for e in batch_to_write:"):]
    loop = loop[:loop.index("apply_display_text(e)")]
    assert "region_zone_tags(getattr(e, \"region\", None))" in loop


def _route():
    import importlib
    return importlib.import_module("services.api_gateway.routes.conflict_zones")


def test_exposure_score_weights_status_then_attention():
    m = _route()
    active = m.exposure_score([m.STATUS_WEIGHT["active"]], 0, 0)
    escalating = m.exposure_score([m.STATUS_WEIGHT["active-escalating"]], 0, 0)
    assert escalating > active
    assert m.exposure_score([0.6], 50, 0) > m.exposure_score([0.6], 0, 0)
    # Log scale: a hundred mentions do not outweigh a second active zone.
    assert m.exposure_score([0.6, 0.6], 0, 0) > m.exposure_score([0.6], 100, 0) - 0.6


def test_exposure_route_is_registered_before_nothing_shadows_it():
    src = (ROOT / "services/api_gateway/routes/conflict_zones.py").read_text(encoding="utf-8")
    assert '@router.get("/conflict-zones/exposure")' in src
    assert '@router.get("/conflict-zones/{' not in src, "a path parameter route would shadow /exposure"


def test_the_map_fetches_it():
    src = (ROOT / "frontend/src/components/GlobalMap.tsx").read_text(encoding="utf-8")
    assert "'/conflict-zones/exposure'" in src
