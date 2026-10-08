"""Global Fishing Watch events and VIIRS boat detections, parsed without credentials."""

import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.vessel_feeds import (  # noqa: E402
    gfw_event, in_watched_area, vbd_csv_lines, vbd_detections, zone_counts,
)

HORMUZ = {"lat": 26.4, "lon": 56.4}
MID_ATLANTIC = {"lat": 30.0, "lon": -40.0}


def _gap(pos, intentional=True, hours=20.0):
    return {
        "id": "g1", "type": "gap", "start": "2026-10-01T00:00:00Z", "position": pos,
        "vessel": {"id": "v1", "ssvid": "422000001", "name": "SHADOW ONE", "flag": "IRN", "type": "bunker"},
        "gap": {"intentionalDisabling": intentional, "durationHours": hours},
    }


def test_an_intentional_gap_in_a_zone_becomes_a_dark_vessel():
    f = gfw_event(_gap(HORMUZ))
    assert f["type"] == "vessel_dark"
    assert f["entity"]["id"] == "422000001"
    assert "intentional_disabling" in f["tags"]
    assert f["anomaly_score"] == 0.7


def test_a_short_unintentional_gap_is_coverage_not_behaviour():
    assert gfw_event(_gap(HORMUZ, intentional=False, hours=3)) is None


def test_events_outside_every_watched_area_are_dropped():
    assert gfw_event(_gap(MID_ATLANTIC)) is None
    assert not in_watched_area(30.0, -40.0)


def test_an_encounter_becomes_ship_to_ship():
    entry = {
        "id": "e1", "type": "encounter", "start": "2026-10-01T00:00:00Z", "position": HORMUZ,
        "vessel": {"ssvid": "422000001", "name": "SHADOW ONE"},
        "encounter": {"vessel": {"ssvid": "538000002", "name": "PARTNER"}, "durationHours": 5.5},
    }
    f = gfw_event(entry)
    assert f["type"] == "vessel_sts"
    assert "PARTNER" in f["headline"]


def test_other_event_types_are_ignored():
    assert gfw_event({"type": "port_visit", "position": HORMUZ, "vessel": {"ssvid": "1"}}) is None


CSV = (
    "id_Key,Date_Mscan,Lat_DNB,Lon_DNB,Rad_DNB,QF_Detect\n"
    "a,2026-10-03 22:10:00,26.40,56.40,12.5,1\n"   # vessel in Hormuz
    "b,2026-10-03 22:10:00,26.41,56.41,300.0,4\n"  # gas flare
    "c,2026-10-03 22:10:00,26.42,56.42,9.0,8\n"    # recurring light (platform)
    "d,2026-10-03 22:10:00,30.00,-40.00,10.0,1\n"  # outside every area
    "e,2026-10-03 22:10:00,24.30,120.00,7.0,2\n"   # weak vessel, Taiwan Strait
)


def test_vbd_keeps_lit_vessels_in_watched_areas_only():
    dets = list(vbd_detections(vbd_csv_lines(CSV)))
    assert [(d["lat"], d["qf"]) for d in dets] == [(26.4, 1), (24.3, 2)]
    assert zone_counts(dets) == {"gulf_hormuz": 1, "taiwan": 1}


def test_vbd_columns_are_matched_case_insensitively():
    text = "lat_dnb,LON_DNB,qf_detect\n26.4,56.4,1\n"
    assert len(list(vbd_detections(vbd_csv_lines(text)))) == 1


def _feeds():
    src = ROOT / "services" / "collector-sar" / "feeds.py"
    spec = importlib.util.spec_from_file_location("collector_sar_feeds", src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_placeholders_count_as_unset(monkeypatch):
    m = _feeds()
    monkeypatch.setenv("GFW_API_TOKEN", "your_gfw_api_token")
    assert not m._configured("GFW_API_TOKEN")
    monkeypatch.setenv("GFW_API_TOKEN", "a-real-looking-token")
    assert m._configured("GFW_API_TOKEN")
    monkeypatch.delenv("GFW_API_TOKEN")
    assert not m._configured("GFW_API_TOKEN")


def test_a_gfw_gap_validates_as_a_normalized_event():
    m = _feeds()
    event = m._normalized(gfw_event(_gap(HORMUZ)))
    assert event.type.value == "vessel_dark"
    assert event.source == "gfw"
    assert any(t.startswith("exposes:") for t in event.tags), "a bunker vessel at Hormuz bears on crude"


def test_gfw_passes_through_enrichment():
    from services.enrichment.enrichers.tradfi import PASSTHROUGH_SOURCES
    assert "gfw" in PASSTHROUGH_SOURCES


def test_env_example_documents_the_keys():
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    for key in ("GFW_API_TOKEN", "EOG_USERNAME", "EOG_PASSWORD", "EOG_CLIENT_SECRET"):
        assert f"{key}=" in text
