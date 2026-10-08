"""Conflict zones: status from live intensity, warnings placed by coordinates."""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.conflict_zones import (  # noqa: E402
    PRIOR_ACTIVE, PRIOR_FLASHPOINT, ZONES, ZONES_BY_ID, assess_status,
    classify_warning, daily_volumes, gdelt_query, keyword_regex,
    parse_coordinates, zone_for_point, zones_for_text,
)
from shared.utils.osint_bridges import region_tag  # noqa: E402


def test_zone_ids_are_unique_and_priors_known():
    assert len(ZONES_BY_ID) == len(ZONES)
    assert {z.prior for z in ZONES} <= {PRIOR_ACTIVE, PRIOR_FLASHPOINT}


def test_every_zone_says_why_and_what_it_moves():
    for z in ZONES:
        assert z.drivers, z.zone_id
        assert z.ripple, z.zone_id
        assert all(r.why for r in z.ripple), z.zone_id
        assert z.keywords, z.zone_id


def test_zone_regions_are_regions_the_platform_emits():
    import json
    names = {f["properties"]["name"] for f in json.loads(
        (ROOT / "regions.geojson").read_text(encoding="utf-8"))["features"]}
    for z in ZONES:
        for r in z.regions:
            assert r in names, f"{z.zone_id}: {r} is not a classify_region name"


def test_a_war_always_in_the_news_is_not_escalating_for_being_reported():
    assert assess_status(PRIOR_ACTIVE, 1000, 1000)["status"] == "active"


def test_a_measured_rise_is_called_out():
    out = assess_status(PRIOR_ACTIVE, 2000, 1000)
    assert out == {"status": "active-escalating", "trend": "rising", "ratio": 2.0}


def test_a_flashpoint_escalates_only_on_a_rise_with_volume():
    assert assess_status(PRIOR_FLASHPOINT, 20, 5)["status"] == "watch"
    assert assess_status(PRIOR_FLASHPOINT, 300, 100)["status"] == "escalating"


def test_no_baseline_is_unmeasured_not_quiet():
    out = assess_status(PRIOR_FLASHPOINT, 40, 0.0)
    assert out["trend"] == "unmeasured" and out["ratio"] is None


def test_daily_volumes_splits_the_last_day_from_the_mean_before_it():
    hourly = [(f"20261001T{h:02d}0000Z", 1.0) for h in range(24)]
    hourly += [(f"20261002T{h:02d}0000Z", 3.0) for h in range(24)]
    assert daily_volumes(hourly) == (72.0, 24.0)
    assert daily_volumes([]) == (0.0, 0.0)


def test_nga_coordinates_in_both_spellings():
    text = "AREA BOUND BY 13-05.00N 066-43.00W, 45-05N 033-33E."
    assert parse_coordinates(text) == [(13.0833, -66.7167), (45.0833, 33.55)]


def test_a_black_sea_mine_warning_lands_in_the_ukraine_zone():
    text = "BLACK SEA.\nUKRAINE.\nDNC 10.\nMINES REPORTED IN AREA WITHIN FIVE MILES\nOF 45-07.10N 030-09.70E.\n"
    out = classify_warning(text)
    assert out["zone_id"] == "ukraine"
    assert out["military"] is True
    assert out["area"] == "BLACK SEA"


def test_overlapping_boxes_take_the_most_specific_zone():
    # Bahrain is inside both the Gulf box and the larger Red Sea box.
    assert zone_for_point(26.0, 50.5) == "gulf_hormuz"
    assert zone_for_point(12.6, 43.3) == "red_sea"
    assert zone_for_point(-40.0, -40.0) is None


def test_headlines_match_whole_words_only():
    assert "taiwan" in zones_for_text("PLA drills encircle Taiwan")
    assert zones_for_text("Iranian navy seizes tanker") == ["gulf_hormuz"]
    # "Iran" inside "Tirana" is not Iran.
    assert "gulf_hormuz" not in zones_for_text("Protests in Tirana")


def test_postgres_regex_uses_word_boundaries():
    rx = keyword_regex(ZONES_BY_ID["sudan"])
    assert rx.startswith(r"\m(") and rx.endswith(r")\M")
    assert re.search(rx.replace(r"\m", r"\b").replace(r"\M", r"\b"), "Fighting in El Fasher", re.I)


def test_gdelt_query_pairs_a_place_with_a_conflict_term():
    q = gdelt_query(ZONES_BY_ID["red_sea"])
    assert q.startswith('("Red Sea" OR Yemen OR Houthi)')
    assert "missile" in q


def test_ripple_instruments_use_platform_symbols():
    for z in ZONES:
        for sym in z.instruments:
            assert sym == sym.upper() and " " not in sym, (z.zone_id, sym)
    assert region_tag("Strait of Hormuz") == "strait_of_hormuz"


def _conflict_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "news_conflict", ROOT / "services" / "collector-news" / "conflict.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_backoff_starts_at_ten_minutes_and_caps_at_an_hour():
    m = _conflict_module()
    assert m.next_backoff(0) == 600
    assert m.next_backoff(600) == 1200
    assert m.next_backoff(3000) == 3600


def test_a_429_stops_the_cycle_rather_than_reading_as_empty():
    import asyncio

    m = _conflict_module()

    class _Resp:
        status = 429

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class _Session:
        def get(self, *a, **k):
            return _Resp()

    try:
        asyncio.run(m._get_json(_Session(), "https://api.gdeltproject.org/x"))
    except m.RateLimited:
        return
    raise AssertionError("a 429 must raise RateLimited")
