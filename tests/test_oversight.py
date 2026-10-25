"""Local surveillance oversight (CCOPS) laws: sourced, and matched to the agencies they bind."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import civic_law as cl  # noqa: E402
from shared.utils import oversight as ov  # noqa: E402


def test_every_law_has_its_text_and_a_source() -> None:
    assert len(ov.OVERSIGHT_LAWS) >= 20
    for law in ov.OVERSIGHT_LAWS:
        assert law.law_url.startswith("https://") and law.source.startswith("https://"), law.place
        assert len(law.state) == 2 and law.level in ("city", "county", "transit")
    assert len({(x.place, x.state) for x in ov.OVERSIGHT_LAWS}) == len(ov.OVERSIGHT_LAWS)


def test_a_city_law_binds_the_city_own_agencies_only() -> None:
    assert ov.law_for_agency("Oakland Police Department", "CA").place == "Oakland"
    assert ov.law_for_agency("City of Davis Police Department", "CA").place == "Davis"
    assert ov.law_for_agency("Metropolitan Nashville Police Department", "TN").place == "Nashville"
    for name, st in [("Oakland County Sheriff's Office", "MI"), ("Oakland Police Department", "MI"),
                     ("East Palo Alto Police Department", "CA"), ("South San Francisco Police Department", "CA"),
                     ("University of California Berkeley Police Department", "CA"),
                     ("San Diego Unified Police Department", "CA"), ("Boston Regional Intelligence Center", "MA"),
                     ("U.S. Customs and Border Protection - Detroit Metro Airport", "MI"),
                     ("University of Illinois Urbana-Champaign Police Department", "IL"), ("", "CA")]:
        assert ov.law_for_agency(name, st) is None, name


def test_county_laws_bind_county_agencies_and_portals_map_by_place() -> None:
    assert ov.law_for_agency("Santa Clara County Sheriff's Department", "CA").level == "county"
    assert ov.law_for_agency("Santa Clara Police Department", "CA") is None
    assert ov.law_for_portal("Oakland", None, "PD", "CA").place == "Oakland"
    assert ov.law_for_portal(None, "Santa Clara", "SD", "CA").place == "Santa Clara County"
    assert ov.law_for_portal("Fresno", None, "PD", "CA") is None


def test_the_records_request_asks_for_what_the_law_requires() -> None:
    law = ov.law_for_portal("Oakland", None, "PD", "CA")
    out = cl.records_request("Oakland PD", "CA", [], "2026-10-15", [ov.records_ask(law)])
    assert "surveillance use policy" in out["asks"][-1] and "Oakland" in out["asks"][-1]
    assert len(cl.records_request("Fresno PD", "CA", [], "2026-10-15")["asks"]) == len(out["asks"]) - 1
    row = ov.summary(law)
    assert row and "aliases" not in row and row["ask_for"] == ov.ASK_FOR and ov.summary(None) is None
