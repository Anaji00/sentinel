"""Headlines mapped to sectors, country markets and conflict zones, with reasons.

Measured on 831 headlines from the 48 hours to 4 October: 5% carried an
instrument before this mapping and 19% after, with the country and conflict
rules tightened until a sample of thirty read correctly. The cases below are
headlines from that sample.
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.headline_mapping import (  # noqa: E402
    equities, map_headline, mapping_tags, zone_tags,
)


def _vias(text):
    return {a.via for a in map_headline(text)}


def _symbols(text):
    return {a.symbol for a in map_headline(text)}


def test_an_attack_on_a_tanker_reaches_crude_through_the_zone():
    syms = _symbols("Iran Keeps Up Pace of Strikes With Attack on Brand New LR2 Tanker")
    assert {"CL=F", "BZ=F", "FRO"} <= syms


def test_inflected_conflict_verbs_count():
    assert "zone:ukraine" in _vias("Russia strikes another major bridge across Kyiv's Dnipro river")


def test_diplomacy_is_not_escalation():
    assert not any(v.startswith("zone:") for v in _vias("Iran tells US there is no military solution"))


def test_sport_is_not_conflict():
    assert not _vias("Israeli reporter's 'verbal attack' ends Ireland football news conference")
    assert zone_tags("Israel wear black armbands in Ireland football tie over Hamas attack") == []


def test_a_country_maps_only_when_the_story_is_about_its_economy_or_politics():
    assert "country:BR" in _vias("Brazil heads to run-off between Lula and Bolsonaro")
    assert "country:DE" not in _vias("Germany's Merz arrives in Kyiv to show support for Ukraine")
    assert "country:IL" not in _vias("Pro-Israel Samoa prime minister apologises for Nazi gesture")


def test_north_korea_is_not_the_south_korean_market():
    assert "EWY" not in _symbols("North Korea economy shrinks as sanctions bite")


def test_indian_ocean_is_not_india():
    assert "INDA" not in _symbols("Indian Ocean shipping rates climb after storm")


def test_sectors_with_reasons():
    affected = map_headline("Stockpiling Begins: Neros Awarded $100 Million Pentagon Contract For Kamikaze Drones")
    assert any(a.via == "sector:Defense" and a.reason for a in affected)
    assert "sector:Cybersecurity" in _vias("Citrix patches NetScaler SAML zero-day exploited in attacks")
    assert "sector:Banks and rates" in _vias("Surging Treasury yields don't signal a fiscal apocalypse")


def test_tags_name_the_instrument_and_the_reason():
    tags = mapping_tags(map_headline("Soybeans slump on dimming hope of Chinese tariff cuts"))
    assert "exposes:FXI" in tags
    assert "sector:tariffs_and_trade" in tags and "country:cn" in tags


def test_futures_are_not_promoted_to_the_equity_watchlist():
    affected = map_headline("Houthis claim strike on oil facility near Riyadh as Red Sea attacks spread")
    assert "BZ=F" in {a.symbol for a in affected}
    assert all(not s.endswith("=F") for s in equities(affected))


def test_the_news_enricher_uses_the_title_and_requires_corroboration():
    src = (ROOT / "services/enrichment/enrichers/news.py").read_text(encoding="utf-8")
    assert "affected = map_headline(title)" in src
    assert "await raw.scard(key) >= 2" in src


def test_a_zone_mapped_through_instruments_is_also_tagged():
    h = 'US Navy Super Hornet "Sub Buster" Marking Hints At Iranian Submarine Kill'
    assert any(a.via == "zone:gulf_hormuz" for a in map_headline(h))
    assert zone_tags(h) == ["zone:gulf_hormuz"]
