"""A name match has to say something, and the list behind it has to be right and stay loaded."""
import asyncio
import json

import pytest

from services.enrichment import ofac_sync
from shared.utils import sanctions as sn

LIST = sn.SANCTIONED_KEYWORDS + [
    "alexandra", "brilliance", "ever bright 88", "akademik cherskiy",
    "unistream commercial bank jsc", "ignatova, anastasia mikhailovna", "bank melli iran",
]


@pytest.fixture(autouse=True)
def _list_in_force():
    sn.rebuild_sanctions_from_list(LIST)
    sn.set_vessel_index({})
    yield
    sn.rebuild_sanctions_from_list(sn.SANCTIONED_KEYWORDS)
    sn.set_vessel_index({})


def strength(name: str) -> str:
    return sn.sanctions_strength(sn.check_sanctions(name, ""))


def test_a_synced_single_word_counts_only_as_the_whole_name() -> None:
    assert strength("ALEXANDRA") == "listed"
    assert strength("ALEXANDRA MAERSK") == ""
    assert strength("STRAITS BRILLIANCE") == ""


def test_a_name_contained_in_an_entry_is_not_a_match() -> None:
    """token_set_ratio gave these 100."""
    assert strength("ANASTASIA") == ""
    assert strength("UNISTREAM") == ""
    assert strength("AKADEMIK") == ""
    assert strength("EVER BRIGHT") == ""


def test_real_entries_are_still_caught() -> None:
    assert strength("EVER BRIGHT 88") == "listed"
    assert strength("AKADEMIK CHERSKIY") == "listed"
    assert strength("Vessel owned by Bank Melli Iran") == "listed"


def test_curated_terms_still_match_inside_a_longer_name() -> None:
    assert strength("SOVCOMFLOT SHIPPING LTD") == "listed"
    assert strength("IRGC navy vessel") == "listed"


def test_a_close_spelling_is_still_caught_as_similar() -> None:
    assert strength("AKADEMIK CHERSKY") == "similar"


def test_a_fragment_is_still_not_a_word() -> None:
    assert strength("PACIFIC") == "" and strength("VTBX") == ""


def test_a_spelling_variant_sharing_no_word_is_compared_at_all() -> None:
    """The length window was handed 92 for 0.92 and came back empty."""
    assert sn._fuzzy_candidates(10, sn.FUZZY_MATCH_THRESHOLD), "the window found nothing to compare"
    assert strength("SOVCOMFLOTT") == "similar"


def test_the_flag_state_check_is_unchanged() -> None:
    assert sn.check_sanctions("ALEXANDRA MAERSK", "273000444") == ["sanctions_adjacent_flag_state"]


SDN = "\n".join([
    '1,"GOOD SHIPPING CO",-0- ,"X"',
    '2,"SOME PERSON",individual,"X"',
    '7,"BIG TANKER NAME",vessel,"X"',
])
# Column 0 owns the alias. Alias 7 belongs to company 1 and alias 1 to the person,
# so reading column 1 as the owner gets both of them backwards.
ALT = "\n".join([
    '1,7,"aka","GOOD SHIPPING ALIAS",-0-',
    '2,1,"aka","PERSON ALIAS NAME",-0-',
    '7,2,"fka","OLD TANKER NAME",-0-',
])


def _fake_request(sdn, alt):
    async def fake(_method, url, **_kwargs):
        return sdn if url == ofac_sync.OFAC_SDN_URL else alt
    return fake


def test_aliases_follow_their_owner_in_column_0(monkeypatch) -> None:
    monkeypatch.setattr(ofac_sync, "guarded_request", _fake_request(SDN, ALT))
    got = set(asyncio.run(ofac_sync.fetch_ofac_keywords()))
    assert {"good shipping alias", "old tanker name", "good shipping co", "big tanker name"} <= got
    assert "person alias name" not in got and "some person" not in got


@pytest.mark.parametrize("sdn, alt", [(None, ALT), (SDN, None)])
def test_a_partial_download_returns_nothing_rather_than_half_a_list(monkeypatch, sdn, alt) -> None:
    monkeypatch.setattr(ofac_sync, "guarded_request", _fake_request(sdn, alt))
    assert asyncio.run(ofac_sync.fetch_ofac_keywords()) == []


def test_a_sync_must_look_like_the_real_list() -> None:
    assert sn.sync_is_plausible(13_000, 13_000)
    assert not sn.sync_is_plausible(0, 31)
    assert not sn.sync_is_plausible(4_000, 31)
    assert not sn.sync_is_plausible(6_000, 13_000), "lost over half overnight"


class _Raw:
    def __init__(self, stored=None) -> None:
        self.stored = stored
        self.published = 0

    async def get(self, _key):
        return self.stored

    async def set(self, _key, value):
        self.stored = value

    async def publish(self, *_args):
        self.published += 1


def _sync(monkeypatch, fetched, raw) -> bool:
    async def fetch():
        if isinstance(fetched, Exception):
            raise fetched
        return fetched, {}

    async def redis():
        return type("R", (), {"raw": raw})()

    monkeypatch.setattr(ofac_sync, "fetch_ofac_lists", fetch)
    monkeypatch.setattr(ofac_sync, "get_redis", redis)
    return asyncio.run(ofac_sync.sync_once())


@pytest.mark.parametrize("fetched", [[], RuntimeError("timeout")])
def test_a_failed_sync_keeps_the_list_in_force(monkeypatch, fetched) -> None:
    big = [f"distinct entity number {i}" for i in range(6000)]
    sn.rebuild_sanctions_from_list(sn.SANCTIONED_KEYWORDS + big)
    assert _sync(monkeypatch, fetched, _Raw()) is False
    assert len(sn._active_keywords) >= 6000


def test_a_process_with_only_the_built_in_terms_loads_the_stored_list(monkeypatch) -> None:
    sn.rebuild_sanctions_from_list(sn.SANCTIONED_KEYWORDS)
    raw = _Raw(json.dumps({"keywords": sn.SANCTIONED_KEYWORDS + ["stored entity name"]}))
    assert _sync(monkeypatch, [], raw) is False
    assert strength("STORED ENTITY NAME") == "listed"


def test_a_good_sync_replaces_the_list_and_records_when(monkeypatch) -> None:
    fresh = [f"fresh entity number {i}" for i in range(6000)]
    raw = _Raw()
    assert _sync(monkeypatch, fresh, raw) is True
    assert strength("FRESH ENTITY NUMBER 7") == "listed"
    assert raw.published == 1
    assert json.loads(raw.stored)["synced_at"]


SDN_VESSELS = "\n".join([
    '10,"WHALES DIAMOND","vessel","IRAN",-0-,-0-,"Tanker",-0-,-0-,"Panama",-0-,"Vessel Registration Identification IMO 9187629; MMSI 470132000."',
    '11,"HAWK","vessel","IRAN",-0-,-0-,"Tanker",-0-,-0-,"Panama",-0-,"Vessel Registration Identification IMO 9000001."',
    '12,"SOME COMPANY LTD",-0- ,"IRAN",-0-,-0-,-0-,-0-,-0-,-0-,-0-,"IMO 7777777."',
])
ALT_VESSELS = '10,5,"aka","DOVE",-0-\n12,6,"aka","LILY",-0-'


def test_the_index_reads_vessels_only() -> None:
    index = sn.build_vessel_index(SDN_VESSELS, ALT_VESSELS)
    assert index["imo"] == {"9187629": "whales diamond", "9000001": "hawk"}
    assert index["mmsi"] == {"470132000": "whales diamond"}
    assert index["names"] == ["dove", "hawk"], "a company's alias is not a ship's name"


def test_a_ship_is_identified_by_number_whatever_it_calls_itself() -> None:
    sn.set_vessel_index(sn.build_vessel_index(SDN_VESSELS, ALT_VESSELS))
    assert strength_of("SPEARFISH ANN 2", mmsi="470132000") == "identified"
    assert strength_of("ANY NAME", mmsi="111111111", imo="9187629") == "identified"
    assert strength_of("", mmsi="470132000") == "identified", "a ship need not report a name"
    assert strength_of("OTHER", mmsi="111111111", imo="1111111") == ""


def test_a_short_listed_name_is_only_a_name_and_only_for_a_ship() -> None:
    sn.set_vessel_index(sn.build_vessel_index(SDN_VESSELS, ALT_VESSELS))
    flags = sn.check_sanctions("HAWK", "111111111")
    assert sn.sanctions_strength(flags) == "similar"
    assert "sanctioned_ofac" not in flags, "a shared name is not a sanctions claim"
    assert sn.check_sanctions("HAWK", "") == [], "a word in the news is not a ship"
    assert sn.check_sanctions("BLACK HAWK", "111111111") == [], "whole name only"


def test_an_imo_is_not_read_as_a_country() -> None:
    """It went in the MMSI slot, so 422xxxx read as Iranian-flagged."""
    assert sn.check_sanctions("PLAIN SHIP", "111111111", imo="4221234") == []


def strength_of(name: str, mmsi: str = "", imo: str = "") -> str:
    return sn.sanctions_strength(sn.check_sanctions(name, mmsi, imo=imo))
