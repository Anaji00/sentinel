"""Gap 6: identity is the backbone, and it is thin.

Cross-domain correlation is this platform's central premise and is worth
exactly as much as its ability to know two mentions are the same subject. That
layer has been the fragile one throughout this audit: one instrument occupied
three graph nodes, an AS registrant became the named subject of a correlation so
a law firm appeared as an actor, 139,047 wallets were spelled one way and 6,366
another, and entity folds collapsed "SA Recycling" and "AB Recycling" onto
RECYCLING until they were repaired.

Every one of those was a merge, and none recorded who made it or how sure they
were. An alias sits at the top of the resolution order and is permanent, so a
wrong one is a wrong link in every correlation that follows -- and appeared as
an inexplicable claim rather than something traceable to the decision that
caused it.

Provenance is deliberately not consulted at resolution time. An alias that
exists is still authoritative, because being told beats being inferred; the
record exists so a bad merge can be found and attributed afterwards.
"""
import inspect
import json

import pytest

from shared.utils.entity_resolution import (
    ALIAS_PROVENANCE_KEY,
    alias_provenance,
    record_alias,
)


class _Raw:
    def __init__(self):
        self.hashes = {}

    async def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[field] = value

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)


class _Client:
    def __init__(self):
        self.raw = _Raw()


def test_a_merge_can_carry_who_made_it():
    assert "source" in inspect.signature(record_alias).parameters
    assert "confidence" in inspect.signature(record_alias).parameters


@pytest.mark.anyio
async def test_provenance_is_recorded_beside_the_alias():
    c = _Client()
    assert await record_alias(c, "AAPL US", "AAPL", source="human:analyst-1", confidence=1.0)

    prov = await alias_provenance(c, "AAPL US", "AAPL")
    assert prov is not None
    assert prov["source"] == "human:analyst-1"
    assert prov["confidence"] == 1.0
    assert prov["recorded_at"]


@pytest.mark.anyio
async def test_the_alias_still_resolves_without_consulting_provenance():
    """Being told beats being inferred; the record is for afterwards."""
    c = _Client()
    await record_alias(c, "APPLE INC", "AAPL", source="collector", confidence=0.4)
    assert c.raw.hashes["sentinel:entities:alias"]["APPLE INC"] == "AAPL"


@pytest.mark.anyio
async def test_a_low_confidence_merge_is_recorded_as_such():
    c = _Client()
    await record_alias(c, "BP PLC", "BP", source="heuristic:fold", confidence=0.55)
    prov = await alias_provenance(c, "BP PLC", "BP")
    assert prov["confidence"] == pytest.approx(0.55)
    assert prov["source"] == "heuristic:fold"


@pytest.mark.anyio
async def test_confidence_is_bounded_not_trusted():
    c = _Client()
    await record_alias(c, "X CORP", "X", source="s", confidence=42.0)
    assert (await alias_provenance(c, "X CORP", "X"))["confidence"] == 1.0
    await record_alias(c, "Y CORP", "Y", source="s", confidence=-3.0)
    assert (await alias_provenance(c, "Y CORP", "Y"))["confidence"] == 0.0


@pytest.mark.anyio
async def test_an_unrecorded_merge_has_no_provenance_rather_than_a_guess():
    c = _Client()
    assert await alias_provenance(c, "NEVER", "SEEN") is None
    assert await alias_provenance(None, "A", "B") is None


@pytest.mark.anyio
async def test_a_default_call_still_works_and_says_it_is_unattributed():
    """Existing callers must not break; they are recorded as unknown."""
    c = _Client()
    await record_alias(c, "OLD CALLER", "OC")
    assert (await alias_provenance(c, "OLD CALLER", "OC"))["source"] == "unknown"


def test_the_human_route_attributes_the_person():
    import pathlib

    src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "services" / "api_gateway" / "routes" / "attribution.py"
    ).read_text(encoding="utf-8")
    assert 'source=f"human:' in src


def test_provenance_lives_beside_the_aliases_not_inside_them():
    """The alias hash is on the resolution hot path and must stay small."""
    assert ALIAS_PROVENANCE_KEY != "sentinel:entities:alias"
