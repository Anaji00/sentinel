"""
tests/test_canonical_entity_ids.py

One spelling per identifier, decided by what the identifier identifies.

primary_entity_id is a single TEXT column carrying six identifier namespaces,
and nothing said how any of them was spelled. tradfi wrote upper-cased tickers,
crypto upper-cased its assets on most paths and not on one, wallet addresses
arrived EIP-55 mixed-case from the RPC, Polymarket slugs were lowercase kebab,
maritime wrote a bare MMSI. Two producers describing the same thing therefore
produced two identities, and a reader comparing with `=` matched or missed
depending on which collector had written the row -- which is how /market-series
came to return a blank chart for symbols that were plainly in the database.

The rule now lives on Entity, the model all nineteen producers already
construct, so it reaches Redis keys and Neo4j node ids as well as Postgres.

What these tests pin is that the rule is keyed off type rather than applied
uniformly -- case is not noise in every namespace -- and that canonicalising an
identifier never changes what a person reads on screen.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.models.events import (  # noqa: E402
    Entity,
    EntityType,
    canonical_entity_id,
)


# -- identifiers with a conventional upper-case form ---------------------------

@pytest.mark.parametrize(
    "entity_type",
    [
        EntityType.INSTRUMENT,
        EntityType.COMPANY,
        EntityType.VULNERABILITY,
        EntityType.COUNTRY,
        EntityType.INFRASTRUCTURE,
    ],
)
def test_symbolic_identifiers_fold_upward(entity_type):
    assert canonical_entity_id("btcusd", entity_type) == "BTCUSD"
    assert canonical_entity_id("BTCUSD", entity_type) == "BTCUSD"


def test_the_two_spellings_that_started_this_now_agree():
    """The concrete failure: crypto wrote one, the chart asked for the other."""
    from_collector = canonical_entity_id("btcusd", EntityType.INSTRUMENT)
    from_caller = canonical_entity_id("BTCUSD", EntityType.INSTRUMENT)
    assert from_collector == from_caller


# -- identifiers that are hex, not words --------------------------------------

def test_a_wallet_is_the_same_wallet_however_it_is_cased():
    """EIP-55 checksum casing is a validation artifact, not an identity.

    The same 40 hex characters are the same account whether the RPC returned
    them checksummed or not, so treating the two as different entities splits
    one wallet's history in half.
    """
    checksummed = "0xAbC28c6c062cDef1a2B3"
    assert canonical_entity_id(checksummed, EntityType.WALLET) == checksummed.lower()


def test_icao24_addresses_fold_downward():
    assert canonical_entity_id("A4B1C9", EntityType.AIRCRAFT) == "a4b1c9"


# -- vessels are numeric ------------------------------------------------------

def test_a_decorated_mmsi_reduces_to_its_digits():
    assert canonical_entity_id("MMSI:311000123", EntityType.VESSEL) == "311000123"
    assert canonical_entity_id("IMO 9321483", EntityType.VESSEL) == "9321483"
    assert canonical_entity_id("311000123", EntityType.VESSEL) == "311000123"


def test_a_vessel_id_with_no_digits_is_not_destroyed():
    """The guard that stops a ship's name becoming an empty identifier."""
    assert canonical_entity_id("MV Ever Given", EntityType.VESSEL) == "MV EVER GIVEN"


# -- identifiers that are human names -----------------------------------------

@pytest.mark.parametrize(
    "entity_type",
    [EntityType.PERSON, EntityType.MEDIA_SOURCE, EntityType.UNKNOWN],
)
def test_names_keep_their_case(entity_type):
    """Folding "Reuters" to "REUTERS" would destroy information to solve a
    problem these types do not have: nothing generates a person's name twice
    with different casing, and case is meaning in a name."""
    assert canonical_entity_id("Reuters", entity_type) == "Reuters"
    assert canonical_entity_id("Vladimir Putin", entity_type) == "Vladimir Putin"


def test_unknown_is_the_default_and_is_the_safe_rule():
    """Many producers rely on the default, so it must never mangle anything."""
    assert Entity(id="some-market-slug").id == "some-market-slug"


# -- venue-minted identifiers -------------------------------------------------

def test_a_polymarket_slug_keeps_the_venue_spelling():
    """The case that made this rule wrong before it was type-aware.

    These were typed INSTRUMENT because the enum had no member for a prediction
    contract, so the ticker rule folded a kebab slug upward. The venue's own
    lowercase slug is the Redis key this deployment writes
    (sentinel:prediction:outcomes:{slug}) and the categorical resolver reads
    back, so upper-casing the database's copy split one contract across two
    stores -- exactly the failure this whole rule exists to close.
    """
    slug = "highest-temperature-in-houston-on-august-27-2026-90-91f"
    assert canonical_entity_id(slug, EntityType.PREDICTION_MARKET) == slug


def test_a_kalshi_ticker_keeps_the_venue_spelling():
    """The same rule, and it happens to already be upper. Nothing is imposed."""
    ticker = "KXHIGHNY-26AUG27-B90"
    assert canonical_entity_id(ticker, EntityType.PREDICTION_MARKET) == ticker


def test_prediction_enricher_uses_the_prediction_type():
    """A regression here silently re-folds every slug on the next backfill."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "prediction.py").read_text(
        encoding="utf-8"
    )
    assert "type=EntityType.PREDICTION_MARKET" in source
    assert "type=EntityType.INSTRUMENT" not in source, "a contract is still typed as a ticker"


# -- an identifier must not carry a mutable value -----------------------------

def test_a_price_inside_an_identifier_is_stripped():
    """Observed in the live graph: SI=F, SI=F ($59.06) and SI=F ($50.78) were
    three separate nodes for one silver future. An identifier that embeds a
    price mints a new identity every time the price moves."""
    assert canonical_entity_id("SI=F ($59.06)", EntityType.INSTRUMENT) == "SI=F"
    assert canonical_entity_id("SI=F ($50.78)", EntityType.INSTRUMENT) == "SI=F"
    assert canonical_entity_id("SI=F", EntityType.INSTRUMENT) == "SI=F"


def test_all_three_spellings_collapse_to_one_identity():
    ids = {
        canonical_entity_id(v, EntityType.INSTRUMENT)
        for v in ("SI=F ($59.06)", "SI=F ($50.78)", "SI=F", "si=f")
    }
    assert ids == {"SI=F"}


def test_a_percentage_is_stripped_too():
    assert canonical_entity_id("NVDA (12.5%)", EntityType.INSTRUMENT) == "NVDA"


def test_a_parenthetical_that_distinguishes_is_kept():
    """The rule is narrow on purpose. "(finorion)" and "(Class A)" tell two
    entities apart; "($59.06)" describes one entity's state at a moment."""
    assert canonical_entity_id("BRK (Class A)", EntityType.COMPANY) == "BRK (CLASS A)"
    assert (
        canonical_entity_id("Orion (finorion)", EntityType.PERSON) == "Orion (finorion)"
    )


# -- whitespace ---------------------------------------------------------------

def test_surrounding_and_repeated_whitespace_is_normalised():
    assert canonical_entity_id("  spaced   out ", EntityType.INSTRUMENT) == "SPACED OUT"


def test_empty_and_none_are_returned_untouched():
    """Nothing is invented for a value that was never supplied."""
    assert canonical_entity_id("", EntityType.INSTRUMENT) == ""
    assert canonical_entity_id(None, EntityType.INSTRUMENT) is None


# -- the model applies it, and display survives -------------------------------

def test_the_model_canonicalises_on_construction():
    assert Entity(id="nvda", type=EntityType.COMPANY).id == "NVDA"


def test_display_is_preserved_when_the_producer_gave_no_name():
    """The regression this guards against.

    Several call sites pass only an id and let db_writer fall back to it for the
    display name. Canonicalising in place without this would put "APPLE INC" on
    screen -- shouting, in the one place a person actually reads.
    """
    e = Entity(id="Apple Inc", type=EntityType.COMPANY)
    assert e.id == "APPLE INC"
    assert e.name == "Apple Inc"


def test_an_explicit_name_is_never_overwritten():
    e = Entity(id="nvda", type=EntityType.COMPANY, name="NVIDIA Corp")
    assert e.id == "NVDA"
    assert e.name == "NVIDIA Corp"


def test_an_already_canonical_id_leaves_name_alone():
    """No name is invented when there was nothing to correct."""
    e = Entity(id="NVDA", type=EntityType.COMPANY)
    assert e.id == "NVDA"
    assert e.name is None


def test_canonicalisation_is_idempotent():
    """Re-reading and re-writing a row must not keep changing it."""
    for entity_type in EntityType:
        once = canonical_entity_id("  Mixed Case-01 ", entity_type)
        assert canonical_entity_id(once, entity_type) == once


# -- the backfill mirrors the Python rule -------------------------------------

def test_the_migration_only_touches_rows_that_differ():
    """The predicate is the whole cost of that migration.

    Selecting by type rewrote 1.4M rows to change 5,728, held a lock on a live
    hypertable, and had not finished after seven minutes.
    """
    migrations = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")
    block = migrations[migrations.index("0011_canonical_entity_ids"):]
    block = block[: block.index('"transactional"')]
    assert block.count("primary_entity_id <>") >= 3, "an update is not guarded by a difference test"
    assert "primary_entity_name" not in block.replace(
        "-- primary_entity_name is deliberately untouched.", ""
    ), "the backfill writes the display name"


# -- the third store: Redis keys ----------------------------------------------

def test_a_cache_key_is_spelled_the_canonical_way():
    """Postgres was brought under the rule by putting it on the Entity model.

    A key built from a raw payload string never constructs that model, so it
    never picked the rule up -- and `sentinel:earnings:{symbol}` is written by
    the collector and read by two other services, none of which normalised.
    A writer and a reader disagreeing about casing simply miss each other,
    silently, with the data sitting right there.
    """
    from shared.models.events import entity_cache_key

    assert entity_cache_key("sentinel:earnings", "nvda") == "sentinel:earnings:NVDA"
    assert entity_cache_key("sentinel:earnings", "NVDA") == "sentinel:earnings:NVDA"
    assert entity_cache_key("sentinel:earnings", " nvda ") == "sentinel:earnings:NVDA"


def test_a_cache_key_honours_the_entity_type():
    """A wallet key must fold down, not up, like the identifier it names."""
    from shared.models.events import entity_cache_key

    key = entity_cache_key("sentinel:wallet", "0xAbC123", EntityType.WALLET)
    assert key == "sentinel:wallet:0xabc123"


@pytest.mark.parametrize(
    "path",
    [
        "services/collector-tradfi/main.py",
        "services/agents/quant_trading_engine.py",
        "services/enrichment/enrichers/tradfi.py",
    ],
)
def test_the_earnings_key_is_built_through_the_helper(path):
    """Writer and both readers, or the mismatch just moves."""
    source = (ROOT / path).read_text(encoding="utf-8")
    assert 'entity_cache_key("sentinel:earnings"' in source
    assert 'f"sentinel:earnings:{' not in source, "a raw-string key remains"
