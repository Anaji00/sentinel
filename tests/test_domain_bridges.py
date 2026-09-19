"""The joins that let one domain's signal reach another's instrument.

These tests exist because the defect they guard against was invisible for the
life of the platform: every cross-label edge in a 305,000-node graph stayed
inside one domain, and nothing reported it. A missing edge type looks exactly
like a quiet one.
"""

import pytest

from shared.models.ontology import (
    ALLOWED_NODE_LABELS,
    VALID_PREDICATES,
    resolve_node_label,
)
from shared.utils.domain_bridges import (
    CHOKEPOINT_COMMODITIES,
    SECTOR_COMMODITY_EXPOSURE,
    PRIOR_CONFIDENCE,
    all_bridge_commodities,
    commodities_for_chokepoint,
    exposures_for_sector,
)
from shared.utils.equities import asset_class


def test_every_bridge_symbol_is_actually_a_commodity():
    """A typo in the tables would otherwise write an edge to nothing.

    `CL=F` mistyped as `CL-F` still produces a perfectly valid-looking edge to
    a node that no price ever arrives for, and the only symptom is a traversal
    that quietly returns less than it should. The classifier is the independent
    check: it was written from the collector's symbol lists, not from these
    tables.
    """
    for symbol in all_bridge_commodities():
        assert asset_class(symbol) == "commodity", (
            f"{symbol!r} appears in a bridge table but the instrument "
            f"classifier calls it {asset_class(symbol)!r}"
        )


def test_every_bridge_predicate_is_in_the_vocabulary():
    """The graph supervisor silently rewrites anything unrecognised to RELATED_TO.

    That default is how 272,124 of roughly 442,000 edges lost their meaning. A
    predicate misspelled here would not raise; it would land in the 62%.
    """
    for _sector, pairs in SECTOR_COMMODITY_EXPOSURE.items():
        for _symbol, direction in pairs:
            assert direction in ("POSITIVE", "INVERSE")
    for _sector in SECTOR_COMMODITY_EXPOSURE:
        for _symbol, predicate in exposures_for_sector(_sector):
            assert predicate in VALID_PREDICATES, (
                f"{predicate!r} is not a canonical predicate and would be "
                f"rewritten to RELATED_TO on write"
            )
    assert "COMMODITY_EXPOSURE" in VALID_PREDICATES


def test_exposure_sign_distinguishes_producer_from_consumer():
    """Exxon and Delta must not get the same edge to crude.

    This is the whole reason the signed predicates are used rather than one
    unsigned COMMODITY_EXPOSURE. If this collapses, an oil shock reads as
    identical news for a producer and an airline.
    """
    energy = dict(exposures_for_sector("ENERGY"))
    airlines = dict(exposures_for_sector("AIRLINES"))
    assert energy["CL=F"] == "POSITIVE_EXPOSURE_TO"
    assert airlines["CL=F"] == "INVERSE_EXPOSURE_TO"
    assert energy["CL=F"] != airlines["CL=F"]


def test_hormuz_reaches_crude():
    """The chokepoint the platform watches most closely must reach a price."""
    assert "CL=F" in commodities_for_chokepoint("STRAIT OF HORMUZ")
    # Spelled as the graph spells its Region nodes: upper case.
    assert commodities_for_chokepoint("strait of hormuz") == commodities_for_chokepoint(
        "STRAIT OF HORMUZ"
    )


def test_unknown_region_and_sector_return_empty_not_a_guess():
    """Absence must stay distinguishable from a default.

    Returning a plausible commodity for an unmapped region would put an
    asserted edge in the graph that nobody chose, and it would be
    indistinguishable from the ones that were.
    """
    assert commodities_for_chokepoint("STRAIT OF NOWHERE") == []
    assert exposures_for_sector("IMAGINARY SECTOR") == []
    assert commodities_for_chokepoint(None) == []
    assert exposures_for_sector("") == []


def test_prior_confidence_is_not_saturated():
    """A stated prior must rank below a measurement.

    Five scores in this platform have been found pinned at 1.0 because they
    were derived from something a filter had already guaranteed. These edges
    are asserted from a table, which is a weaker claim than any of those, and
    must not outrank a measured correlation on the same pair.
    """
    assert 0.0 < PRIOR_CONFIDENCE < 1.0
    assert PRIOR_CONFIDENCE <= 0.7


@pytest.mark.parametrize(
    "raw_label,name,expected",
    [
        # The split that made the prompt query read 3 of 36 edges.
        ("Entity", "NVDA", "Company"),
        ("Company", "NVDA", "Company"),
        # Crude oil was stored as a Company, so the Commodity label had a
        # RANGE index and zero nodes.
        ("Company", "CL=F", "Commodity"),
        ("Entity", "CL=F", "Commodity"),
        ("Company", "QQQ", "Index"),
        ("Entity", "BTC", "CryptoAsset"),
        # Not instruments: the producer knows best and keeps its label.
        ("Vessel", "EVER GIVEN", "Vessel"),
        ("Region", "STRAIT OF HORMUZ", "Region"),
        # Unrecognised label falls back, but is counted rather than silent.
        ("NotALabel", "some organisation", "Entity"),
    ],
)
def test_label_resolution_is_decided_by_the_symbol_not_the_producer(
    raw_label, name, expected
):
    assert resolve_node_label(raw_label, name, source="test") == expected


def test_resolver_only_returns_labels_the_graph_allows():
    """A label outside the allowlist is injected straight into a Cypher string.

    `MERGE (n:{label} ...)` is built with an f-string, so the allowlist is a
    security boundary and not only a tidiness rule.
    """
    for raw_label, name in [
        ("Company", "CL=F"), ("Entity", "BTC"), ("Entity", "NVDA"),
        ("Vessel", "EVER GIVEN"), (None, "QQQ"), ("Bogus", "x"),
    ]:
        assert resolve_node_label(raw_label, name, source="test") in ALLOWED_NODE_LABELS


def test_the_physical_to_financial_path_is_expressible():
    """Vessel -> Region -> Commodity -> Company, end to end.

    Not a graph query -- that needs a live database -- but a check that every
    link in the chain has a defined label and predicate, which is what was
    missing. Before this module the chain broke at hop two and hop three.
    """
    region = "STRAIT OF HORMUZ"
    assert resolve_node_label("Region", region) == "Region"

    commodities = commodities_for_chokepoint(region)
    assert commodities, "the region reaches no commodity"

    crude = commodities[0]
    assert resolve_node_label(None, crude) == "Commodity"

    exposures = exposures_for_sector("ENERGY")
    assert any(symbol == crude for symbol, _ in exposures), (
        "no sector connects back to the commodity the chokepoint carries"
    )
    assert resolve_node_label("Entity", "XOM") == "Company"


# Country codes that are also valid-looking tickers. Every one of these was
# created as a `:Company` node by the first version of resolve_node_label,
# within two minutes of it being deployed.
FLAG_CODES_THAT_LOOK_LIKE_TICKERS = [
    "SG", "RW", "BS", "LR", "HK", "TW", "VN", "GH", "DE", "KR", "MA", "IQ",
    "BB", "SE", "PA", "MH", "CY", "MT",
]


@pytest.mark.parametrize("code", FLAG_CODES_THAT_LOOK_LIKE_TICKERS)
def test_a_ship_registry_code_is_never_rewritten_into_a_company(code):
    """The AIS enricher knows more about a flag than a regex over a symbol does.

    `DE` is Germany and Deere, `KR` is South Korea and Kroger, `MA` is Morocco
    and Mastercard. The node-merge migration refuses to merge these for exactly
    that reason, and the write path has to refuse for the same reason, or the
    corruption simply moves from one place to the other -- which is what
    happened: SG, RW, BS, LR, HK, TW and VN were written as companies in
    production before this test existed.
    """
    assert resolve_node_label("Flag", code, source="test") == "Flag"


@pytest.mark.parametrize(
    "label,name",
    [
        ("Vessel", "9321483"), ("Aircraft", "A320"), ("Region", "BLACK SEA"),
        ("Country", "US"), ("Sector", "ENERGY"), ("Person", "LI"),
        ("AutonomousSystem", "AS13335"), ("Vulnerability", "CVE-2026-1"),
        ("Organization", "NATO"), ("Government", "IRGC"),
    ],
)
def test_non_financial_labels_are_left_alone(label, name):
    """The classifier knows about instruments and must not rule outside that.

    Several of these names are ticker-shaped -- A320, US, LI, NATO -- and the
    only thing standing between them and a `:Company` node is that the producer
    named a domain the classifier has no competence over.
    """
    assert resolve_node_label(label, name, source="test") == label


def test_a_financial_label_is_still_refined():
    """The guard above must not disable the thing this function exists to do.

    Crude oil arrives labelled `Company`, which is wrong and is why the
    `Commodity` label had a RANGE index and zero nodes. `Company` is financial,
    so refining it is in scope; `Flag` is not.
    """
    assert resolve_node_label("Company", "CL=F", source="test") == "Commodity"
    assert resolve_node_label("Company", "QQQ", source="test") == "Index"
    assert resolve_node_label("Entity", "NVDA", source="test") == "Company"


def test_a_wallet_keeps_its_label_and_its_casing():
    """The one omission that made 84% of the graph look untyped.

    `crypto.py` proposes `target_label: "Wallet"` for every transfer
    counterparty. While `Wallet` was missing from ALLOWED_NODE_LABELS the
    supervisor rewrote it to `Entity`, and 254,542 of the 257,689 `:Entity`
    nodes -- 98.8% -- are Ethereum addresses.

    The casing half matters independently. `Entity` maps to EntityType.UNKNOWN,
    which returns an identifier exactly as written; `Wallet` maps to
    EntityType.WALLET, which lower-cases. A hex address has no canonical case,
    so with the label routed through UNKNOWN the same address arriving in two
    spellings became two nodes -- which the graph has recorded before, at 6,366
    and 139,047 nodes for one set of addresses.
    """
    from shared.models.events import graph_node_id

    mixed = "0xB3FA262D0FB521CC93BE83D87B322B8A23DAF3F0"
    assert resolve_node_label("Wallet", mixed, source="test") == "Wallet"
    assert graph_node_id(mixed, "Wallet") == mixed.lower()
    # And the two spellings must converge on one identifier.
    assert graph_node_id(mixed, "Wallet") == graph_node_id(mixed.lower(), "Wallet")


def test_entity_is_a_fallback_and_not_a_domain():
    """Nothing that has a real label should be able to reach `Entity`.

    Each of these was measured landing in `Entity` on the live graph. The test
    is here so that a label removed from the allowlist shows up as a failure
    rather than as several hundred thousand untyped nodes.
    """
    for label, name in [
        ("Wallet", "0xdeadbeef00000000000000000000000000000000"),
        ("AutonomousSystem", "AS13335"),
        ("Prefix", "8.8.8.0/24"),
        ("Vulnerability", "CVE-2026-16812"),
        ("CryptoAsset", "ETHUSDT"),
        ("Country", "US"),
    ]:
        assert resolve_node_label(label, name, source="test") == label, (
            f"{label!r} is not in ALLOWED_NODE_LABELS, so every proposal "
            f"naming it is silently rewritten to Entity"
        )


# ── freight index exposure ───────────────────────────────────────────────────

def test_freight_exposure_predicates_are_canonical():
    """A misspelled predicate here lands in the 62% as RELATED_TO."""
    from shared.utils.domain_bridges import (
        FREIGHT_EXPOSURE, exposures_for_freight_index,
    )
    for index_symbol in FREIGHT_EXPOSURE:
        pairs = exposures_for_freight_index(index_symbol)
        assert pairs, f"{index_symbol} reaches no equity"
        for _ticker, predicate in pairs:
            assert predicate in VALID_PREDICATES


def test_a_charter_rate_spike_is_revenue_for_one_side_and_cost_for_the_other():
    """The clearest case in the file for keeping the sign.

    Danaos leases vessels out; ZIM charters them in. One HARPEX move is revenue
    for the first and cost for the second. The collector's flat
    FREIGHT_SENSITIVE_EQUITIES list contains both and says they respond alike.
    """
    from shared.utils.domain_bridges import exposures_for_freight_index

    harpex = dict(exposures_for_freight_index("HARPEX"))
    assert harpex["DAC"] == "POSITIVE_EXPOSURE_TO"
    assert harpex["ZIM"] == "INVERSE_EXPOSURE_TO"


def test_dry_bulk_and_container_indices_do_not_share_a_cohort():
    """A BDI spike is not news for a container line.

    The Baltic Dry Index prices dry bulk and FBX prices containers: different
    vessels, routes and cargo. The flat list could not express that, so every
    freight event named all nine tickers regardless of which index moved.
    """
    from shared.utils.domain_bridges import exposures_for_freight_index

    bdi = {t for t, _ in exposures_for_freight_index("BDI")}
    fbx = {t for t, _ in exposures_for_freight_index("FBX_GLOBAL")}
    assert "SBLK" in bdi and "SBLK" not in fbx
    assert "ZIM" in fbx and "ZIM" not in bdi


def test_freight_commodity_input_is_a_commodity():
    """Crude is an input to freight, not an equity exposed to it.

    `CL=F` sits in a constant named FREIGHT_SENSITIVE_EQUITIES. It is modelled
    here as the index's own commodity exposure instead, and this pins that it
    really is a commodity rather than something the classifier disagrees about.
    """
    from shared.utils.domain_bridges import commodity_inputs_for_freight_index

    for index_symbol in ("BDI", "FBX_GLOBAL", "HARPEX"):
        inputs = commodity_inputs_for_freight_index(index_symbol)
        assert inputs, f"{index_symbol} names no fuel input"
        for symbol in inputs:
            assert asset_class(symbol) == "commodity"


def test_freight_index_label_is_allowed_and_not_refined_away():
    """The index must not be classified as an equity on its way to the graph.

    `BDI` is three uppercase letters, so the instrument classifier alone would
    call it a plain ticker and label it `Company`. `SupplyChainMetric` is a
    non-financial label, so the resolver has to leave it alone -- the same rule
    that stops a vessel flag code becoming a corporation.
    """
    from shared.utils.domain_bridges import FREIGHT_INDEX_LABEL

    assert FREIGHT_INDEX_LABEL in ALLOWED_NODE_LABELS
    for index_symbol in ("BDI", "FBX_GLOBAL", "HARPEX"):
        assert resolve_node_label(
            FREIGHT_INDEX_LABEL, index_symbol, source="test"
        ) == FREIGHT_INDEX_LABEL


def test_unknown_freight_index_returns_empty():
    from shared.utils.domain_bridges import (
        commodity_inputs_for_freight_index, exposures_for_freight_index,
    )
    assert exposures_for_freight_index("NOT_AN_INDEX") == []
    assert exposures_for_freight_index(None) == []
    assert commodity_inputs_for_freight_index("") == []


@pytest.mark.asyncio
async def test_static_bridges_are_written_even_when_the_universe_is_empty():
    """The bridge must not depend on the watchlist being warm.

    These writes sat below `if not symbols: return`, so a fresh deployment or a
    cold Redis would have skipped the entire physical-to-financial bridge -- and
    the symptom would have been exactly the defect the bridge exists to fix: a
    graph whose domains do not touch. They depend on nothing but tables
    compiled into the image, so they belong above that guard.
    """
    from unittest.mock import AsyncMock, patch

    import services.enrichment.ref_data as ref_data

    graph_writer = AsyncMock()
    with patch.object(ref_data, "_reference_universe", AsyncMock(return_value=[])), \
         patch.object(ref_data, "fetch_index_constituents", AsyncMock(return_value=None)):
        await ref_data.refresh_watchlist_reference_data(
            redis_client=AsyncMock(), graph_writer=graph_writer
        )

    predicates = {
        call.kwargs.get("relation_type")
        for call in graph_writer.link_entities.await_args_list
    }
    assert graph_writer.link_entities.await_count > 0, (
        "an empty symbol universe skipped the static bridge edges entirely"
    )
    assert "COMMODITY_EXPOSURE" in predicates, "no chokepoint-to-commodity edges"
    assert predicates & {"POSITIVE_EXPOSURE_TO", "INVERSE_EXPOSURE_TO"}, (
        "no signed freight-index exposure edges"
    )


# ── route topology and supply chain ──────────────────────────────────────────

def test_chokepoint_adjacency_is_symmetric_and_reaches_both_ways():
    """A corridor read from either end must give the same route.

    Stored as one pair per adjacency and traversed undirected, so the lookup has
    to work from both ends or half the route is invisible depending on which
    chokepoint raised the signal.
    """
    from shared.utils.domain_bridges import (
        CHOKEPOINT_ADJACENCY, adjacent_chokepoints,
    )
    for a, b in CHOKEPOINT_ADJACENCY:
        assert b in adjacent_chokepoints(a), f"{a} does not reach {b}"
        assert a in adjacent_chokepoints(b), f"{b} does not reach {a}"


def test_the_suez_corridor_is_one_chain_not_three_points():
    """Bab-el-Mandeb, the Red Sea and Suez divert the same traffic.

    Treating them as independent watch items is how one disruption reads as
    three unrelated observations.
    """
    from shared.utils.domain_bridges import adjacent_chokepoints

    assert "RED SEA" in adjacent_chokepoints("BAB-EL-MANDEB")
    assert set(adjacent_chokepoints("RED SEA")) == {"BAB-EL-MANDEB", "SUEZ CANAL"}


def test_every_adjacent_chokepoint_is_one_the_platform_watches():
    """An adjacency to a region with no node reaches nothing.

    The Region nodes are created by the AIS and ADS-B collectors from their own
    watched list; naming somewhere outside it would write an edge to a node that
    never receives traffic, which looks identical to a quiet corridor.
    """
    from shared.utils.domain_bridges import (
        CHOKEPOINT_ADJACENCY, CHOKEPOINT_COMMODITIES,
    )
    known = set(CHOKEPOINT_COMMODITIES) | {"TAIWAN STRAIT"}
    for a, b in CHOKEPOINT_ADJACENCY:
        assert a in known, f"{a} is not a known chokepoint"
        assert b in known, f"{b} is not a known chokepoint"


def test_supply_chain_direction_is_goods_flow_not_money_flow():
    """SUPPLIER_TO points the way the goods go, so disruption propagates along it.

    ASML sells to TSM, TSM sells to NVDA. Reversing this would make a fab
    outage propagate to the lithography vendor instead of to the fabless
    customers, which is backwards.
    """
    from shared.utils.domain_bridges import customers_of, suppliers_of

    assert "TSM" in customers_of("ASML")
    assert "NVDA" in customers_of("TSM")
    assert "ASML" in suppliers_of("TSM")
    assert "TSM" in suppliers_of("NVDA")
    # And not the other way round.
    assert "ASML" not in customers_of("TSM")


def test_supply_chain_has_no_self_edges_or_duplicates():
    """A company does not supply itself, and one pair is one edge."""
    from shared.utils.domain_bridges import SUPPLY_CHAIN

    assert len(SUPPLY_CHAIN) == len(set(SUPPLY_CHAIN)), "duplicate supply pair"
    for supplier, customer in SUPPLY_CHAIN:
        assert supplier != customer
        assert (customer, supplier) not in SUPPLY_CHAIN, (
            f"{supplier}/{customer} is listed in both directions, which "
            f"double-counts in any undirected traversal"
        )


def test_supply_chain_tickers_are_equities():
    """A supply edge to something that is not a company reaches nothing tradeable."""
    from shared.utils.domain_bridges import SUPPLY_CHAIN

    for supplier, customer in SUPPLY_CHAIN:
        for ticker in (supplier, customer):
            assert asset_class(ticker) == "equity", (
                f"{ticker!r} classifies as {asset_class(ticker)!r}, not an equity"
            )
            assert resolve_node_label("Company", ticker, source="test") == "Company"


def test_taiwan_strait_now_reaches_an_equity():
    """The point of the supply chain table.

    The Taiwan Strait has been one of the twelve watched chokepoints all along
    and reached no instrument by any path. It is deliberately absent from
    CHOKEPOINT_COMMODITIES -- what transits it is semiconductors, and there is
    no contract for those -- so the supply chain is the only route it has.
    """
    from shared.utils.domain_bridges import adjacent_chokepoints, customers_of

    assert "TAIWAN STRAIT" in adjacent_chokepoints("SOUTH CHINA SEA")
    assert customers_of("TSM"), "the Taiwan foundry reaches no customer"


def test_edge_provenance_survives_the_write_path():
    """A `source` attached by a producer must reach the relationship.

    The supervisor sets relationship properties from a fixed list, and `source`
    was not on it -- so every bridge edge landed carrying confidence 0.55 and no
    record of who asserted it, making a stated prior indistinguishable from a
    measurement at the same number. Verified against the live graph before this
    was written: `keys(r)` on a SUPPLIER_TO edge held nine properties and
    `source` was not among them.

    Comment lines are stripped before scanning, because a check that can match
    its own explanation is not a check -- twice this audit a test passed by
    reading the comment describing what it was looking for.
    """
    import re
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "services" / "agents" / "supervisor.py"
    lines = [
        ln for ln in src.read_text(encoding="utf-8").splitlines()
        if not ln.lstrip().startswith(("#", "//"))
    ]
    body = "\n".join(lines)

    set_clauses = re.findall(r"SET r\.\w+ =.*?(?=\n\s*\"\"\")", body, re.S)
    assert set_clauses, "no relationship SET clause found in the supervisor"
    for clause in set_clauses:
        assert "r.source" in clause, (
            "a relationship SET clause does not carry `source`, so edge "
            "provenance is dropped on write"
        )


def test_bridge_edges_declare_their_provenance():
    """Every asserted edge must be markable as asserted."""
    from shared.utils.domain_bridges import PRIOR_SOURCE

    assert PRIOR_SOURCE and isinstance(PRIOR_SOURCE, str)
    assert "prior" in PRIOR_SOURCE.lower(), (
        "the marker should say what it is, since it is what a reader uses to "
        "discount the edge"
    )


def test_producer_specific_edge_properties_are_not_dropped():
    """The supervisor's named property list is closed, and producers have more to say.

    Measured on the 13F holdings after 605 was fixed: `weight` and `source`
    reached the edge, while `market_value_usd`, `shares` and `change_type` did
    not -- so an OWNS edge recorded that a fund holds 3.9% of a company but not
    that the position is worth billions, or that it was cut this quarter.

    Naming those three would have fixed those three. The passthrough closes the
    class.
    """
    from services.agents.supervisor import _edge_extras

    kept = _edge_extras({
        "market_value_usd": 65_950_296_923.0,
        "shares": 227_917_808.0,
        "change_type": "MAINTAINED",
        "is_new": True,
    })
    assert kept == {
        "market_value_usd": 65_950_296_923.0,
        "shares": 227_917_808.0,
        "change_type": "MAINTAINED",
        "is_new": True,
    }


def test_the_passthrough_cannot_overwrite_a_managed_property():
    """A `confidence` smuggled through extras is indistinguishable from a measured one.

    The whole point of 588's sub-1.0 priors and 590's effect-size ranking is
    that these numbers mean something specific. A producer must not be able to
    set them by another route.
    """
    from services.agents.supervisor import _edge_extras, _MANAGED_EDGE_KEYS

    smuggled = _edge_extras({key: 1.0 for key in _MANAGED_EDGE_KEYS})
    assert smuggled == {}, f"managed keys reachable through extras: {smuggled}"


def test_the_passthrough_refuses_values_neo4j_cannot_store():
    """A nested value raises at write time and takes the whole batch with it."""
    from services.agents.supervisor import _edge_extras

    assert _edge_extras({"nested": {"a": 1}, "listy": [1, 2], "ok": "yes"}) == {"ok": "yes"}
    assert _edge_extras(None) == {}
    assert _edge_extras("not a dict") == {}


# ── company geography ────────────────────────────────────────────────────────

def test_every_chokepoint_with_commodities_also_names_its_countries():
    """The two geography tables must cover the same regions.

    One table without the other leaves half the join: a chokepoint that prices
    a commodity but reaches no country, or the reverse. Both are written from
    the same refresh pass and drifting apart would be silent.
    """
    from shared.utils.domain_bridges import (
        CHOKEPOINT_COMMODITIES, CHOKEPOINT_COUNTRIES,
    )
    missing = sorted(set(CHOKEPOINT_COMMODITIES) - set(CHOKEPOINT_COUNTRIES))
    assert not missing, f"chokepoints with commodities but no countries: {missing}"


def test_country_codes_are_two_letter_and_not_retyped_as_equities():
    """`TW`, `US` and `SG` are all ticker-shaped.

    The instrument classifier would call every one of them a plain ticker, and
    the only thing stopping a `:Company` node named TW is that `Country` is a
    non-financial label the resolver must leave alone -- the same rule that
    keeps a ship registry from becoming a corporation.
    """
    from shared.utils.domain_bridges import CHOKEPOINT_COUNTRIES

    for region, codes in CHOKEPOINT_COUNTRIES.items():
        assert codes, f"{region} names no country"
        for code in codes:
            assert len(code) == 2 and code.isalpha() and code.isupper(), code
            assert resolve_node_label("Country", code, source="test") == "Country"


def test_a_chokepoint_names_its_littoral_states_not_its_customers():
    """Japan depends on Hormuz and is not on it.

    An edge from a strait to everyone with an interest in it would be a claim
    about trade flow wearing the clothes of geography, and it would make the
    country join useless -- every chokepoint would reach every economy.
    """
    from shared.utils.domain_bridges import countries_on_chokepoint

    hormuz = countries_on_chokepoint("STRAIT OF HORMUZ")
    assert set(hormuz) == {"IR", "OM", "AE"}
    assert "JP" not in hormuz and "US" not in hormuz
    assert countries_on_chokepoint("TAIWAN STRAIT") == ["TW", "CN"]
    assert countries_on_chokepoint("NOWHERE") == []
    assert countries_on_chokepoint(None) == []


def test_the_geography_path_is_expressible_end_to_end():
    """Vessel -> Region -> Country -> Company.

    The second of the two cross-domain paths. The commodity path answers what a
    closure prices; this one answers who is registered on it, and before this
    `Company` had no geographic edge of any kind.
    """
    from shared.utils.domain_bridges import countries_on_chokepoint

    region = "TAIWAN STRAIT"
    assert resolve_node_label("Region", region) == "Region"
    countries = countries_on_chokepoint(region)
    assert countries
    assert resolve_node_label("Country", countries[0]) == "Country"
    # And the far end is a company the reference feed can place there.
    assert resolve_node_label("Company", "TSM") == "Company"


@pytest.mark.asyncio
async def test_a_cached_symbol_still_reaches_the_graph():
    """A cache hit must not skip the graph promotion.

    `fetch_and_cache_reference_data` returned the cached dict before reaching
    its graph writes, so with 547 symbols cached a refresh reported
    "400/400 symbols updated" and wrote nothing. OPERATES_IN, COMPETES_WITH and
    the country edges could only ever be written the first time a symbol was
    seen, which meant no later repair could reach them.

    The function does two things and only one of them is idempotent against
    Redis: skipping the vendor call is the point of the cache, skipping the
    graph write was an accident of where the `return` sat.
    """
    import json
    from unittest.mock import AsyncMock, MagicMock, patch

    import services.enrichment.ref_data as ref_data

    cached = json.dumps({
        "symbol": "TSM", "sector": "SEMICONDUCTORS", "industry": "Semiconductors",
        "country": "TW", "index_membership": [],
    })
    redis_client = MagicMock()
    redis_client.raw = MagicMock()
    redis_client.raw.get = AsyncMock(return_value=cached)

    graph_writer = AsyncMock()
    # The function returns before the cache check when no key is configured,
    # which is a separate question from this one.
    with patch.object(ref_data, "FINNHUB_API_KEY", "test-key"):
        result = await ref_data.fetch_and_cache_reference_data(
            redis_client, "TSM", session=None, graph_writer=graph_writer,
        )

    assert result["symbol"] == "TSM", "the cached payload should still be returned"
    graph_writer.upsert_equity.assert_awaited()
    countries = [
        c.kwargs for c in graph_writer.link_entities.await_args_list
        if c.kwargs.get("relation_type") == "REGISTERED_IN"
    ]
    assert countries, "a cached symbol wrote no country edge"
    assert countries[0]["target_id"] == "TW"
    assert countries[0]["target_label"] == "Country"


def test_a_link_proposal_that_types_one_end_types_the_other():
    """An unlabelled endpoint falls back to `Entity`, silently.

    The crypto enricher set `target_label: "Wallet"` and no `source_label`, on
    an edge whose two endpoints are both Ethereum addresses by construction. So
    the receiver was typed and the sender was not, and half of every transfer
    rebuilt the untyped mass that 255,677 nodes had just been relabelled out of
    -- 1,455 fallbacks and climbing when it was found.

    Scanned over source rather than asserted about one call site, because the
    defect is not specific to crypto: any producer that names one end and not
    the other gets the same silent downgrade. Comment lines are stripped first,
    so this cannot pass by reading the explanation above it.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in list((root / "services").rglob("*.py")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        body = "\n".join(
            ln for ln in text.splitlines() if not ln.lstrip().startswith("#")
        )
        # Each dict literal or call that names a target_label.
        for match in re.finditer(r'"target_label"\s*:', body):
            window = body[max(0, match.start() - 700): match.start() + 700]
            if '"source_label"' not in window:
                line = body[: match.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(root)}:{line}")

    assert not offenders, (
        "link proposals naming a target_label but no source_label, so the "
        f"source endpoint falls back to Entity: {offenders}"
    )


def test_every_sector_the_hawkes_engine_models_is_actually_collected():
    """The engine is wired end to end and had no input at all.

    `discover_sector_hawkes_contagion` builds a point process per sector out of
    these funds' returns and fits a Hawkes kernel to find which sector's
    volatility excites which. It writes HAWKES_EXCITES edges and runs on a
    ten-minute loop, and it has produced nothing: `tradfi_bars` held zero bars
    for all eleven ETFs it reads.

    They were excluded on purpose -- `is_valid_primary_equity` calls each one
    INDEX_SECTOR_ETF, "not a company", which is right for an equity watchlist
    and is what kept them out of the tradfi collector's subscription budget.
    Nothing collected them elsewhere, so an exclusion that was correct for one
    consumer starved the only consumer that needed them.

    This pins the two lists together so the next sector added to one has to
    appear in the other.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    discovery = (root / "services" / "correlation" / "statistical_discovery.py").read_text(
        encoding="utf-8"
    )
    collector = (root / "services" / "collector-macro" / "main.py").read_text(
        encoding="utf-8"
    )

    block = discovery.split("SECTOR_ETF_MAP")[1][:800]
    needed = set(re.findall(r'"(XL[A-Z]{1,2})"', block))
    assert len(needed) >= 11, f"expected the GICS sector set, found {sorted(needed)}"

    collected = set(re.findall(r'"(XL[A-Z]{1,2})":', collector))
    missing = sorted(needed - collected)
    assert not missing, (
        f"the Hawkes sector engine reads these and nothing collects them: {missing}"
    )
