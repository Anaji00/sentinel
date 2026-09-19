"""The edges that would let one domain's signal reach another's instrument.

Measured on the live graph before writing any of this. Every cross-label edge
that exists, in full:

    Aircraft->Region  73,085    Vessel->Region  16,184    AS->Prefix  7,638
    Aircraft->Flag    24,673    Vessel->Flag     8,458    Company->Sector 704

Every one of those is *within* a domain. `Region` connects only to `Aircraft`
and `Vessel`, and `Company` only to `Company`, `Sector` and `Entity`. There is
no edge anywhere joining the physical world to the financial one: 9,921 vessels
and 23,865 aircraft on one side, 3,170 companies on the other, and nothing
between them. The platform is four single-domain graphs sharing a database.

So a vessel slowing in the Strait of Hormuz cannot reach an energy equity by
traversal, and the reason is not a missing inference step. Crude oil is stored
as a `:Company`, the `Commodity` label has a RANGE index and zero nodes, and
the predicates that would carry the signal -- COMMODITY_EXPOSURE,
POSITIVE_EXPOSURE_TO, INVERSE_EXPOSURE_TO -- are defined in the vocabulary and
written zero times.

This module supplies the two joins that close that gap:

    Vessel -> Region -> Commodity -> Company

The first hop already exists with 16,184 edges. The other two are below.

WHAT KIND OF CLAIM THIS IS
--------------------------
These tables are a stated prior, not a measurement, and they are kept apart
from anything the platform derives so that the difference survives into the
graph. Edges written from them carry `source` and a confidence well under 1.0,
because the recurring defect in this system is a number computed from something
a filter already guaranteed and then read as if it had been observed. An oil
major's sensitivity to crude is not in doubt; the exact figure is, and this
module does not pretend to supply it.

The platform can check its own priors. `statistical_discovery` already measures
XOM against CL=F, and where a measured edge exists it should be preferred to
one of these. That is why these are written with distinct predicates and a
`prior` marker rather than merged into the statistical family.
"""

from typing import Dict, List, Tuple

# Confidence carried by an edge asserted from the tables below.
#
# Deliberately not 1.0. Four scores in this platform were found saturated at
# 1.0 because they were derived from a quantity some filter had already
# guaranteed, and a fifth -- GRANGER_CAUSES.confidence, which is 1 - p_value
# after a significance test -- sits at exactly 1.0 on 102 of 116 edges. A
# structural prior is a good reason to look, not a measurement, and it should
# rank below anything actually observed.
PRIOR_CONFIDENCE = 0.55

# Marks an edge as asserted rather than observed, so a reader can tell.
PRIOR_SOURCE = "domain_bridge_prior"


# ── CHOKEPOINT → COMMODITY ───────────────────────────────────────────────────
#
# What physically transits each strait. This is the geography of trade, and it
# is stable on a decade timescale -- unlike a correlation, which is why it is
# worth asserting rather than waiting to discover.
#
# Region names are spelled as the graph spells them: upper case, matching the
# nodes the AIS and ADS-B collectors already create. `STRAIT OF HORMUZ` carries
# 1,959 edges today and reaches no instrument through any of them.
#
# Only chokepoints whose cargo is a commodity this platform actually prices
# appear here. Taiwan Strait is deliberately absent: what transits it that
# matters is semiconductors and finished goods, and there is no contract for
# those, so an entry would be a guess dressed as a fact.
CHOKEPOINT_COMMODITIES: Dict[str, List[str]] = {
    "STRAIT OF HORMUZ": ["CL=F", "BZ=F", "NG=F"],
    "GULF OF OMAN": ["CL=F", "BZ=F"],
    "PERSIAN GULF": ["CL=F", "BZ=F", "NG=F"],
    "BAB-EL-MANDEB": ["CL=F", "BZ=F"],
    "RED SEA": ["CL=F", "BZ=F"],
    "SUEZ CANAL": ["CL=F", "BZ=F", "NG=F"],
    "STRAIT OF MALACCA": ["CL=F", "NG=F"],
    "SINGAPORE APPROACH": ["CL=F", "NG=F"],
    "SOUTH CHINA SEA": ["CL=F", "NG=F"],
    # Black Sea grain, and the straits it must pass through to leave.
    "TURKISH STRAITS": ["ZW=F", "ZC=F", "CL=F"],
    "BLACK SEA": ["ZW=F", "ZC=F"],
    "GULF OF GUINEA": ["CL=F", "BZ=F"],
    "PANAMA CANAL": ["NG=F", "ZC=F", "ZS=F"],
}


# ── SECTOR → COMMODITY EXPOSURE, SIGNED ──────────────────────────────────────
#
# Derived through the graph's own 704 OPERATES_IN edges rather than written per
# company. One line here reaches every company the platform has already placed
# in that sector, so the mapping stays small enough to argue with and the
# coverage grows as the sector edges grow.
#
# The sign is the point. An oil producer and an airline are both exposed to
# crude and they are exposed in opposite directions, and an unsigned
# "COMMODITY_EXPOSURE" edge between both and CL=F would tell a reader that
# Exxon and Delta respond to an oil shock the same way. The vocabulary already
# distinguishes POSITIVE_EXPOSURE_TO from INVERSE_EXPOSURE_TO; nothing had
# written either.
#
# Sector names are spelled as the graph spells them. Sectors whose commodity
# link is real but weak or ambiguous -- SEMICONDUCTORS, AEROSPACE & DEFENSE,
# REAL ESTATE -- are left out. A thin edge here costs more than it gives,
# because it is indistinguishable from a thick one once it is in the graph.
SECTOR_COMMODITY_EXPOSURE: Dict[str, List[Tuple[str, str]]] = {
    # Producers: revenue rises with the price of what they sell.
    "ENERGY": [("CL=F", "POSITIVE"), ("BZ=F", "POSITIVE"), ("NG=F", "POSITIVE")],
    "METALS & MINING": [
        ("GC=F", "POSITIVE"), ("SI=F", "POSITIVE"), ("HG=F", "POSITIVE"),
    ],
    # Consumers: the commodity is a cost, so the sign flips.
    "AIRLINES": [("CL=F", "INVERSE")],
    "ROAD & RAIL": [("CL=F", "INVERSE")],
    "LOGISTICS & TRANSPORTATION": [("CL=F", "INVERSE")],
    "CHEMICALS": [("NG=F", "INVERSE"), ("CL=F", "INVERSE")],
    "PACKAGING": [("CL=F", "INVERSE")],
    "FOOD PRODUCTS": [("ZC=F", "INVERSE"), ("ZW=F", "INVERSE")],
    "BEVERAGES": [("KC=F", "INVERSE"), ("SB=F", "INVERSE")],
    "AUTOMOBILES": [("HG=F", "INVERSE")],
    "CONSTRUCTION": [("HG=F", "INVERSE")],
    "BUILDING": [("HG=F", "INVERSE")],
    # Fuel is a cost, but regulated tariffs pass much of it through, so the
    # relationship is real and weaker than the ones above. Kept because gas is
    # the marginal fuel for power in most of this universe.
    "UTILITIES": [("NG=F", "INVERSE")],
}

# Which predicate expresses each direction.
_DIRECTION_PREDICATE = {
    "POSITIVE": "POSITIVE_EXPOSURE_TO",
    "INVERSE": "INVERSE_EXPOSURE_TO",
}


def commodities_for_chokepoint(region: str) -> List[str]:
    """The commodities that physically transit a chokepoint, or an empty list."""
    if not region or not isinstance(region, str):
        return []
    return list(CHOKEPOINT_COMMODITIES.get(region.strip().upper(), []))


def exposures_for_sector(sector: str) -> List[Tuple[str, str]]:
    """[(commodity, predicate)] for a sector, or an empty list.

    The predicate rather than the raw direction, so a caller cannot invent a
    third spelling of "inverse" on its way to the graph.
    """
    if not sector or not isinstance(sector, str):
        return []
    out: List[Tuple[str, str]] = []
    for symbol, direction in SECTOR_COMMODITY_EXPOSURE.get(sector.strip().upper(), []):
        predicate = _DIRECTION_PREDICATE.get(direction)
        if predicate:
            out.append((symbol, predicate))
    return out


def all_bridge_commodities() -> List[str]:
    """Every commodity symbol either table mentions.

    Used to create the `:Commodity` nodes before the edges that need them, so
    an exposure edge cannot be the thing that creates a node and thereby decide
    its label by accident -- which is how crude oil became a `:Company`.
    """
    symbols = set()
    for group in CHOKEPOINT_COMMODITIES.values():
        symbols.update(group)
    for pairs in SECTOR_COMMODITY_EXPOSURE.values():
        symbols.update(sym for sym, _ in pairs)
    return sorted(symbols)


# ── FREIGHT INDEX → EQUITY EXPOSURE, SIGNED AND PER INDEX ────────────────────
#
# `collector-macro/freight.py` carries one flat list for all three indices:
#
#     FREIGHT_SENSITIVE_EQUITIES = ["ZIM","MATX","SBLK","GOGL","DAC",
#                                   "CAT","VALE","NUE","CL=F"]
#
# It has two problems, and they are the same two this module was written to fix.
#
# It is unsigned. ZIM and Star Bulk *earn* the freight rate; Caterpillar and
# Vale *pay* it. A rate spike is revenue for one group and cost for the other,
# and a single list says they respond alike -- exactly the Exxon/Delta mistake
# that SECTOR_COMMODITY_EXPOSURE exists to avoid.
#
# It is undifferentiated. The Baltic Dry Index prices dry bulk and FBX prices
# containers; they are different vessels on different routes carrying different
# cargo. A BDI spike is not news for a container line, and the flat list cannot
# say so.
#
# `CL=F` is also in it, in a constant named for equities. Crude is an *input* to
# freight rather than something exposed to them -- bunker fuel is the largest
# variable cost a ship has -- so it is modelled below as the index's own
# commodity exposure instead.
FREIGHT_INDEX_LABEL = "SupplyChainMetric"

FREIGHT_EXPOSURE: Dict[str, List[Tuple[str, str]]] = {
    # Dry bulk: iron ore, coal, grain. Owners earn the rate; the miners and
    # steelmakers whose cargo it is pay it.
    "BDI": [
        ("SBLK", "POSITIVE"), ("GOGL", "POSITIVE"),
        ("VALE", "INVERSE"), ("NUE", "INVERSE"), ("CAT", "INVERSE"),
    ],
    # Container freight, per forty-foot equivalent. Liner operators earn it;
    # manufacturers shipping finished goods pay it.
    "FBX_GLOBAL": [
        ("ZIM", "POSITIVE"), ("MATX", "POSITIVE"),
        ("CAT", "INVERSE"),
    ],
    # Charter rates: what an operator pays a shipowner for the vessel itself.
    # Danaos is a lessor, so a charter spike is its revenue; ZIM charters in,
    # so the same spike is its cost. The two move oppositely on one index,
    # which is the clearest case in this file for keeping the sign.
    "HARPEX": [
        ("DAC", "POSITIVE"),
        ("ZIM", "INVERSE"),
    ],
}

# Bunker fuel: the largest variable cost in operating a ship, and the reason
# crude appears in a freight equities list at all.
FREIGHT_COMMODITY_INPUT: Dict[str, List[str]] = {
    "BDI": ["CL=F"],
    "FBX_GLOBAL": ["CL=F"],
    "HARPEX": ["CL=F"],
}


def exposures_for_freight_index(index_symbol: str) -> List[Tuple[str, str]]:
    """[(ticker, predicate)] for a freight index, or an empty list."""
    if not index_symbol or not isinstance(index_symbol, str):
        return []
    out: List[Tuple[str, str]] = []
    for symbol, direction in FREIGHT_EXPOSURE.get(index_symbol.strip().upper(), []):
        predicate = _DIRECTION_PREDICATE.get(direction)
        if predicate:
            out.append((symbol, predicate))
    return out


def commodity_inputs_for_freight_index(index_symbol: str) -> List[str]:
    """Commodities whose price drives this index, or an empty list."""
    if not index_symbol or not isinstance(index_symbol, str):
        return []
    return list(FREIGHT_COMMODITY_INPUT.get(index_symbol.strip().upper(), []))


# ── CHOKEPOINT ADJACENCY: ROUTE TOPOLOGY ─────────────────────────────────────
#
# Measured: `ADJACENT_TO` holds 4 edges in the whole graph and not one of them
# touches a Region. So the twelve watched chokepoints are twelve unrelated
# points, and the platform cannot express the thing that makes a chokepoint
# matter -- that it sits on a route, and that closing it moves traffic
# somewhere else.
#
# Without these edges a Hormuz disruption and a Bab-el-Mandeb disruption are
# independent observations. With them, a signal at one is a reason to look at
# its neighbours, which is how a person reads a map.
#
# Pairs are physical contiguity along a single corridor -- a ship sailing the
# route passes through both -- not "somewhere near". Symmetric, so the writer
# emits one edge per pair and queries traverse it undirected.
CHOKEPOINT_ADJACENCY: List[Tuple[str, str]] = [
    # The Gulf corridor: loading terminal to open ocean.
    ("PERSIAN GULF", "STRAIT OF HORMUZ"),
    ("STRAIT OF HORMUZ", "GULF OF OMAN"),
    # The Suez corridor, south to north. A closure at any one of these three
    # diverts the same traffic around the Cape, which is why they belong on one
    # chain rather than as three separate watch items.
    ("BAB-EL-MANDEB", "RED SEA"),
    ("RED SEA", "SUEZ CANAL"),
    # The Malacca corridor into the South China Sea.
    ("STRAIT OF MALACCA", "SINGAPORE APPROACH"),
    ("SINGAPORE APPROACH", "SOUTH CHINA SEA"),
    ("SOUTH CHINA SEA", "TAIWAN STRAIT"),
    # Black Sea grain has exactly one way out.
    ("BLACK SEA", "TURKISH STRAITS"),
]


# ── SUPPLY CHAIN ─────────────────────────────────────────────────────────────
#
# Measured: SUPPLIER_TO, CUSTOMER_OF, SUPPLIES and PURCHASES_FROM together hold
# **zero** edges. All four are defined in the vocabulary, the graph-context
# query reads `supply_chain` for every trade recommendation, and the field has
# been empty for every brief ever produced because nothing has ever written one.
#
# This is the smallest set that makes the field non-empty for the names this
# platform actually watches, and it is the semiconductor chain because that is
# the chain whose physical geography the platform already monitors -- the
# Taiwan Strait is one of the twelve watched chokepoints, and until now a
# disruption there reached no equity by any path.
#
# Direction is (supplier, customer): the arrow points the way the goods go, so a
# supply disruption propagates forward along it. Only relationships that are
# publicly documented and structural are listed. Revenue-concentration figures
# are deliberately absent -- they change quarterly and this table does not.
SUPPLY_CHAIN: List[Tuple[str, str]] = [
    # Lithography. One vendor for EUV, which is why it is the top of the chain.
    ("ASML", "TSM"), ("ASML", "INTC"), ("ASML", "MU"),
    # Deposition, etch and process control.
    ("AMAT", "TSM"), ("AMAT", "INTC"), ("AMAT", "MU"),
    ("LRCX", "TSM"), ("LRCX", "MU"),
    ("KLAC", "TSM"), ("KLAC", "INTC"),
    # Foundry to fabless.
    ("TSM", "NVDA"), ("TSM", "AMD"), ("TSM", "AAPL"),
    ("TSM", "QCOM"), ("TSM", "AVGO"),
    # Accelerators to the hyperscalers that buy most of them.
    ("NVDA", "MSFT"), ("NVDA", "META"), ("NVDA", "GOOGL"), ("NVDA", "AMZN"),
    # Memory into the same systems.
    ("MU", "NVDA"), ("MU", "AAPL"),
]


def adjacent_chokepoints(region: str) -> List[str]:
    """Chokepoints on the same corridor as this one, in either direction."""
    if not region or not isinstance(region, str):
        return []
    key = region.strip().upper()
    out = []
    for a, b in CHOKEPOINT_ADJACENCY:
        if a == key and b not in out:
            out.append(b)
        elif b == key and a not in out:
            out.append(a)
    return out


def customers_of(supplier: str) -> List[str]:
    """Who this company supplies, or an empty list."""
    if not supplier or not isinstance(supplier, str):
        return []
    key = supplier.strip().upper()
    return [c for s, c in SUPPLY_CHAIN if s == key]


def suppliers_of(customer: str) -> List[str]:
    """Who supplies this company, or an empty list."""
    if not customer or not isinstance(customer, str):
        return []
    key = customer.strip().upper()
    return [s for s, c in SUPPLY_CHAIN if c == key]


# ── CHOKEPOINT → LITTORAL STATE ──────────────────────────────────────────────
#
# The other half of the geography join. Companies carry a country of
# registration -- Finnhub returns it for all 547 symbols the platform has
# reference data for -- and nothing put it in the graph, so `Company` had no
# geographic edge of any kind. A chokepoint could reach an energy equity
# through the commodity that transits it and could not reach the companies
# actually *on* it.
#
# With both halves the path is:
#
#     Vessel -> Region -> Country -> Company
#
# which is what answers "a strait is closing, who is registered on it" as
# opposed to "what does it price".
#
# ISO 3166-1 alpha-2, matching what the reference-data feed returns. These are
# the coastal and transit states of each strait, not everyone with an interest
# in it: Japan depends on Hormuz and is not on it, and an edge saying otherwise
# would be a claim about trade flow dressed as geography.
CHOKEPOINT_COUNTRIES: Dict[str, List[str]] = {
    "STRAIT OF HORMUZ": ["IR", "OM", "AE"],
    "GULF OF OMAN": ["OM", "IR", "AE", "PK"],
    "PERSIAN GULF": ["IR", "SA", "AE", "KW", "QA", "BH", "IQ"],
    "BAB-EL-MANDEB": ["YE", "DJ", "ER"],
    "RED SEA": ["EG", "SA", "SD", "ER", "YE"],
    "SUEZ CANAL": ["EG"],
    "STRAIT OF MALACCA": ["MY", "ID", "SG"],
    "SINGAPORE APPROACH": ["SG", "MY", "ID"],
    "SOUTH CHINA SEA": ["CN", "VN", "PH", "MY", "BN"],
    "TAIWAN STRAIT": ["TW", "CN"],
    "TURKISH STRAITS": ["TR"],
    "BLACK SEA": ["TR", "UA", "RU", "BG", "RO", "GE"],
    "GULF OF GUINEA": ["NG", "GH", "CI", "CM"],
    "PANAMA CANAL": ["PA"],
}


def countries_on_chokepoint(region: str) -> List[str]:
    """The coastal and transit states of a chokepoint, or an empty list."""
    if not region or not isinstance(region, str):
        return []
    return list(CHOKEPOINT_COUNTRIES.get(region.strip().upper(), []))
