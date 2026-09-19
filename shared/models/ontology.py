"""
shared/models/ontology.py

Centralized Governance for Sentinel Knowledge Graph:
- Canonical predicate allowlist across all ingestion and reasoning services.
- Node label constraints.
- Predicate categorization and metadata.
- Validation and normalization utilities.
"""

import logging
from typing import Set, Dict, Optional, Any

from shared.utils.quiet_failures import dropped

_LOG = logging.getLogger("sentinel.ontology")


# ── CANONICAL PREDICATE VOCABULARY (§3.4, §3.5) ──────────────────────────────

VALID_PREDICATES: Set[str] = {
    # 1. Geopolitical & Corporate Narrative
    "OPERATES_IN",
    "OWNED_BY",
    "AFFILIATED_WITH",
    "SANCTIONED_BY",
    "TARGETS",
    "CONFLICTS_WITH",         # Geopolitical conflict / nation-state confrontation
    "LOCATED_IN",
    "RELATED_TO",
    "SUBSIDIARY_OF",
    "ADJACENT_TO",
    "ATTACKED",
    "TARGETED_BY",
    "REGISTERED_IN",
    "EMPLOYS",
    "CONTROLS",
    "ALLIED_WITH",
    "OWNS",

    # 2. Equity, Supply Chain & Industrial Structure (§3.5)
    "MEMBER_OF",              # Ticker -> Index / Benchmark (e.g. NVDA -> SPX)
    "CUSTOMER_OF",            # Revenue-concentration risk flow (e.g. NVDA -> TSM)
    "SUPPLIER_TO",            # Supply-disruption risk flow (e.g. TSM -> NVDA)
    "SUPPLIES",               # Legacy / bidirectional supply relation
    "PURCHASES_FROM",         # Procurement relation
    "COMPETES_WITH",          # Corporate rivalry & market-share contest (distinct from geopolitical CONFLICTS_WITH)
    "SYMPATHY_MOVER",         # Lead-lag sympathetic co-movement across equities

    # 3. Macro & Regulatory Exposures (§3.5)
    "COMMODITY_EXPOSURE",     # Sensitivity to physical commodity (e.g. CL=F, GC=F)
    "POSITIVE_EXPOSURE_TO",   # Direct / pro-cyclical beta exposure
    "INVERSE_EXPOSURE_TO",    # Inverse / counter-cyclical beta exposure
    "FX_EXPOSURE_TO",         # International revenue currency sensitivity (e.g. EUR, JPY, CNY)
    "REGULATORY_EXPOSURE",    # Exposure to export controls, antitrust, or sector policies
    "MACRO_CORRELATED",       # Empirical correlation to macro index/indicator
    "HAS_EXPOSURE_IN",        # Geographic or sector asset exposure
    "SANCTIONS_TARGET",       # Direct sanctions designation
    "FLAGGED_BY",             # Intelligence flag from supervisory enricher

    # 4. Statistical & Quantitative Causal Family (§3.5)
    "STATISTICALLY_CORRELATED_WITH", # Empirical correlation (properties: method, window, coefficient, p_value)
    # A measured comparable: two issuers whose *returns* co-move over a common
    # window, which is what an earnings surprise actually travels along. Distinct
    # from STATISTICALLY_CORRELATED_WITH, which records any discovered
    # relationship including cross-asset and cross-domain ones, and from
    # SYMPATHY_MOVER, which is a model's narrative explanation of a transmission
    # path rather than a measurement of one. Carries a signed coefficient, so an
    # inverse peer -- a hedge, or a share-shift pair where one name's loss is
    # another's gain -- is still a peer and still transmits, the other way.
    "PEER_OF",
    "GRANGER_CAUSES",                # Directional predictive causality (properties: lag, f_stat, p_value)
    "HAWKES_EXCITES",

    # 5. On-chain
    # A transfer between two addresses. RELATED_TO accounted for 270,513 of
    # roughly 370,000 edges -- 73% of the graph -- and almost all of it was the
    # crypto enricher recording transfers under the vocabulary's catch-all,
    # beside SYMPATHY_MOVER at 172 and HAS_EXPOSURE_IN at 135. An edge type
    # that covers three quarters of a graph distinguishes nothing, and the one
    # relationship it was mostly standing in for is both specific and the
    # highest-volume fact this platform observes.
    "TRANSACTED_WITH",                # Cross-domain self/mutually exciting point process (branching_ratio, half_life)
}


# ── CANONICAL NODE LABELS ───────────────────────────────────────────────────

ALLOWED_NODE_LABELS: Set[str] = {
    "Entity",
    "Company",
    "Vessel",
    "Aircraft",
    "Index",
    "Sector",
    "Industry",
    "MacroFactor",
    "Commodity",
    "Location",
    "Region",
    "Person",
    "Organization",
    "Government",
    "RegulatoryAction",
    "SupplyChainMetric",
    "InstitutionalFiler",
    "Flag",
    "UnknownEntity",
    # Added after a census: 323 CryptoAsset nodes are already in the graph and
    # this set rejected the label, so every new crypto proposal was rewritten to
    # `Entity`. The allowlist is meant to stop injection, not to quietly delete
    # a domain the platform collects on.
    "CryptoAsset",
    # Likewise present in the graph and absent from this set, so every proposal
    # naming one was rewritten to `Entity`:
    #
    #   AutonomousSystem  2,870 nodes    Vulnerability  1,665
    #   Prefix            6,109          Country            1
    #
    # `Country` mattered most despite holding one node: without it,
    # `is_valid_node_label("Country")` was false, so a producer correctly
    # labelling `US` as a country fell through to the instrument classifier and
    # came back `Company`.
    "AutonomousSystem",
    "Prefix",
    "Vulnerability",
    "Country",
    # The single omission that made this graph look untyped.
    #
    # `crypto.py` proposes `target_label: "Wallet"` for every transfer
    # counterparty. This set rejected it, so the supervisor rewrote it to
    # `Entity` -- and 254,542 of the 257,689 `:Entity` nodes, 98.8% of them,
    # are Ethereum addresses. The "84% of the graph is untyped" finding was
    # almost entirely this one line. With `Wallet` admitted, `:Entity` falls to
    # about 3,100 nodes and starts meaning what it says.
    #
    # It also restores a normalisation that was silently disabled:
    # `_LABEL_TO_ENTITY_TYPE` maps wallet -> EntityType.WALLET, which is in
    # `_LOWERCASE_ID_TYPES`, so a `Wallet` label lower-cases the address.
    # Routed through `Entity` it became UNKNOWN, which returns the identifier
    # exactly as written -- which is how `0x...` and `0X...` once existed as
    # 6,366 and 139,047 separate nodes for the same addresses. That casing is
    # clean today, and nothing was holding it clean except the producer.
    #
    # `supervisor.py` already iterates a label list containing "Wallet",
    # so one part of the system has been expecting these nodes all along.
    "Wallet",
}


# ── ANTI-SURVEILLANCE ARCHITECTURAL BOUNDARY (§2.5) ─────────────────────────
# Sentinel is an open intelligence capability for markets, infrastructure, and
# corporate governance — NOT a mass surveillance system over private citizens.
PROHIBITED_DATA_CATEGORIES: Set[str] = {
    "mobile_carrier_location",
    "consumer_device_telemetry",
    "facial_recognition",
    "biometric_surveillance",
    "private_citizen_communications",
    "personal_credit_surveillance",
}

def validate_data_boundary_compliance(category: str) -> bool:
    """Returns True if the data category complies with Sentinel's anti-surveillance perimeter."""
    return category.lower() not in PROHIBITED_DATA_CATEGORIES


# ── STATISTICAL PREDICATES METADATA ─────────────────────────────────────────

STATISTICAL_PREDICATES: Set[str] = {
    "STATISTICALLY_CORRELATED_WITH",
    "PEER_OF",
    "GRANGER_CAUSES",
    "HAWKES_EXCITES",
    "SYMPATHY_MOVER",
    "MACRO_CORRELATED",
}


# ── VALIDATION & NORMALIZATION HELPERS ──────────────────────────────────────

def is_valid_predicate(predicate: str) -> bool:
    """Checks if a candidate predicate string is in the authorized allowlist."""
    if not predicate or not isinstance(predicate, str):
        return False
    return predicate.strip().upper() in VALID_PREDICATES


def normalize_predicate(
    predicate: str, default: str = "RELATED_TO", *, source: str = "unspecified"
) -> str:
    """
    Normalizes a predicate string to upper case and validates against the allowlist.
    Returns the valid predicate or the safe default.

    The fallback is counted. RELATED_TO holds 272,124 of roughly 442,000 edges
    -- 62% of the graph -- and this line is where most of them were made. A
    default that silently absorbs anything it does not recognise cannot be
    distinguished from a vocabulary that genuinely fits, and nothing anywhere
    reported the difference. `dropped` counts every occurrence, logs at DEBUG,
    and escalates to WARNING on the first, at powers of ten, and once per
    interval after that, so a new producer emitting an unmapped predicate
    becomes visible without anyone having to go looking.
    """
    if not predicate or not isinstance(predicate, str):
        dropped(
            "graph.predicate_missing",
            f"no predicate supplied; using {default}",
            _LOG, detail=source,
        )
        return default
    cleaned = predicate.strip().upper()
    if cleaned in VALID_PREDICATES:
        return cleaned
    dropped(
        "graph.predicate_not_in_vocabulary",
        f"{cleaned!r} is not a canonical predicate; using {default}",
        _LOG, detail=source,
    )
    return default


# Labels the instrument classifier is allowed to refine.
#
# Everything here is either "no opinion" or already a financial instrument, so
# deciding between them from the symbol is an improvement. Every other label in
# ALLOWED_NODE_LABELS -- Flag, Vessel, Aircraft, Region, Country, Person,
# Organization, Government, Sector, Industry and the rest -- belongs to a
# producer that knows what domain it is writing in, and is left alone.
_REFINABLE = frozenset({
    "Entity", "UnknownEntity",
    "Company", "CryptoAsset", "Commodity", "Index", "MacroFactor",
})


def resolve_node_label(
    raw_label: Optional[str], name: Any = None, *, source: str = "unspecified"
) -> str:
    """The single place a node's label is decided.

    Every producer used to choose its own label and pass it to the graph
    supervisor, which merges with `MERGE (n:{label} {name: ...})` -- so the
    label is part of the identity, and two producers disagreeing about one
    company create two nodes rather than merging into one. Measured: 50 tickers
    exist twice, and CL=F exists three times. NVDA's two halves held 32 and 4
    edges, and the query behind every trade prompt read the half with 4.

    Resolution order, and why:

    1. What the symbol *is*, when it is a recognisable instrument. This
       overrides the caller, because the caller is the thing that has been
       wrong: crude oil, gold, Brent, SPY and QQQ are all stored as `:Company`
       today. An issuer of proposals does not get to redefine what a futures
       contract is.
    2. The caller's proposal, when the ontology recognises it. Vessels,
       aircraft, regions and people are not instruments and only the producer
       knows what they are.
    3. `Entity` -- counted, never silent. This is the line that produced
       257,473 of roughly 305,000 nodes.
    """
    # A producer naming a non-financial domain is believed, always.
    #
    # The first version of this function let the symbol classifier override any
    # label, and within two minutes of deployment it had created seven
    # `:Company` nodes named SG, RW, BS, LR, HK, TW and VN -- Singapore,
    # Rwanda, the Bahamas, Liberia, Hong Kong, Taiwan and Vietnam. Those are
    # ship registry codes arriving from the AIS enricher with a correct `Flag`
    # label, and they are ticker-shaped, so the classifier called them
    # companies and overruled a producer that was right.
    #
    # It is the same collision the node-merge migration refuses to touch --
    # DE is Germany and Deere, KR is South Korea and Kroger -- and guarding the
    # migration against it while leaving the write path open simply moved the
    # corruption from one place to another.
    #
    # The rule that holds: the classifier knows about instruments and nothing
    # else, so it may only refine a label that is already financial or absent.
    # A producer that says `Flag`, `Vessel` or `Region` has domain knowledge no
    # regex over a symbol string can have.
    if raw_label and is_valid_node_label(raw_label) and raw_label not in _REFINABLE:
        return raw_label

    inferred = _instrument_label(name)
    if inferred:
        if (
            raw_label
            and raw_label not in ("Entity", inferred)
            and is_valid_node_label(raw_label)
        ):
            # "Entity" is excluded deliberately. A producer that says `Entity`
            # is not asserting a wrong type, it is declining to assert one, and
            # enriching that is this function's whole purpose -- counting it as
            # a conflict would bury the real ones under the common case. What
            # is worth reporting is a producer that positively insists crude
            # oil is a company, because that is a bug upstream and this only
            # papers over it here.
            dropped(
                "graph.label_overridden_by_symbol",
                f"producer said {raw_label!r}, symbol says {inferred!r}",
                _LOG, detail=f"{source}:{name}",
            )
        return inferred

    if raw_label and is_valid_node_label(raw_label):
        return raw_label

    dropped(
        "graph.label_fallback_to_entity",
        f"{raw_label!r} is not a canonical node label; using Entity",
        _LOG, detail=f"{source}:{name}",
    )
    return "Entity"


def _instrument_label(name: Any) -> Optional[str]:
    """`Commodity` for CL=F, `Index` for QQQ, `Company` for NVDA, else None.

    None is returned for anything that is not a recognisable instrument symbol
    -- an OSINT organisation, a vessel, a wallet address -- so the caller's own
    label survives. Guessing `Company` for every unrecognised string is the
    failure this function exists to stop, not one to reproduce.
    """
    try:
        from shared.utils.equities import asset_class, ASSET_CLASS_TO_LABEL
    except Exception:  # pragma: no cover - import guard only
        return None
    if name is None:
        return None
    cls = asset_class(str(name))
    if not cls:
        return None
    label = ASSET_CLASS_TO_LABEL.get(cls)
    return label if label and label in ALLOWED_NODE_LABELS else None


def is_valid_node_label(label: str) -> bool:
    """Checks if a node label is recognized and safe against Cypher injection."""
    if not label or not isinstance(label, str):
        return False
    return label.strip() in ALLOWED_NODE_LABELS
