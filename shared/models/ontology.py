"""
shared/models/ontology.py

Centralized Governance for Sentinel Knowledge Graph:
- Canonical predicate allowlist across all ingestion and reasoning services.
- Node label constraints.
- Predicate categorization and metadata.
- Validation and normalization utilities.
"""

from typing import Set, Dict, Optional, Any


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
    "GRANGER_CAUSES",                # Directional predictive causality (properties: lag, f_stat, p_value)
    "HAWKES_EXCITES",                # Cross-domain self/mutually exciting point process (branching_ratio, half_life)
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
    "UnknownEntity"
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


def normalize_predicate(predicate: str, default: str = "RELATED_TO") -> str:
    """
    Normalizes a predicate string to upper case and validates against the allowlist.
    Returns the valid predicate or the safe default.
    """
    if not predicate or not isinstance(predicate, str):
        return default
    cleaned = predicate.strip().upper()
    return cleaned if cleaned in VALID_PREDICATES else default


def is_valid_node_label(label: str) -> bool:
    """Checks if a node label is recognized and safe against Cypher injection."""
    if not label or not isinstance(label, str):
        return False
    return label.strip() in ALLOWED_NODE_LABELS
