"""
shared/utils/instrument_reach.py

Whether a correlation cluster reaches something that trades.

The reasoning service spends one of the host's two model slots on a scenario,
for several minutes each. Measured on 3 October, 58% of the scenarios it wrote
in thirty days were maritime or aviation clusters, and no vessel or aircraft in
the graph had any edge to a company -- so those scenarios could name no stock
and move no position. On a platform focused on stocks, a cluster earns that
slot by reaching an instrument: naming one, or carrying an `exposes:` tag that
a bridge attached because an OSINT event touches one (a tanker at a chokepoint
exposes crude; a Delta callsign exposes DAL).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Set

from shared.utils.equities import asset_class, is_collected_crypto

# Tag prefix the bridges use to say an event bears on an instrument.
EXPOSES_PREFIX = "exposes:"

# Domains whose clusters are about instruments by construction.
INSTRUMENT_DOMAINS = frozenset({"tradfi", "financial", "macro", "equity", "cross_asset"})

# Classes that are instruments without needing a quote to prove it: each comes
# from an explicit symbol set in shared.utils.equities.
_LISTED_CLASSES = frozenset({"commodity", "index", "macro_factor"})


def exposed_symbols(tags: Optional[Iterable[str]]) -> Set[str]:
    """Symbols named by `exposes:SYM` tags."""
    out = set()
    for t in tags or []:
        t = str(t)
        if t.lower().startswith(EXPOSES_PREFIX):
            sym = t[len(EXPOSES_PREFIX):].strip().upper()
            if sym:
                out.add(sym)
    return out


def exposure_tags(*tag_lists: Optional[Iterable[str]]) -> list:
    """Every `exposes:` tag across the given lists, sorted and de-duplicated.

    Clusters build their own tags and inherit none from their events, so an
    exposure a bridge put on an event was invisible to anything that reads the
    cluster -- including the gate above. Each construction site passes its
    events' tags through this.
    """
    found = set()
    for tags in tag_lists:
        for t in tags or []:
            if str(t).lower().startswith(EXPOSES_PREFIX):
                found.add(EXPOSES_PREFIX + str(t)[len(EXPOSES_PREFIX):].strip().upper())
    return sorted(found)


def _candidates(cluster: Any) -> Set[str]:
    names = set()
    for field in ("entity_ids", "entity_names"):
        for v in getattr(cluster, field, None) or []:
            if v:
                names.add(str(v).strip())
    for field in ("primary_entity_id", "primary_entity_name"):
        v = getattr(cluster, field, None)
        if v:
            names.add(str(v).strip())
    return {n for n in names if n and " " not in n}


def listed_instruments(cluster: Any) -> Set[str]:
    """Instruments the cluster names that need no further check: commodities,
    indices, macro factors, the collected crypto, and anything a bridge tagged."""
    found = set(exposed_symbols(getattr(cluster, "tags", None)))
    for name in _candidates(cluster):
        sym = name.upper()
        if asset_class(sym) in _LISTED_CLASSES or is_collected_crypto(sym):
            found.add(sym)
    return found


def equity_candidates(cluster: Any) -> Set[str]:
    """Names shaped like an equity ticker. Shape is not enough -- a vessel
    called GOLDEN is shaped like one -- so a caller confirms them against a
    live quote."""
    return {n.upper() for n in _candidates(cluster) if asset_class(n.upper()) == "equity"}


async def cluster_reaches_instrument(cluster: Any, redis_client: Any = None) -> bool:
    """True when the cluster is about an instrument, names one, or exposes one."""
    domain = str(getattr(cluster, "primary_domain", "") or "").lower()
    if domain in INSTRUMENT_DOMAINS:
        return True
    if listed_instruments(cluster):
        return True
    candidates = equity_candidates(cluster)
    if not candidates or redis_client is None:
        return False
    from shared.utils.quote_cache import quote_key
    raw = getattr(redis_client, "raw", redis_client)
    try:
        hits = await raw.exists(*[quote_key(c) for c in candidates])
    except Exception:
        # Unknown is not "no": a Redis error must not starve reasoning.
        return True
    return bool(hits)


# Companies kept per commodity and direction in a scenario prompt: enough to
# name the obvious ones, few enough that a small model does not copy the whole
# list into its beneficiaries.
EXPOSURE_NAMES_PER_SIDE = 5

# How each exposure predicate reads to the model.
_DIRECTION = {"POSITIVE_EXPOSURE_TO": "rises", "INVERSE_EXPOSURE_TO": "falls"}


def exposed_commodities(tags: Optional[Iterable[str]]) -> List[str]:
    """Commodities a cluster's `exposes:` tags name, sorted."""
    return sorted(s for s in exposed_symbols(tags) if asset_class(s) == "commodity")


def exposed_equities(tags: Optional[Iterable[str]]) -> List[str]:
    """Equities a bridge named directly (an airline from its callsign), sorted."""
    return sorted(s for s in exposed_symbols(tags) if asset_class(s) == "equity")


def exposure_chain(
    rows: Iterable[Dict[str, Any]],
    dollar_volume: Dict[str, float],
    per_side: int = EXPOSURE_NAMES_PER_SIDE,
) -> List[Dict[str, Any]]:
    """Group graph exposure rows by commodity and direction, most traded first.

    `rows` carry `commodity`, `rel` and `ticker`. A ticker with no recent bars
    is dropped: nothing prices it, so a scenario naming it cannot be checked.
    The 640 exposure edges come from sector priors, and the sector is the same
    for a mega-cap and a shell, so liquidity is what separates them.
    """
    sides: Dict[str, Dict[str, Set[str]]] = {}
    for r in rows:
        direction = _DIRECTION.get(str(r.get("rel") or ""))
        ticker = str(r.get("ticker") or "").upper()
        commodity = str(r.get("commodity") or "").upper()
        if direction and ticker and commodity and dollar_volume.get(ticker, 0.0) > 0:
            sides.setdefault(commodity, {}).setdefault(direction, set()).add(ticker)
    chain = []
    for commodity in sorted(sides):
        entry: Dict[str, Any] = {"commodity": commodity}
        for direction in ("rises", "falls"):
            names = sides[commodity].get(direction, set())
            entry[direction] = sorted(names, key=lambda t: (-dollar_volume[t], t))[:per_side]
        chain.append(entry)
    return chain


def exposure_lines(chain: List[Dict[str, Any]], direct: Iterable[str] = ()) -> str:
    """The chain as prompt lines; empty when there is nothing to say."""
    lines = []
    for c in chain:
        parts = []
        if c.get("rises"):
            parts.append("rise with it: " + ", ".join(c["rises"]))
        if c.get("falls"):
            parts.append("fall with it: " + ", ".join(c["falls"]))
        if parts:
            lines.append(f"• {c['commodity']} -- " + "; ".join(parts))
    direct = list(direct)
    if direct:
        lines.append("• Named directly by the signal: " + ", ".join(direct))
    return "\n".join(lines)


# Organisation names looked up per headline. A headline names two or three;
# the cap bounds the Redis round trips on a pathological one.
MAX_COMPANY_NAMES = 6

# What news calls a company where that differs from its SEC registrant title.
#
# The alias registry is seeded from SEC titles, so "Amazon" (AMAZON COM INC),
# "Meta" (Meta Platforms), "Google" and "JPMorgan" never resolved: replaying
# 1,500 headlines on 3 October found Nvidia, Boeing and Nike and missed every
# one of those. Only names a reader would not take for anything else -- no
# "Visa", "Ford", "Delta" or "Shell" alone -- and each still needs a live quote.
COMMON_COMPANY_NAMES: Dict[str, str] = {
    "AMAZON": "AMZN", "META": "META", "GOOGLE": "GOOGL", "ALPHABET": "GOOGL",
    "JPMORGAN": "JPM", "JPMORGAN CHASE": "JPM", "JP MORGAN": "JPM",
    "EXXON": "XOM", "EXXONMOBIL": "XOM", "EXXON MOBIL": "XOM",
    "MICRON": "MU", "BROADCOM": "AVGO", "TESLA": "TSLA", "NETFLIX": "NFLX",
    "SALESFORCE": "CRM", "ORACLE": "ORCL", "PALANTIR": "PLTR", "QUALCOMM": "QCOM",
    "CONOCOPHILLIPS": "COP", "OCCIDENTAL": "OXY", "HALLIBURTON": "HAL", "SCHLUMBERGER": "SLB",
    "LOCKHEED": "LMT", "RAYTHEON": "RTX", "NORTHROP": "NOC", "NORTHROP GRUMMAN": "NOC",
    "GENERAL DYNAMICS": "GD", "BOEING": "BA",
    "ELI LILLY": "LLY", "NOVO NORDISK": "NVO", "MODERNA": "MRNA", "UNITEDHEALTH": "UNH",
    "WALMART": "WMT", "COSTCO": "COST", "STARBUCKS": "SBUX", "MCDONALD": "MCD", "DISNEY": "DIS",
    "AMERICAN AIRLINES": "AAL", "UNITED AIRLINES": "UAL", "SOUTHWEST AIRLINES": "LUV",
    "FEDEX": "FDX", "CATERPILLAR": "CAT", "JOHN DEERE": "DE",
    "GOLDMAN": "GS", "GOLDMAN SACHS": "GS", "MORGAN STANLEY": "MS", "CITIGROUP": "C",
    "WELLS FARGO": "WFC", "BLACKROCK": "BLK", "COINBASE": "COIN",
    "TSMC": "TSM", "PALO ALTO NETWORKS": "PANW", "CROWDSTRIKE": "CRWD",
    "GENERAL MOTORS": "GM", "STELLANTIS": "STLA", "RIVIAN": "RIVN",
}


async def quoted_company_tickers(names: Iterable[str], redis_client: Any) -> List[str]:
    """Tickers for company names, through the SEC alias registry, kept only
    when the platform quotes them.

    The news enricher found tickers only where a headline spelled one in
    capitals, so "Nvidia Hits Record High" named the entity "AI" and reached no
    instrument. The alias registry already maps every SEC registrant's name to
    its ticker -- NVIDIA to NVDA, Delta Air Lines to DAL -- and nothing on the
    news path read it.

    Two guards. A name must resolve through a recorded alias, not merely fold
    to something ticker-shaped: "Japan" folds to JAPAN, which is no company.
    And the result must carry a live quote: the registry keeps whichever line a
    registrant listed last, so Boeing resolves to the preferred BA-PA, whose
    root BA is the one traded -- the root is tried first.
    """
    if redis_client is None:
        return []
    from shared.utils.entity_resolution import canonical_key, looks_like_ticker, resolve_entity
    from shared.utils.quote_cache import quote_key

    raw = getattr(redis_client, "raw", redis_client)
    found: List[str] = []
    for name in list(dict.fromkeys(str(n).strip() for n in names if n))[:MAX_COMPANY_NAMES]:
        if len(name) < 3:
            continue
        common = COMMON_COMPANY_NAMES.get(canonical_key(name))
        if common:
            resolved = common
        else:
            resolved = await resolve_entity(redis_client, name, record_display=False)
            if not resolved or resolved == canonical_key(name):
                continue
        for candidate in dict.fromkeys((resolved.split("-")[0], resolved)):
            if not looks_like_ticker(candidate):
                continue
            try:
                if await raw.exists(quote_key(candidate)):
                    if candidate not in found:
                        found.append(candidate)
                    break
            except Exception:
                return found
    return found
