"""
shared/utils/osint_bridges.py

What a vessel or a place exposes, in instruments.

Measured on 3 October: 13,539 vessels and 24,778 aircraft in the graph had edges
only to a region and a flag, so nothing the platform saw at sea or in the air
could reach a stock. The bridge that already worked was 14 Commodity nodes with
640 exposure edges to companies. These mappings attach an `exposes:` tag to an
OSINT event so it reaches those commodities, and through them the companies.

Deliberately coarse and stated as data: a tanker at a chokepoint bears on crude,
a cargo ship in the Black Sea on grain, anything around Taiwan on
semiconductors. AIS ship type cannot tell an LNG carrier from an oil tanker or
a container ship from a bulker, so no mapping here claims to.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple

CRUDE = ("CL=F", "BZ=F")
BRENT = ("BZ=F",)
GRAIN = ("ZW=F", "ZC=F")
SEMIS = ("TSM", "SOXX")


def region_tag(region: Optional[str]) -> str:
    """The tag form of a region name: "Strait of Hormuz" -> "strait_of_hormuz"."""
    return (region or "").strip().lower().replace(" ", "_")


# Waters a tanker's behaviour bears on crude through.
TANKER_REGIONS = frozenset(region_tag(r) for r in (
    "Strait of Hormuz", "Persian Gulf", "Gulf of Oman", "Iranian Territorial",
    "Bab-el-Mandeb", "Red Sea", "Gulf of Aden", "Suez Canal",
    "Turkish Straits", "Black Sea", "Danish Straits", "Barents Sea",
    "Strait of Malacca", "Singapore Approach", "Strait of Lombok",
    "Nigerian Territorial", "Gulf of Guinea", "Somali Territorial",
))

# Waters whose dry-cargo traffic is the grain trade.
GRAIN_REGIONS = frozenset(region_tag(r) for r in (
    "Black Sea", "Crimean Waters", "Ukrainian Waters", "Georgian Waters", "Turkish Straits",
))

# Places whose disruption is priced in instruments regardless of what moves
# there: the region's own exposure, applied to vessels, aircraft and gaps alike.
REGION_EXPOSURES: Dict[str, Tuple[str, ...]] = {
    **{region_tag(r): CRUDE for r in (
        "Strait of Hormuz", "Persian Gulf", "Gulf of Oman", "Iranian Territorial",
    )},
    **{region_tag(r): BRENT for r in ("Bab-el-Mandeb", "Red Sea", "Gulf of Aden", "Suez Canal")},
    **{region_tag(r): GRAIN for r in ("Black Sea", "Crimean Waters", "Ukrainian Waters")},
    **{region_tag(r): SEMIS for r in ("Taiwan Strait", "Taiwan Territorial", "Taiwan ADIZ")},
}


def _tags(symbols: Iterable[str]) -> List[str]:
    return sorted({f"exposes:{s}" for s in symbols})


def region_exposure_tags(region: Optional[str]) -> List[str]:
    """What a place exposes, whatever was observed there."""
    return _tags(REGION_EXPOSURES.get(region_tag(region), ()))


def vessel_exposure_tags(region: Optional[str], vessel_type: Optional[str]) -> List[str]:
    """What a vessel exposes, by its type and where it is, plus the place's own."""
    rt = region_tag(region)
    vt = (vessel_type or "").strip().lower()
    symbols = list(REGION_EXPOSURES.get(rt, ()))
    if vt == "tanker" and rt in TANKER_REGIONS:
        symbols += CRUDE
    if vt in ("cargovessel", "cargo") and rt in GRAIN_REGIONS:
        symbols += GRAIN
    return _tags(symbols)


# Region-to-instrument edges for the graph, so a geopolitical scenario's region
# names what it would move. (region label, symbol, target label)
def region_exposure_edges() -> List[Tuple[str, str, str]]:
    names = {
        "Strait of Hormuz", "Persian Gulf", "Gulf of Oman", "Iranian Territorial",
        "Bab-el-Mandeb", "Red Sea", "Gulf of Aden", "Suez Canal",
        "Black Sea", "Crimean Waters", "Ukrainian Waters",
        "Taiwan Strait", "Taiwan Territorial", "Taiwan ADIZ",
    }
    edges = []
    for name in sorted(names):
        for sym in REGION_EXPOSURES[region_tag(name)]:
            target = "Commodity" if sym.endswith("=F") else "Company"
            edges.append((name, sym, target))
    return edges


# What a headline about a commodity exposes. Whole words only, and phrases a
# reader would take as being about the market: "oil" alone also means cooking
# oil and oil paintings, so it needs a market word beside it.
_TEXT_EXPOSURES: List[Tuple[re.Pattern, Tuple[str, ...]]] = [
    (re.compile(r"\b(crude|opec\+?|brent|wti|oil (?:price|prices|output|production|"
                r"supply|supplies|exports?|imports?|reserves?|tankers?|field|fields|"
                r"major|majors|market)|(?:barrels?|bpd) (?:of|a day|per day)|"
                r"\d*(?:million|mn|bn|billion) barrels)\b", re.I), CRUDE),
    (re.compile(r"\b(natural gas|lng|gas (?:price|prices|supply|supplies|pipeline|exports?))\b",
                re.I), ("NG=F",)),
    (re.compile(r"\b(wheat|corn|grain|grains)\b", re.I), GRAIN),
    (re.compile(r"\b(gold (?:price|prices|futures|reserves?)|bullion)\b", re.I), ("GC=F",)),
    (re.compile(r"\b(copper)\b", re.I), ("HG=F",)),
    (re.compile(r"\b(semiconductors?|chipmakers?|chip (?:exports?|controls?|sanctions?|curbs?)|"
                r"export controls? on chips)\b", re.I), SEMIS),
]


def text_exposure_tags(text: Optional[str]) -> List[str]:
    """What a piece of prose exposes, by the commodities it is about."""
    if not text:
        return []
    symbols: List[str] = []
    for pattern, syms in _TEXT_EXPOSURES:
        if pattern.search(text):
            symbols += syms
    return _tags(symbols)


# Listed equities whose price is, in practice, a position in the coin: an
# exchange, a treasury company, and the largest US miners. A BTC liquidation
# burst that cannot name them is a crypto event an equity desk never sees.
CRYPTO_EQUITIES: Dict[str, Tuple[str, ...]] = {
    "BTC": ("COIN", "MSTR", "MARA", "RIOT", "CLSK", "HOOD"),
    "ETH": ("COIN", "HOOD"),
}


def crypto_exposure_tags(asset: Optional[str]) -> List[str]:
    """What a BTC or ETH event exposes on the equity side, by base symbol."""
    from shared.utils.equities import instrument_identity

    return _tags(CRYPTO_EQUITIES.get(instrument_identity(asset or ""), ()))
