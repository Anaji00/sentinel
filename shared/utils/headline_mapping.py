"""
shared/utils/headline_mapping.py

What a headline is about, in instruments: the sectors it touches, the country
equity markets it names, and the conflict zones it reports on -- each with the
reason, so a reader can see why a story was joined to a ticker.

The news enricher already maps commodity phrases (`text_exposure_tags`) and
named companies (`quoted_company_tickers`). Neither reaches a story that is
about an industry, a country or a war rather than a company or a barrel, which
is most of world news: measured on 4 October, 0-2% of headlines from the world
feeds carried an instrument.

Precision over recall. Patterns are whole-word, need a market or industry word
where the bare word is ambiguous, and a conflict zone's instruments are attached
only when the headline also uses conflict language. Pure: text in, mappings out.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Pattern, Tuple

from shared.utils.conflict_zones import CONFLICT_TERMS, ZONES_BY_ID, zones_for_text


@dataclass(frozen=True)
class Theme:
    sector: str
    pattern: Pattern[str]
    instruments: Tuple[str, ...]
    why: str


def _re(expr: str) -> Pattern[str]:
    return re.compile(rf"\b(?:{expr})\b", re.IGNORECASE)


THEMES: Tuple[Theme, ...] = (
    Theme("Semiconductors", _re(
        r"semiconductors?|chipmakers?|chip ?(?:stocks|makers?|exports?|controls?|curbs?|sanctions?|shortage|demand|plant|fab)|"
        r"foundry|foundries|wafers?|lithography|AI chips?|HBM|memory chips?"),
        ("SOXX", "NVDA", "TSM", "AVGO"), "Chip supply, demand or export rules move the semiconductor sector."),
    Theme("Defense", _re(
        r"defen[cs]e (?:spending|budget|contracts?|stocks|industry|firms?)|arms (?:sales?|deal|exports?|package)|"
        r"military aid|weapons (?:package|deliveries|contract)|fighter jets?|munitions|missile defen[cs]e|"
        r"air defen[cs]e systems?|Pentagon contract"),
        ("ITA", "LMT", "RTX", "NOC", "GD"), "Arms orders and defence budgets flow to the primes."),
    Theme("Airlines", _re(
        r"airlines?|air carriers?|jet fuel|airspace (?:closed|closure|closures|shut)|flights? (?:cancell?ed|suspended|grounded)|"
        r"air travel demand"),
        ("JETS", "DAL", "UAL"), "Fuel costs, closed airspace and travel demand set airline margins."),
    Theme("Shipping", _re(
        r"shipping (?:rates?|lines?|costs?|lanes?|disruption|routes?)|container (?:ships?|rates?|shipping|lines?)|"
        r"freight (?:rates?|costs?)|tanker rates?|vessels? (?:attacked|seized|hijacked|struck)|rerout(?:e|ed|ing) (?:ships|vessels)"),
        ("ZIM", "FRO"), "Freight and tanker rates rise with disrupted lanes."),
    Theme("Banks and rates", _re(
        r"interest rates?|rate (?:cut|cuts|hike|hikes|decision)|Federal Reserve|the Fed|FOMC|Treasury yields?|bond yields?|"
        r"central bank"),
        ("XLF", "TLT"), "Rate expectations move bank margins and long bonds."),
    Theme("Pharma and biotech", _re(
        r"FDA (?:approval|approves|approved|rejects?|panel)|drug (?:prices?|pricing|trial|approval)|clinical trials?|"
        r"vaccines?|biotech"),
        ("XLV", "XBI"), "Approvals and drug-pricing policy reprice pharma and biotech."),
    Theme("Cybersecurity", _re(
        r"cyber ?attacks?|ransomware|data breach(?:es)?|hackers?|hacking (?:campaign|group)|zero-day"),
        ("CIBR", "CRWD", "PANW"), "Major attacks pull forward security spending."),
    Theme("Rare earths and critical minerals", _re(
        r"rare earths?|critical minerals?|gallium|germanium|antimony|graphite exports?"),
        ("MP", "REMX"), "Supply curbs raise the value of non-Chinese supply."),
    Theme("Fertilizer", _re(r"fertili[sz]ers?|potash|ammonia (?:prices?|exports?)|urea"),
        ("NTR", "CF", "MOS"), "Gas prices and export curbs drive fertilizer margins."),
    Theme("Uranium and nuclear", _re(r"uranium|nuclear (?:power|reactors?|fuel|plants?)|enrichment plant"),
        ("CCJ", "URA"), "Reactor demand and supply disruption move uranium."),
    Theme("Lithium and batteries", _re(r"lithium|battery (?:makers?|metals?|supply)|cobalt|nickel (?:prices?|exports?)"),
        ("ALB", "LIT"), "Battery-metal supply and EV demand move lithium producers."),
    Theme("Steel and metals", _re(r"steel (?:tariffs?|prices?|imports?|exports?|output)|aluminium|aluminum"),
        ("SLX", "NUE"), "Tariffs and output cuts reprice steel and aluminium."),
    Theme("Tariffs and trade", _re(
        r"tariffs?|trade (?:war|deal|talks|tensions|truce|restrictions?)|export (?:ban|bans|controls?)|import (?:ban|duties)"),
        ("SPY", "FXI"), "Trade policy moves the broad market and China-exposed equities."),
)

# Country equity markets. Whole words; "North Korea" never maps to the South.
COUNTRY_ETFS: Tuple[Tuple[str, Pattern[str], Tuple[str, ...]], ...] = (
    ("CN", _re(r"China|Chinese|Beijing"), ("FXI", "MCHI")),
    ("TW", _re(r"Taiwan|Taiwanese|Taipei"), ("EWT",)),
    ("JP", _re(r"Japan|Japanese|Tokyo"), ("EWJ",)),
    ("KR", re.compile(r"(?<!North )\b(?:South Korea|South Korean|Seoul)\b", re.IGNORECASE), ("EWY",)),
    ("IN", _re(r"India|Indian(?! Ocean)|New Delhi"), ("INDA",)),
    ("BR", _re(r"Brazil|Brazilian"), ("EWZ",)),
    ("MX", _re(r"Mexico|Mexican"), ("EWW",)),
    ("DE", _re(r"Germany|German|Berlin"), ("EWG",)),
    ("GB", _re(r"Britain|British|UK"), ("EWU",)),
    ("IL", _re(r"Israel|Israeli"), ("EIS",)),
    ("SA", _re(r"Saudi Arabia|Saudi"), ("KSA",)),
    ("TR", _re(r"Turkey|Turkish|T[uü]rkiye|Ankara"), ("TUR",)),
)

# Escalation, in any inflection ("strikes", "attacked", "shelling"). Narrower
# than the zone monitor's CONFLICT_TERMS on purpose: "military" and "troops"
# also describe diplomacy ("no military solution"), which moves no price.
_ESCALATION = re.compile(
    r"\b(?:attack|strike|airstrike|missile|drone|shell|offensive|bomb|blockade|seiz|seize|"
    r"invasion|invade|clash|kill|warship|ceasefire|truce|hijack|sank|sink)(?:s|ed|d|ing|ure|es)?\b",
    re.IGNORECASE,
)
# De-escalation reverses the expected move: a ceasefire takes the risk premium
# out of crude. Unless the ceasefire is itself breaking down.
_DEESCALATION = _re(
    r"ceasefires?|truces?|peace (?:talks|deal|agreement|plan)|armistice|de-?escalat\w*|"
    r"withdraws? troops|troop withdrawal|hostages? (?:released|freed)"
)
_BREAKDOWN = _re(r"collaps\w*|breaks? down|broken|violat\w*|ends?|resum\w*|shatter\w*")


def conflict_polarity(text: Optional[str]) -> int:
    """+1 escalation, -1 de-escalation, 0 neither, for conflict headlines.

    "Ceasefire collapses as strikes resume" is escalation; "Truce agreed in
    Gaza" is de-escalation; a headline using neither vocabulary is 0.
    """
    if not text or _NOT_CONFLICT.search(text):
        return 0
    if _DEESCALATION.search(text):
        return 1 if _BREAKDOWN.search(text) else -1
    return 1 if _ESCALATION.search(text) else 0


# Sport and figure of speech: "verbal attack", a football "tie" in Ireland.
_NOT_CONFLICT = _re(
    r"football|soccer|basketball|cricket|rugby|tennis|olympics?|world cup|league|striker|"
    r"verbal attack|heart attack|panic attack|news conference"
)

# A zone named with any conflict word at all, for the zone tag alone.
_CONFLICT_LANGUAGE = _re("|".join(CONFLICT_TERMS) + r"|war|invasion|shelled|airstrikes?|bombing|naval blockade")

# A country maps to its equity market only when the story is about its economy,
# markets or politics. "Germany's Merz arrives in Kyiv" is about Ukraine.
_MARKET_CONTEXT = _re(
    r"econom(?:y|ic|ies)|markets?|stocks?|shares|equities|GDP|inflation|currency|yen|yuan|renminbi|rupee|peso|"
    r"real|lira|shekel|won|central bank|interest rates?|elections?|run-?off|polls?|tariffs?|exports?|imports?|"
    r"growth|recession|budget|bonds?|debt|investors?|stimulus|downgrade|upgrade|IPO|earnings")


@dataclass(frozen=True)
class Affected:
    symbol: str
    via: str        # "sector:<name>", "country:<iso>" or "zone:<id>"
    reason: str


def map_headline(text: Optional[str]) -> List[Affected]:
    """Every instrument a headline bears on, with the reason, de-duplicated.

    The first reason found for a symbol is kept: sectors, then zones, then
    countries, from most to least specific.
    """
    if not text:
        return []
    out: Dict[str, Affected] = {}

    def add(symbols: Tuple[str, ...], via: str, reason: str) -> None:
        for s in symbols:
            out.setdefault(s, Affected(symbol=s, via=via, reason=reason))

    for theme in THEMES:
        if theme.pattern.search(text):
            add(theme.instruments, f"sector:{theme.sector}", theme.why)

    if _ESCALATION.search(text) and not _NOT_CONFLICT.search(text):
        for zone_id in zones_for_text(text):
            zone = ZONES_BY_ID[zone_id]
            for r in zone.ripple:
                if r.kind == "instrument":
                    add((r.target,), f"zone:{zone_id}", f"{zone.name}: {r.why}")

    if _MARKET_CONTEXT.search(text):
        for iso, pattern, etfs in COUNTRY_ETFS:
            if pattern.search(text):
                add(etfs, f"country:{iso}", f"About {iso}'s economy or markets; its equity market.")
    return list(out.values())


def zone_tags(text: Optional[str]) -> List[str]:
    """`zone:<id>` for each conflict zone a headline reports conflict in."""
    if not text or _NOT_CONFLICT.search(text):
        return []
    # Either test: a headline whose instruments were mapped through a zone
    # must also carry that zone's tag.
    if not (_CONFLICT_LANGUAGE.search(text) or _ESCALATION.search(text)):
        return []
    return [f"zone:{z}" for z in zones_for_text(text)]


def mapping_tags(affected: List[Affected]) -> List[str]:
    """`exposes:` tags for the instruments and one tag per sector, zone or country."""
    tags = [f"exposes:{a.symbol}" for a in affected]
    tags += sorted({a.via.lower().replace(" ", "_") for a in affected})
    return list(dict.fromkeys(tags))


def equities(affected: List[Affected]) -> List[str]:
    """The mapped symbols that are listed equities or ETFs, not futures."""
    return [a.symbol for a in affected if not a.symbol.endswith("=F")]


__all__ = [
    "Affected", "COUNTRY_ETFS", "THEMES", "Theme", "conflict_polarity", "equities",
    "map_headline", "mapping_tags", "zone_tags",
]
