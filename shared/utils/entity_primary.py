"""P7 (owner-approved 21 Nov 2026): choose the primary entity of a news headline, and type it honestly.

The old rule took the first token and typed it `company` whenever it was upper-case, ended in a corporate word or held two words. That put Gaza, the NSA, "FINANCIAL STOCKS"
and a first word that is also a ticker (III, RIP, LNG) on the correlation join as companies, and missed Lowe's, Home Depot and Kraft Heinz. The golden set
(tests/data/entity_golden.py) scored it 9 of 41. The rule here: a name is typed `company` only when it maps to a listed issuer; a place or agency is typed `country`;
anything else is `unknown`. Pure: no I/O.
"""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, Sequence, Tuple

# Curated headline spellings that map to one listed issuer, beyond the SEC alias registry (which needs Redis). Keys are upper-case, apostrophes dropped.
HEADLINE_COMPANIES: Dict[str, str] = {
    "NVIDIA": "NVDA", "HOME DEPOT": "HD", "LOWE": "LOW", "KRAFT HEINZ": "KHC", "VODAFONE": "VOD", "SERVICENOW": "NOW",
    "FREEPORT INDONESIA": "FCX", "FREEPORT MCMORAN": "FCX", "FREEPORT": "FCX",
    "AMAZON": "AMZN", "GOOGLE": "GOOGL", "ALPHABET": "GOOGL", "MICROSOFT": "MSFT", "APPLE": "AAPL", "TESLA": "TSLA", "NETFLIX": "NFLX",
    "JPMORGAN": "JPM", "JPMORGAN CHASE": "JPM", "GOLDMAN SACHS": "GS", "MORGAN STANLEY": "MS", "WELLS FARGO": "WFC", "BANK OF AMERICA": "BAC",
    "EXXON MOBIL": "XOM", "CHEVRON": "CVX", "BOEING": "BA", "WALMART": "WMT", "COSTCO": "COST", "TARGET": "TGT", "STARBUCKS": "SBUX",
    "GENERAL MOTORS": "GM", "FORD MOTOR": "F", "INTEL": "INTC", "BROADCOM": "AVGO", "QUALCOMM": "QCOM", "MICRON": "MU", "SALESFORCE": "CRM",
    "PALANTIR": "PLTR", "COINBASE": "COIN", "BERKSHIRE HATHAWAY": "BRK-B", "ELI LILLY": "LLY", "PFIZER": "PFE", "MODERNA": "MRNA",
}

# Places, blocs, central banks and agencies: legitimate subjects of macro and geopolitical stories, never a company. Upper-case.
PLACES_AND_AGENCIES = frozenset({
    "US", "USA", "UK", "EU", "UN", "NATO", "FED", "SEC", "ECB", "IMF", "OPEC", "NSA", "CIA", "FBI", "ITF", "WHO", "WTO", "CISA", "FOMC", "PENTAGON",
    "KREMLIN", "WHITE HOUSE", "TREASURY", "UNITED STATES", "UNITED KINGDOM", "CHINA", "RUSSIA", "UKRAINE", "IRAN", "ISRAEL", "TAIWAN", "GAZA",
    "SAUDI ARABIA", "YEMEN", "INDIA", "JAPAN", "KOREA", "NORTH KOREA", "SOUTH KOREA", "GERMANY", "FRANCE", "INDONESIA", "SINGAPORE", "MALAYSIA",
})

# Real ticker symbols that headlines use as ordinary words or acronyms (World War III, RIP, LNG, MRO, GAAP): never a subject on their own.
NON_SUBJECT_TOKENS = frozenset({"III", "II", "RIP", "LNG", "MRO", "GAAP", "JKM", "ISF"})

_PLACE_LABELS = frozenset({"GPE", "LOC", "NORP"})
_WORD = re.compile(r"[A-Z0-9][A-Z0-9&.\-]*")


def _fold(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[’'`]s?\b", "", str(text or "")).replace("&", " ").upper()).strip()


def headline_company_tickers(title: str) -> List[str]:
    """Tickers of curated companies named in a headline, in order of first appearance. A name must appear capitalised in the original text
    ("Target" the retailer, not "target" the verb), as a whole word or phrase."""
    text = re.sub(r"[’'`]", "", re.sub(r"[’'`]s\b", "", str(title or "")))
    hits: List[Tuple[int, str]] = []
    for name, ticker in sorted(HEADLINE_COMPANIES.items(), key=lambda kv: -len(kv[0])):
        pattern = r"(?<![A-Za-z0-9])" + r"\s+".join(re.escape(w) for w in name.split(" ")) + r"(?![A-Za-z0-9])"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m or not m.group(0)[0].isupper():
            continue
        if not any(t == ticker for _, t in hits):
            hits.append((m.start(), ticker))
    return [t for _, t in sorted(hits)]


def is_place_or_agency(name: str) -> bool:
    return _fold(name) in PLACES_AND_AGENCIES


def pick_primary(resolved: Sequence[str], company_tickers: Sequence[str], extracted_tickers: Sequence[str],
                 spacy_entities: Iterable[Tuple[str, str]], source: str) -> Tuple[str, str]:
    """The primary entity's id and its type value ("company", "country", "unknown" or "media_source").

    Order: tickers the collector resolved; companies mapped to a listed issuer; tickers read from the text that are not ordinary words or places; then the first
    named entity, typed by what it is (a place or agency is `country`, never `company`); the source last."""
    for t in resolved:
        if t:
            return str(t).strip().upper(), "company"
    for t in company_tickers:
        if t:
            return str(t).strip().upper(), "company"
    for t in extracted_tickers:
        up = str(t).strip().upper()
        if up and up not in NON_SUBJECT_TOKENS and not is_place_or_agency(up):
            return up, "company"
    for text, label in spacy_entities:
        name = str(text or "").strip()
        if not name or not any(ch.isalnum() for ch in name):
            continue                         # an emoji or punctuation run is not an entity (spaCy tagged a money-bag emoji as a place on 21 Nov)
        if label in _PLACE_LABELS or is_place_or_agency(name):
            return name, "country"
        return name.upper(), "unknown"      # upper-cased as the old company typing did, so subject joins on the id string still match
    for t in extracted_tickers:
        if is_place_or_agency(t):
            return str(t).strip().upper(), "country"
    return str(source or "").strip(), "media_source"
