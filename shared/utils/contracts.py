"""
shared/utils/contracts.py

Federal contract money to listed companies, from USAspending.gov -- the pure half.

USAspending's award search returns lifetime totals (national-lab contracts from
1978), which is not news. Its transaction search returns each obligation with
its action date: in the week to 3 October that was $342M and $242M to Leidos
units, $402M to Maximus, $216M to CACI and $205M to Amentum -- all listed, all
filed under subsidiary names the SEC registry does not know.
"""

from __future__ import annotations

import re
from typing import Callable, Dict, Iterable, List, Optional

# Recipient name fragments -> listed parent. Curated because federal awards go to
# operating subsidiaries ("ELECTRIC BOAT CORPORATION" is General Dynamics) and a
# wrong match would put government money on the wrong stock. Matched as a
# whole-word prefix of the normalised recipient name, longest first.
CONTRACTOR_PARENTS: Dict[str, str] = {
    "LOCKHEED MARTIN": "LMT", "SIKORSKY": "LMT",
    "RAYTHEON": "RTX", "PRATT & WHITNEY": "RTX", "PRATT AND WHITNEY": "RTX", "COLLINS AEROSPACE": "RTX", "RTX": "RTX",
    "GENERAL DYNAMICS": "GD", "ELECTRIC BOAT": "GD", "BATH IRON WORKS": "GD", "GULFSTREAM": "GD",
    "NATIONAL STEEL AND SHIPBUILDING": "GD",
    "NORTHROP GRUMMAN": "NOC", "HUNTINGTON INGALLS": "HII", "L3HARRIS": "LHX", "HARRIS CORP": "LHX",
    "BOEING": "BA", "LEIDOS": "LDOS", "BOOZ ALLEN": "BAH", "SCIENCE APPLICATIONS INTERNATIONAL": "SAIC",
    "CACI": "CACI", "MAXIMUS": "MMS", "AMENTUM": "AMTM", "KBR": "KBR", "PARSONS": "PSN",
    "V2X": "VVX", "VECTRUS": "VVX", "PALANTIR": "PLTR", "GE AEROSPACE": "GE", "GENERAL ELECTRIC": "GE",
    "HONEYWELL": "HON", "TEXTRON": "TXT", "BELL TEXTRON": "TXT", "BWX": "BWXT", "FLUOR": "FLR",
    "JACOBS": "J", "AECOM": "ACM", "TETRA TECH": "TTEK", "ACCENTURE FEDERAL": "ACN",
    "INTERNATIONAL BUSINESS MACHINES": "IBM", "MICROSOFT": "MSFT", "AMAZON WEB SERVICES": "AMZN",
    "ORACLE": "ORCL", "SPACE EXPLORATION TECHNOLOGIES": "SPCX", "MCKESSON": "MCK", "CARDINAL HEALTH": "CAH",
    "HUMANA": "HUM", "HEALTH NET FEDERAL": "CNC", "CENTENE": "CNC", "UNITEDHEALTH": "UNH", "OPTUM": "UNH",
    "ELBIT SYSTEMS OF AMERICA": "ESLT", "MOTOROLA SOLUTIONS": "MSI", "OSHKOSH": "OSK", "AEROVIRONMENT": "AVAV",
    "KRATOS": "KTOS", "MERCURY SYSTEMS": "MRCY", "CURTISS-WRIGHT": "CW", "HEICO": "HEI", "TRANSDIGM": "TDG",
    "PFIZER": "PFE", "MODERNA": "MRNA", "MERCK SHARP": "MRK", "ABBVIE": "ABBV", "GILEAD": "GILD",
    "VERIZON": "VZ", "AT&T": "T", "LUMEN": "LUMN", "DELL FEDERAL": "DELL", "HP FEDERAL": "HPQ",
    "CISCO": "CSCO", "PERATON": None, "BECHTEL": None, "DELOITTE": None,
}

_SUFFIXES = re.compile(r"\b(INC|INCORPORATED|LLC|L\.L\.C|CORP|CORPORATION|CO|COMPANY|LTD|LP|PLC|THE|FEDERAL|"
                       r"SERVICES|SYSTEMS|HOLDINGS|GROUP)\b\.?", re.I)


def normalize_recipient(name: str) -> str:
    text = re.sub(r"[,.]", " ", str(name or "").upper())
    text = re.sub(r"\s*-\s*", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[4:] if text.startswith("THE ") else text


def parent_ticker(recipient: str, resolve: Optional[Callable[[str], Optional[str]]] = None) -> Optional[str]:
    """The listed parent of a federal award recipient, or None.

    The curated table first, as a whole-word prefix of the name; a known
    private contractor (Peraton, Bechtel, Deloitte) stops there with None.
    Then the SEC registry on the full name, then on the name stripped of
    corporate suffixes -- exact matches only, because a fuzzy one is how money
    lands on the wrong company.
    """
    name = normalize_recipient(recipient)
    for frag in sorted(CONTRACTOR_PARENTS, key=len, reverse=True):
        if name == frag or name.startswith(frag + " ") or name.startswith(frag + "-"):
            return CONTRACTOR_PARENTS[frag]
    if resolve is None:
        return None
    for candidate in (name, _SUFFIXES.sub("", name).strip()):
        if candidate:
            hit = resolve(candidate)
            if hit:
                return hit
    return None


def aggregate_awards(rows: Iterable[Dict[str, object]], ticker_of: Callable[[str], Optional[str]]
                     ) -> Dict[str, Dict[str, object]]:
    """{ticker: {total, actions, agencies, recipients, largest}} over positive obligations."""
    out: Dict[str, Dict[str, object]] = {}
    for r in rows:
        amount = float(r.get("Transaction Amount") or 0.0)
        if amount <= 0:
            continue
        ticker = ticker_of(str(r.get("Recipient Name") or ""))
        if not ticker:
            continue
        agg = out.setdefault(ticker, {"total": 0.0, "actions": 0, "agencies": set(), "recipients": set(),
                                      "largest": None})
        agg["total"] = float(agg["total"]) + amount
        agg["actions"] = int(agg["actions"]) + 1
        agg["agencies"].add(str(r.get("Awarding Agency") or ""))
        agg["recipients"].add(str(r.get("Recipient Name") or ""))
        if agg["largest"] is None or amount > float(agg["largest"]["amount"]):
            agg["largest"] = {"amount": amount, "description": str(r.get("Transaction Description") or "")[:160],
                              "agency": str(r.get("Awarding Agency") or ""), "award_id": r.get("Award ID")}
    return out


def award_score(total_usd: float, market_cap_usd: Optional[float]) -> float:
    """Money against the size of the company that receives it.

    $300M is a quarter's news for CACI and a rounding error for Boeing. 0.5% of
    market cap scores 0.5, 2% scores 0.8; an unsized company gets 0.35.
    """
    if not market_cap_usd or market_cap_usd <= 0:
        return 0.35
    share = total_usd / market_cap_usd
    return round(min(0.9, 0.3 + 40.0 * share), 3)
