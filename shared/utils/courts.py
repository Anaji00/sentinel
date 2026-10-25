"""
shared/utils/courts.py

Surveillance in the federal courts (CourtListener, Free Law Project): the
cases over it, the judges assigned to them, and those judges' disclosed
financial ties to surveillance vendors. Pure; the collector fetches.

Ties are stated as what the Ethics in Government Act disclosures show: a judge
held a vendor's stock in a given year. A *possible* conflict under 28 U.S.C.
§455(b)(4) (a financial interest in a party) is flagged only when the vendor
held is itself a party to the case. Disclosures lag by a year or more and list
value ranges, not amounts; holdings through funds are not itemised.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

API = "https://www.courtlistener.com/api/rest/v4"
SITE = "https://www.courtlistener.com"

# Docket searches (RECAP): the companies and techniques the platform tracks.
CASE_QUERIES: Tuple[Tuple[str, str], ...] = (
    ('"Flock Safety" OR "Flock Group"', "plate readers"),
    ('"license plate reader" OR "license plate recognition"', "plate readers"),
    ('"Clearview AI"', "face recognition"),
    ('ShotSpotter OR SoundThinking', "gunshot detection"),
    ('"Fog Data Science" OR Venntel', "location data"),
    ('"cell-site simulator" OR stingray', "phone tracking"),
)
CASE_PAGES = 2                          # 20 dockets a page, newest first

# Surveillance vendors as they appear in disclosures, by description prefix
# (the API filters on a prefix only; "1. Axon" style numbering is missed).
VENDOR_PREFIXES: Dict[str, Tuple[str, ...]] = {
    "Axon": ("Axon", "Taser"),
    "Motorola Solutions": ("Motorola Solutions", "Motorola Sol", "Motorola Inc", "Motorola,", "Motorola ("),
    "Palantir": ("Palantir",),
    "Rekor": ("Rekor",),
    "Cellebrite": ("Cellebrite",),
    "SoundThinking": ("SoundThinking", "ShotSpotter"),
    "RELX (LexisNexis)": ("RELX", "Reed Elsevier"),
    "Thomson Reuters (CLEAR)": ("Thomson Reuters",),
}
# Names that identify a vendor as a *party* in a case caption.
VENDOR_PARTY_NAMES: Dict[str, Tuple[str, ...]] = {
    "Axon": ("axon", "taser"), "Motorola Solutions": ("motorola",), "Palantir": ("palantir",),
    "Rekor": ("rekor",), "Cellebrite": ("cellebrite",), "SoundThinking": ("soundthinking", "shotspotter"),
    "RELX (LexisNexis)": ("relx", "lexisnexis", "reed elsevier"), "Thomson Reuters (CLEAR)": ("thomson reuters",),
}

# Value codes on judicial disclosures (AO Form 10).
VALUE_CODES = {
    "J": "$15,000 or less", "K": "$15,001–50,000", "L": "$50,001–100,000", "M": "$100,001–250,000",
    "N": "$250,001–500,000", "O": "$500,001–1,000,000", "P1": "$1,000,001–5,000,000",
    "P2": "$5,000,001–25,000,000", "P3": "$25,000,001–50,000,000", "P4": "more than $50,000,000",
}

_DISCLOSURE_ID = re.compile(r"/financial-disclosures/(\d+)/?")


@dataclass
class Case:
    docket_id: int
    case_name: str
    court_id: Optional[str]
    date_filed: Optional[date]
    date_terminated: Optional[date]
    judge: Optional[str]
    judge_id: Optional[int]
    suit_nature: Optional[str]
    docket_number: Optional[str]
    url: str
    topic: str


def _d(x: Any) -> Optional[date]:
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x)[:10]).date()
    except ValueError:
        return None


def parse_search(body: Mapping[str, Any], topic: str) -> List[Case]:
    out: List[Case] = []
    for r in body.get("results") or []:
        did = r.get("docket_id")
        if not did:
            continue
        jid = r.get("assigned_to_id")
        out.append(Case(int(did), (r.get("caseName") or "").strip(), r.get("court_id"), _d(r.get("dateFiled")),
                        _d(r.get("dateTerminated")), r.get("assignedTo") or None,
                        int(jid) if isinstance(jid, int) or (isinstance(jid, str) and jid.isdigit()) else None,
                        r.get("suitNature") or None, r.get("docketNumber"),
                        SITE + (r.get("docket_absolute_url") or f"/docket/{did}/"), topic))
    return out


_CR = re.compile(r"-(?:cr|mj|po)-", re.I)
_CV = re.compile(r"-(?:cv|mc)-", re.I)


def case_kind(case_name: str, docket_number: Optional[str], suit_nature: Optional[str]) -> str:
    """'civil' or 'criminal', from the docket number's type code (1:26-cv-01234,
    2:26-cr-00042), else the caption and nature of suit."""
    dn = docket_number or ""
    if _CR.search(dn):
        return "criminal"
    if _CV.search(dn) or suit_nature:
        return "civil"
    return "criminal" if re.match(r"(?i)^(united states|usa|u\.s\.a?\.?)\s+v", case_name or "") else "civil"


def collapse(cases: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """One row per case: multi-defendant prosecutions open one docket per
    defendant under the same caption, court and day."""
    out: Dict[Tuple[str, Optional[str], Any], Dict[str, Any]] = {}
    for c in cases:
        key = ((c.get("case_name") or "").strip().lower(), c.get("court_id"), c.get("date_filed"))
        if key in out:
            out[key]["dockets"] += 1
            if not out[key].get("judge") and c.get("judge"):
                out[key].update(judge=c.get("judge"), judge_id=c.get("judge_id"))
        else:
            out[key] = {**c, "dockets": 1}
    return list(out.values())


def disclosure_id(url_or_id: Any) -> Optional[int]:
    if isinstance(url_or_id, int):
        return url_or_id
    m = _DISCLOSURE_ID.search(str(url_or_id or ""))
    return int(m.group(1)) if m else None


def vendor_of(description: str) -> Optional[str]:
    """Which tracked vendor an investment line names, if any (prefix match,
    as the API does, so lines the API returned are classified the same way)."""
    d = (description or "").strip().lower()
    for vendor, prefixes in VENDOR_PREFIXES.items():
        if any(d.startswith(p.lower()) for p in prefixes):
            return vendor
    return None


def parse_holdings(body: Mapping[str, Any]) -> List[Tuple[int, int, str, str, Optional[str]]]:
    """(investment_id, disclosure_id, vendor, description, gross_value_code)."""
    out = []
    for r in body.get("results") or []:
        v = vendor_of(r.get("description") or "")
        did = disclosure_id(r.get("financial_disclosure"))
        if v and did and not r.get("redacted"):
            out.append((int(r["id"]), did, v, (r.get("description") or "").strip(), r.get("gross_value_code") or None))
    return out


def is_party(vendor: str, case_name: str) -> bool:
    name = (case_name or "").lower()
    return any(re.search(rf"\b{re.escape(n)}\b", name) for n in VENDOR_PARTY_NAMES.get(vendor, ()))


def judge_ties(case: Mapping[str, Any], holdings: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """What a case's judge disclosed holding in surveillance vendors.

    `holdings`: rows with vendor, description, gross_value_code and year for the
    case's judge. Returns ties by vendor (latest year first) and the vendors
    that are parties to the case -- the only basis for a §455(b)(4) flag."""
    by: Dict[str, Dict[str, Any]] = {}
    for h in sorted(holdings, key=lambda h: -(h.get("year") or 0)):
        e = by.setdefault(h["vendor"], {"vendor": h["vendor"], "years": [], "latest_value": None,
                                         "description": h.get("description")})
        if h.get("year") and h["year"] not in e["years"]:
            e["years"].append(h["year"])
        if e["latest_value"] is None and h.get("gross_value_code"):
            e["latest_value"] = VALUE_CODES.get(str(h["gross_value_code"]), str(h["gross_value_code"]))
    ties = list(by.values())
    party = [t["vendor"] for t in ties if is_party(t["vendor"], case.get("case_name", ""))]
    return {"ties": ties, "party_holdings": party,
            "possible_455_conflict": bool(party),
            "note": ("The judge disclosed holding stock in a party to this case; 28 U.S.C. §455(b)(4) requires "
                     "recusal for a financial interest in a party unless divested. Check the docket for recusal or "
                     "divestment.") if party else
                    ("Holdings in surveillance vendors that are not parties to this case: context, not a "
                     "disqualifying interest.") if ties else None}


__all__ = ["API", "CASE_PAGES", "case_kind", "collapse", "CASE_QUERIES", "Case", "SITE", "VALUE_CODES", "VENDOR_PREFIXES", "disclosure_id",
           "is_party", "judge_ties", "parse_holdings", "parse_search", "vendor_of"]
