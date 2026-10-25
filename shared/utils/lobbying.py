"""
shared/utils/lobbying.py

Federal lobbying by surveillance vendors (Senate Lobbying Disclosure Act
filings, lda.gov). Pure; the civic collector fetches.

Amounts follow the LDA: a lobbying firm reports the income it received from the
client; a company lobbying in-house reports its own expenses. Each filing is a
quarter (Q1-Q4) or a registration/amendment (no amount). Only the client's own
filings are counted -- money spent on its behalf by trade groups is not.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

LDA_API = "https://lda.gov/api/v1"

# Client name searches (LDA's client_name is a contains match) and the vendor
# each counts toward. Broad conglomerates (Amazon for Ring, Motorola's radio
# business) are named narrowly or left out.
LOBBY_CLIENTS: Tuple[Tuple[str, str], ...] = (
    ("Flock Group", "Flock Safety"), ("Flock Safety", "Flock Safety"),
    ("Axon Enterprise", "Axon"), ("Motorola Solutions", "Motorola Solutions"),
    ("Palantir", "Palantir"), ("SoundThinking", "SoundThinking"), ("ShotSpotter", "SoundThinking"),
    ("Clearview AI", "Clearview AI"), ("Rekor", "Rekor"), ("Cellebrite", "Cellebrite"),
    ("Skydio", "Skydio"), ("Dataminr", "Dataminr"), ("Babel Street", "Babel Street"),
    ("LexisNexis Risk", "LexisNexis Risk"), ("Fusus", "Fusus"),
)


def _money(x: Any) -> Optional[float]:
    try:
        return float(x) if x not in (None, "") else None
    except (TypeError, ValueError):
        return None


def parse_filings(body: Mapping[str, Any], vendor: str) -> List[Dict[str, Any]]:
    """LDA filings -> one row each: who lobbied, for how much, on what, whom."""
    out: List[Dict[str, Any]] = []
    for f in body.get("results") or []:
        uid = f.get("filing_uuid")
        if not uid:
            continue
        acts = f.get("lobbying_activities") or []
        issues = sorted({a.get("general_issue_code_display") for a in acts if a.get("general_issue_code_display")})
        entities = sorted({g.get("name") for a in acts for g in (a.get("government_entities") or []) if g.get("name")})
        desc = " | ".join(a.get("description").strip() for a in acts if (a.get("description") or "").strip())[:1500]
        income, expenses = _money(f.get("income")), _money(f.get("expenses"))
        posted = f.get("dt_posted")
        try:
            posted_d: Optional[date] = datetime.fromisoformat(str(posted)).date() if posted else None
        except ValueError:
            posted_d = None
        out.append({
            "filing_uuid": uid, "vendor": vendor, "client_name": (f.get("client") or {}).get("name"),
            "registrant": (f.get("registrant") or {}).get("name"), "year": f.get("filing_year"),
            "period": f.get("filing_period_display"), "filing_type": f.get("filing_type"),
            "amount": income if income is not None else expenses,
            "in_house": income is None and expenses is not None,
            "issues": issues, "entities": entities, "description": desc or None,
            "url": f.get("filing_document_url"), "posted": posted_d,
        })
    return out


def matches_vendor(client_name: Optional[str], query: str) -> bool:
    """LDA's contains match also returns unrelated clients ('Flock Group' could
    match a 'Flock Group Holdings' elsewhere); keep the query's own words."""
    return bool(client_name) and query.lower() in (client_name or "").lower()


__all__ = ["LDA_API", "LOBBY_CLIENTS", "matches_vendor", "parse_filings"]
