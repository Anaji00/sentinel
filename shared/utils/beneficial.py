"""
shared/utils/beneficial.py

Schedule 13D and 13G: who crosses 5% of a company, and whether they mean to act.

13F reports what managers held at quarter end, 45 days late. A Schedule 13D is
due within ten days of crossing 5% with intent to influence the company -- the
filing an activist makes -- and a 13G is the passive version. Since late 2024
both are filed as structured XML under the form names "SCHEDULE 13D" and
"SCHEDULE 13G"; the old "SC 13D" form type returns nothing from EDGAR's current
feed, and nothing on the platform read either.

The pure half: parsing a primary_doc.xml and scoring what it says.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _float(text: Optional[str]) -> Optional[float]:
    try:
        return float(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def parse_schedule(xml_text: str) -> Optional[Dict[str, Any]]:
    """Form, issuer, reporting persons and their stakes, from a primary_doc.xml."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None
    first: Dict[str, str] = {}
    persons: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for el in root.iter():
        name = _local(el.tag)
        text = (el.text or "").strip()
        if name in ("reportingPersonInfo", "coverPageHeaderReportingPersonDetails"):
            current = {}
            persons.append(current)
            continue
        if not text:
            continue
        first.setdefault(name, text)
        if current is not None:
            if name in ("reportingPersonName", "personName") and "name" not in current:
                current["name"] = text
            elif name in ("percentOfClass", "classPercent") and "pct" not in current:
                current["pct"] = _float(text)
            elif name == "aggregateAmountOwned" and "shares" not in current:
                current["shares"] = _float(text)
            elif name == "typeOfReportingPerson" and "kind" not in current:
                current["kind"] = text
    persons = [p for p in persons if p.get("name")]
    form = first.get("submissionType", "")
    pcts = [p["pct"] for p in persons if p.get("pct") is not None]
    top_pct = max(pcts) if pcts else _float(first.get("percentOfClass") or first.get("classPercent"))
    purpose = first.get("transactionPurpose") or first.get("purposeOfTransaction") or ""
    return {
        "form": form,
        "is_13d": "13D" in form.upper(),
        "is_amendment": form.upper().endswith("/A"),
        "issuer_name": first.get("issuerName", ""),
        "issuer_cik": first.get("issuerCIK") or first.get("issuerCik") or "",
        "persons": persons,
        "pct": top_pct,
        "event_date": first.get("dateOfEvent") or first.get("eventDateRequiresFilingThisStatement") or "",
        "purpose": re.sub(r"\s+", " ", purpose)[:400],
        "class_title": first.get("securitiesClassTitle") or first.get("securityTitle") or "",
    }


_NOT_COMMON = re.compile(r"PREFERRED|PREF\b|NOTE|DEBENTURE|WARRANT|RIGHT|BOND|AUCTION|DEMAND", re.I)


def is_common_equity(class_title: str, ticker: str = "") -> bool:
    """A stake in the class that carries control.

    JPMorgan reported 100% of a closed-end fund's preferred shares, and the
    registry resolved another issuer to its preferred line (GPUS-PD). A stake
    in a preferred or note class says nothing about who owns the company.
    """
    if _NOT_COMMON.search(class_title or ""):
        return False
    return not re.search(r"-P[A-Z]?$", (ticker or "").upper())


def score_schedule(parsed: Dict[str, Any], previous_pct: Optional[float]) -> float:
    """How much a 13D or 13G says.

    An initial 13D is the activist filing, and the strongest thing on this list.
    An amendment matters by how far the stake moved, and a 13D holder dropping
    under 5% is an exit. A 13G is passive by declaration, so it scores lower.
    """
    pct = parsed.get("pct")
    moved = abs(pct - previous_pct) if (pct is not None and previous_pct is not None) else None
    if parsed.get("is_13d"):
        if not parsed.get("is_amendment"):
            return 0.85
        if pct is not None and pct < 5.0 and (previous_pct or 0) >= 5.0:
            return 0.7
        if moved is not None:
            return round(min(0.8, 0.45 + 0.1 * moved), 3)
        return 0.45
    if not parsed.get("is_amendment"):
        return 0.5 if (pct or 0) >= 10.0 else 0.4
    if moved is not None and moved >= 2.0:
        return 0.45
    return 0.25


def headline(parsed: Dict[str, Any], ticker: str, filer: str, previous_pct: Optional[float]) -> str:
    kind = "Activist 13D" if parsed.get("is_13d") else "Passive 13G"
    if parsed.get("is_amendment"):
        kind += " amendment"
    pct = parsed.get("pct")
    stake = f"{pct:.2f}%" if pct is not None else "a stake"
    change = ""
    if pct is not None and previous_pct is not None and abs(pct - previous_pct) >= 0.01:
        change = f" (from {previous_pct:.2f}%)"
    return f"{kind}: {filer} reports {stake} of {ticker}{change}"
