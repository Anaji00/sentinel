"""
shared/utils/form144.py

Form 144: an insider's notice of a proposed sale of restricted or control
stock, filed on EDGAR before the sale (electronically since April 2023). It
says who is selling, their relationship to the company, how many shares, their
value and the approximate sale date -- intent to sell, ahead of the Form 4 that
reports the sale. Pure; services/collector-filings/form144.py fetches.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any, Dict, List, Optional

FEED_URL = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=144"
            "&company=&dateb=&owner=include&count=100&output=atom")

_NS = {"own": "http://www.sec.gov/edgar/ownership", "com": "http://www.sec.gov/edgar/common"}
_ATOM = {"a": "http://www.w3.org/2005/Atom"}
_INDEX = re.compile(r"/Archives/edgar/data/(\d+)/(\d{18})/(\d{10}-\d{2}-\d{6})-index\.htm")


def _num(x: Optional[str]) -> Optional[float]:
    try:
        return float(str(x).replace(",", "")) if x not in (None, "") else None
    except ValueError:
        return None


def _date(x: Optional[str]) -> Optional[date]:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(x).strip(), fmt).date()
        except (TypeError, ValueError):
            continue
    return None


def parse_feed(atom: str) -> List[Dict[str, str]]:
    """New Form 144 filings in the EDGAR current feed: accession number and
    the primary document's URL, once each (a filing appears twice, as the
    reporting person and as the subject company)."""
    try:
        root = ET.fromstring(atom)
    except ET.ParseError:
        return []
    out: Dict[str, Dict[str, str]] = {}
    for entry in root.findall("a:entry", _ATOM):
        link = entry.find("a:link", _ATOM)
        href = link.get("href") if link is not None else ""
        m = _INDEX.search(href or "")
        if not m:
            continue
        cik, folder, accession = m.groups()
        out.setdefault(accession, {
            "accession": accession,
            "doc_url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/primary_doc.xml",
        })
    return list(out.values())


def parse_notice(xml: str) -> Optional[Dict[str, Any]]:
    """The fields of one Form 144 primary document, or None if it is not one."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    form = root.find("own:formData", _NS)
    if form is None:
        return None

    def text(path: str) -> Optional[str]:
        el = form.find(path, _NS)
        return el.text.strip() if el is not None and el.text else None

    shares = _num(text("own:securitiesInformation/own:noOfUnitsSold"))
    outstanding = _num(text("own:securitiesInformation/own:noOfUnitsOutstanding"))
    relationships = [el.text.strip() for el in form.findall("own:issuerInfo/own:relationshipsToIssuer/own:relationshipToIssuer", _NS)
                     if el.text and el.text.strip()]
    return {
        "issuer_cik": (text("own:issuerInfo/own:issuerCik") or "").lstrip("0") or None,
        "issuer_name": text("own:issuerInfo/own:issuerName"),
        "seller": text("own:issuerInfo/own:nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold"),
        "relationship": ", ".join(relationships) or None,
        "security": text("own:securitiesInformation/own:securitiesClassTitle"),
        "shares": shares,
        "market_value": _num(text("own:securitiesInformation/own:aggregateMarketValue")),
        "pct_outstanding": round(shares / outstanding * 100, 4) if shares and outstanding else None,
        "approx_sale_date": _date(text("own:securitiesInformation/own:approxSaleDate")),
        "acquired_how": text("own:securitiesToBeSold/own:natureOfAcquisitionTransaction"),
        "notice_date": _date(text("own:noticeSignature/own:noticeDate")),
    }


__all__ = ["FEED_URL", "parse_feed", "parse_notice"]
