"""Reading a Form 4 instead of guessing at its RSS headline.

INSIDER_CLUSTERS has carried zero messages for the platform's lifetime. The
cluster gate wants two distinct insiders and $250,000 of net buying, and
measured over fourteen days of live events it could never have either:

    insider_name         absent on all 56
    transaction_value    null -- every headline reads $0.0M
    role                 "4", the form number, regexed out of the RSS title
    ticker               GP, AIV -- parentheticals from the FILER's name

    "Insider Other: GP $0.0M by 4 - SLTA V (GP), L.L.C. (REPORTING)"

`GP` is "General Partner". Sixteen filings landed under it as though it were a
company. The collector published the RSS index entry -- link, title, summary --
and the enricher tried to regex intelligence out of a headline.

The filing itself carries all of it. Every Form 4 accession directory holds a
`primary_doc.xml` with the issuer's real trading symbol, each reporting owner's
name and relationship, and every transaction's code, share count and price:

    issuerTradingSymbol   JCTC
    rptOwnerName          AJB Investment Fund II LP / AJB Capital LLC / ...
    transactionCode       P
    transactionShares     4767      transactionPricePerShare  2.91

This module is the parsing half only, and deliberately pure: given bytes it
returns a dict, so it is testable against real filings without a network.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sentinel.form4")

# Transaction codes worth treating as a deliberate market decision.
#
# P and S are open-market purchases and sales -- someone chose a price. A is a
# grant and F is shares withheld for tax: both move the holding without anyone
# expressing a view, and counting them as accumulation is how a vesting
# schedule becomes a buy signal.
OPEN_MARKET_CODES = frozenset({"P", "S"})

# Acquired vs disposed, as the filing states it rather than inferred from the
# code: a P is an acquisition and an S a disposal, but the filing says so
# explicitly and the explicit field is the one to trust.
ACQUIRED = "A"


def _text(node: Optional[ET.Element]) -> str:
    """The text of an element, or of its <value> child, or empty."""
    if node is None:
        return ""
    value = node.find("value")
    if value is not None and value.text:
        return value.text.strip()
    return (node.text or "").strip()


def _number(node: Optional[ET.Element]) -> Optional[float]:
    raw = _text(node)
    if not raw:
        return None
    try:
        return float(raw.replace(",", "").replace("$", ""))
    except ValueError:
        return None


def _strip_namespace(xml_text: str) -> str:
    """Form 4 documents appear both with and without a default namespace."""
    return re.sub(r'\sxmlns(:\w+)?="[^"]*"', "", xml_text, count=1)


def parse_form4(payload: bytes | str) -> Optional[Dict[str, Any]]:
    """Issuer, owners and transactions from one Form 4 document.

    Returns None when the bytes are not a parseable ownership document, which
    is a different answer from a filing that parsed and contained nothing.
    """
    if not payload:
        return None
    text = payload.decode("utf-8", "replace") if isinstance(payload, bytes) else payload
    try:
        root = ET.fromstring(_strip_namespace(text))
    except ET.ParseError as e:
        logger.debug("Form 4 did not parse: %s", e)
        return None

    issuer = root.find("issuer")
    ticker = _text(issuer.find("issuerTradingSymbol")) if issuer is not None else ""
    issuer_name = _text(issuer.find("issuerName")) if issuer is not None else ""

    owners: List[Dict[str, Any]] = []
    for owner in root.findall("reportingOwner"):
        ident = owner.find("reportingOwnerId")
        rel = owner.find("reportingOwnerRelationship")
        name = _text(ident.find("rptOwnerName")) if ident is not None else ""
        if not name:
            continue
        owners.append({
            "name": name,
            "is_officer": _text(rel.find("isOfficer")) == "1" if rel is not None else False,
            "is_director": _text(rel.find("isDirector")) == "1" if rel is not None else False,
            "is_ten_percent_owner": (
                _text(rel.find("isTenPercentOwner")) == "1" if rel is not None else False
            ),
            "title": _text(rel.find("officerTitle")) if rel is not None else "",
        })

    transactions: List[Dict[str, Any]] = []
    for table in ("nonDerivativeTable", "derivativeTable"):
        parent = root.find(table)
        if parent is None:
            continue
        for txn in parent:
            if not txn.tag.endswith("Transaction"):
                continue
            coding = txn.find("transactionCoding")
            amounts = txn.find("transactionAmounts")
            if amounts is None:
                continue
            shares = _number(amounts.find("transactionShares"))
            price = _number(amounts.find("transactionPricePerShare"))
            code = _text(coding.find("transactionCode")) if coding is not None else ""
            disposition = _text(amounts.find("transactionAcquiredDisposedCode"))
            transactions.append({
                "code": code,
                "shares": shares,
                "price_per_share": price,
                # None, not 0.0. A filing that states shares without a price is
                # a grant or a gift; pricing it at zero would report a
                # multi-million-share award as a $0 transaction, which is how
                # every headline came to read $0.0M.
                "value_usd": (shares * price) if (shares and price) else None,
                "acquired": disposition == ACQUIRED,
                "is_derivative": table == "derivativeTable",
            })

    return {
        "ticker": ticker.upper(),
        "issuer_name": issuer_name,
        "owners": owners,
        "transactions": transactions,
        "insider_names": [o["name"] for o in owners],
        **_aggregate(transactions),
    }


def _aggregate(transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Open-market buying and selling, in dollars."""
    bought = sum(
        t["value_usd"] for t in transactions
        if t["code"] in OPEN_MARKET_CODES and t["acquired"] and t["value_usd"]
    )
    sold = sum(
        t["value_usd"] for t in transactions
        if t["code"] in OPEN_MARKET_CODES and not t["acquired"] and t["value_usd"]
    )
    return {
        "open_market_buy_usd": round(bought, 2),
        "open_market_sell_usd": round(sold, 2),
        "net_buy_usd": round(bought - sold, 2),
        "has_open_market_activity": any(
            t["code"] in OPEN_MARKET_CODES for t in transactions
        ),
    }


def document_url(index_link: str) -> Optional[str]:
    """The filing's primary document, from the RSS entry's index link.

    The feed gives `.../data/{cik}/{accession}/{accession}-index.htm`, and the
    document sits beside it in the same directory.
    """
    if not index_link or "/Archives/edgar/data/" not in index_link:
        return None
    base = index_link.rsplit("/", 1)[0]
    return f"{base}/primary_doc.xml"
