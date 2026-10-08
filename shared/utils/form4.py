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


# What filers write in issuerTradingSymbol when the issuer has no listing.
#
# The field is free text. Read live on 1 October, an hour after the document
# fetch was fixed, three of eight filings carried "N/A" or "NONE" -- funds and
# private companies -- and each was published with that string as its ticker.
# The insider-cluster rule then keyed a cluster on it: one correlation that
# hour was `rule_insider_activity_cluster_n/a`, grouping unrelated issuers
# because they shared a placeholder.
_UNLISTED_SYMBOLS = frozenset({"", "N/A", "NA", "NONE", "NULL", "-", "--", "NOT APPLICABLE"})


def listed_symbol(raw: str) -> str:
    """The issuer's trading symbol, or "" when the filing names none."""
    symbol = (raw or "").strip().upper()
    return "" if symbol in _UNLISTED_SYMBOLS else symbol


def accession_of(index_link: str) -> str:
    """The filing's accession number, from its index link.

    The current-filings feed lists each Form 4 twice -- once under the
    reporting owner's CIK, once under the issuer's -- so the two entries have
    different links and one accession. Keyed by link, every filing was
    published twice, and a cluster counting net buying counted it twice.
    Falls back to the link itself when the shape is not recognised.
    """
    tail = (index_link or "").rsplit("/", 1)[-1]
    if tail.endswith("-index.htm") or tail.endswith("-index.html"):
        return tail.split("-index.", 1)[0]
    return index_link or ""


def _flag(node: Optional[ET.Element]) -> bool:
    """An ownership-schema boolean, which filers write as 1 or as true.

    Only "1" was accepted. Read live, SPG's filings write isDirector as
    "true", so every director was parsed as no relationship at all, the role
    came out empty, and the enricher fell back to the RSS title: the role
    published was "4 - AEPPEL GLYN (0001340262) (REPORTING)".
    """
    return _text(node).strip().lower() in ("1", "true")


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
            "is_officer": _flag(rel.find("isOfficer")) if rel is not None else False,
            "is_director": _flag(rel.find("isDirector")) if rel is not None else False,
            "is_ten_percent_owner": (
                _flag(rel.find("isTenPercentOwner")) if rel is not None else False
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

    # Traded under a Rule 10b5-1 plan: scheduled in advance, so not a decision
    # made on what the insider knows now. Cohen, Malloy & Pomorski (2012) found
    # such routine trades carry essentially no return information while the
    # opportunistic remainder carries all of it. Since April 2023 the form has
    # an explicit checkbox (aff10b5One); before that, plans were disclosed only
    # in footnotes, so both are read.
    footnotes = " ".join(_text(f) for f in root.iter("footnote")).lower()
    mentions_plan = "10b5-1" in footnotes or "10b5 1" in footnotes
    denies_plan = any(n in footnotes for n in (
        "not pursuant to a rule 10b5-1", "not made pursuant to a rule 10b5-1",
        "not pursuant to a 10b5-1", "not effected pursuant to a rule 10b5-1",
    ))
    planned = _flag(root.find("aff10b5One")) or (mentions_plan and not denies_plan)

    return {
        "ticker": listed_symbol(ticker),
        "issuer_name": issuer_name,
        "owners": owners,
        "transactions": transactions,
        "planned_10b5_1": planned,
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


def document_url(index_link: str, name: str = "primary_doc.xml") -> Optional[str]:
    """The filing's primary document, from the RSS entry's index link.

    The feed gives `.../data/{cik}/{accession}/{accession}-index.htm`, and the
    document sits beside it in the same directory. Its name is not fixed --
    see `primary_document_name` -- so callers pass the one the index lists.
    """
    if not index_link or "/Archives/edgar/data/" not in index_link:
        return None
    if not name or "/" in name:
        return None
    base = index_link.rsplit("/", 1)[0]
    return f"{base}/{name}"


def index_json_url(index_link: str) -> Optional[str]:
    """The machine-readable file list for the filing's directory."""
    if not index_link or "/Archives/edgar/data/" not in index_link:
        return None
    return index_link.rsplit("/", 1)[0] + "/index.json"


def primary_document_name(index_json: Any) -> Optional[str]:
    """The filing's XML document, as the directory listing names it.

    The name was assumed to be `primary_doc.xml`. It is whatever the filing
    agent chose: probed against the live feed on 1 October, four of six
    current Form 4s used `form4.xml` or `doc4.xml`, and every one of those
    returned 404 -- 70 failed fetches in the hour after a restart, each
    published as the RSS stub with no ticker, insider or transactions. The
    filing that was used to verify this parser happened to use the default.

    A Form 4 directory holds one ownership XML. Rendered views live in an
    `xslF345X..` subdirectory and are listed as a directory, not a file.
    """
    if not isinstance(index_json, dict):
        return None
    directory = index_json.get("directory")
    items = directory.get("item") if isinstance(directory, dict) else None
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if name.lower().endswith(".xml") and "/" not in name and item.get("type") != "dir":
            return name
    return None


def dominant_code(payload: Dict[str, Any]) -> Optional[str]:
    """The transaction code a filing is about, from the parsed document.

    The collector publishes every transaction and the open-market totals, and
    no reader turned them into a code: the enricher looked for a
    `transaction_code` nobody writes, then regexed the RSS summary and fell
    back to "J". All 1,160 insider events in the fourteen days to 3 October
    were "Insider Other" -- including the open-market purchases the cluster
    gate exists to count.

    Open-market activity decides first, because it is the decision the signal
    is about; the larger side wins when a filing has both. Otherwise the code
    of the largest transaction, or the first when none is sized.
    """
    if not isinstance(payload, dict):
        return None
    buy = float(payload.get("open_market_buy_usd") or 0.0)
    sell = float(payload.get("open_market_sell_usd") or 0.0)
    if buy or sell:
        return "P" if buy >= sell else "S"
    txns = [t for t in (payload.get("transactions") or []) if isinstance(t, dict) and t.get("code")]
    if not txns:
        return None
    sized = [t for t in txns if t.get("value_usd")]
    pick = max(sized, key=lambda t: float(t["value_usd"])) if sized else txns[0]
    return str(pick["code"]).upper()


def code_value_usd(payload: Dict[str, Any], code: Optional[str]) -> Optional[float]:
    """Dollars behind that code: the open-market side for P and S, otherwise
    the sized transactions carrying it. None when the filing states no price."""
    if not isinstance(payload, dict) or not code:
        return None
    if code == "P":
        value = float(payload.get("open_market_buy_usd") or 0.0)
    elif code == "S":
        value = float(payload.get("open_market_sell_usd") or 0.0)
    else:
        value = sum(
            float(t["value_usd"]) for t in (payload.get("transactions") or [])
            if isinstance(t, dict) and str(t.get("code") or "").upper() == code and t.get("value_usd")
        )
    return value if value > 0 else None
