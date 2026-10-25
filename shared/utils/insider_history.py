"""
shared/utils/insider_history.py

Years of insider open-market trades from the SEC's quarterly Form 3/4/5 data
sets, and the routine-versus-opportunistic split that decides which carry
information. Pure; collector-filings downloads the quarters and stores them.

Cohen, Malloy & Pomorski ("Decoding Inside Information", Journal of Finance
2012): an insider who traded in the same calendar month in each of the three
prior years is a *routine* trader, and routine trades predict nothing; every
other insider with trades in each of those three years is *opportunistic*, and
opportunistic trades carry all the predictive power (82 bps a month value-
weighted). Insiders without three years of history are left unclassified. A
trade made under a Rule 10b5-1 plan is scheduled in advance and counts as
routine whatever its history.

The live feed has only known Form 4 since this autumn, so the reaction study
marked insider clusters "insufficient data". These rows give it the history,
stamped at the end of the filing day (the data set has the date, not the
acceptance time), so the study measures from when the market had certainly
seen the filing.
"""
from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

URL_PREFIXES = (
    "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/",
    "https://www.sec.gov/files/datastandardsinnovation/data/insider-transactions-data-sets/",
)
FIRST_QUARTER = (2016, 1)         # three years of history before the five the study reads
OPEN_MARKET = {"P": 1, "S": -1}   # purchase, sale
CLASSIFY_YEARS = 3
ARCHIVE_SOURCE = "sec_form345_history"
INSIDER_TABLE = "insider_transactions"
NY = ZoneInfo("America/New_York")
_MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV",
                                        "DEC"), start=1)}


@dataclass(frozen=True)
class Trade:
    accession: str
    line: str                   # NONDERIV_TRANS_SK: the row within the filing
    filed: date
    traded: date
    issuer_cik: str
    ticker: str
    owner_cik: str
    owner_name: str
    relationship: str           # "Director,Officer", "TenPercentOwner", ...
    title: str
    code: str                   # P or S
    shares: float
    price: Optional[float]
    planned: bool               # the filing's Rule 10b5-1 box


def quarter_url(year: int, q: int, prefix: str = URL_PREFIXES[0]) -> str:
    return f"{prefix}{year}q{q}_form345.zip"


def quarters(until: date, start: Tuple[int, int] = FIRST_QUARTER) -> List[Tuple[int, int]]:
    """Every complete quarter from `start` up to the one before `until`'s."""
    y, q = start
    last_y, last_q = until.year, (until.month - 1) // 3 + 1
    out = []
    while (y, q) < (last_y, last_q):
        out.append((y, q))
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def sec_date(s: str) -> Optional[date]:
    """'31-OCT-2025' (the data sets' format) or ISO."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        d, m, y = s.split("-")
        return date(int(y), _MONTHS[m.upper()], int(d))
    except (ValueError, KeyError):
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None


def _num(s: str) -> Optional[float]:
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _rows(z: zipfile.ZipFile, name: str) -> Iterator[Dict[str, str]]:
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t",
                                  quoting=csv.QUOTE_NONE)


def parse_quarter(blob: bytes) -> List[Trade]:
    """Open-market purchases and sales from one quarter's zip, one row per
    transaction line and reporting owner. Forms 4 and 4/A only; equity swaps
    and lines without shares are skipped."""
    z = zipfile.ZipFile(io.BytesIO(blob))
    subs: Dict[str, Dict[str, str]] = {}
    for r in _rows(z, "SUBMISSION.tsv"):
        if r.get("DOCUMENT_TYPE") in ("4", "4/A") and (r.get("ISSUERTRADINGSYMBOL") or "").strip():
            subs[r["ACCESSION_NUMBER"]] = r
    owners: Dict[str, List[Dict[str, str]]] = {}
    for r in _rows(z, "REPORTINGOWNER.tsv"):
        if r["ACCESSION_NUMBER"] in subs:
            owners.setdefault(r["ACCESSION_NUMBER"], []).append(r)
    out: List[Trade] = []
    for r in _rows(z, "NONDERIV_TRANS.tsv"):
        code = (r.get("TRANS_CODE") or "").strip().upper()
        acc = r["ACCESSION_NUMBER"]
        sub = subs.get(acc)
        if code not in OPEN_MARKET or sub is None or (r.get("EQUITY_SWAP_INVOLVED") or "0") in ("1", "true"):
            continue
        shares = _num(r.get("TRANS_SHARES", ""))
        filed, traded = sec_date(sub.get("FILING_DATE", "")), sec_date(r.get("TRANS_DATE", ""))
        if not shares or filed is None or traded is None:
            continue
        price = _num(r.get("TRANS_PRICEPERSHARE", ""))
        planned = (sub.get("AFF10B5ONE") or "").strip().lower() in ("1", "true")
        for o in owners.get(acc, []):
            out.append(Trade(
                accession=acc, line=str(r.get("NONDERIV_TRANS_SK") or ""), filed=filed, traded=traded,
                issuer_cik=(sub.get("ISSUERCIK") or "").lstrip("0"),
                ticker=sub["ISSUERTRADINGSYMBOL"].strip().upper().replace("-", "."),
                owner_cik=(o.get("RPTOWNERCIK") or "").lstrip("0"), owner_name=(o.get("RPTOWNERNAME") or "").strip(),
                relationship=(o.get("RPTOWNER_RELATIONSHIP") or "").strip(),
                title=(o.get("RPTOWNER_TITLE") or "").strip(), code=code, shares=shares,
                price=price if price and price > 0 else None, planned=planned,
            ))
    return out


def classify(traded: date, prior_trades: Iterable[date], planned: bool = False) -> Optional[str]:
    """'routine', 'opportunistic' or None for one insider's trade, from the
    dates of that insider's open-market trades before it."""
    if planned:
        return "routine"
    years = [traded.year - k for k in range(1, CLASSIFY_YEARS + 1)]
    months: Dict[int, set] = {y: set() for y in years}
    for d in prior_trades:
        if d.year in months:
            months[d.year].add(d.month)
    if any(not months[y] for y in years):
        return None
    common = set.intersection(*(months[y] for y in years))
    return "routine" if common else "opportunistic"


def known_at(filed: date) -> datetime:
    """When the market had certainly seen a filing made on `filed`: the end of
    that New York day (EDGAR accepts filings until 22:00)."""
    return datetime.combine(filed + timedelta(days=1), time(0, 0), tzinfo=NY).astimezone(timezone.utc)


def archive_tags(code: str, cls: Optional[str], planned: bool) -> List[str]:
    """The tags a live Form 4 event carries (services/enrichment/enrichers/tradfi.py),
    so the signal families read history and live filings alike."""
    tags = ["tradfi", "insider_trade", "open_market_buy" if code == "P" else "open_market_sale"]
    if planned:
        tags.append("planned_10b5_1")
    if cls:
        tags.append(f"insider_{cls}")
    return tags


def owner_months(rows: Sequence[Tuple[str, date]]) -> Dict[str, List[date]]:
    """{owner_cik: [trade dates]} from (owner_cik, traded) rows."""
    out: Dict[str, List[date]] = {}
    for owner, d in rows:
        out.setdefault(owner, []).append(d)
    return out


__all__ = ["ARCHIVE_SOURCE", "FIRST_QUARTER", "INSIDER_TABLE", "OPEN_MARKET", "Trade", "URL_PREFIXES", "archive_tags",
           "classify", "known_at", "owner_months", "parse_quarter", "quarter_url", "quarters", "sec_date"]
