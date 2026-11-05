"""
shared/utils/sec_holdings.py

Parses two free SEC bulk sets. Pure; services/collector-tradfi/sec_holdings.py fetches.

  Fails-to-deliver  twice-monthly pipe-delimited files. The SEC warns a fail is not
                    evidence of short selling, abusive or otherwise.
  Form N-PORT       quarterly zips of funds' monthly holdings. A row becomes public
                    well after its month, so readers must use `first_public`.

The fails files carry CUSIP and symbol together, which lets holdings be matched by
CUSIP without loading N-PORT's identifiers table (256 MB) into memory.
"""
from __future__ import annotations

import csv
import re
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

FTD_PAGE = "https://www.sec.gov/data/foiadocsfailsdatahtm"
NPORT_PAGE = "https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets"
FTD_START_YEAR = 2016
MAX_FILE_ATTEMPTS = 3

csv.field_size_limit(10_000_000)

FtdRow = Tuple[date, str, str, int, Optional[float]]


def file_links(page: str, pattern: str) -> List[str]:
    """Absolute, de-duplicated links on an SEC page matching `pattern`."""
    out: List[str] = []
    for href in re.findall(rf'href="([^"]*{pattern}[^"]*)"', page):
        url = href if href.startswith("http") else "https://www.sec.gov" + href
        if url not in out:
            out.append(url)
    return out


def ftd_year(url: str) -> Optional[int]:
    m = re.search(r"cnsfails(\d{4})", url)
    return int(m.group(1)) if m else None


def parse_ftd(text: str) -> List[FtdRow]:
    """(settlement day, ticker, cusip, shares failed, price). The header, the trailer
    and malformed lines are skipped; repeats of a day and ticker are summed."""
    rows: Dict[Tuple[date, str], List] = {}
    for line in text.splitlines():
        p = line.split("|")
        if len(p) < 6 or not p[0].strip().isdigit():
            continue
        symbol, cusip = p[2].strip().upper(), p[1].strip().upper()
        try:
            day = datetime.strptime(p[0].strip(), "%Y%m%d").date()
            qty = int(p[3].strip())
        except ValueError:
            continue
        if not symbol or qty <= 0:
            continue
        try:
            price: Optional[float] = float(p[5].strip())
        except ValueError:
            price = None
        got = rows.get((day, symbol))
        if got:
            got[3] += qty
        else:
            rows[(day, symbol)] = [day, symbol, cusip, qty, price]
    return [tuple(r) for r in rows.values()]  # type: ignore[misc]


def ftd_public_on(settle: date) -> date:
    """When a settlement day's file is out, at the SEC's stated schedule: the first
    half of a month at its end, the second half about the 15th of the next."""
    first_next = (settle.replace(day=1) + timedelta(days=32)).replace(day=1)
    if settle.day <= 15:
        return first_next - timedelta(days=1)
    return first_next.replace(day=15)


def _day(text: str) -> Optional[date]:
    try:
        return datetime.strptime(text.strip().title(), "%d-%b-%Y").date()
    except (ValueError, AttributeError):
        return None


def latest_filings(submissions: Iterable[Mapping[str, str]],
                   funds: Iterable[Mapping[str, str]]) -> Dict[str, Tuple[date, date]]:
    """accession -> (month reported, filing day), one filing per fund and month: the
    latest, so an amendment replaces its original rather than adding to it."""
    series = {f["ACCESSION_NUMBER"]: f.get("SERIES_ID") or f["ACCESSION_NUMBER"] for f in funds}
    best: Dict[Tuple[str, date], Tuple[date, str]] = {}
    for s in submissions:
        acc = s["ACCESSION_NUMBER"]
        report, filed = _day(s.get("REPORT_DATE", "")), _day(s.get("FILING_DATE", ""))
        if not report or not filed or acc not in series:
            continue
        key = (series[acc], report.replace(day=1))
        if key not in best or filed >= best[key][0]:
            best[key] = (filed, acc)
    return {acc: (key[1], filed) for key, (filed, acc) in best.items()}


def aggregate_holdings(rows: Iterable[Mapping[str, str]], filings: Mapping[str, Tuple[date, date]],
                       tickers: Mapping[str, str]) -> Dict[Tuple[date, str], List]:
    """(month, ticker) -> [positions, shares, value, latest filing day]. Long common
    stock counted in shares only: bonds, derivatives and shorts say something else."""
    out: Dict[Tuple[date, str], List] = {}
    for r in rows:
        if r.get("ASSET_CAT") != "EC" or r.get("PAYOFF_PROFILE") != "Long" or r.get("UNIT") != "NS":
            continue
        filing = filings.get(r.get("ACCESSION_NUMBER", ""))
        ticker = tickers.get((r.get("ISSUER_CUSIP") or "").strip().upper())
        if not filing or not ticker:
            continue
        try:
            shares, value = float(r["BALANCE"]), float(r["CURRENCY_VALUE"])
        except (KeyError, TypeError, ValueError):
            continue
        rate = (r.get("EXCHANGE_RATE") or "").strip()
        if rate:
            try:
                value /= float(rate) or 1.0
            except ValueError:
                continue
        got = out.setdefault((filing[0], ticker), [0, 0.0, 0.0, filing[1]])
        got[0] += 1
        got[1] += shares
        got[2] += value
        got[3] = max(got[3], filing[1])
    return out


def tsv(lines: Iterable[str]) -> Iterable[Dict[str, str]]:
    """N-PORT's tab-separated rows, quotes being ordinary characters there."""
    return csv.DictReader(lines, delimiter="\t", quoting=csv.QUOTE_NONE)


def nport_quarter(url: str) -> Optional[str]:
    m = re.search(r"(\d{4}q[1-4])_nport\.zip", url)
    return m.group(1) if m else None


def retry_allowed(attempts: Optional[int]) -> bool:
    return (attempts or 0) < MAX_FILE_ATTEMPTS


__all__: Sequence[str] = ["FTD_PAGE", "FTD_START_YEAR", "MAX_FILE_ATTEMPTS", "NPORT_PAGE", "aggregate_holdings",
                          "file_links", "ftd_public_on", "ftd_year", "latest_filings", "nport_quarter", "parse_ftd",
                          "retry_allowed", "tsv"]
