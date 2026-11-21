"""Regulation SHO threshold securities list as Nasdaq publishes it daily (`nasdaqth<YYYYMMDD>.txt`, pipe-delimited, no key).

Pure parsing. The list covers securities Nasdaq's market centre reports; the NYSE list is not served by an open endpoint this collector can read, so
NYSE-listed names are absent and the table says which exchange each row came from. The last line of a file is `File Creation Time: YYYYMMDDHHMMSS`
(US Eastern); it is the publication time. A file is the list at the close of its date.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
EXCHANGE = "NASDAQ"
URL_TEMPLATE = "https://www.nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth{ymd}.txt"

Row = Tuple[date, str, str, str, str, Optional[datetime]]   # list_date, symbol, security_name, market_category, exchange, published_at


def url_for(d: date) -> str:
    return URL_TEMPLATE.format(ymd=d.strftime("%Y%m%d"))


def _creation_time(line: str) -> Optional[datetime]:
    digits = "".join(ch for ch in line if ch.isdigit())
    if len(digits) != 14:
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=ET).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_file(text: str, list_date: date) -> List[Row]:
    """File text -> rows. The header and the creation-time line are dropped; only rows whose threshold flag is Y are kept. Empty if the text is not a list."""
    published: Optional[datetime] = None
    body: List[List[str]] = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("file creation time"):
            published = _creation_time(line)
            continue
        parts = line.split("|")
        if parts[0].strip().lower() == "symbol":
            continue
        if len(parts) < 4:
            maybe = _creation_time(line) if len(parts) == 1 else None
            if maybe is not None:
                published = maybe
            continue
        body.append(parts)
    out: List[Row] = []
    for p in body:
        if p[3].strip().upper() != "Y" or not p[0].strip():
            continue
        out.append((list_date, p[0].strip().upper(), p[1].strip(), p[2].strip(), EXCHANGE, published))
    return out


__all__ = ["EXCHANGE", "parse_file", "url_for"]
