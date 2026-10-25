"""
shared/utils/short_data.py

Free short-side data, parsed. Pure: the collector (services/collector-tradfi/
short_data.py) fetches, the factor engine and the model portfolio read.

  FINRA daily short sale volume (cdn.finra.org/equity/regsho/daily): off-exchange
  (TRF/ADF) short and total share volume per stock per day, posted by 6pm ET the
  same day, from 2009. A flow, not a position: it excludes trades that are not
  publicly disseminated, so its level runs high (~40-50% is normal) and only its
  cross-section and change carry information (Diether, Lee & Werner 2009).

  Interactive Brokers' public borrow file (ftp2.interactivebrokers.com, guest
  user "shortstock", file usa.txt): fee rate, rebate rate and shares available
  to borrow per security, refreshed through the day. Today only; the history
  is what this platform keeps.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

FINRA_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{day:%Y%m%d}.txt"
FINRA_START = date(2021, 1, 4)
# Off-exchange volume below this is too thin to read a ratio from; every name in
# the model's top thousand clears it by far. Keeps the table to ~5k rows a day.
FINRA_MIN_TOTAL_VOLUME = 50_000
IBKR_HOST = "ftp2.interactivebrokers.com"
IBKR_USER = "shortstock"
IBKR_FILE = "usa.txt"
# "Available" is reported as ">10000000" above ten million shares.
IBKR_AVAILABLE_CAP = 10_000_001
# A name costing more than this a year to borrow is hard to borrow: the model
# portfolio does not short it, whatever the broker's easy-to-borrow flag says.
HARD_TO_BORROW_FEE = 5.0

_NY = ZoneInfo("America/New_York")
_TICKER = re.compile(r"^[A-Z]{1,5}(\.[A-Z])?$")

FinraRow = Tuple[date, str, float, float, float]          # day, ticker, short, exempt, total
BorrowRow = Tuple[str, Optional[float], Optional[float], Optional[int]]   # ticker, fee %, rebate %, available


def _num(x: str) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_finra(text: str, min_total: float = FINRA_MIN_TOTAL_VOLUME) -> List[FinraRow]:
    """One day's consolidated FINRA file -> rows for common-stock tickers."""
    out: List[FinraRow] = []
    for line in text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 5 or not parts[0].isdigit():
            continue
        sym = parts[1].strip().upper()
        short, exempt, total = _num(parts[2]), _num(parts[3]), _num(parts[4])
        if not _TICKER.match(sym) or short is None or total is None or total < min_total:
            continue
        try:
            day = datetime.strptime(parts[0], "%Y%m%d").date()
        except ValueError:
            continue
        out.append((day, sym, short, exempt or 0.0, total))
    return out


def parse_ibkr(text: str) -> Tuple[Optional[datetime], List[BorrowRow]]:
    """The borrow file -> (when IB wrote it, New York time as given; rows).

    IB writes class shares with a space ("BRK B"); the platform uses a dot."""
    taken: Optional[datetime] = None
    cols: List[str] = []
    out: List[BorrowRow] = []
    for line in text.splitlines():
        if line.startswith("#BOF"):
            bits = line.split("|")
            if len(bits) >= 3:
                try:
                    taken = datetime.strptime(f"{bits[1]} {bits[2]}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=_NY)
                except ValueError:
                    taken = None
            continue
        if line.startswith("#SYM"):
            cols = [c.strip().upper() for c in line[1:].split("|")]
            continue
        if line.startswith("#") or not cols:
            continue
        parts = line.split("|")
        row = dict(zip(cols, parts))
        if row.get("CUR") != "USD":
            continue
        sym = (row.get("SYM") or "").strip().upper().replace(" ", ".")
        if not _TICKER.match(sym):
            continue
        avail_s = (row.get("AVAILABLE") or "").strip()
        avail = IBKR_AVAILABLE_CAP if avail_s.startswith(">") else (int(avail_s) if avail_s.isdigit() else None)
        out.append((sym, _num(row.get("FEERATE", "")), _num(row.get("REBATERATE", "")), avail))
    return taken, out


__all__ = [
    "FINRA_MIN_TOTAL_VOLUME", "FINRA_START", "FINRA_URL", "HARD_TO_BORROW_FEE", "IBKR_FILE", "IBKR_HOST",
    "IBKR_USER", "parse_finra", "parse_ibkr",
]
