"""
shared/utils/ownership.py

Who owns what, from 13F filings -- the pure half: parsing, aggregation and the
"follow the money" measures. The collector that fetches and writes lives in
services/collector-filings/ownership.py.

Measured on 4 October: the graph's ownership was 72 OWNS edges from the top
holdings of nine hedge-fund 13Fs, plus 12 OWNED_BY extraction artifacts ("MSFT
OWNED_BY finnhub_equities", "400 shares OWNED_BY TJX", now deleted). Only top
holdings reached the graph, never a stake as a share of the company, and the
managers that own most of the market -- BlackRock, Vanguard, State Street,
Fidelity -- were not tracked at all.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import IO, Dict, Iterable, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class Filer:
    filer_id: str
    name: str
    listed_as: Optional[str] = None   # the filer's own ticker, or its parent's
    listed_via: str = ""              # "self" or "parent"


# CIKs confirmed against EDGAR's submissions API on 4 October 2026. BlackRock
# reorganised in 2024: 0001364742 is now "BlackRock Finance" with no 13F since
# 2024, and the listed parent files as 0002012383.
OWNERSHIP_FILERS: Dict[str, Filer] = {
    "0002012383": Filer("BLACKROCK", "BlackRock, Inc.", "BLK", "self"),
    "0000102909": Filer("VANGUARD", "Vanguard Group"),
    "0000093751": Filer("STATE_STREET", "State Street Corp", "STT", "self"),
    "0000315066": Filer("FIDELITY", "FMR LLC"),
    "0000080255": Filer("T_ROWE_PRICE", "T. Rowe Price Associates", "TROW", "parent"),
    "0001214717": Filer("GEODE", "Geode Capital Management"),
    "0000902219": Filer("WELLINGTON", "Wellington Management Group"),
    "0001562230": Filer("CAPITAL_INTERNATIONAL", "Capital International Investors"),
    "0000884546": Filer("SCHWAB_IM", "Charles Schwab Investment Management", "SCHW", "parent"),
    "0000895421": Filer("MORGAN_STANLEY", "Morgan Stanley", "MS", "self"),
    "0000019617": Filer("JPMORGAN", "JPMorgan Chase & Co", "JPM", "self"),
    "0000886982": Filer("GOLDMAN_SACHS", "Goldman Sachs Group", "GS", "self"),
    "0000070858": Filer("BANK_OF_AMERICA", "Bank of America Corp", "BAC", "self"),
    "0000073124": Filer("NORTHERN_TRUST", "Northern Trust Corp", "NTRS", "self"),
    "0000914208": Filer("INVESCO", "Invesco Ltd.", "IVZ", "self"),
    "0001067983": Filer("BERKSHIRE_HATHAWAY", "Berkshire Hathaway Inc", "BRK-B", "self"),
    "0001350694": Filer("BRIDGEWATER", "Bridgewater Associates"),
    "0001423053": Filer("CITADEL", "Citadel Advisors"),
    "0001037389": Filer("RENAISSANCE", "Renaissance Technologies"),
    "0001336528": Filer("PERSHING_SQUARE", "Pershing Square Capital Management"),
}

BIG_THREE = ("BLACKROCK", "VANGUARD", "STATE_STREET")


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def iter_infotable(source: IO[bytes]) -> Iterator[Dict[str, object]]:
    """Positions from a 13F information table, one at a time.

    Streamed and cleared as it goes: BlackRock's table is 23 MB, and parsed
    whole it would not fit in the collector's memory. Option rows (putCall) are
    skipped -- a put is not a stake.
    """
    for _, elem in ET.iterparse(source, events=("end",)):
        if _local(elem.tag) != "infoTable":
            continue
        row: Dict[str, object] = {}
        for child in elem.iter():
            name = _local(child.tag)
            text = (child.text or "").strip()
            if name == "nameOfIssuer":
                row["issuer"] = text
            elif name == "cusip":
                row["cusip"] = text.upper()
            elif name == "value":
                row["value"] = _num(text)
            elif name == "sshPrnamt":
                row["shares"] = _num(text)
            elif name == "sshPrnamtType":
                row["share_type"] = text.upper()
            elif name == "putCall":
                row["put_call"] = text
        elem.clear()
        if row.get("put_call") or row.get("share_type") == "PRN":
            continue
        if row.get("cusip") and row.get("shares"):
            yield row


def _num(text: str) -> float:
    try:
        return float(text.replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def aggregate_by_cusip(rows: Iterable[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    """One position per security: a manager reports a holding once per
    sub-adviser and voting arrangement, so the same CUSIP repeats."""
    out: Dict[str, Dict[str, object]] = {}
    for r in rows:
        key = str(r["cusip"])[:9]
        acc = out.setdefault(key, {"issuer": r.get("issuer", ""), "cusip": key, "shares": 0.0, "value": 0.0})
        acc["shares"] = float(acc["shares"]) + float(r.get("shares") or 0.0)
        acc["value"] = float(acc["value"]) + float(r.get("value") or 0.0)
    return out


def pct_outstanding(shares: float, shares_outstanding: Optional[float]) -> Optional[float]:
    """A stake as a share of the company, or None when the denominator is wrong.

    Over 100% is not a stake, it is a mismatched denominator: Berkshire's class
    B position came out at 8,415% of BRK-B against a share count for another
    class. Unknown is reported as unknown.
    """
    if not shares_outstanding or shares_outstanding <= 0:
        return None
    pct = 100.0 * float(shares) / float(shares_outstanding)
    return round(pct, 3) if pct <= 100.0 else None


def stake_pct(shares: float, value_usd: float, shares_out: Optional[float],
              market_cap_usd: Optional[float]) -> Optional[float]:
    """A stake as a share of the company: shares held over shares outstanding.

    Shares are what a 13F reports exactly, so they are primary. The market-value
    figure is used only when the share counts are for different classes, which
    shows as an implied price gap no market move explains: Berkshire's class B
    position against a class A share count gave 8,415%, a thousandfold gap. A
    plain disagreement is not that -- the 13F is priced at quarter end and the
    market cap today, and Intuit's implied price had fallen from $662 to $275, so
    an override on a 2x gap turned Vanguard's 10.6% into 25.5%.
    """
    by_shares = pct_outstanding(shares, shares_out)
    by_value = None
    if market_cap_usd and market_cap_usd > 0 and value_usd > 0:
        v = 100.0 * float(value_usd) / float(market_cap_usd)
        by_value = round(v, 3) if v <= 100.0 else None
    if by_shares is None:
        return by_value
    if shares > 0 and shares_out and market_cap_usd and value_usd > 0:
        filed_price = float(value_usd) / float(shares)
        market_price = float(market_cap_usd) / float(shares_out)
        if filed_price > 0 and market_price > 0 and max(filed_price / market_price, market_price / filed_price) > 10.0:
            return by_value
    return by_shares


# Companies an OSINT event can reach without being quoted: the defence primes,
# and the graph's sectors where physical events move prices.
DEFENSE_PRIMES = ("LMT", "NOC", "RTX", "GD", "LHX", "HII", "BA", "LDOS", "BAH")
OSINT_SECTORS = ("Aerospace & Defense", "Energy", "Semiconductors", "Metals & Mining")


def value_scale(positions: Dict[str, Dict[str, object]]) -> float:
    """1, or 1000 for a table still reported in thousands of dollars.

    Values have been in dollars since 2023, and T. Rowe Price Associates' table
    still read as $3 billion for a manager of about $1 trillion. An implied
    average price under a dollar a share is the tell.
    """
    shares = sum(float(p["shares"]) for p in positions.values())
    value = sum(float(p["value"]) for p in positions.values())
    if shares <= 0:
        return 1.0
    return 1000.0 if value / shares < 1.0 else 1.0


# ── Follow-the-money measures over {ticker: {filer_id: pct_outstanding}} ─────

def top_holders(holders: Dict[str, float], n: int = 5) -> List[Tuple[str, float]]:
    return sorted(((f, p) for f, p in holders.items() if p is not None), key=lambda x: -x[1])[:n]


def big_three_share(holders: Dict[str, float]) -> float:
    """Combined stake of BlackRock, Vanguard and State Street, in percent."""
    return round(sum(holders.get(f) or 0.0 for f in BIG_THREE), 3)


def common_ownership(a: Dict[str, float], b: Dict[str, float]) -> float:
    """The stake two companies' shareholders hold in both: sum over filers of
    the smaller of the two percentages. 0 shares no holder; a high number means
    the same few institutions sit on both registers."""
    return round(sum(min(a[f], b[f]) for f in set(a) & set(b) if a[f] and b[f]), 3)


def cross_holdings(
    stakes: Dict[str, Dict[str, float]], listed: Dict[str, str],
) -> List[Tuple[str, str, float, Optional[float]]]:
    """Listed institutions that hold each other.

    `stakes` is {filer_id: {ticker: pct}}; `listed` is {filer_id: own ticker}.
    Returns (filer_a, filer_b, a's stake in b, b's stake in a or None), one row
    per ordered pair where a holds b. A pair holding each other is the cycle the
    ownership graph exists to show.
    """
    out = []
    by_ticker = {t: f for f, t in listed.items()}
    for a, held in stakes.items():
        for ticker, pct in held.items():
            b = by_ticker.get(ticker)
            if not b or b == a or not pct:
                continue
            back = stakes.get(b, {}).get(listed.get(a, ""), None)
            out.append((a, b, pct, back))
    return sorted(out, key=lambda r: -r[2])


# ── Reading what the collector stored (shared by the API and reasoning) ──────

HOLDERS_KEY = "sentinel:ownership:holders:{ticker}"
MCAP_KEY = "sentinel:mcap_usd:{ticker}"         # USD market cap, from Finnhub's profile
FILER_KEY = "sentinel:ownership:filer:{filer_id}"


async def read_holders(raw, ticker: str) -> Dict[str, Dict[str, object]]:
    """{filer_id: {filer, shares, value_usd, pct_outstanding, period}} for a ticker."""
    import json

    data = await raw.hgetall(HOLDERS_KEY.format(ticker=str(ticker).upper()))
    out: Dict[str, Dict[str, object]] = {}
    for k, v in (data or {}).items():
        key = k.decode() if isinstance(k, bytes) else k
        try:
            out[key] = json.loads(v)
        except (TypeError, ValueError):
            continue
    return out


def holder_pcts(holders: Dict[str, Dict[str, object]]) -> Dict[str, float]:
    return {f: float(h["pct_outstanding"]) for f, h in holders.items()
            if h.get("pct_outstanding") is not None}


def holders_line(ticker: str, holders: Dict[str, Dict[str, object]], n: int = 3) -> str:
    """One prompt line: the largest reported holders of a company."""
    pcts = holder_pcts(holders)
    if not pcts:
        return ""
    names = {f: str(h.get("filer") or f) for f, h in holders.items()}
    top = ", ".join(f"{names[f]} {p:.1f}%" for f, p in top_holders(pcts, n))
    b3 = big_three_share(pcts)
    return f"{ticker}: {top}" + (f" (BlackRock+Vanguard+State Street {b3:.1f}%)" if b3 else "")
