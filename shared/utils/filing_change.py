"""
shared/utils/filing_change.py

How much a company's periodic filing changed from the comparable one before it
(Cohen, Malloy & Nguyen, "Lazy Prices", J. Finance 2020): a 10-K against the
previous 10-K, a 10-Q against the same quarter's 10-Q a year earlier. Firms
whose filings change underperform over the following months, with no reaction
on the filing day; changes in the risk-factor section are among the most
informative. Measured as one minus the cosine similarity of word counts, for
the whole document and for Item 1A. Pure: text in, numbers out.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from datetime import date
from typing import List, Mapping, Optional, Sequence, Tuple

FEATURE = "fil_change"
FEATURE_RISK = "fil_change_risk"
FORMS = ("10-K", "10-Q")
MIN_WORDS = 500                 # a shorter document is a cover page or an exhibit index, not a filing
_WORD = re.compile(r"[a-z]{3,}")
# Item 1A to the next item heading. 10-K: "Item 1A. Risk Factors" .. "Item 1B" or
# "Item 2"; 10-Q: "Item 1A. Risk Factors" in Part II .. "Item 2". The last match
# of the heading wins, so a table of contents entry is skipped.
_RISK_START = re.compile(r"item\s*1a\.?\s*[\-–—:]?\s*risk\s+factors", re.I)
_RISK_END = re.compile(r"item\s*(1b|2|3)\.?\s*[\-–—:]?\s*(unresolved|properties|unregistered|defaults|legal|cybersecurity)",
                       re.I)

Filing = Tuple[str, date, Optional[date], str]       # (form, filed, report date, accession)


def words(text: str) -> Counter:
    return Counter(_WORD.findall((text or "").lower()))


def cosine(a: Mapping[str, int], b: Mapping[str, int]) -> Optional[float]:
    if not a or not b:
        return None
    dot = sum(v * b.get(k, 0) for k, v in a.items())
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    return dot / (na * nb) if na and nb else None


def risk_section(text: str) -> str:
    """Item 1A's text from its last heading (a table of contents comes first),
    or "" when there is none. Falling back to an earlier heading when the last
    section was short measured half a 10-Q as its "risk factors" (JPM's moved
    between 0.30 and 0.002 quarter to quarter)."""
    starts = list(_RISK_START.finditer(text or ""))
    if not starts:
        return ""
    s = starts[-1]
    end = _RISK_END.search(text, s.end())
    return text[s.end(): end.start() if end else s.end() + 200_000]


def change(prev_text: str, text: str, risk: bool = True) -> Tuple[Optional[float], Optional[float]]:
    """(whole-document change, risk-factor change), each 1 - cosine; None when a
    side is too short to measure, or `risk` is False (a 10-Q's Item 1A is
    mostly a line referring back to the 10-K)."""
    a, b = words(prev_text), words(text)
    whole = None
    if sum(a.values()) >= MIN_WORDS and sum(b.values()) >= MIN_WORDS:
        c = cosine(a, b)
        whole = round(1.0 - c, 6) if c is not None else None
    if not risk:
        return whole, None
    ra, rb = words(risk_section(prev_text)), words(risk_section(text))
    risk_change = None
    if sum(ra.values()) >= MIN_WORDS // 2 and sum(rb.values()) >= MIN_WORDS // 2:
        c = cosine(ra, rb)
        risk_change = round(1.0 - c, 6) if c is not None else None
    return whole, risk_change


def pairs(filings: Sequence[Filing]) -> List[Tuple[Filing, Filing]]:
    """(earlier, later) comparable filings: consecutive 10-Ks, and each 10-Q
    with the 10-Q whose report date is 300-430 days before its own (the same
    fiscal quarter a year earlier)."""
    out: List[Tuple[Filing, Filing]] = []
    ks = sorted((f for f in filings if f[0] == "10-K"), key=lambda f: f[1])
    out += list(zip(ks, ks[1:]))
    qs = sorted((f for f in filings if f[0] == "10-Q" and f[2]), key=lambda f: f[2])
    for q in qs:
        prior = [p for p in qs if p[2] and 300 <= (q[2] - p[2]).days <= 430]
        if prior:
            out.append((min(prior, key=lambda p: abs((q[2] - p[2]).days - 365)), q))
    return out


def recent_filings(submissions: Mapping[str, object], since: date) -> List[Tuple[Filing, str]]:
    """((form, filed, report date, accession), primary document) for 10-K and 10-Q
    filings on or after `since`, from an EDGAR submissions record or one of its
    older pages (the same columns at the top level)."""
    rec = (submissions.get("filings") or {}).get("recent") or submissions     # type: ignore[union-attr]
    out = []
    for form, filed, rep, acc, doc in zip(rec.get("form", []), rec.get("filingDate", []), rec.get("reportDate", []),
                                          rec.get("accessionNumber", []), rec.get("primaryDocument", [])):
        if form not in FORMS or not doc:
            continue
        try:
            fd = date.fromisoformat(filed)
            rd = date.fromisoformat(rep) if rep else None
        except ValueError:
            continue
        if fd >= since:
            out.append(((form, fd, rd, acc), doc))
    return out


def older_pages(submissions: Mapping[str, object], since: date) -> List[str]:
    """Names of a submissions record's older pages that reach back to `since`:
    "recent" holds the last ~1,000 filings of every form, which a large filer
    passes within months (XOM's held one 10-K / 10-Q in two years)."""
    out = []
    for f in (submissions.get("filings") or {}).get("files") or []:           # type: ignore[union-attr]
        try:
            if date.fromisoformat(str(f.get("filingTo"))) >= since:
                out.append(str(f["name"]))
        except (KeyError, ValueError):
            continue
    return out


__all__ = ["FEATURE", "FEATURE_RISK", "FORMS", "Filing", "change", "cosine", "older_pages", "pairs", "recent_filings",
           "risk_section", "words"]
