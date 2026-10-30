"""
shared/utils/customer_links.py

Who a company sells to, from its 10-K. Regulation S-K (Item 101) requires a
registrant to name any customer over 10% of revenue, so the 10-K is a free,
dated supplier-customer map; it is what Cohen & Frazzini (2008, "Economic
Links and Predictable Returns") built theirs from, finding that a customer's
return predicts its supplier's next month's.

  passages   sentences that mention a percentage of revenue or sales and a
             customer
  candidates every capitalised name followed, in the same sentence, by a
             link verb and a percentage ("Apple, through sales to ...,
             accounted for 67%"; "Walmart Stores, Inc. and its affiliates,
             were 26%"); kept only if it resolves to a listed company, which
             is what separates a customer from "U.S." or a product line
  model      the extraction model, only on passages the pattern found nothing
             in, and only names that appear verbatim in the passage are kept
  feature    cust_mom_21: a supplier's customers' 21-session return, weighted
             by their share of its revenue, from the 10-K's filing date

Pure: text in, links out; closes in, feature rows out.
"""
from __future__ import annotations

import re
from bisect import bisect_right
from datetime import date, timedelta
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

MAX_PASSAGES = 8
MAX_PASSAGE_CHARS = 600
LINK_VALID_DAYS = 400            # a 10-K stands until the next one, about a year
MOM_WINDOW = 21
FEATURE = "cust_mom_21"

_SENT = re.compile(r"(?<=[.;])\s+(?=[A-Z(])")
_PCT = re.compile(r"\d{1,2}(?:\.\d+)?\s?%")
_TOPIC = re.compile(r"\b(customers?|accounted for|represented)\b", re.I)
_BASE = re.compile(r"\b(revenues?|sales|net sales)\b", re.I)
_RUN = re.compile(r"[A-Z][A-Za-z0-9&'\-]*(?:\.(?=[A-Za-z,]))?(?:(?:,\s|\s)(?:[A-Z][A-Za-z0-9&'\-]*\.?|&))*")
# The verb, then up to a few words without a number before the share
# ("accounted for consolidated net sales of approximately 16 %").
_AFTER = re.compile(r"[^.;]{0,240}?\b(?:accounted for|accounts for|account for|represented|represents|represent|"
                    r"comprised|comprises|made up|were|was)\b[^.;%\d]{0,60}?"
                    r"(?P<pct>\d{1,2}(?:\.\d+)?)\s?%")
_SUFFIX = {"INC", "INC.", "CORP", "CORP.", "CORPORATION", "CO", "CO.", "LTD", "LTD.", "LLC", "PLC", "&"}
_LEAD = re.compile(r"^(?:(?:Sales|Revenues?|Shipments) (?:to|from) |Our |The |In |During |For |Fiscal )+")
_ANON = re.compile(r"^(?:customer|customers|one|two|three|no|each|any|our|this|that|these|such|sales|revenue|"
                   r"revenues|net|total|product|products|segment|distributor|distributors|company|we|it|"
                   r"fiscal|international|domestic|largest|top|ten|five|other|all|direct|during|in|for|the|"
                   r"united|u\.?s|us|north|south|east|west|america|americas|europe|asia|china|japan|taiwan|korea|singapore|"
                   r"india|germany|canada|mexico|ireland|netherlands|switzerland|israel|hong|kingdom|uk|france|"
                   r"brazil|malaysia|vietnam|thailand|philippines|emea|apac|latin|middle|rest|world|"
                   r"government|federal|department|cost|gross|operating|research|selling|general|income|"
                   r"cleaning|consumer|services|service|software|hardware|wireless|data|cloud|retail)\b", re.I)
_ANON_TAIL = re.compile(r"\b(?:Customer|Distributor|Client|Party)\s+[A-Z0-9]\b|\bcustomers?$", re.I)


class Named(BaseModel):
    name: str = Field(max_length=120)
    pct: Optional[float] = Field(default=None, ge=0, le=100)


class Customers(BaseModel):
    customers: List[Named] = Field(default_factory=list, max_length=10)


EXTRACT_SYSTEM = ("From the passage of a company's annual report, list each customer it NAMES as a share of its revenue "
                  "or sales, with the percentage. Skip anonymous ones (Customer A, one customer). Copy names exactly as "
                  'written. Answer in JSON only: {"customers": [{"name": "...", "pct": 21.0}]}.')


def passages(text: str) -> List[str]:
    """The sentences worth reading: a percentage, a customer, and revenue or sales."""
    out: List[str] = []
    for s in _SENT.split(text):
        if _PCT.search(s) and _TOPIC.search(s) and _BASE.search(s):
            out.append(s.strip()[:MAX_PASSAGE_CHARS])
            if len(out) >= MAX_PASSAGES:
                break
    return out


def clean(name: str) -> Optional[str]:
    n = _LEAD.sub("", name.strip(" ,.;")).strip(" ,")
    if len(n) < 2 or _ANON.match(n) or _ANON_TAIL.search(n):
        return None
    return n


def candidates(passage: str) -> List[Tuple[str, float]]:
    """Each capitalised name followed in its sentence by a link verb and a
    percentage, with that percentage (the first, which 10-Ks give for the
    latest year). Names only: whether one is a company is the resolver's call."""
    out: List[Tuple[str, float]] = []
    for m in _RUN.finditer(passage):
        after = _AFTER.match(passage, m.end())
        n = clean(m.group(0)) if after else None
        if n and not any(n == x for x, _ in out):
            out.append((n, float(after.group("pct"))))
    return out


def variants(name: str) -> List[str]:
    """The name, then shorter by trailing words ("Walmart Stores, Inc" ->
    "Walmart Stores" -> "Walmart"), for the resolver to try in turn."""
    words = [w.strip(",") for w in name.split()]
    while words and words[-1].upper() in _SUFFIX:
        words.pop()
    out = [name] if name else []
    for k in range(len(words), 0, -1):
        v = " ".join(words[:k])
        if v not in out and (k > 1 or (len(v) >= 4 and not _ANON.match(v))):
            out.append(v)
    return out


def grounded(found: Sequence[Mapping[str, object]], passage: str) -> List[Tuple[str, Optional[float]]]:
    """The model's names that appear verbatim in the passage and are not anonymous."""
    out = []
    for c in found:
        n = clean(str(c.get("name") or ""))
        if n and n in passage:
            pct = c.get("pct")
            out.append((n, float(pct) if isinstance(pct, (int, float)) else None))
    return out


def momentum_rows(supplier: str, links: Sequence[Tuple[date, str, float]],
                  closes: Mapping[str, Tuple[Sequence[date], Sequence[float]]],
                  sessions: Sequence[date]) -> List[Tuple[str, date, str, float]]:
    """cust_mom_21 on each session: the latest filing's resolved customers
    (filed, customer_ticker, pct), their 21-session returns weighted by pct.
    A session before the first filing, or more than LINK_VALID_DAYS after the
    latest, has none."""
    by_filed: Dict[date, List[Tuple[str, float]]] = {}
    for filed, cust, pct in links:
        by_filed.setdefault(filed, []).append((cust, pct if pct and pct > 0 else 10.0))
    filed_days = sorted(by_filed)
    out = []
    for d in sessions:
        i = bisect_right(filed_days, d) - 1
        if i < 0 or (d - filed_days[i]) > timedelta(days=LINK_VALID_DAYS):
            continue
        num = den = 0.0
        for cust, w in by_filed[filed_days[i]]:
            days, px = closes.get(cust, ((), ()))
            j = bisect_right(days, d) - 1
            if j - MOM_WINDOW < 0 or px[j - MOM_WINDOW] <= 0:
                continue
            num += w * (px[j] / px[j - MOM_WINDOW] - 1)
            den += w
        if den > 0:
            out.append((supplier, d, FEATURE, round(num / den, 6)))
    return out


__all__ = ["Customers", "EXTRACT_SYSTEM", "FEATURE", "candidates", "clean", "grounded", "momentum_rows", "passages",
           "variants"]
