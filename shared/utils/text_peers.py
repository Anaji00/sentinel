"""
shared/utils/text_peers.py

Peers from what companies say they do: the product vocabulary of each 10-K's
business description (Item 1), compared across companies, after Hoberg &
Phillips (TNIC; "Text-Based Industry Momentum", JFQA 2018: momentum through
less visible text peers beats own-firm and SIC-peer momentum). Their data is
keyed to Compustat and stops in 2023, so the method is reproduced here from
the 10-Ks the filing-change job already reads. Words used by more than
COMMON_SHARE of firms carry no industry and are dropped, as they do. Pure.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from typing import Dict, List, Mapping, Sequence, Tuple

FEATURE = "text_peer_mom_21"
TERMS_KEY = "sentinel:text_peers:terms"          # ticker -> {filed, terms} of its latest 10-K's Item 1
TOP_TERMS = 400
COMMON_SHARE = 0.25
MIN_SIM = 0.12
TOP_K = 8
_WORD = re.compile(r"[a-z]{4,}")
_START = re.compile(r"item\s*1\.?\s*[\-–—:]?\s*business\b", re.I)
_END = re.compile(r"item\s*1a\.?\s*[\-–—:]?\s*risk\s+factors", re.I)
# Words every business description uses, which say nothing about the product.
_STOP = frozenset("""
    with that this from have which their they will been were also such other these more than into most some only
    company companies business businesses including include includes products product services service customers
    customer operations operating operate market markets year years fiscal million billion percent approximately
    certain significant various within upon over under through each based could would should those there where
    when what about across among annual report form item part financial information subsidiaries subsidiary
    employees employee competition competitive industry industries global international united states
""".split())

PeerRow = Tuple[str, str, float]


def business_section(text: str) -> str:
    """Item 1's text from its last heading to Item 1A, or "" without both."""
    starts = list(_START.finditer(text or ""))
    if not starts:
        return ""
    s = starts[-1]
    end = _END.search(text, s.end())
    return text[s.end(): end.start()] if end else ""


def terms(section: str, top: int = TOP_TERMS) -> Dict[str, int]:
    """The section's most used content words and their counts."""
    c = Counter(w for w in _WORD.findall((section or "").lower()) if w not in _STOP)
    return dict(c.most_common(top))


def peers(docs: Mapping[str, Mapping[str, int]], k: int = TOP_K, min_sim: float = MIN_SIM,
          common_share: float = COMMON_SHARE) -> List[PeerRow]:
    """(ticker, peer, similarity): each company's k most similar by idf-weighted
    cosine over its terms, at least min_sim, words in more than common_share of
    the documents dropped."""
    n = len(docs)
    if n < 2:
        return []
    df = Counter(w for d in docs.values() for w in d)
    keep = {w: math.log(n / c) for w, c in df.items() if c / n <= common_share and c >= 2}
    vec: Dict[str, Dict[str, float]] = {}
    for t, d in docs.items():
        v = {w: (1 + math.log(c)) * keep[w] for w, c in d.items() if w in keep}
        norm = math.sqrt(sum(x * x for x in v.values()))
        if norm:
            vec[t] = {w: x / norm for w, x in v.items()}
    index: Dict[str, List[Tuple[str, float]]] = {}
    for t, v in vec.items():
        for w, x in v.items():
            index.setdefault(w, []).append((t, x))
    out: List[PeerRow] = []
    for t, v in vec.items():
        sims: Dict[str, float] = {}
        for w, x in v.items():
            for u, y in index[w]:
                if u != t:
                    sims[u] = sims.get(u, 0.0) + x * y
        best = sorted(((u, s) for u, s in sims.items() if s >= min_sim), key=lambda p: -p[1])[:k]
        out += [(t, u, round(s, 4)) for u, s in best]
    return out


def peer_momentum(rows: Sequence[PeerRow], returns_21: Mapping[str, float]) -> Dict[str, float]:
    """Each company's mean 21-session return across its text peers that have one."""
    by: Dict[str, List[float]] = {}
    for t, u, _ in rows:
        if u in returns_21:
            by.setdefault(t, []).append(returns_21[u])
    return {t: round(sum(v) / len(v), 6) for t, v in by.items() if v}


__all__ = ["FEATURE", "MIN_SIM", "TERMS_KEY", "TOP_K", "PeerRow", "business_section", "peer_momentum", "peers", "terms"]
