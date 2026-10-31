"""
shared/utils/money.py

Follow the money, summarised for a reader rather than an analyst: how few
institutions own how much, whether rival companies answer to the same owners,
and which companies live on one customer's orders.

Pure: dicts in, dicts out. The gateway reads 13F holders from Redis
(shared.utils.ownership) and 10-K customer links from Postgres
(shared.utils.customer_links) and hands them here.

13F is long positions only, filed 45 days after quarter end, aggregated per
manager; a 10-K names a customer only above 10% of revenue. Both understate
concentration, never overstate it.
"""
from __future__ import annotations

from statistics import median
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

from shared.utils.ownership import BIG_THREE, big_three_share, common_ownership

# Companies that compete for the same customers, where both are covered by the
# holder data. Curated, so each pair is a rivalry a reader recognises; a pair
# whose holders are missing is skipped, never shown as zero. The page says
# "the two compete for customers", so every pair must: regulated utilities
# with separate service areas (DUK, AEP) and Lilly against Moderna were taken
# out for that reason.
RIVAL_PAIRS: Tuple[Tuple[str, str, str], ...] = (
    ("NVDA", "AMD", "AI chips"),
    ("KLAC", "LRCX", "Chipmaking equipment"),
    ("MU", "WDC", "Memory and storage"),
    ("MCHP", "SWKS", "Analog chips"),
    ("UPS", "FDX", "Parcel delivery"),
    ("EOG", "DVN", "Oil and gas drilling"),
    ("SHEL", "EQNR", "Oil majors"),
    ("VALE", "BHP", "Iron ore mining"),
    ("GD", "RTX", "Defense contractors"),
    ("DE", "AGCO", "Farm machinery"),
    ("DIS", "WBD", "Film and streaming"),
    ("F", "LCID", "Car makers"),
)

# The buyers a 10-K link is trusted for: the customer's name must contain one of
# these stems. The extractor resolves names to tickers by pattern, and a
# loose match turned "On" into On Holding and "People" into a stock; checking the
# name against the buyer it resolved to is what keeps those out.
BIG_BUYERS: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "WMT": ("Walmart", ("walmart", "wal-mart")),
    "TGT": ("Target", ("target",)),
    "AMZN": ("Amazon", ("amazon",)),
    "COST": ("Costco", ("costco",)),
    "AAPL": ("Apple", ("apple",)),
    "MSFT": ("Microsoft", ("microsoft",)),
    "META": ("Meta", ("meta platforms", "facebook")),
    "ORCL": ("Oracle", ("oracle",)),
    "QCOM": ("Qualcomm", ("qualcomm",)),
    "INTC": ("Intel", ("intel",)),
    "AMAT": ("Applied Materials", ("applied materials",)),
    "LRCX": ("Lam Research", ("lam research",)),
    "CVS": ("CVS Health", ("cvs",)),
    "CAH": ("Cardinal Health", ("cardinal health",)),
    "MCK": ("McKesson", ("mckesson",)),
    "COR": ("Cencora", ("cencora", "amerisourcebergen")),
    "RTX": ("RTX", ("rtx", "raytheon")),
    "EFX": ("Equifax", ("equifax",)),
    "PEP": ("PepsiCo", ("pepsico",)),
    "T": ("AT&T", ("at&t",)),
    "NEE": ("NextEra Energy", ("nextera",)),
    "VLO": ("Valero", ("valero",)),
    "HD": ("Home Depot", ("home depot",)),
    "LOW": ("Lowe's", ("lowe",)),
    "ULTA": ("Ulta Beauty", ("ulta",)),
    "SNX": ("TD Synnex", ("synnex",)),
    "ARW": ("Arrow Electronics", ("arrow electronics",)),
    "INGM": ("Ingram Micro", ("ingram micro",)),
}

# A company "answers to" the big three when they hold at least this much of it.
BIG_THREE_NOTABLE_PCT = 15.0


def concentration(pcts_by_ticker: Mapping[str, Mapping[str, float]]) -> Dict[str, object]:
    """How much of each company the three index giants hold, across companies.

    `pcts_by_ticker` is {ticker: {filer_id: pct_outstanding}}. Companies with no
    reported holders are left out rather than counted as unowned.
    """
    shares = {t: big_three_share(dict(p)) for t, p in pcts_by_ticker.items() if p}
    tracked = {t: round(sum(p.values()), 3) for t, p in pcts_by_ticker.items() if p}
    if not shares:
        return {"companies": 0, "median_big_three_pct": None, "notable": 0, "top": []}
    top = sorted(shares.items(), key=lambda kv: -kv[1])[:12]
    return {
        "companies": len(shares),
        "median_big_three_pct": round(median(shares.values()), 2),
        "median_tracked_pct": round(median(tracked.values()), 2),
        "notable": sum(1 for v in shares.values() if v >= BIG_THREE_NOTABLE_PCT),
        "notable_threshold_pct": BIG_THREE_NOTABLE_PCT,
        "top": [{"ticker": t, "big_three_pct": v, "tracked_pct": tracked[t]} for t, v in top],
    }


def largest_holder_counts(
    pcts_by_ticker: Mapping[str, Mapping[str, float]], names: Mapping[str, str],
) -> List[Dict[str, object]]:
    """For each institution, how many companies it is the single largest holder of."""
    counts: Dict[str, int] = {}
    for pcts in pcts_by_ticker.values():
        live = {f: p for f, p in pcts.items() if p}
        if not live:
            continue
        top = max(live, key=lambda f: live[f])
        counts[top] = counts.get(top, 0) + 1
    rows = [{"filer_id": f, "filer": names.get(f, f), "companies": n, "big_three": f in BIG_THREE}
            for f, n in counts.items()]
    return sorted(rows, key=lambda r: -int(r["companies"]))


def rival_overlaps(
    pcts_by_ticker: Mapping[str, Mapping[str, float]],
    pairs: Sequence[Tuple[str, str, str]] = RIVAL_PAIRS,
) -> List[Dict[str, object]]:
    """For each rival pair with holders on both sides: the stake the same
    institutions hold in both (common ownership) and how many hold both."""
    out = []
    for a, b, label in pairs:
        pa, pb = pcts_by_ticker.get(a) or {}, pcts_by_ticker.get(b) or {}
        if not pa or not pb:
            continue
        shared = [f for f in set(pa) & set(pb) if pa[f] and pb[f]]
        out.append({
            "a": a, "b": b, "label": label,
            "common_pct": common_ownership(dict(pa), dict(pb)),
            "shared_holders": len(shared),
            "big_three_a": big_three_share(dict(pa)),
            "big_three_b": big_three_share(dict(pb)),
        })
    return sorted(out, key=lambda r: -float(r["common_pct"]))


def trusted_links(rows: Iterable[Mapping[str, object]]) -> List[Dict[str, object]]:
    """10-K customer links worth showing: the latest filing per supplier and
    buyer, a percentage present, and a customer name that names the buyer.

    Each row: supplier, customer_name, customer_ticker, pct, filed.
    """
    latest: Dict[Tuple[str, str], Mapping[str, object]] = {}
    for r in rows:
        buyer = str(r.get("customer_ticker") or "").upper()
        supplier = str(r.get("supplier") or "").upper()
        pct = r.get("pct")
        if buyer not in BIG_BUYERS or not supplier or supplier == buyer or pct is None:
            continue
        name = str(r.get("customer_name") or "").lower()
        # A combined line ("Target, Walmart, Amazon") is not one buyer's share.
        if "," in name or not any(stem in name for stem in BIG_BUYERS[buyer][1]):
            continue
        key = (supplier, buyer)
        if key not in latest or str(r.get("filed") or "") > str(latest[key].get("filed") or ""):
            latest[key] = r
    # One filing giving the same percentage to several buyers is a combined
    # figure ("our three largest retailers accounted for 81%"), not each one's.
    seen: Dict[Tuple[str, str, float], int] = {}
    for (s, _b), r in latest.items():
        k = (s, str(r.get("filed") or ""), float(r["pct"]))  # type: ignore[arg-type]
        seen[k] = seen.get(k, 0) + 1
    latest = {key: r for key, r in latest.items()
              if seen[(key[0], str(r.get("filed") or ""), float(r["pct"]))] == 1}  # type: ignore[arg-type]
    return [
        {"supplier": s, "buyer": b, "buyer_name": BIG_BUYERS[b][0],
         "pct": float(r["pct"]), "filed": str(r.get("filed") or "")}  # type: ignore[arg-type]
        for (s, b), r in latest.items()
    ]


def dependence_by_buyer(links: Sequence[Mapping[str, object]], min_suppliers: int = 2) -> List[Dict[str, object]]:
    """Group trusted links by buyer: which companies each big buyer keeps alive."""
    groups: Dict[str, List[Mapping[str, object]]] = {}
    for link in links:
        groups.setdefault(str(link["buyer"]), []).append(link)
    out = []
    for buyer, ls in groups.items():
        if len(ls) < min_suppliers:
            continue
        ls = sorted(ls, key=lambda x: -float(x["pct"]))  # type: ignore[arg-type]
        out.append({
            "buyer": buyer, "buyer_name": BIG_BUYERS[buyer][0],
            "suppliers": [{"ticker": x["supplier"], "pct": x["pct"], "filed": x["filed"]} for x in ls],
        })
    return sorted(out, key=lambda g: -len(g["suppliers"]))  # type: ignore[arg-type]


def mutual_pairs(rows: Sequence[Mapping[str, object]]) -> List[Dict[str, object]]:
    """Cross-holdings where both institutions hold each other, one row per pair."""
    seen = set()
    out = []
    for r in rows:
        if r.get("reverse_pct") is None:
            continue
        key = frozenset((str(r["holder"]), str(r["held"])))
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(r))
    return out


# ── Board interlocks: operating companies only ───────────────────────────────

# SIC codes of investment funds, which file Form 4s like companies do. A
# director on ten of one manager's closed-end funds is one fund family's board,
# not ten companies'; on 30 October every top "interlock" was of that kind.
FUND_SIC = frozenset({6722, 6726})

_ENTITY = ("inc", "corp", "llc", "l.p.", "lp", "ltd", "plc", "capital", "management", "bank",
           "trust", "fund", "partners", "holdings", "group", "advisors", "securities", "&", "co.")


def operating_tickers(profiles: Iterable[Mapping[str, object]]) -> set:
    """Tickers with a real industry code that is not a fund's."""
    out = set()
    for p in profiles:
        sic = p.get("sic")
        if isinstance(sic, int) and sic > 0 and sic not in FUND_SIC:
            out.add(str(p.get("ticker") or "").upper())
    return out


def display_name(raw: str) -> str:
    """A Form 4 reporting name, readable: "FLORES LORENZO" -> "Lorenzo Flores",
    "SABA CAPITAL MANAGEMENT, L.P." -> "Saba Capital Management, L.P."."""
    name = " ".join(str(raw or "").replace("/DE/", "").split())
    if not name:
        return name
    words = name.split(" ")
    is_entity = any(w.lower().strip(",") in _ENTITY for w in words)

    def cap(i: int, w: str) -> str:
        if w.count(".") >= 2:             # L.P., N.A., U.S.
            return w.upper()
        if i > 0 and w.lower() in ("of", "and", "the", "for"):
            return w.lower()
        return w[:1].upper() + w[1:].lower() if w.isupper() else w

    words = [cap(i, w) for i, w in enumerate(words)]
    # Form 4 lists people surname first; companies are left in their order.
    if not is_entity and 2 <= len(words) <= 5 and name.isupper():
        # A generational suffix goes last wherever the filing put it: EDGAR
        # writes "WARD JR STEPHEN M", which reads "Stephen M Ward Jr".
        suffixes = ("jr", "sr", "ii", "iii", "iv")
        suffix = [w.upper() if w.lower() in ("ii", "iii", "iv") else w
                  for w in words if w.lower().rstrip(".") in suffixes]
        words = [w for w in words if w.lower().rstrip(".") not in suffixes]
        words = words[1:] + words[:1] + suffix
    return " ".join(words)


def operating_interlocks(
    rows: Iterable[Mapping[str, object]], operating: set, limit: int = 50,
) -> List[Dict[str, object]]:
    """Insiders of two or more operating companies, funds removed."""
    out = []
    for r in rows:
        companies = [c for c in (r.get("companies") or []) if str(c).upper() in operating]  # type: ignore[union-attr]
        if len(companies) < 2:
            continue
        out.append({"person": display_name(str(r.get("person") or "")),
                    "companies": companies, "roles": list(r.get("roles") or [])})  # type: ignore[arg-type]
    out.sort(key=lambda x: -len(x["companies"]))  # type: ignore[arg-type]
    return out[:limit]


# ── Company names a reader recognises ────────────────────────────────────────

_CORP_SUFFIX = {"INC", "CO", "CORP", "CORPORATION", "LTD", "PLC", "LLC", "LP", "NV", "SA", "AG", "DE",
                "HOLDINGS", "COMPANY", "INCORPORATED", "THE"}


def company_label(aliases: Sequence[str]) -> str:
    """A readable company name from the SEC titles that point at one ticker.

    "CHURCH & DWIGHT CO INC /DE/" -> "Church & Dwight"; "CAMPBELL'S CO" ->
    "Campbell's". A title keeping its "&" or apostrophe wins over the folded
    spellings ("CHURCH DWIGHT"), then the shortest.
    """
    best = ""
    best_key = (2, 10**6)
    for raw in aliases:
        words = [w.strip(",.") for w in str(raw).replace("/", " ").split()]
        while words and words[-1].upper() in _CORP_SUFFIX:
            words.pop()
        name = " ".join(w for w in words if w)
        if len(name) < 2:
            continue
        key = (0 if ("&" in name or "'" in name) else 1, len(name))
        if key < best_key:
            best, best_key = name, key
    if not best:
        return ""
    best = best.rstrip(" &")  # "DEERE & CO" loses its "CO" above, then its "&"

    def word(w: str) -> str:
        if not w.isupper():
            return w
        # A short all-caps word is an acronym (EOG, RTX, KLA, BHP), not a name.
        if len(w) <= 3 and w not in _SHORT_WORDS:
            return w
        return w[:1] + w[1:].lower()

    return " ".join(word(w) for w in best.split())


_SHORT_WORDS = {"DR", "ST", "OF", "AND", "THE", "NEW", "BIG", "ONE", "TWO", "AIR", "OIL", "GAS", "CAR"}

# How readers know these companies, where the SEC title spells them otherwise
# or the registry has no title for the ticker.
BRAND_NAMES: Dict[str, str] = {
    "FDX": "FedEx", "PEP": "PepsiCo", "SN": "SharkNinja", "NVDA": "NVIDIA", "AMD": "AMD",
    "MCHP": "Microchip", "F": "Ford", "SHEL": "Shell", "EQNR": "Equinor", "UPS": "UPS",
    "KMB": "Kimberly-Clark", "AGCO": "AGCO", "WBD": "Warner Bros. Discovery", "DIS": "Disney",
    "GD": "General Dynamics", "RTX": "RTX", "LCID": "Lucid", "KLAC": "KLA", "LRCX": "Lam Research",
    "AMZN": "Amazon", "KO": "Coca-Cola", "WMT": "Walmart", "AAPL": "Apple", "MSFT": "Microsoft",
}


def company_name(ticker: str, aliases: Sequence[str]) -> str:
    """The name to show for a ticker: a known brand spelling, else a readable
    SEC title, else the ticker."""
    t = str(ticker or "").upper()
    return BRAND_NAMES.get(t) or company_label(aliases) or t
