"""
Ownership: who holds what, and who holds whom.

Read from what services/collector-filings/ownership.py stores from 13F filings:
each tracked institution's positions in the companies the platform quotes and
in the listed institutions themselves, sized as a share of shares outstanding.
13F covers long equity positions only, is filed 45 days after quarter end, and
reports each manager's aggregate across its funds -- these are stakes as filed,
not live positions.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from services.api_gateway.dependencies import get_db
from shared.db import get_neo4j, get_redis
from shared.utils.entity_resolution import ALIAS_KEY
from shared.utils.money import (
    company_name, concentration, dependence_by_buyer, largest_holder_counts, mutual_pairs, operating_interlocks,
    operating_tickers, rival_overlaps, trusted_links,
)
from shared.utils.ownership import (
    BIG_THREE, FILER_KEY, HOLDERS_KEY, OWNERSHIP_FILERS, big_three_share, common_ownership,
    cross_holdings, holder_pcts, read_holders,
)
from shared.utils.rbac import Role, require_role
from shared.utils.response_cache import cache_key, cached_json

logger = logging.getLogger("api.ownership")

router = APIRouter(prefix="/api/v1/ownership", tags=["Ownership & Follow the Money"])


class Holder(BaseModel):
    filer_id: str
    filer: str
    pct_outstanding: Optional[float] = None
    shares: float
    value_usd: float
    period: str


class HoldersResponse(BaseModel):
    ticker: str
    name: Optional[str] = None
    holders: List[Holder]
    tracked_institutions_pct: float
    big_three_pct: float
    note: str = "13F long positions as filed; 45-day lag; manager-level aggregates."


class CrossHolding(BaseModel):
    holder: str
    holder_ticker: Optional[str]
    held: str
    held_ticker: Optional[str]
    pct: float
    reverse_pct: Optional[float] = None


class CommonOwnership(BaseModel):
    a: str
    b: str
    common_ownership_pct: float
    shared_holders: List[Dict[str, Any]]


async def _raw():
    r = await get_redis()
    return getattr(r, "raw", r)


async def _resolve_ticker(raw, query: str) -> str:
    """A ticker for what someone typed: the ticker itself when it has holders,
    else a company name looked up in the SEC titles the entity resolver keeps
    ("walmart" -> WMT). Falls back to the query, upper-cased."""
    q = " ".join(query.strip().upper().split())
    if await raw.exists(HOLDERS_KEY.format(ticker=q)):
        return q
    # The SEC titles drop most punctuation: "Coca-Cola" is filed as COCA COLA.
    folded = " ".join(q.replace("-", " ").replace(".", " ").replace(",", " ").split())
    for base in dict.fromkeys((q, folded)):
        for alias in (base, f"{base} INC", f"{base} INC.", f"{base}, INC.", f"{base} CORP", f"{base} CO",
                      f"{base} CORPORATION", f"{base} COM INC"):
            hit = await raw.hget(ALIAS_KEY, alias)
            if hit:
                return hit.decode() if isinstance(hit, bytes) else str(hit)
    return q


@router.get("/holders/{ticker}", response_model=HoldersResponse)
async def holders(ticker: str, user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """Who holds a company. `ticker` may also be a company name."""
    raw = await _raw()
    resolved = await _resolve_ticker(raw, ticker)
    data = await read_holders(raw, resolved)
    rows = sorted(
        (Holder(filer_id=f, filer=str(h.get("filer") or f), pct_outstanding=h.get("pct_outstanding"),
                shares=float(h.get("shares") or 0), value_usd=float(h.get("value_usd") or 0),
                period=str(h.get("period") or ""))
         for f, h in data.items()),
        key=lambda h: -(h.pct_outstanding or 0.0),
    )
    pcts = holder_pcts(data)
    return HoldersResponse(
        ticker=resolved, name=(await _ticker_names(raw, [resolved]))[resolved] if rows else None, holders=rows,
        tracked_institutions_pct=round(sum(pcts.values()), 3),
        big_three_pct=big_three_share(pcts),
    )


@router.get("/filer/{filer_id}")
async def filer(filer_id: str, limit: int = Query(50, le=500),
                user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    raw = await _raw()
    blob = await raw.get(FILER_KEY.format(filer_id=filer_id.upper()))
    if not blob:
        raise HTTPException(status_code=404, detail="filer not collected yet")
    data = json.loads(blob)
    data["holdings"] = sorted(data.get("holdings", []), key=lambda h: -(h.get("value_usd") or 0))[:limit]
    return data


async def _stakes(raw) -> Dict[str, Dict[str, float]]:
    stakes: Dict[str, Dict[str, float]] = {}
    for f in OWNERSHIP_FILERS.values():
        blob = await raw.get(FILER_KEY.format(filer_id=f.filer_id))
        if not blob:
            continue
        stakes[f.filer_id] = {h["ticker"]: h["pct_outstanding"] for h in json.loads(blob).get("holdings", [])
                              if h.get("pct_outstanding") is not None}
    return stakes


@router.get("/cross-holdings", response_model=List[CrossHolding])
async def cross(user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """Listed institutions holding each other: the cycles in who-owns-whom."""
    raw = await _raw()
    listed = {f.filer_id: f.listed_as for f in OWNERSHIP_FILERS.values() if f.listed_as}
    names = {f.filer_id: f.name for f in OWNERSHIP_FILERS.values()}
    return [
        CrossHolding(holder=names[a], holder_ticker=listed.get(a), held=names[b],
                     held_ticker=listed.get(b), pct=pct, reverse_pct=back)
        for a, b, pct, back in cross_holdings(await _stakes(raw), listed)
    ]


@router.get("/common", response_model=CommonOwnership)
async def common(a: str, b: str, user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """How much of two companies the same institutions hold."""
    raw = await _raw()
    ha, hb = await read_holders(raw, a), await read_holders(raw, b)
    pa, pb = holder_pcts(ha), holder_pcts(hb)
    shared = sorted(
        ({"filer_id": f, "filer": ha[f].get("filer"), "pct_a": pa[f], "pct_b": pb[f],
          "big_three": f in BIG_THREE} for f in set(pa) & set(pb)),
        key=lambda r: -min(r["pct_a"], r["pct_b"]),
    )
    return CommonOwnership(a=a.upper(), b=b.upper(), common_ownership_pct=common_ownership(pa, pb),
                           shared_holders=shared)


class Interlock(BaseModel):
    person: str
    companies: List[str]
    roles: List[str]


@router.get("/interlocks", response_model=List[Interlock])
async def interlocks(ticker: Optional[str] = None, limit: int = Query(50, le=500),
                     db=Depends(get_db), user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """People who are insiders of more than one operating company -- board interlocks.

    From Form 4 (INSIDER_OF edges): the same director or officer reporting at
    several issuers. Investment funds are left out: they file Form 4s too, and
    a director on one manager's ten closed-end funds is one fund family's
    board, not ten companies'. Filtered to those touching `ticker` when one is
    given. Builds up as filings arrive; there is no backfill.
    """
    neo = await get_neo4j()
    rows = await neo.query(
        """
        MATCH (p)-[r:INSIDER_OF]->(c:Company)
        WITH p, collect(DISTINCT c.name) AS companies, collect(DISTINCT r.role) AS roles
        WHERE size(companies) >= 2 AND ($ticker IS NULL OR $ticker IN companies)
        RETURN coalesce(p.name, p.id) AS person, companies, roles
        ORDER BY size(companies) DESC LIMIT 2000
        """,
        {"ticker": ticker.upper() if ticker else None},
    )
    profiles = await db.query("SELECT ticker, sic FROM company_profile")
    return [Interlock(person=r["person"], companies=r["companies"], roles=[x for x in r["roles"] if x])
            for r in operating_interlocks(rows or [], operating_tickers(profiles or []), limit)]


class ContractRecipient(BaseModel):
    agency: str
    amount_usd: float
    period: Optional[str] = None


@router.get("/contracts/{ticker}", response_model=List[ContractRecipient])
async def contracts(ticker: str, user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """Federal agencies obligating money to a company (USAspending), from the graph."""
    neo = await get_neo4j()
    rows = await neo.query(
        """
        MATCH (c:Company {name: $t})-[r:CONTRACTED_BY]->(a)
        RETURN coalesce(a.name, a.id) AS agency, r.amount_usd AS amount, r.period AS period
        ORDER BY amount DESC LIMIT 25
        """,
        {"t": ticker.upper()},
    )
    return [ContractRecipient(agency=r["agency"], amount_usd=float(r["amount"] or 0), period=r.get("period"))
            for r in rows or []]


# ── Follow the money: the summary the /money page reads ─────────────────────


class OwnershipOverview(BaseModel):
    concentration: Dict[str, Any]
    largest_holder: List[Dict[str, Any]]
    rivals: List[Dict[str, Any]]
    mutual: List[CrossHolding]
    note: str = "13F long positions as filed; 45-day lag; manager-level aggregates. Understates concentration."


async def _ticker_names(raw, tickers) -> Dict[str, str]:
    """Readable company names for tickers, from the SEC titles the entity
    resolver already maps to them. A ticker with no title keeps its ticker."""
    want = {t.upper() for t in tickers}
    titles: Dict[str, List[str]] = {}
    async for k, v in raw.hscan_iter(ALIAS_KEY, count=5000):
        ticker = v.decode() if isinstance(v, bytes) else str(v)
        if ticker in want:
            titles.setdefault(ticker, []).append(k.decode() if isinstance(k, bytes) else str(k))
    return {t: company_name(t, titles.get(t, [])) for t in want}


async def _all_pcts(raw) -> Dict[str, Dict[str, float]]:
    prefix = HOLDERS_KEY.format(ticker="")
    out: Dict[str, Dict[str, float]] = {}
    async for key in raw.scan_iter(prefix + "*", count=500):
        k = key.decode() if isinstance(key, bytes) else key
        ticker = k[len(prefix):]
        out[ticker] = holder_pcts(await read_holders(raw, ticker))
    return out


@router.get("/overview", response_model=OwnershipOverview)
async def overview(user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """Across every company with holder data: how much the three index giants
    hold, who is most often the largest holder, rivals sharing owners, and
    institutions holding each other.

    Cached an hour: it reads all 549 companies' holders and scans the name map
    (1.2 s), and 13F data changes quarterly."""
    raw = await _raw()
    return await cached_json(raw, cache_key("ownership_overview"), lambda: _overview(raw),
                             ttl=3600, max_stale=7 * 86400)


async def _overview(raw) -> Dict[str, Any]:
    pcts = await _all_pcts(raw)
    names = {f.filer_id: f.name for f in OWNERSHIP_FILERS.values()}
    listed = {f.filer_id: f.listed_as for f in OWNERSHIP_FILERS.values() if f.listed_as}
    cross_rows = [
        {"holder": names[a], "holder_ticker": listed.get(a), "held": names[b],
         "held_ticker": listed.get(b), "pct": pct, "reverse_pct": back}
        for a, b, pct, back in cross_holdings(await _stakes(raw), listed)
    ]
    rivals = rival_overlaps(pcts)
    labels = await _ticker_names(raw, [t for r in rivals for t in (r["a"], r["b"])])
    for r in rivals:
        r["a_name"], r["b_name"] = labels[str(r["a"])], labels[str(r["b"])]
    return OwnershipOverview(
        concentration=concentration(pcts),
        largest_holder=largest_holder_counts(pcts, names),
        rivals=rivals,
        mutual=[CrossHolding(**r) for r in mutual_pairs(cross_rows)],
    ).model_dump()


class SupplyGroup(BaseModel):
    buyer: str
    buyer_name: str
    suppliers: List[Dict[str, Any]]


@router.get("/supply-chain", response_model=List[SupplyGroup])
async def supply_chain(db=Depends(get_db), user: Dict[str, Any] = Depends(require_role(Role.VIEWER))):
    """Which companies depend on one big customer, from the customers each
    names in its 10-K (Regulation S-K requires any above 10% of revenue).
    Only links whose named customer matches the buyer it resolved to."""
    rows = await db.query(
        "SELECT supplier, customer_name, customer_ticker, pct, filed FROM customer_links "
        "WHERE customer_ticker IS NOT NULL AND pct IS NOT NULL"
    )
    groups = dependence_by_buyer(trusted_links(rows or []))
    raw = await _raw()
    labels = await _ticker_names(raw, [str(x["ticker"]) for g in groups for x in g["suppliers"]])
    for g in groups:
        for x in g["suppliers"]:
            x["name"] = labels[str(x["ticker"])]
    return [SupplyGroup(**g) for g in groups]
