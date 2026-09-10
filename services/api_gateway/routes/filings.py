"""
services/api_gateway/routes/filings.py

SEC EDGAR FILINGS & 13F INSTITUTIONAL INTELLIGENCE ENDPOINTS
============================================================
Surfaces structured corporate disclosures and prominent hedge fund
portfolio holdings for institutional intelligence.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_db_optional
from shared.db import get_redis
from shared.utils.rbac import require_role, Role

logger = logging.getLogger("api-gateway.filings")

import importlib.util
from pathlib import Path
from shared.utils.quiet_failures import swallowed

ROOT = Path(__file__).resolve().parents[3]
tf_path = ROOT / "services" / "collector-filings" / "thirteen_f.py"
spec_tf = importlib.util.spec_from_file_location("thirteen_f", tf_path)
thirteen_f_mod = importlib.util.module_from_spec(spec_tf)
spec_tf.loader.exec_module(thirteen_f_mod)

PROMINENT_FILERS = thirteen_f_mod.PROMINENT_FILERS
generate_curated_seed_13f = thirteen_f_mod.generate_curated_seed_13f
ThirteenFPortfolioReport = thirteen_f_mod.ThirteenFPortfolioReport

router = APIRouter(prefix="/api/v1/filings", tags=["Corporate Filings & 13F Intelligence"])


class FilingItemSummary(BaseModel):
    ticker: str
    company_name: str
    form_type: str
    filing_date: str
    is_material_8k: bool
    items: List[str] = Field(default_factory=list)
    summary: str
    primary_doc_url: str


class FilerSummaryItem(BaseModel):
    # Carried through from ThirteenFPortfolioReport.
    #
    # The report has marked seeded portfolios `is_synthetic` since the 13F
    # audit, and this summary dropped the field -- so the list view presented
    # curated seed data and a real filing identically, which is the exact
    # distinction the flag was added to preserve.
    is_synthetic: bool = False
    filer_id: str
    filer_name: str
    manager_name: str
    cik: str
    style: str
    total_value_usd: float
    holdings_count: int
    top_10_concentration_pct: float
    report_period: str


class ConsensusHoldersResponse(BaseModel):
    ticker: str
    institutional_buyers: List[str] = Field(default_factory=list)
    total_prominent_holders: int = 0
    consensus_sentiment: str = "NEUTRAL"
    # Whether the names above came from filings this platform actually
    # ingested. False means no 13F basis exists for this ticker -- which is a
    # different statement from "no institution holds it", and the two used to
    # be indistinguishable in this response.
    derived_from_filings: bool = False


@router.get("/latest", response_model=List[FilingItemSummary])
async def get_latest_filings(
    form_type: Optional[str] = Query(None, description="Filter by form type (e.g. 8-K, 10-K, 13F)"),
    ticker: Optional[str] = Query(None, description="Filter by equity ticker"),
    limit: int = Query(25, ge=1, le=200),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
    db=Depends(get_db_optional),
):
    """Recent SEC filings the platform has actually ingested.

    This returned four hand-written filings with fabricated sec.gov URLs --
    an NVIDIA foundry agreement, a Microsoft executive departure, a Tesla FSD
    approval in Europe -- invented corporate events attributed to real
    companies and served from an intelligence platform's filings endpoint.
    They were the same four rows for every caller, and the collector had 564
    real filings in the database at the time.
    """
    if db is None:
        raise HTTPException(status_code=503, detail="Filing history is temporarily unavailable.")

    clauses = ["type = 'filing'", "filing_data IS NOT NULL"]
    args: List[Any] = []
    if form_type:
        args.append(form_type.upper())
        clauses.append(f"upper(filing_data->>'form_type') LIKE '%%' || ${len(args)} || '%%'")
    if ticker:
        args.append(ticker.upper())
        clauses.append(f"upper(coalesce(filing_data->>'ticker', primary_entity_id)) = ${len(args)}")
    args.append(limit)

    query = (
        "SELECT primary_entity_id, primary_entity_name, url, filing_data "
        "FROM events WHERE " + " AND ".join(clauses) +
        f" ORDER BY occurred_at DESC LIMIT ${len(args)}"
    )
    try:
        rows = await db.query(query, *args)
    except Exception as e:
        logger.warning("Filing lookup failed: %s", e)
        raise HTTPException(status_code=503, detail="Filing history is temporarily unavailable.")

    out: List[FilingItemSummary] = []
    for r in rows:
        fd = r.get("filing_data") or {}
        if isinstance(fd, str):
            try:
                fd = json.loads(fd)
            except ValueError:
                continue
        doc_url = fd.get("primary_doc_url") or r.get("url")
        if not doc_url:
            # A filing summary without its source document is an assertion the
            # reader cannot check, which is what the fixtures were.
            continue
        out.append(
            FilingItemSummary(
                ticker=str(fd.get("ticker") or r.get("primary_entity_id") or ""),
                company_name=str(fd.get("company_name") or r.get("primary_entity_name") or ""),
                form_type=str(fd.get("form_type") or ""),
                filing_date=str(fd.get("filing_date") or ""),
                is_material_8k=bool(fd.get("is_material_8k", False)),
                items=list(fd.get("items") or []),
                summary=str(fd.get("description") or ""),
                primary_doc_url=str(doc_url),
            )
        )
    return out


@router.get("/13f/prominent", response_model=List[FilerSummaryItem])
async def get_prominent_13f_filers(
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Lists all tracked prominent hedge fund managers with latest portfolio totals.
    """
    redis_client = await get_redis()
    raw_redis = getattr(redis_client, "raw", redis_client) if redis_client else None
    results = []

    for cik, meta in PROMINENT_FILERS.items():
        report_data = None
        if raw_redis:
            try:
                cached = await raw_redis.get(f"sentinel:13f:{cik}:latest")
                if cached:
                    report_data = json.loads(cached.decode("utf-8") if isinstance(cached, bytes) else str(cached))
            except Exception as _exc:
                swallowed("api_gateway.routes.filings.get_prominent_13f_filers", _exc, logger)

        if not report_data:
            report_data = generate_curated_seed_13f(cik).model_dump()

        results.append(
            FilerSummaryItem(
                filer_id=meta["id"],
                filer_name=meta["name"],
                manager_name=meta["manager"],
                cik=cik,
                style=meta["style"],
                total_value_usd=report_data.get("total_portfolio_value_usd", 0.0),
                holdings_count=report_data.get("total_positions_count", 0),
                top_10_concentration_pct=report_data.get("top_10_concentration_pct", 0.0),
                report_period=report_data.get("report_period", "2026-Q2"),
                is_synthetic=bool(report_data.get("is_synthetic", False)),
            )
        )

    return results


@router.get("/13f/{filer_id}", response_model=ThirteenFPortfolioReport)
async def get_13f_portfolio_details(
    filer_id: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Returns complete 13F portfolio holdings, top concentration, and QoQ position differential.
    """
    target_cik = None
    for cik, meta in PROMINENT_FILERS.items():
        if meta["id"] == filer_id.lower() or cik == filer_id:
            target_cik = cik
            break

    if not target_cik:
        raise HTTPException(status_code=404, detail=f"Prominent institutional filer '{filer_id}' not found.")

    redis_client = await get_redis()
    raw_redis = getattr(redis_client, "raw", redis_client) if redis_client else None

    if raw_redis:
        try:
            cached = await raw_redis.get(f"sentinel:13f:{target_cik}:latest")
            if cached:
                data = json.loads(cached.decode("utf-8") if isinstance(cached, bytes) else str(cached))
                return ThirteenFPortfolioReport(**data)
        except Exception as _exc:
            swallowed("api_gateway.routes.filings.get_13f_portfolio_details", _exc, logger)

    return generate_curated_seed_13f(target_cik)


@router.get("/13f/consensus/{ticker}", response_model=ConsensusHoldersResponse)
async def get_13f_consensus_for_ticker(
    ticker: str,
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Returns prominent hedge fund consensus and accumulation status for a given ticker.
    """
    t_clean = ticker.upper().strip()
    redis_client = await get_redis()
    raw_redis = getattr(redis_client, "raw", redis_client) if redis_client else None

    buyers = set()
    if raw_redis:
        try:
            raw_buyers = await raw_redis.smembers(f"sentinel:13f:consensus:{t_clean}:buyers")
            for b in raw_buyers:
                buyers.add(b.decode("utf-8") if isinstance(b, bytes) else str(b))
        except Exception as _exc:
            swallowed("api_gateway.routes.filings.get_13f_consensus_for_ticker", _exc, logger)

    # No filings, no consensus. The names are not ours to supply.
    #
    # This endpoint used to invent one when Redis held nothing: NVDA, AAPL,
    # MSFT and AMZN returned "Warren Buffett, Ken Griffin, Jim Simons, Cathie
    # Wood"; BABA, JD and BIDU returned "Michael Burry, David Tepper"; HLT,
    # QSR, NKE and CMG returned "Bill Ackman". Those sets were hardcoded, were
    # not derived from any filing, carried no flag saying so, and were then run
    # through the sentiment rule below -- so a caller asking about BABA got two
    # institutional buyers and the verdict ACCUMULATING, all of it invented.
    #
    # It was also wrong on its own terms, which is how it was found: Scion
    # Asset Management has been dissolved, so Michael Burry files no 13F to be
    # a buyer in, and Jim Simons died in 2024. A hardcoded roster of people
    # does not merely lack evidence, it decays -- and the decay is invisible,
    # because nothing about a constant looks stale.
    #
    # An empty answer is the correct one. The distinction that matters to a
    # caller is between "no prominent filer holds this" and "we have no filing
    # data for this ticker", and those were indistinguishable before because
    # both returned NEUTRAL. derived_from_filings separates them.
    derived = bool(buyers)
    sentiment = "ACCUMULATING" if len(buyers) >= 2 else ("MODERATE_HOLD" if len(buyers) == 1 else "NEUTRAL")

    return ConsensusHoldersResponse(
        ticker=t_clean,
        institutional_buyers=sorted(list(buyers)),
        total_prominent_holders=len(buyers),
        consensus_sentiment=sentiment,
        derived_from_filings=derived,
    )
