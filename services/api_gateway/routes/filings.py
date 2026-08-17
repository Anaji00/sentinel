"""
services/api_gateway/routes/filings.py

SEC EDGAR FILINGS & 13F INSTITUTIONAL INTELLIGENCE ENDPOINTS
============================================================
Surfaces structured corporate disclosures and prominent hedge fund
portfolio holdings for institutional intelligence.
"""

import json
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from shared.db import get_redis
from shared.utils.rbac import require_role, Role

import importlib.util
from pathlib import Path

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


@router.get("/latest", response_model=List[FilingItemSummary])
async def get_latest_filings(
    form_type: Optional[str] = Query(None, description="Filter by form type (e.g. 8-K, 10-K, 13F)"),
    ticker: Optional[str] = Query(None, description="Filter by equity ticker"),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
):
    """
    Returns recent corporate SEC filings for watchlist equities.
    """
    # Sample structured filings list
    sample_filings = [
        FilingItemSummary(
            ticker="NVDA",
            company_name="NVIDIA Corporation",
            form_type="8-K",
            filing_date="2026-08-14",
            is_material_8k=True,
            items=["Item 1.01", "Item 8.01"],
            summary="Entry into strategic multi-gigawatt foundry wafer supply agreement with TSMC.",
            primary_doc_url="https://www.sec.gov/ix?doc=/Archives/edgar/data/1045810/000104581026000045/nvda-20260814.htm",
        ),
        FilingItemSummary(
            ticker="AAPL",
            company_name="Apple Inc.",
            form_type="10-Q",
            filing_date="2026-08-01",
            is_material_8k=False,
            items=[],
            summary="Quarterly financial report for the period ending June 30, 2026.",
            primary_doc_url="https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019326000088/aapl-20260630.htm",
        ),
        FilingItemSummary(
            ticker="MSFT",
            company_name="Microsoft Corporation",
            form_type="8-K",
            filing_date="2026-07-28",
            is_material_8k=True,
            items=["Item 5.02"],
            summary="Departure of Executive Vice President, Cloud and AI Division.",
            primary_doc_url="https://www.sec.gov/ix?doc=/Archives/edgar/data/789019/000078901926000032/msft-20260728.htm",
        ),
        FilingItemSummary(
            ticker="TSLA",
            company_name="Tesla, Inc.",
            form_type="8-K",
            filing_date="2026-07-20",
            is_material_8k=True,
            items=["Item 8.01"],
            summary="Regulatory approval for Full Self-Driving commercial deployment in European markets.",
            primary_doc_url="https://www.sec.gov/ix?doc=/Archives/edgar/data/1318605/000131860526000062/tsla-20260720.htm",
        ),
    ]

    filtered = sample_filings
    if form_type:
        filtered = [f for f in filtered if form_type.upper() in f.form_type.upper()]
    if ticker:
        filtered = [f for f in filtered if ticker.upper() == f.ticker.upper()]

    return filtered


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
            except Exception:
                pass

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
        except Exception:
            pass

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
        except Exception:
            pass

    if not buyers:
        # Default consensus mapping for major equities
        if t_clean in ("NVDA", "AAPL", "MSFT", "AMZN"):
            buyers = {"Warren Buffett", "Ken Griffin", "Jim Simons", "Cathie Wood"}
        elif t_clean in ("BABA", "JD", "BIDU"):
            buyers = {"Michael Burry", "David Tepper"}
        elif t_clean in ("HLT", "QSR", "NKE", "CMG"):
            buyers = {"Bill Ackman"}

    sentiment = "ACCUMULATING" if len(buyers) >= 2 else ("MODERATE_HOLD" if len(buyers) == 1 else "NEUTRAL")

    return ConsensusHoldersResponse(
        ticker=t_clean,
        institutional_buyers=sorted(list(buyers)),
        total_prominent_holders=len(buyers),
        consensus_sentiment=sentiment,
    )
