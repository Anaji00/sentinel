"""
services/api_gateway/routes/reports.py

Executive Reporting & Intelligence Brief API.
Supports on-demand intelligence brief synthesis across observation windows.
"""

import logging
from typing import Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.reporting.report_generator import ReportGenerator
from shared.utils.rbac import require_role, Role
from services.api_gateway.dependencies import get_db_optional, get_redis_optional

logger = logging.getLogger("api-gateway.reports")

router = APIRouter(prefix="/api/v1/reports", tags=["Executive Intelligence Reports"])


class GenerateReportRequest(BaseModel):
    timeframe_hours: int = Field(default=24, ge=1, le=168)
    title: Optional[str] = None


@router.post("/generate", dependencies=[Depends(require_role(Role.ANALYST))])
async def generate_intelligence_report(
    req: GenerateReportRequest,
    request: Request,
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional),
):
    """Generates an executive intelligence brief summarizing recent cross-domain surveillance data."""
    actor = getattr(request.state, "identity", "Sentinel Analyst")
    generator = ReportGenerator(db_client=db, redis_client=redis)
    return await generator.generate_brief(
        timeframe_hours=req.timeframe_hours,
        title=req.title,
        author=actor,
    )


@router.get("/templates", dependencies=[Depends(require_role(Role.VIEWER))])
async def list_report_templates():
    """List available pre-configured report templates."""
    return {
        "templates": [
            {
                "id": "DAILY_EXECUTIVE_BRIEF",
                "name": "Daily Executive Threat & Market Brief",
                "default_timeframe_hours": 24,
                "description": "Comprehensive cross-domain synthesis of high-confidence anomalies and macro market shifts.",
            },
            {
                "id": "WEEKLY_PORTFOLIO_RISK",
                "name": "Weekly Quantitative Portfolio Risk Audit",
                "default_timeframe_hours": 168,
                "description": "Analysis of parametric VaR, CVaR tail risk, and sector concentration under geopolitical stress.",
            },
            {
                "id": "INCIDENT_FLASH_REPORT",
                "name": "Rapid Incident Flash Report",
                "default_timeframe_hours": 4,
                "description": "Focused summary of recent critical-tier anomalies and active AI-generated scenarios.",
            },
        ]
    }
