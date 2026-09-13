"""
services/api_gateway/routes/reports.py

Executive Reporting & Intelligence Brief API.
Supports on-demand intelligence brief synthesis across observation windows.
"""

import logging
from typing import Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from services.reporting.report_generator import (
    DEFAULT_TEMPLATE_ID,
    REPORT_TEMPLATES,
    ReportGenerator,
)
from shared.utils.rbac import require_role, Role
from services.api_gateway.dependencies import get_db_optional, get_redis_optional

logger = logging.getLogger("api-gateway.reports")

router = APIRouter(prefix="/api/v1/reports", tags=["Executive Intelligence Reports"])


class GenerateReportRequest(BaseModel):
    # Optional, so a caller that names a template gets that template's window
    # rather than the default 24 hours.
    timeframe_hours: Optional[int] = Field(default=None, ge=1, le=168)
    title: Optional[str] = None
    # The choice /reports/templates has always offered and this endpoint had
    # no field to receive. Three ids were advertised with prose describing
    # different content; picking one could not change anything.
    template_id: Optional[str] = None


@router.post("/generate", dependencies=[Depends(require_role(Role.ANALYST))])
async def generate_intelligence_report(
    req: GenerateReportRequest,
    request: Request,
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional),
):
    """Generates an executive intelligence brief over a window.

    `template_id` selects the sections; an unknown one is refused rather than
    silently treated as the default, because a caller asking for an incident
    flash report and receiving a daily brief has no way to tell.
    """
    actor = getattr(request.state, "identity", "Sentinel Analyst")

    template_id = (req.template_id or DEFAULT_TEMPLATE_ID).upper()
    template = REPORT_TEMPLATES.get(template_id)
    if template is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"'{req.template_id}' is not a report template. Known templates: "
                f"{', '.join(sorted(REPORT_TEMPLATES))}."
            ),
        )

    generator = ReportGenerator(db_client=db, redis_client=redis)
    return await generator.generate_brief(
        timeframe_hours=req.timeframe_hours or int(template["default_timeframe_hours"]),
        title=req.title,
        author=actor,
        template_id=template_id,
    )


@router.get("/templates", dependencies=[Depends(require_role(Role.VIEWER))])
async def list_report_templates():
    """The templates /reports/generate will actually honour.

    This used to be a separate hand-written list, and the generator had no
    notion of a template -- so the catalogue described three reports the
    platform could not produce differently, one of them promising parametric
    VaR and CVaR tail risk that this generator has never computed. It is now
    the generator's own catalogue, so the two cannot describe different things.
    """
    return {
        "templates": [
            {
                "id": t["id"],
                "name": t["name"],
                "default_timeframe_hours": t["default_timeframe_hours"],
                "sections": t["sections"],
                "description": t["description"],
            }
            for t in REPORT_TEMPLATES.values()
        ]
    }
