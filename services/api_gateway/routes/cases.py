"""
services/api_gateway/routes/cases.py

Investigation Case Management API.
Enables analysts to correlate cross-domain evidence, track incident resolution,
and record multi-agent/human collaborative findings.
"""

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from shared.models.cases import InvestigationCase, CaseStatus, CasePriority, CaseNote
from shared.utils.rbac import require_role, Role
from shared.utils.audit_ledger import AuditLedger
from services.api_gateway.dependencies import get_redis_optional, get_db_optional

logger = logging.getLogger("api-gateway.cases")

router = APIRouter(prefix="/api/v1/cases", tags=["Investigation Cases"])

REDIS_CASES_PREFIX = "sentinel:cases:"
REDIS_CASES_INDEX = "sentinel:cases:index"


class CreateCaseRequest(BaseModel):
    title: str = Field(..., min_length=3, max_length=200)
    description: str
    priority: CasePriority = CasePriority.MEDIUM
    linked_event_ids: List[str] = Field(default_factory=list)
    linked_correlation_ids: List[str] = Field(default_factory=list)
    linked_entities: List[str] = Field(default_factory=list)
    linked_filings: List[str] = Field(default_factory=list)
    linked_regulatory_docs: List[str] = Field(default_factory=list)
    linked_social_signals: List[str] = Field(default_factory=list)
    linked_freight_metrics: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class UpdateCaseRequest(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[CaseStatus] = None
    priority: Optional[CasePriority] = None
    lead_analyst: Optional[str] = None
    linked_filings: Optional[List[str]] = None
    linked_regulatory_docs: Optional[List[str]] = None
    linked_social_signals: Optional[List[str]] = None
    linked_freight_metrics: Optional[List[str]] = None
    tags: Optional[List[str]] = None


class AddCaseNoteRequest(BaseModel):
    content: str = Field(..., min_length=1)


@router.get("", dependencies=[Depends(require_role(Role.VIEWER))])
async def list_cases(
    status: Optional[CaseStatus] = None,
    priority: Optional[CasePriority] = None,
    limit: int = Query(50, ge=1, le=100),
    redis = Depends(get_redis_optional),
):
    """List investigation cases with optional status and priority filtering."""
    if not redis:
        return {"count": 0, "cases": []}

    raw_redis = getattr(redis, "raw", redis)
    case_ids = await raw_redis.zrevrange(REDIS_CASES_INDEX, 0, limit * 2)

    cases = []
    for cid in case_ids:
        cid_str = cid.decode() if isinstance(cid, bytes) else str(cid)
        raw_val = await raw_redis.get(f"{REDIS_CASES_PREFIX}{cid_str}")
        if raw_val:
            data = json.loads(raw_val.decode() if isinstance(raw_val, bytes) else raw_val)
            if status and data.get("status") != status.value:
                continue
            if priority and data.get("priority") != priority.value:
                continue
            cases.append(data)
            if len(cases) >= limit:
                break

    return {
        "count": len(cases),
        "cases": cases,
    }


@router.post("", dependencies=[Depends(require_role(Role.ANALYST))])
async def create_case(
    req: CreateCaseRequest,
    request: Request,
    redis = Depends(get_redis_optional),
    db = Depends(get_db_optional),
):
    """Create a new investigation case."""
    actor = getattr(request.state, "identity", "analyst")
    new_case = InvestigationCase(
        title=req.title,
        description=req.description,
        priority=req.priority,
        lead_analyst=actor,
        linked_event_ids=req.linked_event_ids,
        linked_correlation_ids=req.linked_correlation_ids,
        linked_entities=req.linked_entities,
        linked_filings=req.linked_filings,
        linked_regulatory_docs=req.linked_regulatory_docs,
        linked_social_signals=req.linked_social_signals,
        linked_freight_metrics=req.linked_freight_metrics,
        tags=req.tags,
    )

    case_dict = new_case.model_dump(mode="json")
    if redis:
        raw_redis = getattr(redis, "raw", redis)
        pipe = raw_redis.pipeline()
        pipe.set(f"{REDIS_CASES_PREFIX}{new_case.case_id}", json.dumps(case_dict))
        pipe.zadd(REDIS_CASES_INDEX, {new_case.case_id: datetime.now(timezone.utc).timestamp()})
        await pipe.execute()

    # Record in audit ledger
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="CREATE_INVESTIGATION_CASE",
        resource_type="CASE",
        resource_id=new_case.case_id,
        details={"title": req.title, "priority": req.priority.value},
    )

    return case_dict


@router.get("/{case_id}", dependencies=[Depends(require_role(Role.VIEWER))])
async def get_case(case_id: str, redis = Depends(get_redis_optional)):
    """Retrieve detailed investigation case including evidence links and notes."""
    if not redis:
        raise HTTPException(status_code=404, detail=f"Investigation Case '{case_id}' not found.")

    raw_redis = getattr(redis, "raw", redis)
    raw_val = await raw_redis.get(f"{REDIS_CASES_PREFIX}{case_id.upper()}")
    if not raw_val:
        raise HTTPException(status_code=404, detail=f"Investigation Case '{case_id}' not found.")

    return json.loads(raw_val.decode() if isinstance(raw_val, bytes) else raw_val)


@router.patch("/{case_id}", dependencies=[Depends(require_role(Role.ANALYST))])
async def update_case(
    case_id: str,
    req: UpdateCaseRequest,
    request: Request,
    redis = Depends(get_redis_optional),
    db = Depends(get_db_optional),
):
    """Update investigation case status, priority, or metadata."""
    if not redis:
        raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found.")

    raw_redis = getattr(redis, "raw", redis)
    case_key = f"{REDIS_CASES_PREFIX}{case_id.upper()}"
    raw_val = await raw_redis.get(case_key)
    if not raw_val:
        raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found.")

    data = json.loads(raw_val.decode() if isinstance(raw_val, bytes) else raw_val)

    if req.title is not None:
        data["title"] = req.title
    if req.description is not None:
        data["description"] = req.description
    if req.status is not None:
        data["status"] = req.status.value
    if req.priority is not None:
        data["priority"] = req.priority.value
    if req.lead_analyst is not None:
        data["lead_analyst"] = req.lead_analyst
    if req.linked_filings is not None:
        data["linked_filings"] = req.linked_filings
    if req.linked_regulatory_docs is not None:
        data["linked_regulatory_docs"] = req.linked_regulatory_docs
    if req.linked_social_signals is not None:
        data["linked_social_signals"] = req.linked_social_signals
    if req.linked_freight_metrics is not None:
        data["linked_freight_metrics"] = req.linked_freight_metrics
    if req.tags is not None:
        data["tags"] = req.tags

    data["updated_at"] = datetime.now(timezone.utc).isoformat()

    await raw_redis.set(case_key, json.dumps(data))

    actor = getattr(request.state, "identity", "analyst")
    ledger = AuditLedger(redis_client=redis, db_client=db)
    await ledger.record_entry(
        actor=actor,
        action="UPDATE_INVESTIGATION_CASE",
        resource_type="CASE",
        resource_id=case_id.upper(),
        details={"status": data.get("status"), "priority": data.get("priority")},
    )

    return data


@router.post("/{case_id}/notes", dependencies=[Depends(require_role(Role.ANALYST))])
async def add_case_note(
    case_id: str,
    req: AddCaseNoteRequest,
    request: Request,
    redis = Depends(get_redis_optional),
):
    """Append an analyst collaboration note to the investigation case."""
    if not redis:
        raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found.")

    raw_redis = getattr(redis, "raw", redis)
    case_key = f"{REDIS_CASES_PREFIX}{case_id.upper()}"
    raw_val = await raw_redis.get(case_key)
    if not raw_val:
        raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found.")

    data = json.loads(raw_val.decode() if isinstance(raw_val, bytes) else raw_val)
    actor = getattr(request.state, "identity", "analyst")

    new_note = CaseNote(author=actor, content=req.content)
    if "notes" not in data or not isinstance(data["notes"], list):
        data["notes"] = []

    data["notes"].append(new_note.model_dump(mode="json"))
    data["updated_at"] = datetime.now(timezone.utc).isoformat()

    await raw_redis.set(case_key, json.dumps(data))

    return {
        "status": "NOTE_ADDED",
        "case_id": case_id.upper(),
        "note": new_note.model_dump(mode="json"),
        "total_notes": len(data["notes"]),
    }
