"""
services/api_gateway/routes/audit.py

Immutable Audit Ledger API.
Exposes cryptographic hash chain verification and paginated audit history.
"""

import logging
from fastapi import APIRouter, Depends, Query, Request
from shared.utils.rbac import require_role, Role
from shared.utils.audit_ledger import AuditLedger
from services.api_gateway.dependencies import get_redis_client, get_db_optional

logger = logging.getLogger("api-gateway.audit")

router = APIRouter(prefix="/api/v1/audit", tags=["Security & Governance Audit"])


@router.get("/trail", dependencies=[Depends(require_role(Role.ANALYST))])
async def get_audit_trail(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """Retrieve immutable, hash-chained audit ledger entries."""
    ledger = AuditLedger(redis_client=redis, db_client=db)
    entries = await ledger.get_entries(limit=limit, offset=offset)
    return {
        "count": len(entries),
        "offset": offset,
        "limit": limit,
        "entries": entries,
    }


@router.post("/verify", dependencies=[Depends(require_role(Role.ADMIN))])
async def verify_audit_ledger_integrity(
    redis = Depends(get_redis_client),
    db = Depends(get_db_optional),
):
    """
    Cryptographically verifies the entire SHA-256 hash chain from Genesis to head.
    Detects any unauthorized tampering, deletions, or retroactive mutations.
    """
    ledger = AuditLedger(redis_client=redis, db_client=db)
    return await ledger.verify_chain()
