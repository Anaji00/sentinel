"""
services/api_gateway/audit_actions.py

One call for "this admin changed something", so coverage is a property of the
route rather than of whoever wrote it.

The hash-chained ledger was sound and thinly used. Measured on the live
deployment: thirteen route modules expose a POST, PUT or DELETE and four wrote
to the ledger -- cases, portfolio, scenarios and watchlists. A $4,862 paper
trade was recorded in full while tripping the platform-wide kill switch,
replaying eleven thousand dead letters into the pipeline, and declaring that
two companies are one entity were recorded nowhere.

Those three are also the ones whose effects are hardest to reconstruct
afterwards. A kill switch leaves no events, so the evidence of it is an absence.
A replay is indistinguishable from fresh ingestion once the events land. An
alias silently rewrites what the platform thinks things *are*, at the top of
the resolution order, for every correlation that follows.
"""

import logging
from typing import Any, Dict, Optional

from shared.utils.audit_ledger import AuditLedger

logger = logging.getLogger("api-gateway.audit")


def actor_of(user: Optional[Dict[str, Any]]) -> str:
    """The person a route is acting for, in the ledger's one spelling.

    Matches what cases.py and portfolio.py already write, so entries from
    different routes are attributable to the same identity rather than to three
    conventions.
    """
    if not isinstance(user, dict):
        return "unknown"
    return str(user.get("sub") or user.get("email") or user.get("identity") or "unknown")


async def record_admin_action(
    *,
    redis: Any,
    db: Any,
    user: Optional[Dict[str, Any]],
    action: str,
    resource_type: str,
    resource_id: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Appends one governance action to the tamper-evident ledger.

    Never raises into the caller. The audited action has already happened by the
    time this runs, and failing the request afterwards would tell the operator
    their kill switch did not trip when it did. A ledger write that fails is
    logged at ERROR, because an unrecorded governance change is a real gap and
    not a detail -- `AuditLedger.record_entry` itself refuses to write to Redis
    alone for the same reason.
    """
    try:
        ledger = AuditLedger(redis_client=redis, db_client=db)
        await ledger.record_entry(
            actor=actor_of(user),
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
        )
    except Exception as exc:
        logger.error(
            "Audit ledger write failed for %s on %s/%s by %s: %s",
            action, resource_type, resource_id, actor_of(user), exc,
        )
