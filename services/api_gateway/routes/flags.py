"""
services/api_gateway/routes/flags.py

API endpoints for real-time feature flags, gradual rollouts, and emergency signal kill switches.
"""

import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from shared.utils.rbac import require_role, Role
from services.api_gateway.dependencies import get_redis_optional, get_db_optional
from services.api_gateway.audit_actions import record_admin_action
from shared.utils.feature_flags import FeatureFlagManager

logger = logging.getLogger("api-gateway.flags")
router = APIRouter(prefix="/api/v1/flags", tags=["Feature Flags & Kill Switches"])


class ToggleFlagRequest(BaseModel):
    flag_name: str
    enabled: bool
    rollout_pct: float = Field(default=100.0, ge=0.0, le=100.0)
    enabled_tickers: Optional[List[str]] = None
    reason: Optional[str] = "Operator configuration update"


class KillSwitchRequest(BaseModel):
    flag_name: str  # "MASTER" or specific flag e.g. "covered_calls"
    reason: str = "Emergency manual override"


class ResetFlagRequest(BaseModel):
    flag_name: str


# ADMIN: kill-switch and rollout state is operational control surface. The
# toggle already required ADMIN; reading which signals are live and at what
# rollout percentage is the reconnaissance that precedes wanting to.
@router.get("", dependencies=[Depends(require_role(Role.ADMIN))])
async def get_feature_flags(redis=Depends(get_redis_optional)):
    """Returns the real-time operational status of all signal flags and master kill switch."""
    manager = FeatureFlagManager(redis)
    return await manager.get_all_flags()


@router.post("/toggle")
async def toggle_feature_flag(
    req: ToggleFlagRequest,
    redis=Depends(get_redis_optional),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.ADMIN)),
):
    """Updates a signal feature flag, rollout percentage, or ticker whitelist (ADMIN only).

    The role guard moved from the decorator to the signature so the ledger entry
    below can name who did it. `dependencies=[...]` enforces the role and
    discards the user.
    """
    manager = FeatureFlagManager(redis)
    res = await manager.set_flag(
        flag_name=req.flag_name,
        enabled=req.enabled,
        rollout_pct=req.rollout_pct,
        enabled_tickers=req.enabled_tickers,
        reason=req.reason or "Operator update",
    )
    await record_admin_action(
        redis=redis, db=db, user=user,
        action="SET_FEATURE_FLAG",
        resource_type="FEATURE_FLAG",
        resource_id=req.flag_name,
        details={
            "enabled": req.enabled,
            "rollout_pct": req.rollout_pct,
            "enabled_tickers": req.enabled_tickers,
            "reason": req.reason,
        },
    )
    return {"status": "success", "flag": res}


@router.post("/kill-switch")
async def trip_kill_switch(
    req: KillSwitchRequest,
    redis=Depends(get_redis_optional),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.ADMIN)),
):
    """
    Emergency kill switch: instantly shuts down a specific signal type or activates
    the platform-wide MASTER kill switch (ADMIN only).

    Recorded in the audit ledger. A tripped kill switch produces no events, so
    the only evidence it happened is an absence -- which is precisely the case
    a tamper-evident record exists for, and precisely the one it was missing.
    """
    manager = FeatureFlagManager(redis)
    is_master = req.flag_name.upper() == "MASTER"
    if is_master:
        res = await manager.trip_master_kill_switch(reason=req.reason)
    else:
        res = await manager.trip_kill_switch(flag_name=req.flag_name, reason=req.reason)
    await record_admin_action(
        redis=redis, db=db, user=user,
        action="TRIP_MASTER_KILL_SWITCH" if is_master else "TRIP_KILL_SWITCH",
        resource_type="FEATURE_FLAG",
        resource_id=req.flag_name.upper(),
        details={"reason": req.reason},
    )
    if is_master:
        return {"status": "master_kill_tripped", "details": res}
    return {"status": "kill_switch_tripped", "details": res}


@router.post("/reset")
async def reset_kill_switch(
    req: ResetFlagRequest,
    redis=Depends(get_redis_optional),
    db=Depends(get_db_optional),
    user: Dict[str, Any] = Depends(require_role(Role.ADMIN)),
):
    """Resets an emergency kill switch back to normal operational status (ADMIN only)."""
    manager = FeatureFlagManager(redis)
    res = await manager.reset_kill_switch(flag_name=req.flag_name)
    await record_admin_action(
        redis=redis, db=db, user=user,
        action="RESET_KILL_SWITCH",
        resource_type="FEATURE_FLAG",
        resource_id=req.flag_name.upper(),
        details={},
    )
    return {"status": "reset_complete", "details": res}
