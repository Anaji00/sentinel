"""
services/api_gateway/routes/flags.py

API endpoints for real-time feature flags, gradual rollouts, and emergency signal kill switches.
"""

import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from services.api_gateway.dependencies import get_redis_optional
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


@router.get("")
async def get_feature_flags(redis=Depends(get_redis_optional)):
    """Returns the real-time operational status of all signal flags and master kill switch."""
    manager = FeatureFlagManager(redis)
    return await manager.get_all_flags()


@router.post("/toggle")
async def toggle_feature_flag(
    req: ToggleFlagRequest,
    redis=Depends(get_redis_optional),
):
    """Updates a signal feature flag, rollout percentage, or ticker whitelist."""
    manager = FeatureFlagManager(redis)
    res = await manager.set_flag(
        flag_name=req.flag_name,
        enabled=req.enabled,
        rollout_pct=req.rollout_pct,
        enabled_tickers=req.enabled_tickers,
        reason=req.reason or "Operator update",
    )
    return {"status": "success", "flag": res}


@router.post("/kill-switch")
async def trip_kill_switch(
    req: KillSwitchRequest,
    redis=Depends(get_redis_optional),
):
    """
    Emergency kill switch: instantly shuts down a specific signal type or activates
    the platform-wide MASTER kill switch.
    """
    manager = FeatureFlagManager(redis)
    if req.flag_name.upper() == "MASTER":
        res = await manager.trip_master_kill_switch(reason=req.reason)
        return {"status": "master_kill_tripped", "details": res}
    else:
        res = await manager.trip_kill_switch(flag_name=req.flag_name, reason=req.reason)
        return {"status": "kill_switch_tripped", "details": res}


@router.post("/reset")
async def reset_kill_switch(
    req: ResetFlagRequest,
    redis=Depends(get_redis_optional),
):
    """Resets an emergency kill switch back to normal operational status."""
    manager = FeatureFlagManager(redis)
    res = await manager.reset_kill_switch(flag_name=req.flag_name)
    return {"status": "reset_complete", "details": res}
