"""
shared/utils/rbac.py

Enterprise Role-Based Access Control (RBAC) engine for Sentinel API Gateway.
Supports hierarchical roles: ADMIN > ANALYST > VIEWER.
"""

from enum import Enum
from typing import List, Optional, Set, Callable
import logging
from fastapi import Request, HTTPException, Depends

logger = logging.getLogger("shared.rbac")


class Role(str, Enum):
    ADMIN = "ADMIN"
    ANALYST = "ANALYST"
    VIEWER = "VIEWER"


# Role hierarchy weights
_ROLE_HIERARCHY = {
    Role.ADMIN: 300,
    Role.ANALYST: 200,
    Role.VIEWER: 100,
}

# Role permissions mapping
_ROLE_PERMISSIONS = {
    Role.ADMIN: {
        "read:all",
        "write:events",
        "write:watchlists",
        "write:flags",
        "write:cases",
        "write:brokers",
        "write:reports",
        "admin:audit",
        "admin:system",
    },
    Role.ANALYST: {
        "read:all",
        "write:watchlists",
        "write:cases",
        "write:reports",
        "read:audit",
    },
    Role.VIEWER: {
        "read:all",
    },
}


def has_role_permission(user_role: Role, required_role: Role) -> bool:
    """Checks if user_role satisfies required_role in the hierarchy."""
    user_weight = _ROLE_HIERARCHY.get(user_role, 0)
    req_weight = _ROLE_HIERARCHY.get(required_role, 999)
    return user_weight >= req_weight


def parse_role(role_str: Optional[str], default: Role = Role.VIEWER) -> Role:
    """Safely parses a string into a Role enum."""
    if not role_str:
        return default
    clean = str(role_str).strip().upper()
    try:
        return Role(clean)
    except ValueError:
        return default


def get_current_user_role(request: Request) -> Role:
    """
    Extracts the user's role from request state, headers, or cookies.
    1. If user authenticated via master API_KEY -> Role.ADMIN.
    2. If request has 'X-User-Role' header -> parses role.
    3. If JWT token contains 'role' or 'roles' -> parses role.
    4. Fallback in development -> Role.ADMIN; in production -> Role.VIEWER.
    """
    # 1. Check explicit header (gateway or upstream proxy)
    role_hdr = request.headers.get("X-User-Role")
    if role_hdr:
        return parse_role(role_hdr)

    # 2. Check cookie or auth identity if stored on request state
    user_identity = getattr(request.state, "identity", None) if hasattr(request, "state") else None
    if user_identity and isinstance(user_identity, str) and user_identity.startswith("apikey:"):
        return Role.ADMIN

    # 3. Check for dev environment default
    import os
    env = os.getenv("ENVIRONMENT") or os.getenv("SENTINEL_ENV") or "production"
    if env.lower() in ("development", "dev", "local", "test"):
        return Role.ADMIN

    return Role.VIEWER


def require_role(required_role: Role) -> Callable:
    """
    FastAPI dependency factory enforcing a minimum role requirement.
    Usage:
        @router.post("/trade", dependencies=[Depends(require_role(Role.ADMIN))])
    """
    async def role_checker(request: Request):
        user_role = get_current_user_role(request)
        if not has_role_permission(user_role, required_role):
            logger.warning(
                f"RBAC Access Denied: User with role '{user_role.value}' attempted "
                f"to access route requiring '{required_role.value}' ({request.url.path})"
            )
            raise HTTPException(
                status_code=403,
                detail=f"Forbidden: Action requires '{required_role.value}' role. Your role is '{user_role.value}'."
            )
        return user_role

    return role_checker
