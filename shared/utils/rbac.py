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


def permissions_for(role: Role) -> Set[str]:
    """Every permission a role holds, including those inherited from below it.

    The hierarchy is a total order, so a role holds its own grants plus every
    grant of every weaker role. Computing it rather than restating it keeps the
    table above the single statement of the model.
    """
    weight = _ROLE_HIERARCHY.get(role, 0)
    granted: Set[str] = set()
    for candidate, perms in _ROLE_PERMISSIONS.items():
        if _ROLE_HIERARCHY.get(candidate, 0) <= weight:
            granted |= perms
    return granted


def has_permission(user_role: Role, permission: str) -> bool:
    """Whether *user_role* may perform *permission*.

    `_ROLE_PERMISSIONS` above was, until now, referenced nowhere but its own
    definition -- there was no function that read it and no caller that could
    have. Authorisation was the three-level hierarchy comparison alone, so the
    distinction the table draws (an ANALYST may write cases and watchlists but
    not flags or brokers) existed only as documentation that reads like
    enforcement, and drift between the two was invisible.

    Unknown permissions are refused. A typo in a route decorator should fail
    closed rather than grant everything.
    """
    if not permission:
        return False
    return permission in permissions_for(user_role)


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
    Extracts the user's role from cryptographically verified request state.
    1. If request.state has verified 'role' from signed session/token -> return role.
    2. If user authenticated via master API_KEY (identity starts with 'apikey:') -> Role.ADMIN.
    3. Fallback in development when no API key configured -> Role.ADMIN.
    4. Fallback in production -> Role.VIEWER.
    
    Security note: X-User-Role header is strictly ignored to prevent client-side role spoofing.
    """
    # 1. Check cryptographically verified role on request state
    if hasattr(request, "state"):
        verified_role = getattr(request.state, "role", None)
        if verified_role:
            if isinstance(verified_role, Role):
                return verified_role
            return parse_role(verified_role)

        # 2. Check auth identity if stored on request state
        user_identity = getattr(request.state, "identity", None)
        if user_identity and isinstance(user_identity, str) and user_identity.startswith("apikey:"):
            return Role.ADMIN

    # 3. Check for dev environment default
    import os
    env = os.getenv("ENVIRONMENT") or os.getenv("SENTINEL_ENV") or "production"
    api_key_set = bool(os.getenv("API_GATEWAY_KEY") or os.getenv("API_KEY") or os.getenv("SENTINEL_API_KEY"))
    if env.lower() in ("development", "dev", "local", "test") and not api_key_set:
        return Role.ADMIN

    return Role.VIEWER


def require_permission(permission: str) -> Callable:
    """FastAPI dependency enforcing a named permission from `_ROLE_PERMISSIONS`.

    Preferred over `require_role` for anything that mutates state: it says what
    the route does rather than which tier happens to be allowed to do it, so the
    table and the routes cannot drift apart the way they had.
    """
    async def permission_checker(request: Request):
        user_role = get_current_user_role(request)
        if not has_permission(user_role, permission):
            logger.warning(
                "RBAC Access Denied: role '%s' lacks permission '%s' (%s)",
                user_role.value, permission, request.url.path,
            )
            raise HTTPException(
                status_code=403,
                detail=f"Forbidden: this action requires '{permission}'.",
            )
        return user_role

    return permission_checker


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
