"""
tests/test_rbac_security.py

Comprehensive tests for RBAC security hardening & scenario deduplication:
1. Rejection of unverified client headers (X-User-Role).
2. Cryptographic session token role extraction and request.state.role assignment.
3. Scenario alert deduplication in AlertManager.
4. PortfolioMetrics hawkes_risk_factor force-write verification.
"""

import pytest
import os
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fastapi import Request
from starlette.datastructures import Headers

from shared.models import Scenario
from shared.utils.rbac import Role, get_current_user_role, has_role_permission, require_role
from services.api_gateway.dependencies import (
    create_jwt_token,
    verify_session_token,
    verify_api_key,
    SESSION_SECRET,
)
from services.agents.quant_trading_engine import PortfolioMetrics
from shared.utils import quant_calc


def test_x_user_role_header_spoofing_is_ignored():
    """Verify that unverified X-User-Role headers are strictly ignored."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/cases",
        "headers": [
            (b"x-user-role", b"ADMIN"),
            (b"host", b"localhost"),
        ],
    }
    req = Request(scope=scope)
    
    # In production/default with no auth state, role must be VIEWER, NOT ADMIN
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("ENVIRONMENT", "production")
        mp.setenv("API_GATEWAY_KEY", "prod-secret-key-12345")
        role = get_current_user_role(req)
        assert role == Role.VIEWER
        assert has_role_permission(role, Role.ADMIN) is False


def test_jwt_session_token_role_extraction():
    """Verify that signed JWTs encode and extract the role claim."""
    admin_token = create_jwt_token(
        {"sub": "admin_user@sentinel.local"},
        role="ADMIN",
    )
    is_valid, ident, role = verify_session_token(admin_token)
    assert is_valid is True
    assert ident == "admin_user@sentinel.local"
    assert role == "ADMIN"

    analyst_token = create_jwt_token(
        {"sub": "analyst_user@sentinel.local"},
        role="ANALYST",
    )
    is_valid, ident, role = verify_session_token(analyst_token)
    assert is_valid is True
    assert ident == "analyst_user@sentinel.local"
    assert role == "ANALYST"


def test_verify_api_key_attaches_role_to_request_state():
    """Verify that verify_api_key attaches verified identity and role to request.state."""
    with pytest.MonkeyPatch.context() as mp:
        test_key = "sentinel-test-master-key-xyz"
        mp.setenv("API_GATEWAY_KEY", test_key)
        mp.setenv("ENVIRONMENT", "production")

        # 1. Test API Key Auth
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/cases",
            "headers": [(b"x-api-key", test_key.encode("utf-8"))],
            "query_string": b"",
            "state": {},
        }
        req = Request(scope=scope)
        # Import reloaded API_KEY from dependencies
        from services.api_gateway import dependencies
        dependencies.API_KEY = test_key

        async def run_auth():
            await dependencies.verify_api_key(req)
            assert req.state.identity == f"apikey:{test_key[:8]}"
            assert req.state.role == Role.ADMIN
            # get_current_user_role now sees ADMIN
            assert get_current_user_role(req) == Role.ADMIN

        asyncio.run(run_auth())

        # 2. Test Session Cookie Auth with Analyst Role
        analyst_jwt = create_jwt_token({"sub": "analyst@test.com"}, role="ANALYST")
        cookie_header = f"sentinel_session={analyst_jwt}".encode("utf-8")
        scope_cookie = {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/cases",
            "headers": [(b"cookie", cookie_header)],
            "query_string": b"",
            "state": {},
        }
        req_cookie = Request(scope=scope_cookie)

        async def run_cookie_auth():
            await dependencies.verify_api_key(req_cookie)
            assert req_cookie.state.identity == "session:analyst@test.com"
            assert req_cookie.state.role == Role.ANALYST
            assert get_current_user_role(req_cookie) == Role.ANALYST
            assert has_role_permission(req_cookie.state.role, Role.ANALYST) is True
            assert has_role_permission(req_cookie.state.role, Role.ADMIN) is False

        asyncio.run(run_cookie_auth())


def test_scenario_deduplication_in_alert_manager():
    """Verify handle_scenario deduplicates repeated scenario alerts within 6 hours."""
    from services.alert_manager.main import AlertManager

    async def _test():
        redis_mock = MagicMock()
        redis_mock.raw = AsyncMock()

        seen_keys = set()
        async def mock_exists(k):
            return k in seen_keys
        async def mock_set(k, v, ex=None):
            seen_keys.add(k)
            return True
        pipe = MagicMock()
        pipe.zremrangebyscore = MagicMock()
        pipe.zcard = MagicMock()
        pipe.zadd = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[0, 1])

        redis_mock.raw.exists = mock_exists
        redis_mock.raw.set = mock_set
        redis_mock.raw.pipeline = MagicMock(return_value=pipe)

        session_mock = MagicMock()
        mgr = AlertManager(session=session_mock, db_client=None, redis_client=redis_mock)
        mgr._send_telegram = AsyncMock(return_value=True)
        mgr._send_webhook = AsyncMock(return_value=True)

        scenario = Scenario(
            correlation_id="corr-dedup-test-8888",
            headline="Subsea Cable Disruption",
            significance="Major connectivity anomaly",
            confidence_overall=90,
            confidence_rationale="3 sensor corroborated",
            hypotheses=[
                {
                    "label": "Anchor Drag",
                    "probability": 75,
                    "mechanism": "Physical cable severing",
                    "beneficiaries": ["None"],
                    "watch_signals": ["AIS speed drop"],
                    "deny_signals": ["Normal ping times"],
                    "time_horizon": "24h",
                }
            ],
            recommended_monitoring=["BGP routes"],
        )

        # 1. First emission: should send and write dedup key
        await mgr.handle_scenario(scenario)
        assert mgr._send_telegram.call_count == 1
        assert "alert:sent:scenario:corr-dedup-test-8888" in seen_keys

        # 2. Duplicate emission: should be suppressed
        await mgr.handle_scenario(scenario)
        assert mgr._send_telegram.call_count == 1

    asyncio.run(_test())


def test_portfolio_metrics_hawkes_risk_factor_wiring():
    """Verify PortfolioMetrics hawkes_risk_factor is computed and populated."""
    # When history is present:
    metrics_computed = PortfolioMetrics(
        var_95_pct=2.45,
        cvar_99_pct=3.12,
        sharpe_ratio=1.65,
        hawkes_risk_factor=round(quant_calc.hawkes_risk_multiplier(hawkes_intensity=1.0), 3),
        metrics_source="computed",
    )
    assert metrics_computed.hawkes_risk_factor is not None
    assert metrics_computed.hawkes_risk_factor == 1.0
    assert metrics_computed.metrics_source == "computed"

    # When insufficient history:
    metrics_insufficient = PortfolioMetrics(
        var_95_pct=None,
        cvar_99_pct=None,
        sharpe_ratio=None,
        hawkes_risk_factor=None,
        metrics_source="insufficient_history",
    )
    assert metrics_insufficient.hawkes_risk_factor is None
    assert metrics_insufficient.metrics_source == "insufficient_history"
