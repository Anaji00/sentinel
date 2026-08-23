"""Pins what a signed-up stranger may and may not reach.

Open signup changed the meaning of every role gate in the system. Before it,
VIEWER meant a colleague the operator had provisioned; after it, VIEWER means
anyone on the internet who filled in a form. Endpoints written under the old
assumption became public without a line of their code changing.

The distinction this file enforces: product capability is free to everyone,
deployment configuration is not. Being generous with features is a pricing
decision; being generous with operator state is a disclosure.
"""
import pathlib
import sys

import pytest
from fastapi.testclient import TestClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.api_gateway.routes.main import app  # noqa: E402
from services.api_gateway.dependencies import create_jwt_token  # noqa: E402


@pytest.fixture
def stranger():
    """A default account from open signup: role VIEWER, no provisioning."""
    return {"sentinel_session": create_jwt_token({"sub": "stranger@example.com"}, role="VIEWER")}


@pytest.fixture
def operator():
    return {"sentinel_session": create_jwt_token({"sub": "operator@example.com"}, role="ADMIN")}


# Endpoints that disclose how the deployment is put together.
CONFIGURATION_ENDPOINTS = [
    "/api/v1/integrations",          # which upstreams are set up, which env vars are unset
    "/api/v1/flags",                 # kill-switch state, rollout percentages, whitelists
    "/api/v1/watchlists/equities",   # the tracked-symbol configuration
]


@pytest.mark.parametrize("path", CONFIGURATION_ENDPOINTS)
def test_stranger_cannot_read_deployment_configuration(stranger, path):
    res = TestClient(app).get(path, cookies=stranger)
    assert res.status_code in (401, 403), (
        f"{path} returned {res.status_code} to a signed-up stranger. Open signup "
        f"makes VIEWER equivalent to anonymous; configuration must not sit behind it."
    )


def test_operator_can_still_read_configuration(operator):
    """The gate must not lock the operator out of their own deployment."""
    res = TestClient(app).get("/api/v1/integrations", cookies=operator)
    assert res.status_code == 200


def test_integration_status_never_returns_secret_values(operator):
    """Presence only. A key's value, length or prefix must not leave the process."""
    import os
    os.environ["FINNHUB_API_KEY"] = "sk-should-never-appear-in-a-response"
    try:
        body = TestClient(app).get("/api/v1/integrations", cookies=operator).text
        assert "sk-should-never-appear-in-a-response" not in body
    finally:
        os.environ.pop("FINNHUB_API_KEY", None)


def test_the_master_gateway_key_cannot_mint_a_browser_session():
    """It is a service-to-service credential, not a login.

    The sign-in page offered an "API KEY" tab, so anyone who learned or guessed
    the master key held an ADMIN session -- with a form inviting them to try.
    """
    login_route = (ROOT / "frontend/src/app/api/auth/login/route.ts").read_text(encoding="utf-8")
    assert "apiKey" not in login_route, "the login route still accepts a raw API key"

    login_page = (ROOT / "frontend/src/app/login/page.tsx").read_text(encoding="utf-8")
    assert "API KEY" not in login_page, "the sign-in page still offers API-key auth"


def test_no_endpoint_issues_a_sentinel_api_key():
    """No per-user key issuance anywhere: keys are not a thing this platform hands out."""
    import re
    routes = (ROOT / "services/api_gateway/routes")
    offenders = []
    for f in routes.glob("*.py"):
        src = f.read_text(encoding="utf-8")
        for m in re.finditer(r"def\s+(\w*(?:create|generate|issue|rotate)\w*api\w*key\w*)", src, re.I):
            offenders.append(f"{f.name}:{m.group(1)}")
    assert not offenders, f"API-key issuance endpoints found: {offenders}"
