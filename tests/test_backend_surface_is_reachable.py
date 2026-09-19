"""Every gateway route, against every path the frontend actually fetches.

A route nobody calls is a route nobody has tested. Two of the surfaces this
comparison first turned up were not merely unreachable -- semantic search named
a collection that had never existed, and the analyst feedback loop had no first
step -- and in both cases the only way to find out was to try to call them.

The comparison has to understand template literals. A first pass at it scanned
for quoted paths only, and reported `/explain/signal/{id}` and
`/events/detail/{id}` as unreached when both are called as

    apiClient.get(`/events/detail/${encodeURIComponent(id)}`)

which is exactly how a parameterised route is called. A scanner that cannot
read the normal spelling of the thing it looks for produces a list of gaps that
are not gaps, and this audit has spent enough entries on checks that could not
see their own subject.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ROUTES_DIR = ROOT / "services" / "api_gateway" / "routes"
FRONTEND = ROOT / "frontend" / "src"

# Reached by something other than a browser, each with the reason.
NOT_A_UI_SURFACE = {
    # Stripe posts here. A browser never does.
    "/api/v1/billing/webhook",
    # Container probes. A credential-free liveness check is the point.
    "/api/v1/health/liveness",
    "/api/v1/health/readiness",
    # Prometheus scrape targets, behind a scrape token.
    "/api/v1/health/metrics",
    # Deployment introspection for an operator at a shell, not a screen.
    "/api/v1/health/secrets",
    # The BFF calls these server-side; the browser talks to /api/auth/* instead,
    # which is the whole point of the proxy.
    "/api/v1/auth/login",
    "/api/v1/auth/account",
    "/api/v1/auth/oidc/start",
    "/api/v1/auth/oidc/callback",
    "/api/v1/auth/oidc/status",
    # Stripe-hosted redirects: the browser is sent to Stripe, not served here.
    "/api/v1/billing/checkout",
    "/api/v1/billing/portal",
}

# Deliberately absent from the product, each with the decision behind it.
WITHDRAWN = {
    # The knowledge-graph view was removed. Neo4j and these routes remain,
    # feeding the agent swarm; `test_frontend_reachability` asserts the nav
    # does not bring the view back.
    "/api/v1/graph/entities",
    "/api/v1/graph/entity/{entity_id}",
    "/api/v1/graph/network",
    "/api/v1/graph/shortest-path",
}


def _routes() -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    for path in sorted(ROUTES_DIR.glob("*.py")):
        src = path.read_text(encoding="utf-8")
        prefix_match = re.search(
            r'APIRouter\((?:[^)]*?)prefix\s*=\s*["\']([^"\']+)["\']', src, re.S
        )
        prefix = prefix_match.group(1) if prefix_match else ""
        for m in re.finditer(
            r'@router\.(get|post|put|delete|patch)\(\s*\n?\s*["\']([^"\']*)["\']', src
        ):
            out.append((m.group(1).upper(), (prefix + m.group(2)) or prefix, path.name))
    return sorted(set(out))


def _frontend_paths() -> set[str]:
    """Every API path the browser asks for, quoted or interpolated.

    Template literals are collapsed to their static prefix: `` `/cases/${id}` ``
    contributes `/cases/`, which is what makes a parameterised route reachable.
    """
    found: set[str] = set()
    for path in FRONTEND.rglob("*.ts*"):
        if "__tests__" in path.parts:
            continue
        src = path.read_text(encoding="utf-8")
        src = re.sub(r"/\*(?:.|\n)*?\*/", "", src)
        src = re.sub(r"^\s*//.*$", "", src, flags=re.M)

        for m in re.finditer(r"""['"]([/][a-zA-Z0-9_\-/.]*)['"?]""", src):
            found.add(m.group(1))
        # A template literal: keep everything up to the first interpolation.
        for m in re.finditer(r"`([/][a-zA-Z0-9_\-/.]*)", src):
            found.add(m.group(1))
    return found


def _reaches(route: str, calls: set[str]) -> bool:
    tail = route.replace("/api/v1", "")
    base = tail.split("{")[0].rstrip("/") if "{" in tail else None
    for call in calls:
        c = call.replace("/api/proxy/api/v1", "").replace("/api/v1", "").rstrip("/")
        if c == tail.rstrip("/"):
            return True
        if base and c.startswith(base) and len(base) > 1:
            return True
    return False


ROUTES = _routes()


def test_the_route_table_was_actually_parsed() -> None:
    """An empty parse would make every assertion below vacuous."""
    assert len(ROUTES) > 80, f"only {len(ROUTES)} routes parsed out of the gateway"


@pytest.mark.parametrize(
    "verb, route, module",
    [pytest.param(v, r, m, id=f"{v} {r}") for v, r, m in ROUTES],
)
def test_every_route_is_reachable_or_accounted_for(verb: str, route: str, module: str) -> None:
    """Built, served, and callable from the product -- or listed with a reason.

    The two lists above are the whole allowance. Adding a route to one of them
    is a decision someone has to write down; leaving a route out of both means
    the platform grew a capability nobody can use.
    """
    if route in NOT_A_UI_SURFACE or route in WITHDRAWN:
        return
    calls = _frontend_paths()
    assert _reaches(route, calls), (
        f"{verb} {route} ({module}) is served and no component calls it. "
        f"Wire it, or record it in NOT_A_UI_SURFACE / WITHDRAWN with the reason."
    )


def test_the_exemptions_still_name_real_routes() -> None:
    """An allowlist that outlives its entries becomes the contract."""
    served = {r for _, r, _ in ROUTES}
    for entry in sorted(NOT_A_UI_SURFACE | WITHDRAWN):
        assert entry in served, f"{entry} is exempted and no longer exists; drop the entry"
