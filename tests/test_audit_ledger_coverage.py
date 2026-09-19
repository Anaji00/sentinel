"""
tests/test_audit_ledger_coverage.py

Which mutating routes write to the tamper-evident ledger, and why the rest do not.

The hash-chained ledger was sound and thinly used. Measured on the live
deployment, thirteen route modules exposed a POST, PUT or DELETE and four wrote
to it. Wiring the kill switch, the dead-letter replay and the alias declaration
closed the governance gap; billing closed the entitlement one.

What is left is not "six modules nobody got to". It is a list of modules that do
not change platform state, written down so the claim can be checked rather than
assumed -- the same shape as the collector telemetry allowlist. A route that
starts mutating state and is still on this list will fail here.
"""

import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
ROUTES = ROOT / "services" / "api_gateway" / "routes"

_MUTATING = re.compile(r"@router\.(post|put|delete|patch)\b")
_LEDGER = re.compile(r"record_admin_action|AuditLedger")


# Modules that expose a mutating endpoint and deliberately do not write.
#
# Each entry is a claim about what the endpoint does, not a note that nobody has
# wired it yet.
EXEMPT = {
    # Authentication, not governance. A login is an access event with its own
    # trail; writing every one into a governance ledger would bury the handful
    # of entries that record somebody changing the platform. Account state
    # changes that matter -- role grants -- do not live here.
    "auth": "authentication events, recorded as access rather than governance",
    "oidc": "the SSO half of auth, same reason",
    # Compute over existing data. A backtest reads bars and returns a result;
    # running one changes nothing anybody could later dispute.
    "backtest": "runs a simulation, changes no platform state",
    # User-generated content about the platform, not changes to it.
    "feedback": "records an opinion, changes no platform state",
    # Renders existing data into a document. Worth logging as access if data
    # egress is ever in scope; it is not a governance change.
    "reports": "renders existing data, changes no platform state",
}


def _modules():
    return sorted(p for p in ROUTES.glob("*.py") if not p.name.startswith("__"))


def _mutating_modules():
    out = []
    for path in _modules():
        src = path.read_text(encoding="utf-8", errors="replace")
        if _MUTATING.search(src):
            out.append(path)
    return out


def test_the_scan_can_see_its_subject():
    """A check that matches nothing passes for the wrong reason."""
    found = _mutating_modules()
    assert len(found) >= 10, (
        f"only {len(found)} mutating route modules found; the scan is broken"
    )


@pytest.mark.parametrize("path", _mutating_modules(), ids=lambda p: p.stem)
def test_a_mutating_route_writes_to_the_ledger_or_says_why_not(path):
    name = path.stem
    src = path.read_text(encoding="utf-8", errors="replace")
    if _LEDGER.search(src):
        assert name not in EXEMPT, (
            f"{name} now writes to the ledger -- remove it from EXEMPT so the "
            "list keeps meaning what it says."
        )
        return
    assert name in EXEMPT, (
        f"{name} exposes a mutating endpoint and writes nothing to the "
        "tamper-evident ledger. Wire record_admin_action, or add it to EXEMPT "
        "with the reason it changes no platform state."
    )


def test_the_exemptions_name_real_modules():
    """A stale exemption is a permission nobody reviews."""
    present = {p.stem for p in _modules()}
    stale = sorted(set(EXEMPT) - present)
    assert not stale, f"EXEMPT names route modules that no longer exist: {stale}"


def test_billing_records_entitlement_changes():
    """The line where an account gains or loses paid access.

    A `subscription_events` row is an ordinary table an operator with database
    access can edit. The ledger is hash-chained, which is the difference between
    a record and an audit trail.
    """
    src = (ROUTES / "billing.py").read_text(encoding="utf-8")
    assert "record_admin_action" in src
    assert "subscription_changed" in src
    hook = src.index("async def stripe_webhook")
    body = src[hook : hook + 6000]
    assert "status_before" in body and "status_after" in body, (
        "the entry must carry what changed, not only that something did"
    )


def test_the_webhook_does_not_attribute_stripe_to_a_person():
    """Unauthenticated by necessity, verified by signature instead.

    Naming a signed-in user on an endpoint that has none would put a name to an
    action that person did not take.
    """
    src = (ROUTES / "billing.py").read_text(encoding="utf-8")
    hook = src.index("async def stripe_webhook")
    body = src[hook : hook + 6000]
    assert '"sub": f"stripe:{event_type}"' in body
