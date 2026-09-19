"""Every route that changes what the platform does is in the tamper-evident ledger.

The hash chain was sound and thinly used. Measured on the live deployment
before this: thirteen route modules expose a POST, PUT or DELETE and four wrote
to the ledger. A $4,862 paper trade was recorded in full; tripping the
platform-wide kill switch, replaying eleven thousand dead letters into the
pipeline, and declaring that two companies are one entity were recorded
nowhere -- and those three are the ones whose effects are hardest to
reconstruct afterwards, because a kill switch leaves no events, a replay is
indistinguishable from fresh ingestion, and an alias rewrites what the platform
thinks things *are*.

The list below is the contract. A new mutating route either records a
governance action or names itself here with a reason, and "nobody remembered"
stops being one of the ways this can go wrong.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ROUTES = ROOT / "services" / "api_gateway" / "routes"

MUTATING = {"post", "put", "patch", "delete"}

# The ways a handler can reach the ledger.
LEDGER_NAMES = {"record_admin_action", "AuditLedger", "record_entry"}

# Routes that deliberately do not write a governance entry, and why.
#
# Each of these changes something, so none is exempt by being read-only. They
# are exempt because the ledger is for *governance* -- who changed the
# platform's behaviour or its idea of the world -- and these either have their
# own stronger record or are the subject's own data rather than the platform's.
NOT_GOVERNANCE = {
    # Authentication has its own audited path: failed attempts are rate limited
    # and logged by identity, and writing credentials-adjacent events into a
    # chain that is served to ADMINs would widen their exposure, not narrow it.
    "auth.py": "authentication has its own log; ledger entries would widen credential exposure",
    "oidc.py": "same path as auth.py, via the identity provider",
    # Stripe is the system of record for money, and the webhook is not a user
    # action at all -- it is the processor telling us what already happened.
    "billing.py": "Stripe is the system of record; the webhook reports rather than decides",
    # A reader's own opinion about a correlation. It changes ranking, not
    # platform behaviour, and it is already attributed per row in its own table.
    "feedback.py": "analyst verdicts are attributed in their own table",
    # Produces a document from data that already exists. Nothing about the
    # platform changes, and the report itself carries its parameters.
    "reports.py": "generates a document; changes no platform state",
    # Compute, not governance. A backtest reads history and returns a number.
    "backtest.py": "runs a computation over stored history; changes no state",
}


def _module_mutating_routes(path: Path):
    """(function name, method) for every mutating handler in a route module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            call = dec if isinstance(dec, ast.Call) else None
            func = call.func if call else dec
            # @router.post(...) -- the attribute is the HTTP method.
            if isinstance(func, ast.Attribute) and func.attr.lower() in MUTATING:
                out.append((node.name, func.attr.lower()))
    return out


def _route_modules():
    return sorted(p for p in ROUTES.glob("*.py") if p.name != "__init__.py")


def test_the_scan_can_see_its_subject():
    """A parser that finds nothing would make every assertion below vacuous."""
    modules = _route_modules()
    assert len(modules) > 8, f"only {len(modules)} route modules found"

    total = sum(len(_module_mutating_routes(p)) for p in modules)
    assert total >= 20, f"only {total} mutating routes parsed; the decorator scan is not working"

    # And it must find the ones we know are there.
    flags = dict.fromkeys(n for n, _ in _module_mutating_routes(ROUTES / "flags.py"))
    assert {"trip_kill_switch", "reset_kill_switch", "toggle_feature_flag"} <= set(flags), flags


@pytest.mark.parametrize(
    "path", _route_modules(), ids=lambda p: p.name
)
def test_mutating_routes_record_a_governance_action(path: Path):
    mutating = _module_mutating_routes(path)
    if not mutating:
        return

    reason = NOT_GOVERNANCE.get(path.name)
    source = path.read_text(encoding="utf-8")
    writes_ledger = any(name in source for name in LEDGER_NAMES)

    if reason:
        # An exemption that has quietly started writing to the ledger is not a
        # failure -- but an exemption whose reason no longer applies should be
        # removed rather than left as a standing permission.
        return

    assert writes_ledger, (
        f"{path.name} exposes {len(mutating)} mutating route(s) "
        f"({', '.join(n for n, _ in mutating)}) and never reaches the audit "
        f"ledger. Either record a governance action, or add it to "
        f"NOT_GOVERNANCE with the reason it is not one."
    )


def test_the_three_that_were_missing_are_covered():
    """The specific gaps this test was written for, named so a regression is legible."""
    for name, action in (
        ("flags.py", "TRIP_MASTER_KILL_SWITCH"),
        ("dlq.py", "REPLAY_DEAD_LETTERS"),
        ("attribution.py", "MERGE_ENTITY_ALIAS"),
    ):
        source = (ROUTES / name).read_text(encoding="utf-8")
        assert "record_admin_action" in source, f"{name} no longer records anything"
        assert action in source, f"{name} no longer records {action}"


def test_every_exemption_names_a_real_module():
    """A stale exemption is a standing permission nobody reviews."""
    present = {p.name for p in _route_modules()}
    stale = sorted(set(NOT_GOVERNANCE) - present)
    assert not stale, f"NOT_GOVERNANCE names modules that no longer exist: {stale}"
