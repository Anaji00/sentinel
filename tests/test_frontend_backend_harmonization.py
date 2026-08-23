"""
tests/test_frontend_backend_harmonization.py

Cross-boundary contract tests.

The frontend restates a number of backend facts -- enum values, rate limits,
field names. Nothing in the type system connects the two sides, so drift is
silent: the UI keeps rendering a stale constant and looks entirely correct while
being wrong. These tests read both sides and assert they still agree.

They also assert the display layer's half of the zero-fabrication rule: the
frontend must not carry invented personas, quotas, or metrics that no backend
endpoint produces.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND = REPO_ROOT / "frontend" / "src"


def _read(rel: str) -> str:
    p = FRONTEND / rel
    assert p.exists(), f"expected frontend file missing: {rel}"
    return p.read_text(encoding="utf-8")


# ── 1. PROVENANCE ENUM PARITY ────────────────────────────────────────────────

def test_provenance_enum_matches_across_the_boundary():
    """The TS union must list exactly the backend's ProvenanceSourceType values.

    A mismatch here is how a badge silently falls through to its default branch:
    the value arrives, matches no case, and renders as "Computed" regardless of
    what it actually was.
    """
    from shared.models.provenance import ProvenanceSourceType

    backend = {m.value for m in ProvenanceSourceType}

    ts = _read("components/ProvenanceBadge.tsx")
    union = ts.split("export type ProvenanceType =", 1)[1].split(";", 1)[0]
    frontend = set(re.findall(r'"([a-z_]+)"', union))

    assert frontend == backend, (
        f"provenance enum drift\n"
        f"  backend only: {sorted(backend - frontend)}\n"
        f"  frontend only: {sorted(frontend - backend)}"
    )


def test_provenance_badge_handles_every_backend_value():
    """Every backend value needs a switch branch, not just a type-level listing."""
    from shared.models.provenance import ProvenanceSourceType

    ts = _read("components/ProvenanceBadge.tsx")
    for member in ProvenanceSourceType:
        assert f'case "{member.value}"' in ts, (
            f"ProvenanceBadge has no case for {member.value!r}; it would render "
            f"the default label instead"
        )


# ── 2. RATE LIMIT PARITY ─────────────────────────────────────────────────────

def test_rate_limit_constants_match_the_gateway():
    """The account panel quotes a rate limit; it must be the real one.

    The panel previously advertised "2,500 req/min per IP" against an actual
    token bucket of 120 burst / 10 per second. An operator sizing a client
    against the published figure would have been throttled at a quarter of it.
    """
    import inspect
    from services.api_gateway.dependencies import check_rate_limit

    sig = inspect.signature(check_rate_limit)
    backend_burst = sig.parameters["max_tokens"].default
    backend_rate = sig.parameters["refill_rate_per_sec"].default

    tsx = _read("components/AccountProfileModal.tsx")
    fe_burst = int(re.search(r"RATE_LIMIT_BURST\s*=\s*(\d+)", tsx).group(1))
    fe_rate = int(re.search(r"RATE_LIMIT_PER_SEC\s*=\s*(\d+)", tsx).group(1))

    assert fe_burst == backend_burst, f"burst drift: UI {fe_burst} vs gateway {backend_burst}"
    assert fe_rate == int(backend_rate), f"refill drift: UI {fe_rate} vs gateway {backend_rate}"


# ── 3. NO FABRICATED DATA IN THE DISPLAY LAYER ───────────────────────────────

FABRICATED_MARKERS = [
    "Alexander Vance",
    "vance@sentinel-quant.io",
    "INSTITUTIONAL SUBSCRIPTION",
    "Level 4 Institutional Access",
    "usr_inst_9402",
    "apiUsageMonth",
    "apiQuotaMonth",
]


def test_account_panel_carries_no_invented_identity_or_quota():
    """The account panel must render session facts, not a fictional persona.

    It previously hardcoded a named user, a subscription tier, a monthly API
    quota and a usage figure -- none of which any backend endpoint produces.
    """
    tsx = _read("components/AccountProfileModal.tsx")
    for marker in FABRICATED_MARKERS:
        assert marker not in tsx, f"fabricated value still present in account panel: {marker!r}"

    # It must actually consult the session rather than local state.
    assert "/api/auth/session" in tsx, "account panel does not read the real session"


def test_no_fabricated_persona_anywhere_in_the_frontend():
    """The invented persona must not reappear in another component."""
    offenders = []
    for path in FRONTEND.rglob("*.tsx"):
        text = path.read_text(encoding="utf-8")
        for marker in ("Alexander Vance", "vance@sentinel-quant.io", "usr_inst_9402"):
            if marker in text:
                offenders.append(f"{path.relative_to(FRONTEND)}: {marker}")
    assert not offenders, "fabricated persona found:\n  " + "\n  ".join(offenders)


# ── 4. AGENT TELEMETRY IS DERIVED, NOT HARDCODED ─────────────────────────────

def test_agent_endpoint_does_not_fabricate_decisions():
    """The agents endpoint must not invent decisions when the swarm is quiet.

    Nothing writes `sentinel:agent:decision:*`, so the previous fallback was not
    a rare degraded path -- it was what every caller saw, including an invented
    claim that three executives had purchased $4.2M of stock.
    """
    src = (REPO_ROOT / "services" / "api_gateway" / "routes" / "agents.py").read_text(encoding="utf-8")

    assert "C-Suite executives purchased" not in src, "fabricated insider claim still present"
    assert "cluster_smci_099" not in src, "fabricated cluster id still present"
    assert "EVICT_WATCHLIST" not in src, "fabricated decision fixture still present"

    # An empty result must be distinguishable from an unreachable one.
    assert "decisions_source" in src, "endpoint cannot distinguish 'no decisions' from 'could not ask'"


def test_agent_endpoint_derives_models_from_runtime():
    """Model names must come from runtime metadata, not a literal.

    The endpoint hardcoded "Qwen 2.5 7B" for every agent. That string survived a
    change of the heavy tier to a 3B model and would have kept reporting the old
    one indefinitely.
    """
    src = (REPO_ROOT / "services" / "api_gateway" / "routes" / "agents.py").read_text(encoding="utf-8")

    assert "Qwen 2.5 7B" not in src, "hardcoded model string still present"
    assert 'meta.get("model")' in src, "model is not read from heartbeat metadata"
    assert "get_all_heartbeats_status" in src, "agent liveness is not derived from heartbeats"


def test_agent_tiers_publish_liveness():
    """The swarm must register a heartbeat like every other service.

    Seventeen services publish liveness; the agent tiers did not, so the gateway
    had no signal to report and fell back to a hardcoded "RUNNING".
    """
    src = (REPO_ROOT / "services" / "agents" / "main.py").read_text(encoding="utf-8")
    assert "start_heartbeat_task" in src, "agents service never registers liveness"
    assert "touch_heartbeat" in src, "agents service never publishes its roster"


def test_agent_tiers_are_known_components():
    """Health scoring must know about the tiers, and not penalise opt-in absence."""
    from shared.utils.heartbeat import ALL_KNOWN_COMPONENTS, OPTIONAL_COMPONENTS

    for tier in ("agents-heavy", "agents-fast"):
        assert tier in ALL_KNOWN_COMPONENTS, f"{tier} invisible to health scoring"
        assert tier in OPTIONAL_COMPONENTS, (
            f"{tier} runs behind an opt-in profile; counting its absence as a "
            f"failure reports the default deployment mode as degraded"
        )


# ── 5. DISPLAY FORMATTERS ARE CENTRALISED ────────────────────────────────────

def test_canonical_formatter_module_exists():
    """A single formatter module backs every rendered number."""
    fmt = _read("lib/format.ts")
    for fn in ("formatCurrency", "formatPercent", "formatNumber", "formatCompact",
               "formatTimestamp", "formatAge", "formatRatio"):
        assert f"export function {fn}" in fmt, f"missing canonical formatter: {fn}"

    # Absence must be representable and distinct from zero.
    assert "export const ABSENT" in fmt


def test_percent_formatter_forces_an_explicit_convention():
    """The backend emits both ratios and pre-scaled percents.

    Without an explicit convention the same helper silently produces values
    wrong by 100x that still look plausible on screen.
    """
    fmt = _read("lib/format.ts")
    assert "'percent' | 'ratio'" in fmt, "formatPercent does not force a stated convention"


# ── 6. DASHBOARD PRESENTATION HYGIENE ────────────────────────────────────────

def test_no_raw_json_dumps_rendered_in_the_ui():
    """Dashboards must present fields, not a serialization.

    Four panels previously rendered `JSON.stringify(obj, null, 2)` inside a
    <pre>, exposing snake_case keys, 17-digit floats and raw epochs to an
    operator reading under time pressure.
    """
    offenders = []
    for path in list(FRONTEND.rglob("*.tsx")):
        if path.name == "DataGrid.tsx":
            continue  # its docstring references the pattern it replaces
        text = path.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), 1):
            if "JSON.stringify" in line and "body:" not in line:
                offenders.append(f"{path.relative_to(FRONTEND)}:{i}")
    assert not offenders, "raw JSON rendered in UI:\n  " + "\n  ".join(offenders)


def test_structured_grid_component_exists():
    grid = _read("components/ui/DataGrid.tsx")
    assert "export const DataGrid" in grid
    # Must bound precision and represent absence rather than defaulting to zero.
    assert "formatNumber" in grid and "ABSENT" in grid


def test_dashboard_has_route_level_loading_and_error_boundaries():
    """Every dashboard view needs standardized loading and error states."""
    dash = FRONTEND / "app" / "(dashboard)"
    assert (dash / "loading.tsx").exists(), "no route-level loading state"
    assert (dash / "error.tsx").exists(), "no route-level error boundary"

    err = (dash / "error.tsx").read_text(encoding="utf-8")
    assert err.lstrip().startswith("'use client'"), "error boundaries must be client components"
    assert "reset" in err, "error boundary offers no recovery path"
    # A stack trace is not an operator-facing message.
    assert "error.message" not in err, "raw error message rendered to the operator"


def test_pages_are_server_components_unless_they_need_interactivity():
    """A page must not be client-rendered merely to host a dynamic import.

    Five pages carried 'use client' solely because `next/dynamic({ssr:false})`
    requires it, which pushed their entire static shell onto the client.
    """
    pages = sorted((FRONTEND / "app").rglob("page.tsx"))
    client_pages = [
        p for p in pages
        if p.read_text(encoding="utf-8").lstrip().startswith(("'use client'", '"use client"'))
    ]

    # Pages that legitimately need client execution: real user interaction,
    # form state, or a canvas/WebGL visualizer.
    # The auth pages are forms end to end -- the interaction *is* the page, so
    # there is no widget to split into an islands.tsx. Same category as login.
    allowed = {"charts", "methodology", "login", "map",
               "signup", "verify", "reset", "forgot"}
    for p in client_pages:
        assert p.parent.name in allowed, (
            f"{p.relative_to(FRONTEND)} is a client component; if only a widget "
            f"needs the client, move it into an islands.tsx and keep the page RSC"
        )


def test_client_islands_isolate_dynamic_imports():
    """Islands, not pages, carry the ssr:false imports."""
    islands = list((FRONTEND / "app").rglob("islands.tsx"))
    assert islands, "no client islands found"

    for island in islands:
        text = island.read_text(encoding="utf-8")
        assert text.lstrip().startswith("'use client'"), f"{island.name} is not a client module"
        assert "ssr: false" in text, f"{island.name} holds no dynamic import"

    # And the pages that use them must not have kept the directive.
    for island in islands:
        page = island.parent / "page.tsx"
        if page.exists():
            head = page.read_text(encoding="utf-8").lstrip()
            assert not head.startswith(("'use client'", '"use client"')), (
                f"{page.relative_to(FRONTEND)} still client-rendered despite having an island"
            )


def test_service_status_badges_are_driven_by_health_not_hardcoded():
    """Status pills must reflect backend state.

    The graph view rendered NEO4J LIVE with a pulsing green dot unconditionally,
    which is reassuring exactly when it should not be.
    """
    graph = _read("app/(dashboard)/graph/page.tsx")
    assert "NEO4J LIVE" not in graph, "hardcoded status pill still present"
    assert "ServiceStatusBadges" in graph, "graph view has no live status source"

    badges = _read("components/ui/ServiceStatusBadges.tsx")
    assert "/health/data" in badges, "status badges do not consult the health endpoint"
    assert "unknown" in badges, "status badges have no explicit unknown state"


# ── 7. NO FABRICATED NUMERIC DEFAULTS ────────────────────────────────────────

def test_no_invented_numeric_fallbacks_in_display_code():
    """`value || 0.85` renders an invented measurement when the real one is absent.

    These read as defensive coding but are indistinguishable on screen from a
    measured value -- the same defect as a fabricated backtest, at the display
    layer. Absence must render as the em dash.
    """
    import re as _re
    pattern = _re.compile(r"\|\|\s*\d+\.\d+")
    offenders = []
    for path in FRONTEND.rglob("*.tsx"):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line) and "//" not in line.split("||")[0]:
                offenders.append(f"{path.relative_to(FRONTEND)}:{i}  {line.strip()[:90]}")
    assert not offenders, "fabricated numeric defaults:\n  " + "\n  ".join(offenders)


def test_explainability_panel_does_not_assert_unearned_claims():
    """The audit-trail panel must not assert significance or governance itself."""
    tsx = _read("components/ExplainabilityModal.tsx")

    # Significance is the backend's determination, applied against a threshold.
    assert "data?.is_significant" in tsx, "significance is not read from the payload"
    assert "|| 0.85" not in tsx, "anomaly score still has an invented default"
    assert "|| 0.042" not in tsx, "PSI still has an invented default"
    assert "|| 14.8" not in tsx, "latency still has an invented default"
    assert "'VALIDATED'" not in tsx, "hardcoded governance verdict still rendered"


def test_model_card_names_the_scorer_that_actually_runs():
    """Naming a retired model in an audit trail misdescribes the system."""
    src = (REPO_ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(encoding="utf-8")

    assert "IsolationForest-SpatialMicrostructure" not in src, "model card names a retired scorer"
    assert "ACTIVE_MODEL_NAME" in src and "RRCF" in src
    # Governance status was a constant asserting approval nothing grants.
    assert "VALIDATED_PRODUCTION" not in src


def test_drift_status_is_measured_or_declared_unknown():
    """An absent drift evaluation must not be reported as STABLE."""
    src = (REPO_ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(encoding="utf-8")
    assert "_read_drift_status" in src, "drift status is not sourced at runtime"
    assert '"drift_state": "UNKNOWN"' in src, "no explicit unknown drift state"

    # And the scheduler must actually publish what the endpoint reads.
    sched = (REPO_ROOT / "services" / "telemetry-worker" / "drift_scheduler.py").read_text(encoding="utf-8")
    assert "DRIFT_REPORT_KEY" in sched, "drift scheduler does not publish its report"
    assert "DRIFT_REPORT_KEY" in src, "explain endpoint reads a different key"


def test_audit_payload_hash_is_content_addressed():
    """A 'sha256:' field must be a digest of content, not of a process-local hash."""
    src = (REPO_ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(encoding="utf-8")
    assert "hashlib.sha256(" in src
    assert "abs(hash(" not in src, "payload hash still uses randomized builtin hash()"


# ── 8. FORMATTER ADOPTION ────────────────────────────────────────────────────

def test_percent_call_sites_state_their_convention():
    """Every formatPercent call on a ratio must say so explicitly."""
    import re as _re
    offenders = []
    for path in FRONTEND.rglob("*.tsx"):
        text = path.read_text(encoding="utf-8")
        # A ratio-valued field passed without from:'ratio' would render 100x small.
        for m in _re.finditer(r"formatPercent\(([^,)]+)([^)]*)\)", text):
            arg, opts = m.group(1).strip(), m.group(2)
            looks_ratio = any(k in arg for k in ("confidence", "anomaly_score", "funding_rate", "conviction"))
            if looks_ratio and "ratio" not in opts:
                line = text[:m.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(FRONTEND)}:{line}  {arg}")
    assert not offenders, (
        "ratio-valued fields formatted without from:'ratio' (renders 100x too small):\n  "
        + "\n  ".join(offenders)
    )


def test_manual_percent_arithmetic_is_gone_from_components():
    """`(x * 100).toFixed(n)}%` is the pattern formatPercent replaced."""
    import re as _re
    pattern = _re.compile(r"\*\s*100\s*\)\s*\.toFixed\(\d\)\s*\}?%")
    offenders = []
    for path in (FRONTEND / "components").rglob("*.tsx"):
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                offenders.append(f"{path.relative_to(FRONTEND)}:{i}")
    assert not offenders, "manual percent arithmetic remains:\n  " + "\n  ".join(offenders)
