"""
tests/test_frontend_shell_and_swarm_api.py

The console showed invented numbers, and the swarm had no API to show instead.

AgentSwarmTelemetry fetched /api/v1/health/agents -- a route that does not exist
-- and on failure rendered a hardcoded array of six agents with invented
throughput, beneath a header reading "REAL-TIME AGENT HEALTH". It also printed
"6/6 AGENTS ACTIVE" and "HEALTH: 100% NOMINAL" as literals. The deployment runs
ten agents on qwen2.5:3b and 1.5b; the fallback named six on qwen2.5:7b, which
is not deployed.

There was nothing better to point it at. /agents/processes existed but reported
active_agents_count: 0 with null names, because start_heartbeat_task rewrote the
tier heartbeat every 15 seconds with no metadata -- erasing the roster that main()
published once at startup. And the swarm's conclusions (consensus, bulletins,
scorecards, calibration) were persisted to Redis and exposed by no endpoint at all.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TELEMETRY = ROOT / "frontend/src/components/AgentSwarmTelemetry.tsx"
SIDEBAR = ROOT / "frontend/src/components/ui/Sidebar.tsx"
PALETTE = ROOT / "frontend/src/components/ui/CommandPalette.tsx"
LAYOUT = ROOT / "frontend/src/app/(dashboard)/layout.tsx"
AGENTS_ROUTE = ROOT / "services/api_gateway/routes/agents.py"


def _src(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _code(path: Path) -> str:
    """Source with comments stripped.

    These assertions name the values that must not come back, and the file
    documents those same values in its header. Matching raw text would fail on
    the explanation rather than on the defect.
    """
    text = _src(path)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)      # block comments
    text = re.sub(r"^\s*//.*$", "", text, flags=re.M)       # line comments
    return text


# ── the console must not invent data ─────────────────────────────────────────

def test_telemetry_no_longer_ships_a_fabricated_agent_roster():
    src = _code(TELEMETRY)
    for invented in ("qwen2.5:7b", "processed: 890", "6/6 AGENTS ACTIVE", "100% NOMINAL"):
        assert invented not in src, f"fabricated telemetry is back: {invented!r}"


def test_telemetry_calls_endpoints_that_exist():
    src = _code(TELEMETRY)
    assert "/api/v1/health/agents" not in src, "that route does not exist"
    assert "'/agents/processes'" in src
    assert "'/agents/swarm'" in src


def test_missing_values_render_as_missing():
    """An operator cannot act on a panel that always shows activity."""
    src = _src(TELEMETRY)
    assert "'—'" in src, "absent values must be shown as absent"


# ── the heartbeat must carry the roster on every beat ────────────────────────

def test_the_heartbeat_loop_can_publish_metadata():
    src = _src(ROOT / "shared/utils/heartbeat.py")
    sig = src[src.index("async def start_heartbeat_task"):][:400]
    assert "metadata" in sig, "the loop cannot publish a roster"
    body = src[src.index("async def start_heartbeat_task"):][:1800]
    assert "metadata=meta" in body, "metadata is accepted and then not sent"


def test_the_roster_is_republished_not_written_once():
    """Writing it once and then overwriting it with {} is the original defect."""
    src = _src(ROOT / "services/agents/main.py")
    assert "def _tier_metadata() -> dict:" in src
    assert "metadata=_tier_metadata" in src, "the loop must republish the roster"


def test_the_roster_reports_real_models_not_a_guess():
    src = _src(ROOT / "services/agents/main.py")
    meta = src[src.index("def _tier_metadata"):][:900]
    assert '"agents": sorted(active_agents.keys())' in meta
    assert '"model"' in meta and "AGENT_MODEL" in meta


# ── the swarm's conclusions need an API ──────────────────────────────────────

def test_the_swarm_endpoint_exists():
    src = _src(AGENTS_ROUTE)
    assert '@router.get("/swarm"' in src


def test_single_agent_signals_are_reported_apart_from_corroborated_ones():
    """One agent agreeing with itself is a lead, not a consensus."""
    src = _src(AGENTS_ROUTE)
    swarm = src[src.index('@router.get("/swarm"'):]
    assert '"corroborated_signals"' in swarm
    assert '"single_agent_signals"' in swarm
    assert "contributing_agents" in swarm


def test_the_swarm_endpoint_reports_emptiness_honestly():
    """Absent data is returned as absent, never as a plausible default.

    Asserted on the shape the handler builds rather than on incidental syntax:
    every collection is seeded empty and only filled from what Redis actually
    held, and the calibration counters start at zero rather than at a figure
    that would read as a measurement.
    """
    src = _src(AGENTS_ROUTE)
    swarm = src[src.index('@router.get("/swarm"'):]
    assert 'consensus: dict = {}' in swarm
    assert 'bulletins = []' in swarm and 'scorecards = []' in swarm
    assert 'open_predictions = 0' in swarm
    assert '"paired_forecasts": 0, "resolved": 0' in swarm
    # Every accessor tolerates a missing key instead of inventing one.
    assert swarm.count("or []") >= 3


# ── navigation ───────────────────────────────────────────────────────────────

def test_navigation_is_grouped_not_a_flat_list_of_thirteen():
    src = _src(SIDEBAR)
    assert "NAV_GROUPS" in src
    for group in ("Overview", "Markets", "Intelligence", "Reference"):
        assert f"label: '{group}'" in src


def test_labels_are_not_hidden_behind_hover():
    """`w-16 hover:w-64` made the navigation unreadable without a pointer."""
    src = _code(SIDEBAR)
    assert "hover:w-64" not in src
    assert "aria-expanded" in src, "the expand state must be operable and announced"


def test_active_state_survives_nested_routes():
    """/graph/entity/NVDA must keep 'Graph & Correlations' lit."""
    src = _src(SIDEBAR)
    assert "startsWith(`${href}/`)" in src
    assert "href === '/' ? pathname === '/'" in src, "root would match every route"


# ── command palette ──────────────────────────────────────────────────────────

def test_the_palette_is_mounted():
    assert "<CommandPalette />" in _src(LAYOUT)


def test_the_palette_opens_on_the_expected_chord():
    src = _src(PALETTE)
    assert "metaKey || e.ctrlKey" in src
    assert "'k'" in src


def test_the_palette_does_not_read_analyst_only_configuration():
    """/watchlists/equities is deployment configuration; a VIEWER gets 403.

    Sourcing the palette from it would have made ticker search silently empty
    for every account open signup creates.
    """
    src = _src(PALETTE)
    assert "/watchlists/equities" not in src.split("*/", 1)[1], (
        "the palette is reading an ANALYST-gated endpoint again"
    )
    assert "/events/tradfi" in src


def test_the_palette_degrades_to_navigation_when_data_is_unavailable():
    src = _src(PALETTE)
    # One source remains since the graph entity lookup was removed with the
    # graph view; the property -- a failed fetch must not break the palette --
    # is what matters, not the count.
    assert src.count("} catch {") >= 1, "a failed fetch must not break the palette"


# ── text must be selectable ──────────────────────────────────────────────────

def test_the_app_shell_does_not_forbid_text_selection():
    """select-none on the root made every ticker and headline uncopyable."""
    layout = _src(LAYOUT)
    root = layout[layout.index("<div className=\"flex h-screen"):][:200]
    assert "select-none" not in root


@pytest.mark.parametrize("component", [
    "IntelligenceFeed.tsx", "OsintThreatMatrix.tsx", "DarkPoolFlowPanel.tsx",
])
def test_content_panels_allow_selection(component):
    path = ROOT / "frontend/src/components" / component
    if not path.exists():
        pytest.skip(f"{component} not present")
    assert "select-none" not in _src(path)


# ── the header must not invent an identity or a status ───────────────────────

HEADER = ROOT / "frontend/src/components/ui/Header.tsx"
SIGNUP = ROOT / "frontend/src/app/signup/page.tsx"


def test_the_header_shows_the_signed_in_user_not_a_persona():
    """It rendered "A. VANCE / INSTITUTIONAL" with initials "AV" as literals.

    Every user who signed in saw a fictional person's name in place of their own.
    """
    src = _code(HEADER)
    for invented in ("A. VANCE", "INSTITUTIONAL", '"AV"', ">AV<"):
        assert invented not in src, f"fabricated identity is back: {invented!r}"
    assert "/api/auth/session" in src, "identity must come from the session"


def test_the_header_reports_a_measured_agent_count():
    """"AGENT SWARM: ACTIVE (8)" was fixed text; the deployment runs ten."""
    src = _code(HEADER)
    assert "ACTIVE (8)" not in src
    assert "'/agents/processes'" in src
    assert "active_agents_count" in src


def test_the_header_does_not_assert_domain_status_it_never_read():
    src = _code(HEADER)
    for claim in ("AIS TANKERS", "ADS-B FLIGHTS", "TRACKING", "v2.4 EDA"):
        assert claim not in src, f"unmeasured status is back: {claim!r}"


def test_a_failed_latency_probe_is_not_reported_as_a_latency():
    """The old code returned the elapsed time of a request that never completed."""
    src = _code(HEADER)
    assert "setLatency(null)" in src
    assert "unreachable" in src


# ── sign-up must not promise mail that was never sent ────────────────────────

def test_signup_honours_whether_a_mail_actually_went_out():
    """The API answers email_sent:false when SMTP is unconfigured.

    The page discarded it and always rendered "We sent a confirmation link",
    leaving users waiting for something that was never sent -- which is exactly
    what happens on this deployment, where SMTP is not configured.
    """
    src = _code(SIGNUP)
    assert "email_sent" in src, "the page ignores whether mail was sent"
    assert "emailSent ?" in src, "both outcomes must be rendered"


def test_signup_prefers_the_servers_own_explanation():
    src = _code(SIGNUP)
    assert "serverMessage" in src


def test_the_deployment_documents_how_to_enable_email():
    env = (ROOT / ".env").read_text(encoding="utf-8")
    for key in ("SMTP_HOST", "SMTP_FROM", "PUBLIC_BASE_URL"):
        assert key in env, f"{key} is undocumented, so email cannot be turned on"


# ── the visual system ────────────────────────────────────────────────────────

CSS = ROOT / "frontend/src/app/globals.css"


def _css(path: Path) -> str:
    """Stylesheet with comments stripped: the file explains what it removed."""
    return re.sub(r"/\*.*?\*/", "", path.read_text(encoding="utf-8"), flags=re.S)


def test_panels_are_opaque_rather_than_blurred():
    """Sixteen simultaneous 20px backdrop blurs cost frame time and contrast."""
    css = _css(CSS)
    glass = css[css.index(".glass-panel {"):css.index(".glass-panel-hover")]
    assert "backdrop-filter" not in glass


def test_neon_glow_is_retired():
    """Glow on a border lowers the contrast of the edge it exists to define."""
    css = CSS.read_text(encoding="utf-8")
    block = css[css.index(".glow-cyan,"):]
    assert "box-shadow: none" in block[:220]


def test_motion_stops_for_anyone_who_asks():
    css = CSS.read_text(encoding="utf-8")
    assert "prefers-reduced-motion" in css


def test_keyboard_focus_is_visible():
    css = CSS.read_text(encoding="utf-8")
    assert ":focus-visible" in css and "outline" in css


def test_figures_use_tabular_numerals():
    """Columns of numbers have to line up to be comparable by eye."""
    css = CSS.read_text(encoding="utf-8")
    assert "tabular-nums" in css
