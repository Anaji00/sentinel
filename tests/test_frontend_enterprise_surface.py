"""The properties an operator-facing console has to hold, checked every build.

Each of these was a real defect found by measuring the frontend rather than
reading it, and each is the kind that comes back the moment someone adds a
panel by copying the one next to it.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"
PACKAGE_JSON = ROOT / "frontend" / "package.json"


def _components() -> list[Path]:
    return [
        p
        for p in sorted(SRC.rglob("*.ts*"))
        if "__tests__" not in p.parts and "node_modules" not in p.parts
    ]


def _strip_comments(text: str) -> str:
    """Prose about a defect naturally quotes the defect."""
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"^\s*//.*$", "", text, flags=re.M)


# ── times ───────────────────────────────────────────────────────────────────


def test_no_timestamp_renders_in_the_machines_own_zone() -> None:
    """The header let an operator choose a zone; nothing else read the choice.

    Eleven event timestamps called `toLocaleTimeString()` with no zone, so the
    clock could read 14:32 EDT beside a row reading 20:32, neither labelled, on
    a platform whose job is correlating events in time.
    """
    offenders = [
        f"{p.relative_to(SRC)}:{i}"
        for p in _components()
        for i, line in enumerate(_strip_comments(p.read_text(encoding="utf-8")).split("\n"), 1)
        if "toLocaleTimeString()" in line or "toLocaleDateString()" in line
    ]
    assert not offenders, (
        "these render in the browser's zone rather than the selected one. Use "
        "<ClockTime> or formatClock(value, zone):\n  " + "\n  ".join(offenders)
    )


def test_figures_do_not_inherit_the_readers_locale() -> None:
    """`toLocaleString()` with no locale renders 1.234,50 on a German machine.

    Allowed only inside `lib/format.ts`, which pins the locale explicitly.
    """
    offenders = []
    for p in _components():
        if p.name == "format.ts":
            continue
        for i, line in enumerate(_strip_comments(p.read_text(encoding="utf-8")).split("\n"), 1):
            if "toLocaleString(" in line:
                offenders.append(f"{p.relative_to(SRC)}:{i}")
    assert not offenders, (
        "route these through lib/format.ts, which pins en-US:\n  " + "\n  ".join(offenders)
    )


def test_the_display_locale_is_stated_once() -> None:
    fmt = (SRC / "lib" / "format.ts").read_text(encoding="utf-8")
    assert "DISPLAY_LOCALE" in fmt
    assert "toLocaleString(undefined" not in fmt, (
        "format.ts was itself locale-dependent: `undefined` means the browser's"
    )


# ── dialogs ─────────────────────────────────────────────────────────────────

# Surfaces that are deliberately not modal, each with the reason.
NON_MODAL = {
    # A toast: it must not take focus, because it appears while the operator is
    # doing something else.
    "components/ui/Feedback.tsx",
    # The nav drawer: it has its own Escape handler and is a navigation
    # surface, not a dialog -- trapping focus in it would strand anyone who
    # opened it to look rather than to move.
    "components/ui/Sidebar.tsx",
    "components/ui/NavContext.tsx",
    # The command palette manages its own focus, because the input is the
    # entire interface.
    "components/ui/CommandPalette.tsx",
}


def test_every_modal_overlay_traps_focus_and_answers_escape() -> None:
    """An overlay a keyboard user can enter and not leave.

    Measured before `useDialog` existed: fourteen `fixed inset-0` overlays, two
    with `role="dialog"`, none trapping focus, and `tabIndex` appearing zero
    times in the whole codebase. Tab walked straight out of the open dialog
    into the page behind it -- still focusable, now invisible under the
    backdrop.
    """
    offenders = []
    for p in _components():
        rel = str(p.relative_to(SRC)).replace("\\", "/")
        if rel in NON_MODAL:
            continue
        src = p.read_text(encoding="utf-8")
        if "fixed inset-0" not in _strip_comments(src):
            continue
        if "useDialog" not in src:
            offenders.append(rel)
    assert not offenders, (
        "these render a modal overlay without focus management. Use "
        "useDialog(isOpen, onClose, label), or record the surface in NON_MODAL "
        "with the reason it is not a dialog:\n  " + "\n  ".join(offenders)
    )


def test_the_dialog_hook_does_all_four_things() -> None:
    """A trap that only handles Escape is not a trap."""
    src = (SRC / "components" / "ui" / "useDialog.ts").read_text(encoding="utf-8")
    for needed, why in [
        ("'Escape'", "Escape must close it"),
        ("role: 'dialog'", "it must announce itself as a dialog"),
        ("'aria-modal': true", "it must say the rest of the page is inert"),
        ("shiftKey", "Shift+Tab must cycle backwards inside it"),
        ("opener", "focus must return to whatever opened it"),
    ]:
        assert needed in src, why


# ── fetching ────────────────────────────────────────────────────────────────


def test_poll_cadences_come_from_the_shared_steps() -> None:
    """39 hand-written intervals, with 60000 spelled two different ways."""
    offenders = [
        f"{p.relative_to(SRC)}:{i}"
        for p in _components()
        for i, line in enumerate(p.read_text(encoding="utf-8").split("\n"), 1)
        if re.search(r"refreshInterval:\s*[0-9]", line)
    ]
    assert not offenders, (
        "use POLL.live / POLL.standard / POLL.slow / POLL.rare from "
        "components/ui/DataProvider:\n  " + "\n  ".join(offenders)
    )


def test_session_loss_has_exactly_one_handler() -> None:
    """The cookie lasts 24 hours and there was no way back from its expiry.

    Every panel discovered the 401 independently and printed "Session expired"
    into its own empty state; nothing offered a route to sign in again.
    """
    provider = (SRC / "components" / "ui" / "DataProvider.tsx").read_text(encoding="utf-8")
    assert "isUnauthenticated" in provider
    assert "/login?next=" in provider, "the operator should land back where they were"
    assert "redirecting" in provider, "one redirect per session loss, not one per panel"

    layout = (SRC / "app" / "(dashboard)" / "layout.tsx").read_text(encoding="utf-8")
    assert "<DataProvider>" in layout, "the handler has to actually be mounted"


# ── dependencies ────────────────────────────────────────────────────────────

# Packages with no import of their own, for a reason.
NOT_IMPORTED_DIRECTLY = {
    "next", "react", "react-dom",           # the framework and the runtime
    "tailwindcss", "@tailwindcss/postcss",  # build-time, via postcss.config
    "typescript", "eslint", "eslint-config-next", "vitest",
    "@types/node", "@types/react", "@types/react-dom",
}


def test_no_dependency_ships_without_being_used() -> None:
    """92 MB of unused packages were in the production image.

    deck.gl, mapbox-gl, react-map-gl, react-flow-renderer and three d3 packages
    were declared, installed, and imported by nothing. Each is image size,
    install time and supply-chain surface bought for nothing -- and one of them
    (react-flow-renderer) peer-requires React 18 against this project's 19, so
    it could never have worked had anything tried to use it.
    """
    pkg = json.loads(PACKAGE_JSON.read_text(encoding="utf-8"))
    declared = set(pkg.get("dependencies", {})) | set(pkg.get("devDependencies", {}))

    sources = "\n".join(p.read_text(encoding="utf-8") for p in _components())
    unused = []
    for name in sorted(declared - NOT_IMPORTED_DIRECTLY):
        # Static and dynamic imports both. The dynamic form is how a dead
        # `import('d3-selection')` polyfill survived a static-import scan.
        if re.search(rf"""from ['"]{re.escape(name)}(/|['"])""", sources):
            continue
        if re.search(rf"""import\(['"]{re.escape(name)}(/|['"])""", sources):
            continue
        unused.append(name)

    assert not unused, (
        "declared in package.json and imported nowhere: "
        + ", ".join(unused)
        + ". Remove them, or add to NOT_IMPORTED_DIRECTLY with the reason."
    )


# ── what the browser talks to ───────────────────────────────────────────────


def test_nothing_is_fetched_from_a_third_party_at_runtime() -> None:
    """The map's basemap came from jsdelivr, and the CSP blocked it.

    `connect-src 'self'` was tightened earlier in this audit; the fetch then
    failed silently, because `<Geographies>` with no data draws nothing rather
    than raising. The map was a field of markers on an empty background, and
    had been since the CSP changed.

    Fetching it from a CDN was wrong for this product regardless of the CSP:
    `DataSovereigntyModal` tells the operator nothing about them leaves the
    deployment, while every page view announced itself to a third party.
    """
    origins = re.compile(r"""['"`](https?://[^'"`\s]+)['"`]""")
    offenders = []
    for path in _components():
        rel = str(path.relative_to(SRC)).replace("\\", "/")
        # `app/api/**` is the BFF: route handlers that run in Node inside the
        # container and address the gateway over the Docker network. No browser
        # ever executes them, so no CSP applies -- and the whole point of the
        # proxy is that the browser talks to this origin instead of to the
        # gateway directly.
        if rel.startswith("app/api/"):
            continue
        text = _strip_comments(path.read_text(encoding="utf-8"))
        for i, line in enumerate(text.split("\n"), 1):
            for url in origins.findall(line):
                # localhost is the server-side default for SSR and tests, which
                # never runs in a browser.
                if "localhost" in url or "127.0.0.1" in url:
                    continue
                # Schema/namespace identifiers are not fetched.
                if "w3.org" in url or "schema.org" in url:
                    continue
                offenders.append(f"{path.relative_to(SRC)}:{i} {url}")
    assert not offenders, (
        "the CSP is `connect-src 'self'` and this platform states that nothing "
        "leaves the deployment. Vendor the asset into frontend/public/:\n  "
        + "\n  ".join(offenders)
    )


def test_the_basemap_is_vendored_and_intact() -> None:
    """A path that 404s renders an empty map just as silently as a blocked one."""
    basemap = ROOT / "frontend" / "public" / "countries-110m.json"
    assert basemap.exists(), "public/countries-110m.json is missing"

    src = (SRC / "components" / "GlobalMap.tsx").read_text(encoding="utf-8")
    assert "'/countries-110m.json'" in src

    topology = json.loads(basemap.read_text(encoding="utf-8"))
    assert topology.get("type") == "Topology"
    countries = topology["objects"]["countries"]["geometries"]
    # 177 at world-atlas v2. A truncated or wrong file would still parse.
    assert len(countries) > 150, f"only {len(countries)} country geometries"


# ── who is signed in, and where they are allowed to go ──────────────────────


def test_protected_routes_are_actually_protected() -> None:
    """There was no middleware at all.

    Every route under `(dashboard)` rendered for anyone who typed the URL: an
    anonymous visitor got the full command centre, sixteen panels each failing
    its own 401, a header reading "Not signed in", and nothing offering a way
    to sign in. The application looked broken rather than closed, which is the
    worse of the two -- a person cannot tell whether to report an outage or to
    log in.
    """
    middleware = ROOT / "frontend" / "src" / "middleware.ts"
    assert middleware.exists(), "no middleware: every page renders for anyone"

    src = middleware.read_text(encoding="utf-8")
    assert "'/login'" in src and "next" in src, "turning someone away must carry where they were"
    # An absolute `next` would make the sign-in flow an open redirect.
    assert "startsWith('//')" in src, "`next` must be rejected unless it is a same-origin path"
    assert "matcher" in src and "api" in src, (
        "/api/* must be excluded: those answer 401, and redirecting a fetch to "
        "an HTML login page gives every panel a parse error instead of a status"
    )


def test_the_session_is_asked_for_once() -> None:
    """Three components fetched /api/auth/session and could disagree."""
    src_files = [p for p in _components() if p.name != "SessionContext.tsx"]
    callers = sorted(
        str(p.relative_to(SRC)).replace("\\", "/")
        for p in src_files
        if "/api/auth/session" in _strip_comments(p.read_text(encoding="utf-8"))
    )
    # useLiveEvents probes at the moment a socket closes, to tell a refused
    # handshake from a network fault. That is a point-in-time question, not
    # held state, and threading the context into a non-React reconnect handler
    # would buy nothing.
    assert callers in ([], ["lib/useLiveEvents.ts"]), (
        "read the shared session via useSession() instead of fetching it: " + ", ".join(callers)
    )


def test_there_is_a_way_in_and_a_way_out() -> None:
    """Neither existed.

    The account button opened a profile panel whether or not there was a
    profile, and `/api/auth/logout` -- which clears the cookie correctly -- had
    no caller anywhere in the codebase. The only way out of a session was to
    wait 24 hours or to clear cookies by hand.
    """
    header = (SRC / "components" / "ui" / "Header.tsx").read_text(encoding="utf-8")
    assert "signInHref" in header, "the header must offer a way in when anonymous"
    assert "status === 'anonymous'" in header
    # Three states. Flashing "Sign in" at a signed-in operator on every page
    # load is how an interface teaches someone to distrust what it says.
    assert "status === 'loading'" in header, "loading is not the same as anonymous"

    joined = "\n".join(p.read_text(encoding="utf-8") for p in _components())
    assert "/api/auth/logout" in joined, "nothing signs the user out"


def test_the_login_screen_offers_no_way_around_itself() -> None:
    """A "DEV MODE ACTIVE — Proceed to Dashboards" link shipped to production.

    It was gated on nothing: no NODE_ENV check, no flag. The label was
    decoration.
    """
    login = (SRC / "app" / "login" / "page.tsx").read_text(encoding="utf-8")
    body = _strip_comments(login)
    assert "Proceed to Dashboards" not in body
    assert "DEV MODE" not in body


def test_health_is_fetched_without_a_trailing_slash() -> None:
    """Reaching for the slash looks right and costs a browser round trip.

    The gateway answers `/api/v1/health/` and 307s the slashless form -- but
    Next normalises trailing slashes on the proxy route, so `/health/` 308s in
    the browser before anything is forwarded. Without it the proxy's own fetch
    follows the gateway's redirect server-side and the browser sees one 200.

    Measured through the deployment: `health` -> 200, `health/` -> 308.
    """
    offenders = [
        f"{p.relative_to(SRC)}"
        for p in _components()
        if "'/health/'" in _strip_comments(p.read_text(encoding="utf-8"))
    ]
    assert not offenders, "use '/health', not '/health/': " + ", ".join(offenders)


# ── the loops that had no first step ────────────────────────────────────────


def test_the_verdict_vocabulary_matches_the_server() -> None:
    """An analyst marking a rule wrong reached nothing at all.

    `POST /feedback` existed, `/feedback/rules` aggregated it worst-first, the
    operations console rendered that aggregate, and the agent that decides
    which rules survive was handed the names it produced. Every part of the
    loop was built except the one where a person says anything -- so the
    scorecard was always empty and the retirement agent was always asked to
    judge on no evidence.

    The four verdicts are the server's vocabulary. A fifth on either side, or a
    rename, files verdicts under a value the aggregate ignores.
    """
    from services.api_gateway.routes.feedback import VERDICTS

    component = (SRC / "components" / "RuleVerdict.tsx").read_text(encoding="utf-8")
    declared = set(re.findall(r"id: '([a-z_]+)'", component))
    assert declared == set(VERDICTS), (
        f"the UI offers {sorted(declared)} and the server accepts {sorted(VERDICTS)}"
    )


def test_the_filings_page_shows_the_filings() -> None:
    """The page's own subtitle promised a disclosure stream it did not render.

    It showed one panel -- 13F institutional holdings -- while
    `/filings/latest` served the 8-Ks, 424Bs and 10-Ks it names, with no
    caller.
    """
    page = (SRC / "app" / "(dashboard)" / "filings" / "page.tsx").read_text(encoding="utf-8")
    assert "FilingsStream" in page
    stream = (SRC / "components" / "FilingsStream.tsx").read_text(encoding="utf-8")
    assert "/filings/latest" in stream
    # Materiality is the server's judgement, not a keyword match made here.
    assert "is_material_8k" in stream


def test_the_sovereignty_claims_are_read_rather_than_written() -> None:
    """The modal asserted the platform's integrity claims as hardcoded copy.

    `/system/sovereignty` measures them -- including `audit_chain_status`,
    which reads the real ledger and distinguishes an empty chain from a
    verified one.
    """
    modal = (SRC / "components" / "DataSovereigntyModal.tsx").read_text(encoding="utf-8")
    assert "/system/sovereignty" in modal, "the manifest must be read, not restated"
    body = _strip_comments(modal)
    assert "100% Local" not in body, "the score is served; it must not be a literal"
    # EMPTY_LEDGER is not a pass, and the backend was changed to say so.
    assert "EMPTY_LEDGER" in modal and "VERIFIED_VALID" in modal, (
        "an intact chain and no chain at all must not render the same way"
    )


def test_failed_events_carry_their_cause() -> None:
    """The console offered Replay with the exception withheld.

    `/dlq/summary` gives counts per topic and that is all it rendered.
    `/dlq/events` carries the rows and the error that killed each one -- which
    on the running deployment read `Couldn't connect to neo4j`, and replaying a
    hundred of those at a dead Neo4j just fails them again.
    """
    console = (SRC / "components" / "OperationsConsole.tsx").read_text(encoding="utf-8")
    assert "/api/v1/dlq/events" in console
    assert "last_replay_error" in console, "a replay that failed again must say so"
