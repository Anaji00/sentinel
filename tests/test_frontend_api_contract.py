"""The boundary between the two tiers, which nothing tested.

3,015 backend tests stop at the response object and 71 frontend tests start
after it has been parsed. Nothing met in the middle, and that single gap
accounts for a specific cluster of defects found by hand:

  * a health panel reading five fields the endpoint has never returned, so
    three healthy datastores rendered as DISCONNECTED forever;
  * a kill switch POSTing to a path with no route, with no error branch, so it
    silently did nothing;
  * a 13F panel whose fetch always 404'd, so it always rendered fabricated
    holdings -- badged as a live measurement;
  * two panels reading `data?.events` from an endpoint that returns a bare
    array, so their invented fallback rows fired on every successful fetch.

Every one is a contract failure, and every one is invisible to a type
annotation: `useSWR<T>` asserts over parsed JSON rather than checking it, so
declaring an interface the API does not implement compiles cleanly.

These tests read the frontend source and the gateway's own route table. They
need no running backend.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend" / "src"

# Paths the Next.js origin serves itself. Everything else must go through the
# BFF, which is the only place the session cookie is verified.
_NEXT_OWN_ROUTES = ("/api/proxy/", "/api/auth/")


def _tsx_files():
    if not FRONTEND.exists():                      # frontend not checked out
        pytest.skip("frontend sources not present")
    for p in sorted(FRONTEND.rglob("*.tsx")):
        yield p
    for p in sorted(FRONTEND.rglob("*.ts")):
        yield p


def _gateway_paths() -> set[str]:
    """Every path the gateway actually serves, from the app's own router."""
    from services.api_gateway.routes.main import app
    return {r.path for r in app.routes if getattr(r, "path", None)}


# ── 1. Every URL the frontend fetches must exist ────────────────────────────

def test_no_component_fetches_a_path_the_frontend_does_not_serve():
    """A bare /api/v1/... fetch hits Next.js, not the gateway.

    Five call sites did this -- three flag controls, the trade-execution button
    and the 13F panel. Next returns its 404 HTML page, `res.ok` is false, and
    two of the five had no else branch at all.
    """
    offenders = []
    for path in _tsx_files():
        src = path.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r"""fetch\(\s*[`'"]([^`'"]+)[`'"]""", src):
            url = m.group(1)
            if not url.startswith("/"):
                continue                            # absolute or templated host
            if url.startswith(_NEXT_OWN_ROUTES):
                continue
            offenders.append(f"{path.relative_to(ROOT)}: fetch('{url}')")
    assert not offenders, (
        "these fetch a path the Next.js origin does not route; they reach its "
        "404 page rather than the gateway:\n  " + "\n  ".join(offenders)
    )


# ── 2. Every endpoint a panel binds to must exist on the gateway ────────────

def test_every_swr_endpoint_exists_on_the_gateway():
    bindings = []
    for path in _tsx_files():
        src = path.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r"""useSWR[^(]*\(\s*[`'"](/[^`'"?]+)""", src):
            bindings.append((path.relative_to(ROOT), m.group(1)))

    assert bindings, "no useSWR bindings found -- has the extraction broken?"

    served = _gateway_paths()
    # Gateway routes are parameterised -- /api/v1/events/{domain} serves
    # /events/crypto, /events/maritime and the rest -- so compare by pattern.
    patterns = [
        re.compile("^" + re.sub(r"\{[^}]+\}", "[^/]+", re.escape(r).replace(r"\{", "{").replace(r"\}", "}")) + "$")
        for r in served
    ]

    def _served(ep: str) -> bool:
        for candidate in (ep, f"/api/v1{ep}"):
            if any(p.match(candidate) for p in patterns):
                return True
        return False

    missing = [
        f"{p}: {ep}" for p, ep in bindings
        if not ep.startswith(_NEXT_OWN_ROUTES) and not _served(ep)
    ]
    assert not missing, (
        "components poll endpoints the gateway does not serve:\n  " + "\n  ".join(missing)
    )


# ── 3. A panel must not read a field off a list response ────────────────────

_LIST_ENDPOINTS = ("/events/", "/filings/latest", "/correlations", "/scenarios")


def test_no_panel_reads_a_property_off_an_array_endpoint():
    """`data?.events` on an endpoint that returns a bare array is always undefined.

    Which means the `||` beside it fires on the *successful* path, forever --
    and both panels that did this fell back to invented records stamped
    `occurred_at: new Date()`, refreshed every few seconds.
    """
    offenders = []
    for path in _tsx_files():
        src = path.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(
            r"""useSWR[^(]*\(\s*[`'"](/[^`'"?]+)[^)]*?\)[\s\S]{0,400}?data\?\.(\w+)""",
            src,
        ):
            endpoint, field = m.group(1), m.group(2)
            if any(endpoint.startswith(e) for e in _LIST_ENDPOINTS):
                offenders.append(
                    f"{path.relative_to(ROOT)}: reads data?.{field} from {endpoint}, "
                    "which returns a bare array"
                )
    assert not offenders, "\n  ".join(offenders)


# ── 4. Fabricated records must not sit behind a `||` ────────────────────────

def test_no_panel_falls_back_to_invented_records():
    """A fixture dated `now` is indistinguishable from a live detection.

    Three panels carried hardcoded rows -- institutional block trades with
    real-format OCC option symbols, an OFAC sanctions alert in the Strait of
    Hormuz, a Berkshire top-ten -- and two of them stamped `occurred_at:
    new Date()`, so they refreshed as live on a timer.
    """
    offenders = []
    for path in _tsx_files():
        if "__tests__" in str(path):
            continue
        src = path.read_text(encoding="utf-8", errors="replace")
        # A literal record array carrying a live timestamp is the shape that
        # cannot be told apart from real data.
        code = chr(10).join(
            line for line in src.splitlines()
            if not line.lstrip().startswith(("//", "*", "/*"))
        )
        if re.search(r"occurred_at:\s*new Date\(\)", code):
            offenders.append(f"{path.relative_to(ROOT)}: literal record stamped with new Date()")
    assert not offenders, (
        "panels fall back to invented records that present as live:\n  " + "\n  ".join(offenders)
    )


# ── 5. The health panel's contract, which was wrong in every field ──────────

def test_the_health_panel_reads_fields_the_endpoint_publishes():
    hud = FRONTEND / "components" / "SystemHealthHUD.tsx"
    if not hud.exists():
        pytest.skip("SystemHealthHUD not present")
    src = hud.read_text(encoding="utf-8", errors="replace")

    reads = set(re.findall(r"healthData\?\.(\w+)", src))
    assert reads, "extraction found no field reads"

    health_src = (ROOT / "services" / "api_gateway" / "routes" / "health.py").read_text(
        encoding="utf-8", errors="replace"
    )
    published = set(re.findall(r'status\["(\w+)"\]', health_src))
    published |= set(re.findall(r'"(\w+)":', health_src))
    published |= set(re.findall(r'\.get\("(\w+)"', health_src))
    published |= set(re.findall(r'status\.setdefault\("(\w+)"', health_src))

    missing = sorted(r for r in reads if r not in published)
    assert not missing, (
        "the health panel reads fields /api/v1/health/data does not publish: "
        f"{missing}. It publishes `system_status`; reading `status` is how three "
        "healthy datastores rendered as DISCONNECTED."
    )


# ── 6. Provenance: the vocabulary both tiers switch on ──────────────────────

def _frontend_provenance_union() -> set[str]:
    """The ProvenanceType union the badge component switches on."""
    src = (FRONTEND / "components" / "ProvenanceBadge.tsx").read_text(encoding="utf-8")
    match = re.search(r"export type ProvenanceType\s*=\s*(.*?);", src, re.S)
    assert match, "ProvenanceBadge no longer declares a ProvenanceType union"
    return set(re.findall(r'"([a-z_]+)"', match.group(1)))


def test_the_provenance_vocabulary_is_the_same_on_both_sides():
    """A frontend union and a backend enum, neither of which imports the other.

    `ProvenanceBadge` switches on four literal strings. `ProvenanceSourceType`
    declares four members. Nothing has ever checked that they are the same four:
    a member added or renamed on either side falls through the switch to the
    default styling and renders as "Computed", which is a claim about the data
    rather than a missing case.
    """
    from shared.models.provenance import ProvenanceSourceType

    backend = {member.value for member in ProvenanceSourceType}
    frontend = _frontend_provenance_union()
    assert frontend == backend, (
        f"ProvenanceType and ProvenanceSourceType disagree. "
        f"Only in the frontend: {sorted(frontend - backend)}. "
        f"Only in the backend: {sorted(backend - frontend)}."
    )


def test_the_badge_handles_every_declared_provenance_type():
    src = (FRONTEND / "components" / "ProvenanceBadge.tsx").read_text(encoding="utf-8")
    handled = set(re.findall(r'case\s+"([a-z_]+)":', src))
    declared = _frontend_provenance_union()
    missing = sorted(declared - handled)
    assert not missing, (
        f"ProvenanceBadge declares these types and has no case for them: {missing}. "
        "They fall through to the neutral 'Computed' styling, which asserts the "
        "opposite of what an unhandled case means."
    )


# Panels that render a provenance-carrying value through the badge. This is a
# ratchet: 22 panels exist and most render numbers with no epistemic label at
# all, so a reader cannot tell a Treasury print from a model's guess. The number
# may go up. It may not go down.
_MIN_PANELS_WITH_PROVENANCE = 3


def test_provenance_coverage_does_not_regress():
    # Recursive: the chart components live in components/charts/, and a glob
    # that stopped at the top level would have counted them as absent.
    panels = sorted(
        p for p in (FRONTEND / "components").rglob("*.tsx") if p.parent.name != "ui"
    )
    using = [
        p.name for p in panels
        if p.name not in ("ProvenanceBadge.tsx", "ProvenanceValue.tsx")
        and re.search(r"\bProvenance(Badge|Value)\b", p.read_text(encoding="utf-8"))
    ]
    assert len(using) >= _MIN_PANELS_WITH_PROVENANCE, (
        f"{len(using)} of {len(panels)} panels label their numbers with provenance, "
        f"down from {_MIN_PANELS_WITH_PROVENANCE}: {using}"
    )
