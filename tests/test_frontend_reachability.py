"""Every page must be reachable, and every new signal must be visible.

A route with no link is a page that does not exist as far as a user is
concerned. /account was built with the plan and billing state on it and linked
from nowhere; /filings -- one of this deployment's priority domains -- and
/methodology were the same.

The corroboration assessment has the same failure mode in a different form: a
signal computed, persisted and never rendered is work nobody can act on.
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend/src"
sys.path.insert(0, str(ROOT))


def _all_links() -> str:
    return "\n".join(
        p.read_text(encoding="utf-8")
        for p in FRONTEND.rglob("*.tsx")
    )


def _routes():
    """Dashboard routes a user can navigate to, ignoring route groups."""
    found = set()
    for page in (FRONTEND / "app").rglob("page.tsx"):
        rel = page.relative_to(FRONTEND / "app").parent.as_posix()
        segments = [s for s in rel.split("/") if s and not s.startswith("(")]
        found.add("/" + "/".join(segments) if segments else "/")
    return found


# Reached by redirect or by an unauthenticated flow rather than by a nav link.
_ENTRY_ROUTES = {"/", "/login", "/signup", "/verify", "/reset", "/forgot"}


@pytest.mark.parametrize("route", sorted(_routes() - _ENTRY_ROUTES))
def test_every_page_is_linked_from_somewhere(route):
    links = _all_links()
    assert f'"{route}"' in links or f"'{route}'" in links, (
        f"{route} exists but nothing links to it, so no user can reach it"
    )


def test_the_plan_page_is_reachable_from_the_account_surface():
    """Billing lives beside account settings, not in the intelligence nav."""
    modal = (FRONTEND / "components/AccountProfileModal.tsx").read_text(encoding="utf-8")
    assert "/account" in modal


def test_priority_domains_appear_in_the_navigation():
    """Filings is a named priority domain and had no entry."""
    sidebar = (FRONTEND / "components/ui/Sidebar.tsx").read_text(encoding="utf-8")
    # /graph is deliberately absent: the knowledge-graph view was removed from
    # the product. Its backend and Neo4j remain, feeding the agent swarm.
    for route in ("/filings", "/osint", "/intelligence"):
        assert route in sidebar, f"{route} is missing from the navigation"
    assert "/graph" not in sidebar, "the removed graph view is back in the nav"


def test_corroboration_is_rendered_not_merely_computed():
    feed = (FRONTEND / "components/IntelligenceFeed.tsx").read_text(encoding="utf-8")
    assert "corroboration" in feed, "the assessment is computed and never shown"
    assert "is_single_sourced" in feed, "the state worth flagging is not surfaced"


def test_the_badge_renders_nothing_when_corroboration_does_not_apply():
    """A market tick has no second source; labelling it would be noise."""
    feed = (FRONTEND / "components/IntelligenceFeed.tsx").read_text(encoding="utf-8")
    # Named CorroborationBadge since it was rebuilt as a component rather than a
    # helper returning JSX. The guarded property is unchanged.
    fn = feed[feed.index("function CorroborationBadge"):]
    assert "if (!c) return null;" in fn[:400], (
        "events without an assessment would render a misleading badge"
    )


def test_the_client_type_matches_what_the_backend_emits():
    """A field renamed on one side and not the other renders undefined."""
    from shared.utils.corroboration import CorroborationAssessment, CorroborationTracker

    tracker = CorroborationTracker()
    emitted = set(tracker.observe("a claim about a thing", source="s", now=0).to_dict())

    types_src = (FRONTEND / "lib/types.ts").read_text(encoding="utf-8")
    block = types_src[types_src.index("export interface Corroboration"):]
    block = block[:block.index("}")]
    declared = set(re.findall(r"(\w+)\??:", block))
    assert emitted == declared, f"backend emits {emitted}, client declares {declared}"
