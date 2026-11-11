"""The relationship explorer: a page and a sidebar entry for graph routes that had no screen."""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_the_page_is_reachable_from_the_sidebar() -> None:
    assert "href: '/connections'" in _read("components/ui/Sidebar.tsx")
    assert (SRC / "app/(dashboard)/connections/page.tsx").exists()


def test_it_reads_the_graph_routes_and_says_when_a_request_failed() -> None:
    page = _read("components/Connections.tsx")
    for route in ("/graph/entities", "/graph/entity/", "/graph/shortest-path"):
        assert route in page, route
    assert page.count("failedWithout(") >= 3 and "<PanelError" in page


def test_a_missing_path_is_reported_not_guessed() -> None:
    page = _read("components/Connections.tsx")
    assert "No stored link joins" in page and "does not prove they are unrelated" in page
    logic = _read("lib/connections.ts")
    assert "never a guessed chain" in logic and "names.length < 2" in logic


def test_links_from_events_are_marked_as_hints() -> None:
    page = _read("components/Connections.tsx")
    assert "inferred from recent events" in page and "fromEvents" in page


def test_node_colours_are_theme_tokens_only() -> None:
    import re
    page = _read("components/Connections.tsx")
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b", page)
