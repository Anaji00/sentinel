"""The header search: one visible box on every page, opening the drawer without losing the page."""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_the_header_carries_the_search_on_every_page() -> None:
    header = _read("components/ui/Header.tsx")
    assert "<HeaderSearch />" in header, "the search belongs in the header, which every dashboard page shares"
    assert "Ctrl" in header, "the palette shortcut stays visible next to it"


def test_the_search_is_a_labelled_combobox_that_opens_the_drawer() -> None:
    box = _read("components/ui/HeaderSearch.tsx")
    assert "<Input" in box and "<input" not in box, "use the shared Input, which requires a label"
    assert 'role="combobox"' in box and 'role="listbox"' in box and 'role="option"' in box
    assert "aria-activedescendant" in box and "aria-controls" in box
    assert "stockHref(" in box and "scroll: false" in box, "open the drawer in place, keeping the page's other parameters"


def test_the_slash_key_never_steals_typing() -> None:
    box = _read("components/ui/HeaderSearch.tsx")
    assert "isTypingTarget(e.target)" in box
    for tag in ("INPUT", "TEXTAREA", "SELECT", "isContentEditable"):
        assert tag in box


def test_names_load_only_when_the_box_is_used_and_never_per_keystroke() -> None:
    box = _read("components/ui/HeaderSearch.tsx")
    assert "useStockNames(touched)" in box and "started.current" in box
    assert box.count("fetcher(") == 1
