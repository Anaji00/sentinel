"""The stock drawer opens short: the essentials first, the rest folded and loaded only when opened.

It had grown to seven sections and about ten requests on open. Nothing was removed: owners, patterns and
related stocks sit under one "More about" row, and the secondary measures sit under "more measures".
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_the_essentials_come_first_and_the_rest_is_folded() -> None:
    drawer = _read("components/StockDrawer.tsx")
    order = [drawer.index(x) for x in ("<TickerHero", "<WhatChanged", "<ThesisPanel", '<Disclosure variant="row"')]
    assert order == sorted(order), "hero, what changed, bull and bear, then the folded section"
    start, end = drawer.index('<Disclosure variant="row"'), drawer.index("</Disclosure>")
    folded = drawer[start:end]
    for part in ("<Owners", "<TickerEvidence", "<RelatedStocks"):
        assert part in folded, f"{part} should sit under More about"
    for part in ("<TickerHero", "<WhatChanged", "<ThesisPanel"):
        assert part not in folded, f"{part} should stay visible"


def test_a_folded_section_is_not_rendered_or_fetched_until_opened() -> None:
    disclosure = _read("components/ui/Disclosure.tsx")
    assert "<details" in disclosure, "native element: keyboard operable and announced as expandable"
    assert "{open && <div" in disclosure
    assert "readOpen(storageKey, defaultOpen)" in disclosure and "React.useEffect" in disclosure, (
        "the remembered choice is read after mount, so server and first client render agree"
    )


def test_what_changed_keeps_trades_visible_and_folds_the_secondary_measures() -> None:
    changed = _read("components/WhatChanged.tsx")
    fold_start = changed.index('<Disclosure label="more measures"')
    fold = changed[fold_start:changed.index("</Disclosure>")]
    for label in ('label="Options"', 'label="Short sellers"', 'label="Moves with"'):
        assert label in fold, label
    for label in ('label="Price"', 'label="Model rank"', 'label="Large trades"', 'label="Recent items"'):
        assert label in changed and label not in fold, f"{label} stays visible"
    assert "slice(0, 4)" in changed and "Show all" in changed


def test_the_unknown_side_money_is_never_folded_away() -> None:
    changed = _read("components/WhatChanged.tsx")
    assert changed.index('label="Large trades"') < changed.index('<Disclosure label="more measures"')
    assert "v.caution" in changed
