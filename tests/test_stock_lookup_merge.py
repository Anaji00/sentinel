"""Look Up a Stock lives in Calls: a search box, the stock drawer, and a redirect for the old address.

The page had its own sidebar entry, a full-page layout and ~17 links from other
pages. Merging it into Calls means none of those may point at it again, and what
only it showed (candles, fund holders, options, surveillance business) has to
still be reachable from the drawer.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_the_old_page_is_gone_and_redirects() -> None:
    assert not (SRC / "app" / "(dashboard)" / "ticker").exists()
    config = (ROOT / "frontend" / "next.config.ts").read_text(encoding="utf-8")
    assert re.search(r'source: "/ticker",.*destination: "/calls\?t=:symbol"', config)
    assert re.search(r'source: "/ticker", destination: "/calls"', config)


def test_no_component_links_to_the_old_address() -> None:
    offenders = []
    for path in SRC.rglob("*.ts*"):
        if "__tests__" in path.parts or "node_modules" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if re.search(r"/ticker\?symbol=|href: '/ticker'|\['/ticker'", text):
            offenders.append(str(path.relative_to(SRC)))
    assert not offenders, offenders


def test_the_sidebar_and_palette_no_longer_list_a_separate_page() -> None:
    assert "Look Up a Stock" not in _read("components/ui/Sidebar.tsx")
    assert "'p:/ticker'" not in _read("components/ui/CommandPalette.tsx")


def test_calls_has_the_search_and_the_drawer_has_the_full_detail() -> None:
    assert "<StockSearch />" in _read("app/(dashboard)/calls/page.tsx")
    drawer = _read("components/StockDrawer.tsx")
    assert "<TickerInspector symbol={ticker} embedded />" in drawer
    assert "<TickerSurveillance symbol={ticker} />" in drawer
    assert "aria-expanded={detail}" in drawer
    assert "import('./TickerInspector')" in drawer, "the inspector must load on demand: the drawer is in every page's layout"


def test_the_embedded_inspector_does_not_repeat_the_evidence_the_drawer_shows() -> None:
    inspector = _read("components/TickerInspector.tsx")
    assert "{!embedded && <TickerEvidence ticker={ticker} />}" in inspector
    assert "{!embedded && (<>" in inspector, "the drawer already carries the search"
