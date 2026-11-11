"""Backend features that had no screen now have one, wired where a reader will find them."""
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_chip_prices_are_a_markets_view_reading_the_new_route() -> None:
    assert "key: 'chips'" in _read("app/(dashboard)/markets/page.tsx") and "<ChipPrices />" in _read("app/(dashboard)/markets/page.tsx")
    assert "/commodities/chips" in _read("components/ChipPrices.tsx")


def test_source_facts_sit_in_the_drawer_and_say_when_no_case_file_exists() -> None:
    assert "<SourceFacts" in _read("components/StockDrawer.tsx")
    facts = _read("components/SourceFacts.tsx")
    assert "isMissing" in facts and "No case file is built" in facts


def test_exposure_and_oil_panels_are_on_their_pages() -> None:
    assert "<BookExposure />" in _read("app/(dashboard)/performance/page.tsx")
    assert "<OilRoutes />" in _read("app/(dashboard)/markets/page.tsx")
    assert "Estimates, not forecasts" in _read("components/BookExposure.tsx")
    assert "not proof" in _read("components/OilRoutes.tsx")


def test_what_if_is_on_the_scenarios_tab_and_handles_the_paywall() -> None:
    assert "<WhatIf />" in _read("components/IntelligenceFeed.tsx")
    form = _read("components/WhatIf.tsx")
    assert "/scenarios/request" in form and "402" in form and "not a forecast" in form


def test_jargon_has_a_plain_english_hover_and_shortcuts_are_discoverable() -> None:
    assert "<Term word=" in _read("components/ModelRanking.tsx") and "<Term word=" in _read("components/ModelPortfolio.tsx")
    assert 'role="tooltip"' in _read("components/ui/Term.tsx") and "aria-describedby" in _read("components/ui/Term.tsx")
    assert "<ShortcutsSheet />" in _read("app/(dashboard)/layout.tsx")
    assert "OPEN_SHORTCUTS_EVENT" in _read("components/ui/Sidebar.tsx"), "the sheet must be reachable without knowing the key"
    assert "e.key !== '?'" in _read("components/ui/ShortcutsSheet.tsx") and "typingIn(e.target)" in _read("components/ui/ShortcutsSheet.tsx")


def test_calls_markets_and_intel_open_with_one_headline_number() -> None:
    assert "<CallsHeadline />" in _read("app/(dashboard)/calls/page.tsx")
    assert "<MarketsHeadline />" in _read("app/(dashboard)/markets/page.tsx")
    assert "<IntelHeadline />" in _read("app/(dashboard)/intel/page.tsx")
    heads = _read("components/PageHeadlines.tsx")
    assert heads.count("failedWithout(") == 1 and "<PanelError" in heads, "an unreadable headline says so, never a blank that looks like zero"
    assert "Ship tracking is not reporting right now." in _read("lib/headlines.ts")


def test_panels_say_when_they_were_updated_and_slow_loads_say_what_they_wait_for() -> None:
    assert "recordUpdate(key)" in _read("components/ui/DataProvider.tsx")
    for name in ("ChipPrices", "CommodityBoard", "OilRoutes", "BookExposure", "ModelPortfolio", "AutoTradePanel", "TopCalls"):
        assert "<Updated source=" in _read(f"components/{name}.tsx"), name
    assert "toLocale" not in _read("components/ui/Updated.tsx"), "times follow the shared zone, not the machine's"
    assert "CAPTION_MS" in _read("components/ui/Skeleton.tsx")


def test_the_drawer_teaches_its_layout_once_and_markets_groups_its_cards() -> None:
    assert "<FirstLook />" in _read("components/StockDrawer.tsx")
    first = _read("components/FirstLook.tsx")
    assert "hasSeenFirstLook" in first and "markFirstLookSeen" in first
    assert "labelled groups" in _read("components/MarketCharts.tsx")


def test_13f_filer_names_are_never_chopped_mid_word() -> None:
    panel = _read("components/Institutional13FPanel.tsx")
    assert ".slice(0, 22)" not in panel, "Pershing Square Capita, Renaissance Technologi: a hard cut shows half a name"
    assert "truncate" in panel and "title={f.filer_name}" in panel, "overflow ends in an ellipsis and the full name is one hover away"


def test_13f_tiles_read_the_detail_routes_field_names_and_values_are_readable() -> None:
    panel = _read("components/Institutional13FPanel.tsx")
    assert "reportValueUsd(report)" in panel and "reportPositions(report)" in panel, "the detail route has its own names for the totals"
    assert "report.total_value_usd" not in panel and "report.holdings_count" not in panel
    assert "/ 1e6).toFixed" not in panel and "formatCurrency(pos.market_value_usd" in panel, "$65950.30M reads as noise; $65.95B does not"
    assert "'N/A'" not in panel, "a missing ticker is left out, not shown as a chip reading N/A"


def test_13f_switching_manager_or_tab_never_mixes_holdings() -> None:
    panel = _read("components/Institutional13FPanel.tsx")
    assert "let cancelled = false" in panel and "cancelled = true" in panel, "a slower earlier answer must not overwrite the manager now selected"
    assert "setReport(null);\n      try" in panel.replace("\r\n", "\n"), "the old manager's companies are cleared the moment another is chosen"
    assert "key={`${activeTab}-${pos.cusip" in panel, "two share classes of one company need distinct keys"
    assert "setActiveTab('top')" in panel


def test_the_prominent_filer_roster_names_current_leaders() -> None:
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "services/collector-filings/thirteen_f.py").read_text(encoding="utf-8")
    assert '"manager": "Greg Abel / Warren Buffett (chair)"' in src
    assert "Ray Dalio" not in src.split("PROMINENT_FILERS")[1].split("_DYNAMIC_TITLE_TO_TICKER")[0].replace("Ray Dalio has fully exited", "")


def test_divergence_alerts_show_their_stage_and_all_seven_brief_sections() -> None:
    feed = _read("components/IntelligenceFeed.tsx")
    assert "<StageBadge situation={c.situation}" in feed and "<DivergenceBrief" in feed
    words = _read("lib/divergence.ts")
    for key in ("what_happened", "catalyst", "peers", "evidence", "positioning", "watch", "limits"):
        assert f"'{key}'" in words, key
    assert "NOT_PROVIDED" in words and "isNotMeasured" in words, "'not measured' is shown, never hidden"


def test_the_look_is_flat_and_the_live_pieces_are_wired() -> None:
    import re
    css = _read("app/globals.css")
    assert "--radius: 6px;" in css and "radial-gradient" not in css, "square hairline panes, no decorative glow"
    for rel in ("components", "app"):
        for p in (SRC / rel).rglob("*.tsx"):
            text = p.read_text(encoding="utf-8")
            assert not re.search(r"\brounded-(?:[tblr]-)?(?:xl|2xl)\b|backdrop-blur|shadow-(?:lg|xl|2xl)\b", text), f"{p.name}: puffy card styling is back"
    assert "<MarketMood />" in _read("components/ui/Header.tsx")
    feed = _read("components/IntelligenceFeed.tsx")
    assert "ALERT_KINDS.map" in feed and "filterAlerts(" in feed


def test_two_stocks_can_be_compared_from_the_address_the_drawer_and_the_desk() -> None:
    assert "useCompare" in _read("components/RelatedStocks.tsx") and "Compare" in _read("components/RelatedStocks.tsx")
    assert "<ComparePanel" in _read("components/StockDrawer.tsx") and "<ComparePanel" in _read("components/Desk.tsx")
    panel = _read("components/ComparePanel.tsx")
    assert "/predictions/closes/" in panel and "failedWithout(" in panel and "do not share enough trading days" in panel
    assert "next.delete('c')" in _read("components/StockDrawer.tsx"), "closing the drawer drops the comparison"
