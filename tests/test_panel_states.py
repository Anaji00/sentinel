"""A failed request must not look like an empty panel: failures are recorded centrally and said aloud.

26 components fetch without reading `error`, and several draw lists from `data ?? []`, which is
empty whether the server had nothing or could not be reached. Rather than wrapping each one, the
data provider records every failure, the shared spinner turns into an error with a retry, and a strip
at the top of the page says that information is not updating.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"


def _read(rel: str) -> str:
    return (SRC / rel).read_text(encoding="utf-8")


def test_the_provider_records_failures_and_clears_them_on_success() -> None:
    provider = _read("components/ui/DataProvider.tsx")
    assert "recordFailure(key)" in provider and "onSuccess:" in provider and "recordSuccess(key)" in provider
    assert "isUnauthenticated || error.isMissing" in provider, "401 has its redirect and 404 often means nothing collected yet"


def test_the_shared_spinner_turns_into_an_error_with_a_retry() -> None:
    skeleton = _read("components/ui/Skeleton.tsx")
    assert "useFailingSources()" in skeleton and "Try again" in skeleton and "mutate(() => true)" in skeleton


def test_every_dashboard_page_gets_the_strip() -> None:
    layout = _read("app/(dashboard)/layout.tsx")
    assert "<DataHealthStrip />" in layout
    assert layout.index("<DataHealthStrip />") < layout.index("{children}")


def test_the_strip_is_announced_and_keyboard_operable() -> None:
    strip = _read("components/ui/DataHealthStrip.tsx")
    assert 'role="status"' in strip and 'type="button"' in strip


def test_failures_only_count_after_a_grace_period() -> None:
    health = _read("lib/dataHealth.ts")
    assert "GRACE_MS = 6_000" in health, "one dropped request the next poll repairs must not raise a warning"


# Panels that draw a list from `data ?? []` and used to say "none found" when their own request failed.
LIST_PANELS = (
    "CryptoAnalytics", "PredictionMarketPanel", "QuantRadarPanel", "TradingSignals", "LiveTape", "WhoIsWatching",
    "TodayOverview", "FinancialAdvisorAdvice", "OsintThreatMatrix", "DarkPoolFlowPanel",
)


def test_list_panels_say_so_when_their_own_request_failed() -> None:
    for name in LIST_PANELS:
        src = _read(f"components/{name}.tsx")
        assert "failedWithout(" in src and "<PanelError" in src, f"{name} would show an empty list for a failed request"
        assert "import { PanelError } from './ui/PanelError';" in src, name


def test_the_failure_notice_is_announced_and_never_replaces_older_data() -> None:
    notice = _read("components/ui/PanelError.tsx")
    assert 'role="status"' in notice and 'type="button"' in notice
    health = _read("lib/dataHealth.ts")
    assert "Boolean(error) && (data === undefined || data === null)" in health, (
        "a failed refresh with older data on screen is the page strip's job, not a reason to blank a panel"
    )
