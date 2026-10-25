"""The daily check that minute bars add up to the day."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.bar_quality import RECONCILE_SQL, reconcile  # noqa: E402


def _row(t, dv, mv, n):
    return {"ticker": t, "day": "2026-10-14", "daily_volume": dv, "minute_volume": mv, "minutes": n}


def test_consolidated_minutes_pass_and_iex_minutes_are_flagged() -> None:
    ok = reconcile([_row("NVDA", 119.4e6, 113.4e6, 391), _row("SPY", 55.8e6, 48.4e6, 391),
                    _row("TSLA", 31.3e6, 29.0e6, 391)])
    assert ok["ok"] is True and ok["below_floor"] == 0 and ok["median_coverage"] > 0.9
    bad = reconcile([_row("NVDA", 119.4e6, 3.4e6, 390), _row("AAPL", 50.2e6, 3.2e6, 380),
                     _row("F", 32.3e6, None, 0)])
    assert bad["ok"] is False and bad["below_floor"] == 3 and bad["missing"] == 1
    assert [w["ticker"] for w in bad["worst"]] == ["F", "NVDA", "AAPL"]


def test_nothing_to_check_is_not_a_pass() -> None:
    assert reconcile([])["ok"] is None


def test_the_closing_auction_minute_is_counted() -> None:
    assert "b.time <= (d.day + TIME '16:00')" in RECONCILE_SQL
