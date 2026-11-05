"""What polling can and cannot say about an option print's time and side."""

import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.market_session import session_liquidity_factor  # noqa: E402
from shared.utils.options_flow import aggressor, poll_capture_weight, side_miss_reason  # noqa: E402

TRADFI = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
T = datetime(2026, 10, 30, 15, 0, tzinfo=timezone.utc)


def test_a_print_is_sized_against_the_session_it_happened_in() -> None:
    # 15:55 New York, read at 16:12: a regular-session print, not an after-hours one.
    assert session_liquidity_factor(datetime(2026, 10, 30, 19, 55, tzinfo=timezone.utc), asset_class="equities") == 1.0
    assert session_liquidity_factor(datetime(2026, 10, 30, 20, 12, tzinfo=timezone.utc), asset_class="equities") == 0.10
    assert 'session_liquidity_factor(raw.occurred_at, asset_class="equities")' in TRADFI
    assert 'session_liquidity_factor(asset_class="equities")' not in TRADFI


def test_each_unread_side_has_its_reason() -> None:
    assert side_miss_reason(None, 1.1, T, T) == "no_quote"
    assert side_miss_reason(1.2, 1.1, T, T) == "no_quote"
    assert side_miss_reason(1.0, 1.1, T, None) == "no_quote"
    assert side_miss_reason(1.0, 1.1, T, T + timedelta(seconds=301)) == "quote_too_old"
    assert side_miss_reason(1.0, 1.1, T, T - timedelta(seconds=120)) == "inside_stale_quote"
    assert side_miss_reason(1.0, 1.1, T, T + timedelta(seconds=30)) == "at_midpoint"


def test_the_reasons_agree_with_the_classifier() -> None:
    # Every reason names a case the classifier leaves unread.
    assert aggressor(1.05, 1.0, 1.1, T, T + timedelta(seconds=301)) is None
    assert aggressor(1.05, 1.0, 1.1, T, T - timedelta(seconds=120)) is None
    assert aggressor(1.05, 1.0, 1.1, T, T + timedelta(seconds=30)) is None


def test_a_trade_is_seen_only_until_the_next_one() -> None:
    assert poll_capture_weight(30.0, 300.0) == 0.1
    assert poll_capture_weight(600.0, 300.0) == 1.0
    assert poll_capture_weight(0.0, 300.0) == 0.0
    assert poll_capture_weight(None, 300.0) == 1.0    # the contract's last trade stays visible
