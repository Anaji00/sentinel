"""G3 fixes 4 and 5: names the free feed cannot serve are counted untradeable, and the daily-only gap list is longer than the minute-bar one."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import asset_universe as au  # noqa: E402
from shared.utils import signal_store as ss  # noqa: E402

META = {"AAPL": ["stock", True, True, "Apple"], "SPY": ["fund", True, True, "SPDR"], "WNTS": ["other", False, False, "Warrants"]}


def test_only_active_stocks_and_funds_are_priceable_and_no_listing_rules_nothing_out() -> None:
    assert au.priceable_now("AAPL", META) and au.priceable_now("spy", META)
    assert not au.priceable_now("WNTS", META)                  # a warrant
    assert not au.priceable_now("GONE", META)                  # delisted, foreign OTC or a placeholder: Alpaca lists nothing
    assert au.priceable_now("GONE", None) and au.priceable_now("GONE", {})        # no listing to read: nothing is hidden


def test_the_daily_only_list_is_five_times_the_minute_bar_list() -> None:
    assert ss.MAX_NEEDS_BARS == 300 and ss.MAX_NEEDS_DAILY == 1500
    assert ss.NEEDS_DAILY_KEY != ss.NEEDS_BARS_KEY


def test_the_study_counts_untradeable_names_and_writes_both_lists_and_the_collector_reads_the_daily_one() -> None:
    se = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert "if priceable_now(inst, asset_meta):" in se and '"instruments_untradeable": untradeable' in se
    assert "NEEDS_DAILY_KEY" in se and "[:MAX_NEEDS_DAILY]" in se
    col = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "smembers(NEEDS_DAILY_KEY)" in col and "shallow = [t for t in daily_universe if t not in deep]" in col
    assert col.index("smembers(NEEDS_DAILY_KEY)") < col.index("shallow = [t for t in daily_universe if t not in deep]")
