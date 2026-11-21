"""A short is not opened on a name the venue marks unshortable (shared/utils/auto_trade.py)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import auto_trade as at  # noqa: E402


def test_opens_and_adds_to_a_short_are_skipped_but_exits_and_longs_are_kept() -> None:
    plans = [at.Plan("USAR", 80, "sell", "open"), at.Plan("USAR", 5, "sell", "add"), at.Plan("USAR", 9, "sell", "flip-open"),
             at.Plan("USAR", 9, "buy", "close"), at.Plan("USAR", 3, "buy", "trim"), at.Plan("USAR", 9, "sell", "flip-close"),
             at.Plan("USAR", 4, "buy", "open"), at.Plan("AAPL", 2, "sell", "open")]
    kept, skipped = at.without_unshortable(plans, {"USAR"})
    assert [(p.side, p.reason) for p in skipped] == [("sell", "open"), ("sell", "add"), ("sell", "flip-open")]
    assert len(kept) == 5 and any(p.symbol == "AAPL" for p in kept) and any(p.reason == "flip-close" for p in kept)


def test_an_unlisted_name_or_an_empty_set_changes_nothing() -> None:
    plans = [at.Plan("ZZZZ", 1, "sell", "open")]
    assert at.without_unshortable(plans, set()) == (plans, [])
    assert at.without_unshortable(plans, {"USAR"}) == (plans, [])
    assert at.without_unshortable([at.Plan("usar", 1, "sell", "open")], {"USAR"})[1] != []


def test_the_trader_applies_it_after_planning_and_counts_each_skip() -> None:
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    i = src.index("plans = at.plan_orders(")
    block = src[i:i + 500]
    assert "at.without_unshortable(plans, await self._unshortable())" in block and "dropped(" in block


def test_orders_wait_when_the_models_scores_miss_completed_sessions():
    """Seven nightly runs published the same 6 Nov scores while 96 to 147 orders a day went out on them (13 Nov 2026). Unknown is not a pass."""
    from shared.utils import auto_trade as at
    assert at.scores_too_old(0) is None and at.scores_too_old(1) is None
    assert "4 completed sessions behind" in at.scores_too_old(4) and at.scores_too_old(2) is not None
    assert "could not be read" in at.scores_too_old(None)
