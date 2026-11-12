"""The close a live price's move is measured from (shared/utils/recommendations.py change_base)."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import recommendations as rc  # noqa: E402

DAYS = [date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11)]
CLOSES = [170.0, 168.89, 173.73]


def test_after_the_close_the_base_is_todays_final_close_not_the_day_before() -> None:
    # NVDA on 11 Nov 2026, 19:58 ET: the bar dated today is final; the 158.85 print is after hours.
    base, day = rc.change_base(DAYS, CLOSES, date(2026, 11, 11), session_open=False)
    assert (base, day) == (173.73, date(2026, 11, 11))
    assert round((158.85 / base - 1) * 100, 2) == -8.56


def test_during_the_session_the_bar_dated_today_is_partial_so_the_base_is_the_close_before() -> None:
    base, day = rc.change_base(DAYS, CLOSES, date(2026, 11, 11), session_open=True)
    assert (base, day) == (168.89, date(2026, 11, 10))


def test_before_the_open_or_on_a_non_trading_day_the_last_bar_is_the_base() -> None:
    assert rc.change_base(DAYS, CLOSES, date(2026, 11, 12), session_open=False) == (173.73, date(2026, 11, 11))
    assert rc.change_base(DAYS, CLOSES, date(2026, 11, 12), session_open=True) == (173.73, date(2026, 11, 11))


def test_nothing_to_measure_from_gives_none() -> None:
    assert rc.change_base([], [], date(2026, 11, 11), True) == (None, None)
    assert rc.change_base(DAYS[:1], CLOSES[:1], date(2026, 11, 9), True) == (None, None)


def test_the_engine_uses_it_for_the_stock_cards_and_the_spy_pulse_and_publishes_the_base() -> None:
    src = (ROOT / "services/agents/recommendation_engine.py").read_text(encoding="utf-8")
    assert src.count("rc.change_base(") == 2 and '"change_pct_base_date"' in src and '"change_pct_base"' in src
