"""A price stamped while the instrument's market is shut is not a bar."""

import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.market_session import instrument_closed_at  # noqa: E402

SAT_NOON = datetime(2026, 10, 3, 16, 0, tzinfo=timezone.utc)       # Sat 12:00 ET
SUN_2319 = datetime(2026, 10, 5, 3, 19, tzinfo=timezone.utc)        # Sun 23:19 ET
SUN_1500 = datetime(2026, 10, 4, 19, 0, tzinfo=timezone.utc)        # Sun 15:00 ET
MON_1000 = datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc)        # Mon 10:00 ET
MON_1730 = datetime(2026, 10, 5, 21, 30, tzinfo=timezone.utc)       # Mon 17:30 ET
TUE_0200 = datetime(2026, 10, 6, 6, 0, tzinfo=timezone.utc)         # Tue 02:00 ET


def test_equities_are_closed_on_weekends_and_overnight() -> None:
    assert instrument_closed_at("NVDA", SAT_NOON)
    assert instrument_closed_at("NVDA", SUN_2319)
    assert instrument_closed_at("SPY", TUE_0200)
    assert not instrument_closed_at("NVDA", MON_1000)
    assert not instrument_closed_at("NVDA", MON_1730)  # after-hours trades


def test_futures_follow_the_globex_week() -> None:
    assert instrument_closed_at("CL=F", SAT_NOON)
    assert instrument_closed_at("CL=F", SUN_1500)
    assert not instrument_closed_at("CL=F", SUN_2319)
    assert instrument_closed_at("CL=F", MON_1730)      # daily halt
    assert not instrument_closed_at("ES=F", TUE_0200)


def test_crypto_never_closes() -> None:
    assert not instrument_closed_at("BTC", SAT_NOON)
    assert not instrument_closed_at("ETH-USD", SUN_1500)
