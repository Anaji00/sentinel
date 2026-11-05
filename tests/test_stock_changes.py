"""A stock's recent changes: dated, honest about side, and a gap is never "nothing changed"."""

import asyncio
import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes import stock_changes as route  # noqa: E402
from shared.utils import stock_changes as sc  # noqa: E402


def test_moves_need_enough_history() -> None:
    d = date(2026, 11, 4)
    assert sc.price_change([]) is None
    short = sc.price_change([{"day": d, "close": 110}, {"day": d, "close": 100}])
    assert short["change_1d_pct"] == 10.0 and short["change_5d_pct"] is None
    full = sc.price_change([{"day": d, "close": 110}] + [{"day": d, "close": 100}] * 5)
    assert full["change_5d_pct"] == 10.0


def test_unread_flow_stays_apart() -> None:
    out = sc.flow_summary([
        {"type": "options_flow", "side": "buyer", "trades": 2, "usd": 100.0},
        {"type": "options_flow", "side": "unknown", "trades": 3, "usd": None},
    ])[0]
    assert (out["buyer_usd"], out["seller_usd"], out["unknown_usd"]) == (100.0, 0.0, 0.0)
    assert out["unknown_trades"] == 3
    assert "no edge" in out["note"]


def test_every_measured_macro_has_a_name() -> None:
    link = sc.macro_links([{"macro": "CL=F", "beta": -0.2, "t": -4.0, "n": 252, "as_of": date(2026, 11, 4)}])[0]
    assert link["name"] == "WTI crude" and link["direction"] == "against"
    assert len(sc.MACRO_NAMES) == 15


class _DB:
    """Fails the flow read, is empty elsewhere."""

    async def query(self, sql: str, *args: object) -> list:
        if "equity_block" in sql and "GROUP BY" in sql:
            raise RuntimeError("down")
        return []


def test_failure_and_absence_are_told_apart() -> None:
    out = asyncio.run(route._compute(_DB(), "NVDA"))
    assert out["sections"]["flow"] == "unavailable"
    assert out["sections"]["price"] == "no_data"
    assert out["flow"] == [] and out["price"] is None
