"""The block-trade panel asks for blocks and sweeps, not every tradfi event."""

import asyncio
import pathlib
import sys
from typing import Any, List, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes.events import get_domain_events  # noqa: E402


class _DB:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, Tuple[Any, ...]]] = []

    async def query(self, sql: str, *params: Any) -> list:
        self.calls.append((sql, params))
        return []


def _run(**kw: Any) -> _DB:
    db = _DB()
    defaults = dict(limit=40, region_spread=0, min_anomaly=0.0, start_time=None, end_time=None, types=None)
    defaults.update(kw)
    asyncio.run(get_domain_events("tradfi", db=db, **defaults))
    return db


def test_types_narrow_the_query_to_those_event_types() -> None:
    sql, params = _run(types="equity_block, options_flow").calls[0]
    assert "type = ANY(" in sql
    assert ["equity_block", "options_flow"] in [list(p) for p in params if isinstance(p, list)]


def test_without_types_every_tradfi_event_is_eligible() -> None:
    sql, _ = _run().calls[0]
    assert "type = ANY(" not in sql
