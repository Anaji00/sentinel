"""
tests/test_scenarios_reach_an_instrument.py

A scenario has to be about something that trades.

58% of the scenarios written in thirty days were maritime or aviation clusters
that named no instrument, at minutes of a model slot each, while no vessel or
aircraft had any edge to a company. A cluster now reaches reasoning by naming an
instrument or by carrying an `exposes:` tag a bridge attached.
"""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.instrument_reach import (
    cluster_reaches_instrument, equity_candidates, exposed_symbols, listed_instruments,
)


def _c(domain="maritime", names=(), tags=(), primary=None):
    return SimpleNamespace(primary_domain=domain, entity_ids=[], entity_names=list(names),
                           primary_entity_id=primary, primary_entity_name=None, tags=list(tags))


def _redis(exists=0, error=False):
    r = MagicMock()
    r.raw.exists = AsyncMock(side_effect=ConnectionError("down")) if error else AsyncMock(return_value=exists)
    return r


def _reach(c, r=None):
    return asyncio.run(cluster_reaches_instrument(c, r))


def test_an_equity_domain_cluster_reaches():
    assert _reach(_c("tradfi"))


def test_a_vessel_only_cluster_does_not():
    assert not _reach(_c(names=["SEA STAR", "SANDPIPER 1"]), _redis(0))


def test_a_ticker_shaped_vessel_name_needs_a_quote():
    c = _c(names=["GOLD"])
    assert equity_candidates(c) == {"GOLD"}
    assert not _reach(c, _redis(0))
    assert _reach(_c(names=["XOM"]), _redis(1))


def test_a_commodity_or_exposure_tag_reaches_without_a_lookup():
    assert listed_instruments(_c(names=["CL=F"])) == {"CL=F"}
    assert _reach(_c(tags=["exposes:XOM", "tanker"]))
    assert exposed_symbols(["exposes:dal", "region:x"]) == {"DAL"}


def test_unknown_is_not_no():
    """A Redis failure must not starve reasoning."""
    assert _reach(_c(names=["XOM"]), _redis(error=True))


def test_reasoning_gates_before_ranking():
    src = (ROOT / "services" / "reasoning" / "main.py").read_text(encoding="utf-8")
    assert src.index("cluster_reaches_instrument(cluster, redis_client)") < src.index("eligible.append((cluster, raw_data))")
