"""A cluster's commodity exposure reaches the companies exposed to it.

A bridge tags a tanker in Hormuz `exposes:CL=F`, and the graph holds which
companies rise and fall with crude. The scenario prompt now joins the two, so
an OSINT scenario can name the stocks it would move -- and watch signals naming
those stocks survive the pruning that drops entities the cluster never held.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from shared.utils.instrument_reach import (
    EXPOSURE_NAMES_PER_SIDE,
    exposed_commodities,
    exposed_equities,
    exposure_chain,
    exposure_lines,
)


def _rows(commodity, rel, tickers):
    return [{"commodity": commodity, "rel": rel, "ticker": t} for t in tickers]


def test_tags_split_into_commodities_and_equities():
    tags = ["exposes:CL=F", "exposes:dal", "region:hormuz", "exposes:ZW=F", "exposes:TSM"]
    assert exposed_commodities(tags) == ["CL=F", "ZW=F"]
    assert exposed_equities(tags) == ["DAL", "TSM"]


def test_chain_ranks_by_dollar_volume_and_drops_unpriced_names():
    rows = (
        _rows("CL=F", "POSITIVE_EXPOSURE_TO", ["XOM", "CVX", "AMPY", "SHELLCO"])
        + _rows("CL=F", "INVERSE_EXPOSURE_TO", ["DAL", "UAL"])
    )
    volume = {"XOM": 9e9, "CVX": 5e9, "AMPY": 1e6, "DAL": 2e9, "UAL": 3e9}
    chain = exposure_chain(rows, volume)
    assert chain == [{"commodity": "CL=F", "rises": ["XOM", "CVX", "AMPY"], "falls": ["UAL", "DAL"]}]


def test_chain_caps_each_side():
    tickers = [f"T{i:02d}" for i in range(12)]
    volume = {t: float(i + 1) for i, t in enumerate(tickers)}
    chain = exposure_chain(_rows("NG=F", "POSITIVE_EXPOSURE_TO", tickers), volume)
    assert len(chain[0]["rises"]) == EXPOSURE_NAMES_PER_SIDE
    assert chain[0]["rises"][0] == "T11"
    assert chain[0]["falls"] == []


def test_unknown_predicates_are_ignored():
    rows = _rows("CL=F", "SUPPLIES", ["XOM"])
    assert exposure_chain(rows, {"XOM": 1.0}) == []


def test_lines_name_both_directions_and_direct_tickers():
    chain = [{"commodity": "CL=F", "rises": ["XOM"], "falls": ["DAL"]}]
    text = exposure_lines(chain, ["UAL"])
    assert "CL=F -- rise with it: XOM; fall with it: DAL" in text
    assert "Named directly by the signal: UAL" in text
    assert exposure_lines([], []) == ""


def _cluster(tags):
    return SimpleNamespace(tags=tags)


def test_context_builder_joins_graph_and_liquidity():
    from services.reasoning.context_builder import ContextBuilder

    db = MagicMock()
    db.query = AsyncMock(return_value=[
        {"ticker": "XOM", "dv": 9e9}, {"ticker": "DAL", "dv": 2e9},
    ])
    neo = MagicMock()
    neo.query = AsyncMock(return_value=(
        _rows("CL=F", "POSITIVE_EXPOSURE_TO", ["XOM", "NOBARS"])
        + _rows("CL=F", "INVERSE_EXPOSURE_TO", ["DAL"])
    ))
    builder = ContextBuilder(db)
    with patch("services.reasoning.context_builder.get_neo4j", AsyncMock(return_value=neo)):
        lines, tickers = asyncio.run(
            builder._fetch_instrument_exposure(_cluster(["exposes:CL=F", "exposes:UAL"]))
        )
    assert "rise with it: XOM" in lines and "fall with it: DAL" in lines
    assert "NOBARS" not in lines
    assert tickers == ["XOM", "DAL", "UAL"]
    assert neo.query.await_args.args[1] == {"ids": ["CL=F"]}


def test_context_builder_survives_a_graph_failure():
    from services.reasoning.context_builder import ContextBuilder

    builder = ContextBuilder(MagicMock())
    with patch("services.reasoning.context_builder.get_neo4j",
               AsyncMock(side_effect=ConnectionError("down"))):
        lines, tickers = asyncio.run(
            builder._fetch_instrument_exposure(_cluster(["exposes:CL=F", "exposes:DAL"]))
        )
    assert tickers == ["DAL"]
    assert "Named directly by the signal: DAL" in lines


def test_prompt_carries_the_exposure_section():
    from services.reasoning.scenario_generator import ScenarioGenerator

    gen = ScenarioGenerator.__new__(ScenarioGenerator)
    gen.model = "qwen3:0.6b"
    cluster = SimpleNamespace(
        rule_name="CHOKEPOINT_DARK", alert_tier=SimpleNamespace(name="WATCH", value="watch"),
        description="Tanker dark in Hormuz", tags=["exposes:CL=F"],
        detected_at=datetime(2026, 10, 3, tzinfo=timezone.utc),
        entity_ids=["538001234"], entity_names=["ATLAS"], primary_entity_id="538001234",
        primary_entity_name="ATLAS", primary_domain="maritime",
    )
    context = {"instrument_exposure": "• CL=F -- rise with it: XOM; fall with it: DAL"}
    prompt = gen._build_user_prompt(cluster, context, [], [])
    assert "=== INSTRUMENTS EXPOSED ===" in prompt
    assert "rise with it: XOM" in prompt


def test_exposed_tickers_survive_signal_pruning():
    from services.reasoning.scenario_generator import _prune_unresolvable_signals

    signal = SimpleNamespace(entity="XOM")
    hypothesis = SimpleNamespace(watch_signals=[signal], deny_signals=[SimpleNamespace(entity="ACME")])
    output = SimpleNamespace(hypotheses=[hypothesis])
    removed = _prune_unresolvable_signals(output, {"atlas", "xom"})
    assert removed == 1
    assert hypothesis.watch_signals == [signal]
