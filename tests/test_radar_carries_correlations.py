"""An event should be traceable to the correlations involving its subject.

`correlation_ids` on an event is the link back from a market event to the
statistical relationships its ticker is part of. Two of the three tradfi paths
fetch it -- the equity-trade path and the candle path both read
`sentinel:correlation:active_ids:{ticker}` -- and the radar path did not.

Measured over 24 hours: `equity_block` carried them on 13 of 90 events, which is
correct (only tickers discovery has actually run on have a set).
`market_anomaly` carried them on **0 of 990** -- and every one of those 990 came
from `alpaca_quant_radar`, firing on exactly the tickers that do hold a set:
SPY, NVDA and TSM all have one live.

So the highest-volume of the three paths was the one that could not be traced
back, and the two that could are an order of magnitude rarer.
"""
import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")


def _method(name):
    for n in ast.walk(ast.parse(SRC)):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return ast.unparse(n)
    return None


@pytest.mark.parametrize(
    "method",
    ["_enrich_quant_radar", "_enrich_equity_candle", "_finalize_equity_trade"],
)
def test_every_tradfi_path_links_back_to_its_correlations(method):
    body = _method(method)
    assert body is not None, f"{method} no longer exists"
    assert "correlation_ids" in body, (
        f"{method} publishes an event that cannot be traced to the statistical "
        "relationships its ticker is part of"
    )


def test_the_radar_path_reads_the_same_key_as_the_others():
    """One key, so the three cannot drift into disagreeing about the link."""
    body = _method("_enrich_quant_radar")
    assert "sentinel:correlation:active_ids:" in body


def test_a_lookup_failure_is_counted_not_swallowed():
    body = _method("_enrich_quant_radar")
    assert "swallowed(" in body
    assert "logger.debug" not in body.split("active_ids")[-1][:400]


def test_an_absent_set_yields_no_ids_rather_than_an_error():
    """Most tickers have no discovered correlation, and that is not a fault."""
    body = _method("_enrich_quant_radar")
    assert "radar_corr_ids = []" in body
