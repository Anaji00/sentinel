"""
tests/test_options_ingestion.py

Two correlation rules that could never fire, because of one field name.

    ⏱️ Options Poller Heartbeat: Checked 50 equity tickers | Sweeps published: 0

Every five minutes since the collector was written, with no error. Alpaca's
market-data API abbreviates its trade objects --

    "latestTrade": {"c":"a", "p":0.05, "s":50, "t":"...", "x":"I"}

-- and the poller read `.get("price")` and `.get("size")`, which return None.
`float(None or 0.0)` is 0.0, so premium computed as 0 x 0 x 100 = $0 for every
contract, and the filter `premium >= 50000 or size >= 100` could never pass.

Measured against the live endpoint: 99 of 100 NVDA contracts carry a
latestTrade, and the maximum premium across all of them was $0.00. With the
field names corrected, 22 of 385 contracts across five tickers clear the filter
and TSLA's largest is $6,828,000.

The blast radius is the part worth keeping. `options_flow` has never been
produced, so:

  * rule_options_darkpool_surge        triggers on options_flow -- 0 fires, ever
  * rule_insider_options_convergence   needs options_flow too   -- 0 fires, ever
  * _enrich_options_flow()             fully implemented, never received an event

Seven declared correlation rules exist and two of them were starved by a parse
that silently returned zero. A zero is indistinguishable from a quiet market,
which is why nothing anywhere reported a problem.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COLLECTOR = ROOT / "services" / "collector-tradfi" / "main.py"


def _parse(latest_trade: dict):
    """The collector's parse, mirrored so the fix is pinned by behaviour."""
    price = float(latest_trade.get("p", latest_trade.get("price")) or 0.0)
    size = float(latest_trade.get("s", latest_trade.get("size")) or 0.0)
    return price, size, price * size * 100.0


def test_the_vendors_short_keys_are_read():
    """The exact object the live API returns."""
    price, size, premium = _parse({"c": "a", "p": 0.05, "s": 50, "t": "...", "x": "I"})
    assert (price, size) == (0.05, 50.0)
    assert premium == pytest.approx(250.0)


def test_a_real_sweep_clears_the_filter():
    """TSLA's largest contract, measured: $6,828,000 of premium."""
    _p, _s, premium = _parse({"p": 22.76, "s": 3000})
    assert premium >= 50_000.0


def test_the_long_names_still_work():
    """Kept as a fallback: another endpoint, or a vendor change back, should not
    silently zero the field again."""
    price, size, premium = _parse({"price": 1.25, "size": 40})
    assert (price, size) == (1.25, 40.0)
    assert premium == pytest.approx(5000.0)


def test_a_missing_trade_is_zero_not_an_exception():
    assert _parse({}) == (0.0, 0.0, 0.0)


def test_the_collector_reads_the_short_keys():
    source = COLLECTOR.read_text(encoding="utf-8")
    assert 'latest_trade.get("p", latest_trade.get("price"))' in source
    assert 'latest_trade.get("s", latest_trade.get("size"))' in source


def test_the_long_only_read_is_gone():
    """`float(latest_trade.get("price") or 0.0)` is the bug, and it reads as
    perfectly ordinary defensive code."""
    source = COLLECTOR.read_text(encoding="utf-8")
    assert 'float(latest_trade.get("price") or 0.0)' not in source
    assert 'float(latest_trade.get("size") or 0.0)' not in source


def test_the_rules_that_were_starved_still_exist():
    """Fixing the parse is only useful if the rules waiting on it are intact."""
    rules = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "rule_options_darkpool_surge" in rules
    assert "rule_insider_options_convergence" in rules
    assert '"trigger_event_type": "options_flow"' in rules


def test_the_enricher_waiting_on_it_still_exists():
    enricher = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert "_enrich_options_flow" in enricher
