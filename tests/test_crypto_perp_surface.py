"""
tests/test_crypto_perp_surface.py

The entire crypto derivatives surface was declared and never populated.

Measured before the change: across 24 hours and 34,344 stored crypto events, the
count of rows with a non-null funding_rate, open_interest, basis_bps, mark_price,
leverage or market_microstructure was zero -- for every field, on every row and
every event type. Only `price` was ever filled.

The cause was a chain, not a single bug:

  1. Binance answers HTTP 451 ("unavailable for legal reasons") to this host, on
     fstream (websocket) and fapi (REST) alike. stream_binance_funding_rates
     therefore never connected and logged nothing.
  2. _observed_perp_symbols is filled only by that stream, so the open-interest
     poller sat on an empty set logging "No perp symbols observed yet" once a
     minute, indefinitely.
  3. The enricher's routing required source == "binance_futures", so any
     replacement venue would have had its events dropped after collection.

OKX serves the same surface from here and is what now fills these fields.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def crypto_collector():
    spec = importlib.util.spec_from_file_location(
        "collector_crypto_main", ROOT / "services/collector-crypto/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["collector_crypto_main"] = module
    spec.loader.exec_module(module)
    return module


def test_okx_returns_every_number_as_a_string(crypto_collector):
    """OKX quotes numbers as strings and uses "" for "not set"."""
    f = crypto_collector._f
    assert f("0.0001000000000000") == 0.0001
    assert f("77805.6") == 77805.6
    assert f("") == 0.0
    assert f(None) == 0.0
    assert f("not-a-number") == 0.0
    assert f("", 1.5) == 1.5


def test_the_poller_is_actually_started(crypto_collector):
    """A collector nothing launches is the defect one level up."""
    source = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    assert "poll_okx_perpetuals(producer, redis_client)" in source


def test_enrichment_routes_on_trade_type_not_venue():
    """Pinning to binance_futures would drop every OKX event after collection."""
    source = (ROOT / "services/enrichment/enrichers/crypto.py").read_text(encoding="utf-8")
    assert 'elif trade_type == "CRYPTO_PERP_FUNDING":' in source
    assert 'elif trade_type == "OPEN_INTEREST":' in source
    assert 'source == "binance_futures" and trade_type == "CRYPTO_PERP_FUNDING"' not in source


def test_the_model_can_carry_the_whole_surface():
    from shared.models import CryptoData
    cd = CryptoData(
        pair="BTC-USDT-SWAP", trade_type="CRYPTO_PERP_FUNDING", side="POSITIVE",
        price=77805.6, size_tokens=0.0,
        funding_rate=0.0001, mark_price=77805.6, index_price=77823.1,
        basis_bps=-2.25, open_interest=2920507.19,
    )
    assert cd.funding_rate == 0.0001
    assert cd.open_interest == 2920507.19
    assert cd.basis_bps == -2.25


def test_basis_is_not_computed_without_an_index_price():
    """Dividing by a missing index would report a fabricated premium.

    Both venues guard this, by different means: the Binance websocket path skips
    the message outright (`if mark_price <= 0 or index_price <= 0: continue`),
    while the OKX poller keeps the row -- funding and open interest are still
    real without an index -- and leaves basis at zero.
    """
    source = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    okx = source[source.index("async def poll_okx_perpetuals"):]
    basis = okx[okx.index("basis_bps = ("):]
    assert "if index_price > 0 else 0.0" in basis[:240]

    binance = source[source.index("async def stream_binance_funding_rates"):]
    assert "if mark_price <= 0 or index_price <= 0:" in binance


def test_unchanged_funding_is_not_republished():
    """Funding settles every 8h; a 5-minute poll would emit the same number ~96x."""
    source = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    assert "OKX_FUNDING_DELTA_TRIGGER" in source
    assert "if not moved:" in source


# ── the frontend must not invent what the backend does not have ──────────────

def test_the_browser_no_longer_fabricates_microstructure():
    """These rendered in the Crypto Analytics panel beside genuine funding data.

    open_interest was markPrice * 25000, order-flow imbalance was
    Math.sin(markPrice), and Kyle's lambda and Amihud illiquidity were the fixed
    constants 0.00045 and 0.000012 -- all displayed as measurements.
    """
    api = (ROOT / "frontend/src/lib/api.ts").read_text(encoding="utf-8")
    code = "\n".join(l for l in api.splitlines() if not l.strip().startswith("//"))
    assert "Math.sin(" not in code
    assert "markPrice * 25000" not in code
    assert "kyles_lambda" not in code
    assert "amihud_illiquidity" not in code


def test_browser_fetched_rows_are_labelled():
    """The user has to be able to tell platform analysis from a raw feed."""
    api = (ROOT / "frontend/src/lib/api.ts").read_text(encoding="utf-8")
    assert api.count("data_provenance:") >= 3

    types = (ROOT / "frontend/src/lib/types.ts").read_text(encoding="utf-8")
    assert "data_provenance?: string;" in types

    panel = (ROOT / "frontend/src/components/CryptoAnalytics.tsx").read_text(encoding="utf-8")
    assert "e.data_provenance &&" in panel, "the badge is typed but never rendered"


# ── ranking and open interest ────────────────────────────────────────────────

def test_instruments_are_ranked_by_usd_notional(crypto_collector):
    """volCcy24h counts base units, so cheap tokens dominate it.

    The first live run of this poller tracked SATS, PEPE, SHIB, BONK and FLOKI
    and ignored BTC and ETH entirely: SATS trades at $0.00000001265, so its
    24h base volume is 100 trillion units. Priced in dollars the order is
    ETH $7.97B, BTC $5.26B, SOL $0.96B.
    """
    f = crypto_collector._f
    tickers = [
        {"instId": "SATS-USDT-SWAP", "volCcy24h": "100122250000000", "last": "0.00000001265"},
        {"instId": "PEPE-USDT-SWAP", "volCcy24h": "62282905000000", "last": "0.000004144"},
        {"instId": "ETH-USDT-SWAP", "volCcy24h": "2300000", "last": "3465.0"},
        {"instId": "BTC-USDT-SWAP", "volCcy24h": "67000", "last": "78500.0"},
    ]
    by_notional = sorted(tickers, key=lambda t: f(t.get("volCcy24h")) * f(t.get("last")), reverse=True)
    assert [t["instId"] for t in by_notional][:2] == ["ETH-USDT-SWAP", "BTC-USDT-SWAP"]

    by_base = sorted(tickers, key=lambda t: f(t.get("volCcy24h")), reverse=True)
    assert by_base[0]["instId"] == "SATS-USDT-SWAP", "the defect this guards"


def test_the_poller_uses_notional_ranking(crypto_collector):
    source = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    okx = source[source.index("async def poll_okx_perpetuals"):]
    assert '_f(t.get("volCcy24h")) * _f(t.get("last"))' in okx


def test_open_interest_on_the_event_is_used():
    """The enricher read a Redis key only the dead Binance poller ever wrote.

    So open_interest came out null even on events that carried it -- which is
    exactly what the first OKX rows showed: funding_rate and basis_bps
    populated, open_interest empty.
    """
    source = (ROOT / "services/enrichment/enrichers/crypto.py").read_text(encoding="utf-8")
    block = source[source.index("oi_value = None"):]
    assert 'payload_oi = p.get("open_interest")' in block[:400]
    assert "if oi_value is None:" in block[:800], "the Redis path must be the fallback"


def test_the_collector_also_feeds_the_shared_oi_key(crypto_collector):
    """Other consumers read sentinel:crypto:oi:{asset}; it must not go stale."""
    source = (ROOT / "services/collector-crypto/main.py").read_text(encoding="utf-8")
    okx = source[source.index("async def poll_okx_perpetuals"):]
    assert 'f"sentinel:crypto:oi:{asset}"' in okx
