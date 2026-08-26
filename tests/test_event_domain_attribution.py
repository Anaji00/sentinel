"""
tests/test_event_domain_attribution.py

Crypto ticks were presented as stock-market events from a vendor never used.

Reported from the running console: BCHUSDT, DOTUSDT and ADAUSDT rendered as
"TRADFI ... via AlphaVantage Feed" at CRITICAL severity. Both halves were wrong,
and each had its own cause:

  1. The client derived the domain from the event *type* by substring.
     "market_anomaly" contains "market", so it matched the TRADFI branch -- and
     market_anomaly is the type the Coinbase candle enricher emits. Every crypto
     candle anomaly was labelled a stock event. The gateway never sent a domain,
     so the client had nothing better to go on.

  2. When an event carried no `source`, the client invented one per domain:
     "AlphaVantage Feed" for TRADFI, "CoinGecko On-Chain" for CRYPTO, "AISStream
     Telemetry" for MARITIME. This deployment uses none of them -- equities come
     from Alpaca and Finnhub, crypto from Coinbase and OKX. And /events/all did
     not return `source` at all, so the invented name was what users saw.

The domain is now decided in SQL from which payload column the row carries, and
an unattributed row shows no attribution.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EVENTS_ROUTE = ROOT / "services/api_gateway/routes/events.py"
FEED = ROOT / "frontend/src/components/IntelligenceFeed.tsx"


def _code(path: Path) -> str:
    """Source with comments stripped; the files document the defects by name."""
    text = path.read_text(encoding="utf-8")
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r"^\s*//.*$", "", text, flags=re.M)
    text = re.sub(r"^\s*#.*$", "", text, flags=re.M)
    return text


# ── the gateway must say which domain a row belongs to ───────────────────────

def test_both_queries_return_the_domain():
    """Neither branch could be trusted to the client's guesswork."""
    src = _code(EVENTS_ROUTE)
    assert src.count("END AS domain") == 2, "one of the two query branches omits it"


def test_the_domain_follows_the_payload_column_not_the_type():
    """The payload column is the only authoritative answer."""
    src = _code(EVENTS_ROUTE)
    block = src[src.index("WHEN crypto_data IS NOT NULL"):]
    for column, domain in (
        ("crypto_data", "crypto"),
        ("prediction_market_data", "prediction"),
        ("vessel_data", "maritime"),
        ("flight_data", "aviation"),
        ("security_data", "cyber"),
        ("financial_data", "tradfi"),
    ):
        assert f"WHEN {column} IS NOT NULL THEN '{domain}'" in block


def test_crypto_is_decided_before_financial():
    """An event carrying both must read as crypto, not as an equity."""
    src = _code(EVENTS_ROUTE)
    block = src[src.index("WHEN crypto_data IS NOT NULL"):]
    assert block.index("crypto_data") < block.index("financial_data")


def test_both_queries_return_the_source():
    src = _code(EVENTS_ROUTE)
    # Once per branch, projected out of the CTE in the domain query.
    assert src.count("source,") >= 3, "the client cannot attribute without it"


# ── the client must not guess or invent ──────────────────────────────────────

def test_the_feed_uses_the_declared_domain():
    src = _code(FEED)
    assert "function domainMetaFor(" in src
    assert "domainMetaFor(e)" in src, "the row still classifies by type alone"


def test_payload_fallbacks_prefer_crypto_over_financial():
    """Fallback order matters for the same reason the SQL order does."""
    src = _code(FEED)
    fn = src[src.index("function domainMetaFor("):]
    fn = fn[:fn.index("function getDomainMeta(")]
    assert fn.index("e.crypto_data") < fn.index("e.financial_data")


@pytest.mark.parametrize("vendor", [
    "AlphaVantage Feed", "CoinGecko On-Chain", "AISStream Telemetry",
    "BGP Monitoring Network", "PolyMarket API", "Sentinel Intelligence Collector",
])
def test_no_vendor_is_invented(vendor):
    """None of these produced a single row of this deployment's data."""
    assert vendor not in _code(FEED), f"invented attribution is back: {vendor!r}"


def test_an_unattributed_row_shows_no_attribution():
    src = _code(FEED)
    fn = src[src.index("function getCleanSource("):]
    fn = fn[:fn.index("\n}") + 2]
    assert "return '';" in fn
    assert "{sourceName && (" in src, "an empty source would render a bare 'via'"


# ── the corroboration badge ──────────────────────────────────────────────────

def test_the_corroboration_badge_exists():
    """Rebuilt after I truncated this file; it is the UI for the tracker."""
    src = _code(FEED)
    assert "function CorroborationBadge(" in src
    assert "<CorroborationBadge e={e} />" in src, "defined but never rendered"


def test_a_single_sourced_claim_is_marked_as_a_lead():
    src = _code(FEED)
    fn = src[src.index("function CorroborationBadge("):]
    assert "c.is_single_sourced" in fn
    assert "single source" in fn


def test_syndication_is_distinguished_from_corroboration():
    """Four outlets running one wire story is not four sources."""
    src = _code(FEED)
    fn = src[src.index("function CorroborationBadge("):]
    assert "c.is_syndicated" in fn


def test_events_without_an_assessment_render_nothing():
    """A market tick has no second source; marking it single-sourced is noise."""
    src = _code(FEED)
    fn = src[src.index("function CorroborationBadge("):]
    assert "if (!c) return null;" in fn
