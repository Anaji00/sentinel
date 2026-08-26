"""
tests/test_subscription_budget.py

Fifty scarce subscriptions, allocated by accident.

Finnhub permits 50 concurrent symbol subscriptions. The collector filled them
with `zrevrange(watchlist, 0, 49)` -- the 50 most recently *added* entries, the
watchlist being scored by time.time() as the radar and quant agents encounter
anomalies. So the budget went to whatever was last stumbled upon.

Observed on a live pre-market session: the subscription set was AAL, ACGL, ACM,
BIIB, BRKR, CAG, CB, CCI, CLH, DLR, DTM, EIX, EME, ESTC, ETSY, EXR, GDS, GFS,
GS, H ... with no NVDA, AAPL, MSFT, TSLA, AMZN, META or GOOGL anywhere in it.
Those mid-caps print minutes apart before the open, so the equity feed went
quiet: the trade counter sat unchanged at 7,787 across 217 consecutive
heartbeats while the collector reported "Streaming 50/50 symbols" -- a figure
that counts subscriptions, not data.

A desk always watches the same handful of anchors and rotates the rest onto
whatever is moving. That is what the selection now does.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def collector():
    spec = importlib.util.spec_from_file_location(
        "collector_tradfi_subs", ROOT / "services/collector-tradfi/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["collector_tradfi_subs"] = module
    spec.loader.exec_module(module)
    return module


# The exact set observed subscribed during the live pre-market session.
OBSERVED = ("AAL ACGL ACM BIIB BRKR CAG CB CCI CLH DLR DTM EIX EME ESTC ETSY "
            "EXR GDS GFS GS H HTFL INSM JBHT KIM KKR KMX LNG LNTH MRK NDSN NOV "
            "OKE OKLO ONON OVV PBF PCOR PI POST RBLX RBRK SLS SNX TLN UNM V "
            "WELL WULF XOM ZTO").split()


def test_the_anchors_survive_the_observed_watchlist(collector):
    """The regression: none of these were subscribed during pre-market."""
    selected = collector.select_subscription_symbols(OBSERVED)
    for anchor in ("NVDA", "AAPL", "MSFT", "AMZN", "META"):
        assert anchor in selected, f"{anchor} is missing from the feed again"


def test_discovery_still_gets_room(collector):
    """The core must not swallow the budget; the radar needs slots too."""
    selected = collector.select_subscription_symbols(OBSERVED)
    discoveries = [s for s in selected if s not in collector.CORE_EQUITY_SYMBOLS]
    assert len(discoveries) >= len(selected) // 2


def test_the_core_share_is_capped(collector):
    """A long core list must not crowd discovery out entirely."""
    limit = collector.FINNHUB_SUBSCRIPTION_LIMIT
    selected = collector.select_subscription_symbols([f"D{i}" for i in range(limit * 2)])
    core_used = [s for s in selected if s in collector.CORE_EQUITY_SYMBOLS]
    assert len(core_used) <= int(limit * collector.MAX_CORE_SHARE)


def test_the_budget_is_never_exceeded(collector):
    """Finnhub rejects subscriptions past its limit."""
    selected = collector.select_subscription_symbols([f"T{i}" for i in range(500)])
    assert len(selected) <= collector.FINNHUB_SUBSCRIPTION_LIMIT


def test_no_symbol_is_subscribed_twice(collector):
    """A duplicate wastes a slot and Finnhub counts it."""
    overlapping = ["NVDA", "AAPL", "nvda"] + OBSERVED
    selected = collector.select_subscription_symbols(overlapping)
    assert len(selected) == len(set(selected))


def test_discovery_order_is_preserved(collector):
    """zrevrange arrives newest-first; recency is a fine tiebreak among finds."""
    selected = collector.select_subscription_symbols(OBSERVED)
    discoveries = [s for s in selected if s not in collector.CORE_EQUITY_SYMBOLS]
    expected = [s for s in OBSERVED if s not in collector.CORE_EQUITY_SYMBOLS]
    assert discoveries == expected[:len(discoveries)]


def test_an_empty_watchlist_still_yields_the_anchors(collector):
    """A cold start must not leave the platform blind to the whole market."""
    selected = collector.select_subscription_symbols([])
    assert len(selected) > 0
    assert "NVDA" in selected


def test_a_zero_budget_selects_nothing(collector):
    assert collector.select_subscription_symbols(OBSERVED, limit=0) == []


def test_symbols_are_normalised(collector):
    selected = collector.select_subscription_symbols(["  msft  ", "aapl"])
    assert all(s == s.strip().upper() for s in selected)


def test_the_selection_is_actually_used():
    """A selector nothing calls is the defect one level up."""
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "select_subscription_symbols(discovered)" in src
    assert "zrevrange(REDIS_EQUITIES_KEY, 0, 49)" not in src, "recency-only read is back"


def test_every_subscribed_symbol_is_also_ingested():
    """A slot spent on a symbol the ingest discards is a slot wasted.

    is_valid_primary_equity() rejects SPY and QQQ -- correctly, they are funds
    and do not belong in the equity watchlist -- but they are deliberate feed
    anchors, and the trade handler applied the watchlist rule to arriving data.
    """
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    handler = src[src.index('ticker = item.get("s")'):][:900]
    assert "ticker.upper() in CORE_EQUITY_SYMBOLS" in handler


def test_the_core_includes_the_index_proxies(collector):
    """SPY and QQQ are the market's pulse even though they are not companies."""
    assert "SPY" in collector.CORE_EQUITY_SYMBOLS
    assert "QQQ" in collector.CORE_EQUITY_SYMBOLS


# -- extended-hours bars must reach the table ---------------------------------

def test_extended_hours_bars_are_routed():
    """The collector stamps a different source outside regular hours.

    `source="finnhub_equities" if session_name == "REGULAR" else
    "alpaca_extended_hours"` -- and only the first spelling was routed, so every
    pre-market and after-hours bar fell through the source chain to `return
    None`. Observed: 109 bars flushed in one after-hours cycle, none of which
    reached tradfi_bars, and a table whose newest equity row was 23 minutes into
    a single regular session.
    """
    enricher = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert 'source in ("finnhub_equities", "alpaca_extended_hours")' in enricher
    assert enricher.count('source in ("finnhub_equities", "alpaca_extended_hours")') >= 2, (
        "the batch path and the single path must agree"
    )


def test_the_collector_still_stamps_both_spellings():
    """If the producer stops emitting one, the routing above is dead weight."""
    collector = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert 'source="finnhub_equities" if session_name == "REGULAR" else "alpaca_extended_hours"' in collector


def test_the_session_is_carried_on_the_bar():
    """A pre-market bar must be distinguishable from a regular-hours one."""
    collector = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert '"session": session_name' in collector
    enricher = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert 'session_tag = p.get("session", "REGULAR")' in enricher


# -- the feed must notice when it stops receiving -----------------------------

def test_reads_are_bounded():
    """A bare `await ws.recv()` blocks forever on a socket that is healthy but
    silent.

    Measured: the feed delivered nothing from 12:13 to 20:11 UTC -- the entire
    regular session -- while pings were still being answered, so the protocol
    keepalive never fired. WS heartbeats per hour ran 4, 0, 0, 0, 1, 0, 0, 0, 49.
    """
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "asyncio.wait_for(ws.recv(), timeout=WS_READ_TIMEOUT_SEC)" in src
    assert "message = await ws.recv()" not in src, "the unbounded read is back"


def test_a_stall_during_an_open_session_forces_a_reconnect(collector):
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    # Anchored on the watchdog itself: an earlier TimeoutError handler in this
    # file guards the pubsub poll and would match first.
    block = src[src.index("stalled_for = time.monotonic()"):][:700]
    assert "WS_MAX_STALL_SEC" in block
    assert "raise ConnectionError" in block


def test_silence_is_tolerated_when_the_market_is_shut(collector):
    """Otherwise the collector would reconnect in a loop all night."""
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    block = src[src.index("stalled_for = time.monotonic()"):][:700]
    assert 'session_name != "CLOSED"' in block


def test_the_stall_window_is_sane(collector):
    """Long enough for a quiet minute on an illiquid name, far short of hours."""
    assert 60 <= collector.WS_MAX_STALL_SEC <= 900
    assert collector.WS_READ_TIMEOUT_SEC < collector.WS_MAX_STALL_SEC


def test_sends_cannot_wedge_the_heartbeat_task():
    """ws.send awaits a drain that never comes on a half-open socket, and that
    task is the one that logs the heartbeat -- so a wedged send silences the
    only signal that would have revealed the outage."""
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "timeout=WS_SEND_TIMEOUT_SEC" in src
    assert 'await ws.send(json.dumps({"type": "subscribe"' not in src


def test_the_session_names_the_watchdog_checks_are_real(collector):
    """A typo here would disable the watchdog silently."""
    name, _ = collector.get_market_session()
    assert name in ("CLOSED", "PRE_MARKET", "REGULAR", "AFTER_HOURS")
