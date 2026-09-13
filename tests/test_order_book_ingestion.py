"""What the platform does with an order book, and what it still cannot do with one.

I told the user this platform "collects executions and disclosures, not orders",
and that the standard order-based manipulation typologies -- spoofing, layering,
wash trading -- were therefore inexpressible. That was wrong, and wrong in the
direction that matters: the platform asks a Tier-1 FIX venue for a **full order
book** and then discards the book at three separate points.

    build_market_data_request:  264=0   Full Book
                                263=1   Snapshot + Updates

The reply carries MDEntries as a repeating group -- 269 says bid, offer or
trade; 271 says the size resting there; **279 says New, Change or Delete**. The
parser read `fix_msg.get(270) or fix_msg.get(44)`, which takes the first price
in the message and drops the rest, so a level-five bid being *cancelled* and a
trade printing at the touch reached Kafka as the same `{ticker, price}` payload.

Order-based manipulation is defined by cancellations. The venue was being asked
for the one field those patterns are made of, and the parser threw it away.

Three separate gates, and only the first is closed here:

  1. the parser kept one price per message               -- fixed, tested below
  2. `ENABLE_FIX_CLIENT` defaults to false, so the client idles in standby
  3. `source="institutional_fix"` has no branch in the tradfi enricher, so even
     a connected venue's messages reach `dropped(...unrouted_source)` and are
     discarded before becoming events

2 and 3 are deliberately not closed by guesswork: 2 is an operator's decision
about a paid venue connection, and 3 needs a detector that consumes a book,
which is a feature rather than a repair. They are recorded here so the next
person reads a measurement instead of a confident sentence -- which is exactly
how I got this wrong the first time.
"""
import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _collector():
    """The tradfi collector, loaded without its package machinery.

    `services/collector-tradfi` is not an importable package name, and the
    module imports heavy clients at module scope, so the extractor is reached
    by loading the file directly.
    """
    spec = importlib.util.spec_from_file_location(
        "collector_tradfi_under_test",
        ROOT / "services" / "collector-tradfi" / "main.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def md():
    return _collector().market_data_entries


# FIX 4.4 wire shapes, as tag/value pairs in the order they arrive.

SNAPSHOT = [  # 35=W, two levels a side
    (35, b"W"), (55, b"AAPL"), (268, b"4"),
    (269, b"0"), (270, b"189.40"), (271, b"1200"), (1023, b"1"),
    (269, b"0"), (270, b"189.39"), (271, b"3400"), (1023, b"2"),
    (269, b"1"), (270, b"189.42"), (271, b"900"), (1023, b"1"),
    (269, b"1"), (270, b"189.43"), (271, b"5100"), (1023, b"2"),
]

INCREMENTAL_CANCEL = [  # 35=X, a resting bid pulled
    (35, b"X"), (268, b"1"),
    (279, b"2"), (269, b"0"), (55, b"AAPL"),
    (270, b"189.39"), (271, b"3400"), (278, b"ORD-88231"), (1023, b"2"),
]

TRADE_PRINT = [
    (35, b"X"), (268, b"1"),
    (279, b"0"), (269, b"2"), (55, b"AAPL"), (270, b"189.41"), (271, b"200"),
]


def test_a_snapshot_yields_every_level_on_both_sides(md):
    entries = md(SNAPSHOT)
    assert len(entries) == 4
    assert [e["entry_type"] for e in entries] == ["bid", "bid", "offer", "offer"]
    assert [e["price"] for e in entries] == [189.40, 189.39, 189.42, 189.43]
    assert [e["size"] for e in entries] == [1200.0, 3400.0, 900.0, 5100.0]
    assert [e["price_level"] for e in entries] == ["1", "2", "1", "2"]


def test_a_cancellation_survives_the_parser(md):
    """The single field order-based manipulation is defined by.

    `MDUpdateAction=2` is Delete. Without it a pulled bid and a new bid are the
    same message, and no amount of downstream cleverness can tell them apart.
    """
    entries = md(INCREMENTAL_CANCEL)
    assert len(entries) == 1
    assert entries[0]["entry_type"] == "bid"
    assert entries[0]["update_action"] == "delete"
    assert entries[0]["price"] == 189.39
    assert entries[0]["size"] == 3400.0
    assert entries[0]["entry_id"] == "ORD-88231"


def test_a_cancelled_bid_and_a_trade_print_are_distinguishable(md):
    """They were not. Both arrived as {ticker, price} and nothing else."""
    cancel = md(INCREMENTAL_CANCEL)[0]
    trade = md(TRADE_PRINT)[0]
    assert (cancel["entry_type"], cancel["update_action"]) == ("bid", "delete")
    assert (trade["entry_type"], trade["update_action"]) == ("trade", "new")
    assert cancel != trade


MULTI_ENTRY_INCREMENTAL = [  # 35=X, three changes to two instruments at once
    (35, b"X"), (268, b"3"),
    (279, b"2"), (269, b"0"), (55, b"AAPL"), (270, b"189.39"), (271, b"3400"), (1023, b"2"),
    (279, b"0"), (269, b"0"), (55, b"AAPL"), (270, b"189.38"), (271, b"5000"), (1023, b"3"),
    (279, b"1"), (269, b"1"), (55, b"MSFT"), (270, b"412.10"), (271, b"700"), (1023, b"1"),
]


def test_several_entries_in_one_incremental_message_stay_separate(md):
    """A repeating group has no delimiter: an entry ends where the next begins.

    This is the realistic shape of a busy book -- a pull, a replacement a tick
    lower, and an unrelated instrument, in one frame. Collapsing them would
    report one event where three things happened.
    """
    entries = md(MULTI_ENTRY_INCREMENTAL)
    assert len(entries) == 3
    assert [e["update_action"] for e in entries] == ["delete", "new", "change"]
    assert [e["symbol"] for e in entries] == ["AAPL", "AAPL", "MSFT"]
    assert [e["price"] for e in entries] == [189.39, 189.38, 412.10]


def test_a_pull_and_a_replacement_a_tick_lower_are_two_entries(md):
    """The shape layering is made of, and the reason sizes must survive.

    Nothing here detects it. What matters is that the data reaching Kafka can
    still express it -- a cancel at one level and a new order at another, with
    their sizes, rather than two identical prices.
    """
    pull, replace, _ = md(MULTI_ENTRY_INCREMENTAL)
    assert (pull["update_action"], pull["price_level"], pull["size"]) == ("delete", "2", 3400.0)
    assert (replace["update_action"], replace["price_level"], replace["size"]) == ("new", "3", 5000.0)


def test_string_pairs_parse_the_same_as_bytes(md):
    """simplefix yields bytes; a replay fixture or a log yields str."""
    as_str = [(str(t), v.decode()) for t, v in INCREMENTAL_CANCEL]
    assert md(as_str) == md(INCREMENTAL_CANCEL)


def test_a_message_with_no_entries_yields_none(md):
    assert md([(35, b"0"), (49, b"VENUE")]) == []
    assert md([]) == []
    assert md(None) == []


def test_an_unparseable_price_does_not_lose_the_entry(md):
    """A malformed field costs that field, not the cancellation beside it."""
    entries = md([(279, b"2"), (269, b"0"), (270, b"n/a"), (271, b"3400")])
    assert len(entries) == 1
    assert entries[0]["update_action"] == "delete"
    assert "price" not in entries[0]
    assert entries[0]["size"] == 3400.0


def test_the_full_book_is_what_the_client_asks_for():
    """If the request narrows, the parser above is solving a problem nobody has."""
    source = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    request = source[source.index("def build_market_data_request"):]
    request = request[: request.index("return msg.encode()")]
    assert 'append_pair(264, "0")' in request, "no longer requesting the full book"
    assert 'append_pair(263, "1")' in request, "no longer requesting incremental updates"


def test_the_collector_no_longer_keeps_only_the_first_price():
    source = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    loop = source[source.index("async def run_institutional_fix"):]
    assert '"md_entries": entries' in loop, (
        "the FIX loop publishes a payload with no book in it; a collector that "
        "discards what the venue sent has made a decision nothing downstream "
        "can undo"
    )


# ── What is still not possible, stated as a measurement ─────────────────────


def test_the_fix_venue_is_off_by_default_and_says_so():
    """Not a defect. A paid venue connection is an operator's decision.

    Recorded because "the platform has an order book" and "the platform is
    receiving an order book" are different claims, and the gap between them is
    one environment variable.
    """
    source = (ROOT / "services" / "collector-tradfi" / "main.py").read_text(encoding="utf-8")
    assert 'os.getenv("ENABLE_FIX_CLIENT", "false")' in source
    assert "Standby mode active" in source


def test_no_enricher_claims_the_fix_source_and_that_is_recorded():
    """The third gate, and the one that would waste a venue connection.

    `services/enrichment/enrichers/tradfi.py` dispatches on `source`. There is
    no branch for `institutional_fix`, so every FIX message -- book and all --
    falls through to `dropped(...unrouted_source)` and never becomes an event.
    Turning the client on without this would produce a counter going up and
    nothing else, which is the exact shape of defect this audit keeps finding.

    Asserted rather than fixed: consuming a book needs a detector that reasons
    about resting liquidity and cancellations, and inventing one to make a test
    green is how mechanism-for-its-own-sake gets written.
    """
    enricher = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(
        encoding="utf-8"
    )
    routed = 'source == "institutional_fix"' in enricher or (
        '"institutional_fix"' in enricher
    )
    assert not routed, (
        "An enricher now claims institutional_fix. Delete this test and write "
        "one that asserts what it does with MDEntries -- particularly with a "
        "cancellation, which is the field the order-based typologies are "
        "defined by."
    )


def test_order_flow_imbalance_is_computed_from_trades_not_depth():
    """The name says book; the inputs are aggressor volume.

    Worth pinning because a reader of `MarketMicrostructure` would reasonably
    assume OFI means the book-depth quantity, and conclude the platform has
    depth. It does not use any: both call sites pass buy and sell *trade*
    volume.
    """
    for path in (
        ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py",
        ROOT / "services" / "enrichment" / "enrichers" / "crypto.py",
    ):
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            if "quant_calc.order_flow_imbalance(" in line:
                assert "vol" in line or "window" in line, (
                    f"{path.name}: order_flow_imbalance is now called with "
                    f"something other than trade volume -- {line.strip()!r}. If "
                    f"it is book depth, this platform has depth and the "
                    f"limitation recorded in this file is out of date."
                )
