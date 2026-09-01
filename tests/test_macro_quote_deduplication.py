"""
tests/test_macro_quote_deduplication.py

The macro tier polled on a fixed cadence and published on every cycle whether or
not the quote had moved.

Outside regular hours the IEX feed's latestTrade is the previous session's last
print, so it does not change. A six-hour overnight window therefore produced
~211 rows per ticker carrying exactly ONE distinct close -- and not for one
instrument but for all twelve macro anchors (CL=F, GC=F, NQ=F, ES=F, TLT, SI=F,
VXX, TIP, NG=F, BZ=F, ZC=F, ZW=F). QQQ, on a live feed over the same window,
wrote 45 distinct closes in 61 rows.

Three consumers read the duplicates as fact:

  * The hourly continuous aggregate SUMs volume, so a single real print of 1,000
    was reported as 111,000. That number measured the poll count.
  * Bar-over-bar returns are exactly zero on a repeat, so two frozen series
    correlate at |r| = 1.000 across the few bars that did move. The first live
    peer-graph run published six such pairs before this was found.
  * Volume-spike and trend detectors were reading the polling cadence.

The fix is to publish an observation only when it is one. Gaps where the market
was closed are the correct output; rows asserting a print that never happened
are not.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_macro_main():
    """Loaded by path: the directory name (collector-macro) is not importable."""
    path = ROOT / "services" / "collector-macro" / "main.py"
    spec = importlib.util.spec_from_file_location("collector_macro_main", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["collector_macro_main"] = module
    spec.loader.exec_module(module)
    return module


macro = pytest.importorskip("collector_macro_main") if False else _load_macro_main()


def test_an_unchanged_quote_has_the_same_fingerprint():
    """The frozen overnight case: nothing moved, so nothing is new."""
    quote = {"close": 716.76, "high": 717.58, "low": 713.16, "volume": 1000.0}
    assert macro._quote_fingerprint(quote) == macro._quote_fingerprint(dict(quote))


def test_a_moved_price_is_a_new_observation():
    a = {"close": 716.76, "high": 717.58, "low": 713.16, "volume": 1000.0}
    b = {**a, "close": 716.77}
    assert macro._quote_fingerprint(a) != macro._quote_fingerprint(b)


def test_volume_alone_makes_it_new():
    """A print at the same price is still a print. Volume moving while price
    holds is ordinary, and suppressing it would discard real activity."""
    a = {"close": 716.76, "high": 717.58, "low": 713.16, "volume": 1000.0}
    b = {**a, "volume": 2000.0}
    assert macro._quote_fingerprint(a) != macro._quote_fingerprint(b)


def test_the_range_is_part_of_the_fingerprint():
    """High and low move when the session extends even if the last trade holds."""
    a = {"close": 716.76, "high": 717.58, "low": 713.16, "volume": 1000.0}
    assert macro._quote_fingerprint(a) != macro._quote_fingerprint({**a, "high": 718.0})
    assert macro._quote_fingerprint(a) != macro._quote_fingerprint({**a, "low": 712.0})


def test_a_missing_field_does_not_raise():
    """Tier 2 and Tier 3 providers do not all return every field, and a
    fingerprint that throws would take the whole publish path down with it."""
    assert macro._quote_fingerprint({}) == (0.0, 0.0, 0.0, 0.0)
    assert macro._quote_fingerprint({"close": None, "volume": None})[0] == 0.0


def test_the_event_id_is_derived_from_the_quote_not_the_clock():
    """A clock-derived id is unique by construction, which means no consumer
    downstream can ever recognise a repeat. The id must repeat when the
    observation does."""
    import re

    source = (ROOT / "services" / "collector-macro" / "main.py").read_text(encoding="utf-8")
    event_id_line = re.search(r'f"macro_\{ticker\}_[^"]*"', source).group(0)
    assert "time.time()" not in event_id_line
    assert "fingerprint" in event_id_line
