"""
tests/test_adaptive_poll.py

Poll slower when polling is not producing anything.

Measured over seven days, SEC filings by hour UTC:

    hour 20        527      the post-close 8-K and Form 4 surge
    hours 10-21    107-212
    hours 2-3      163, 30
    hours 0,1,4-9,22   ZERO, every day

Nine hours a day that have never produced a filing, polled at a flat 90s in
cycles taking 21-26 seconds each. That is ~360 cycles a night to ingest nothing.

The obvious fix -- pace it by the market session, as the radar and tradfi
collectors do -- is wrong here, and the tests below pin why: filings peak in the
hour the equity calendar calls AFTER_HOURS, which carries a 3x slowdown. Session
pacing would back this feed off exactly when it is busiest.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.adaptive_poll import AdaptivePoll, DEFAULT_MAX_POLL_SEC


def test_a_productive_cycle_polls_at_the_base_rate():
    p = AdaptivePoll(base_seconds=90)
    assert p.after(3) == 90
    assert p.empty_cycles == 0


def test_empty_cycles_back_off_geometrically():
    p = AdaptivePoll(base_seconds=90)
    assert p.after(0) == 180
    assert p.after(0) == 360
    assert p.after(0) == 720


def test_the_interval_is_capped():
    """A feed unpolled for an hour cannot notice that it broke.

    The same reason market_session bounds its own idle interval: past some
    point, "quiet" and "dead" stop being distinguishable, and the freshness
    tracker would rightly call it stale.
    """
    p = AdaptivePoll(base_seconds=90)
    for _ in range(30):
        interval = p.after(0)
    assert interval == DEFAULT_MAX_POLL_SEC
    assert DEFAULT_MAX_POLL_SEC == 900.0


def test_one_result_resets_immediately():
    """Backing off gradually after a hit would keep a feed slow through the
    first half hour of its busy period, which is the half hour that matters
    most on a publication schedule."""
    p = AdaptivePoll(base_seconds=90)
    for _ in range(10):
        p.after(0)
    assert p.after(1) == 90
    assert p.empty_cycles == 0


def test_a_cycle_that_cannot_say_what_it_produced_counts_as_empty():
    """None is not evidence of a result."""
    p = AdaptivePoll(base_seconds=90)
    assert p.after(None) == 180
    p2 = AdaptivePoll(base_seconds=90)
    assert p2.after("nonsense") == 180


def test_nine_dead_hours_cost_far_fewer_cycles():
    """The measured case: hours 0, 1, 4-9 and 22 carry no filings at all."""
    flat = int(9 * 3600 / 90)
    p = AdaptivePoll(base_seconds=90)
    elapsed, cycles = 0.0, 0
    while elapsed < 9 * 3600:
        elapsed += p.after(0)
        cycles += 1
    assert cycles < flat / 5, f"{cycles} cycles vs {flat} flat"


def test_the_filings_collector_paces_on_yield_not_the_clock():
    src = (ROOT / "services" / "collector-filings" / "main.py").read_text(encoding="utf-8")
    assert "AdaptivePoll" in src
    assert "_poll_pacer.after(new_filings)" in src, (
        "the pacer must be fed what the cycle produced, not a constant"
    )
    assert "asyncio.sleep(POLL_INTERVAL_SEC)" not in src


def test_the_filings_session_does_not_reuse_connections():
    """The same stale keep-alive defect the Form 4 poller had.

    157 suppressed TimeoutErrors on `collector_filings.firehose.fetch (8-K)`,
    and poll cycles taking 21-26 seconds to ingest nothing.
    """
    src = (ROOT / "services" / "collector-filings" / "main.py").read_text(encoding="utf-8")
    assert "force_close=True" in src


def test_session_pacing_would_slow_the_busiest_filing_hour():
    """Pins the reason this feed is not paced by the market session.

    Hour 20 UTC is 4pm ET and carries 527 of the week's filings -- and the
    equity calendar calls it AFTER_HOURS.
    """
    from shared.utils.market_session import SESSION_POLL_MULTIPLIER, Session

    assert SESSION_POLL_MULTIPLIER[Session.AFTER_HOURS] > 1.0, (
        "if this ever becomes 1.0 the argument above needs rechecking"
    )
