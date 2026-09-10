"""An event nothing knows how to enrich must be counted, not vanish.

Every enricher dispatches on `raw.source` through an if/elif chain that ends in
a bare `return None` -- and the crypto batch loop ended with no else at all, so
an unmatched event simply left the iteration. Nothing logged, nothing counted,
no dead letter.

That is not hypothetical. Two defects already found in this codebase have
exactly this shape, both recorded in comments next to the branches that now
handle them: every pre-market and after-hours equity bar was discarded for as
long as only `finnhub_equities` was routed, and the OKX funding poller's output
was thrown away after being collected correctly because the branch matched on
venue instead of trade type. Both were found by accident.

These tests assert on the counter rather than on the log, because the log level
is the thing that failed last time: these paths spoke at DEBUG into a
deployment running at INFO.
"""
import pytest

from shared.utils.quiet_failures import dropped, reset, snapshot, swallowed


def test_dropped_counts_by_site():
    reset()
    for _ in range(3):
        dropped("enrichment.tradfi.unrouted_source", "no branch for source='smoke_test'")
    dropped("enrichment.crypto.unrouted_source", "no branch for source='okx'")

    counts = snapshot()
    assert counts["enrichment.tradfi.unrouted_source"]["count"] == 3
    assert counts["enrichment.crypto.unrouted_source"]["count"] == 1


def test_dropped_and_swallowed_share_one_counter_space():
    """Both are 'something failed and nobody raised'; one snapshot shows both."""
    reset()
    dropped("site.a", "unrouted")
    swallowed("site.b", ValueError("boom"))
    assert set(snapshot()) == {"site.a", "site.b"}


def test_first_occurrence_is_loud(caplog):
    """A routing gap that has never been seen says so at WARNING immediately."""
    reset()
    with caplog.at_level("WARNING"):
        dropped("site.first", "no branch for source='new_collector'")
    assert any("site.first" in r.getMessage() for r in caplog.records)


def test_repeat_occurrences_do_not_flood(caplog):
    """The 500th unmatched event of the same kind must not print 500 warnings."""
    reset()
    with caplog.at_level("WARNING"):
        for _ in range(50):
            dropped("site.repeat", "unrouted")
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    # Escalates at 1 and 10 only; the interval throttle covers the rest.
    assert len(warnings) == 2, [r.getMessage() for r in warnings]


@pytest.mark.anyio
async def test_tradfi_enricher_counts_an_unroutable_source():
    """The real chain, not a stand-in for it."""
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    class _Raw:
        source = "definitely_not_a_real_collector"
        raw_payload = {"ticker": "ZZPROBE", "price": 1.0, "volume": 1}

    reset()
    enricher = TradFiEnricher(scorer=None, redis_client=None, graph_writer=None)
    assert await enricher.enrich(_Raw()) is None
    assert snapshot()["enrichment.tradfi.unrouted_source"]["count"] == 1


def test_a_clean_heartbeat_is_unchanged():
    """Nothing suppressed means nothing appended."""
    reset()
    from shared.utils.quiet_failures import heartbeat_line

    assert heartbeat_line() == ""


def test_the_heartbeat_names_the_busiest_sites():
    """snapshot() existed from day one and nothing called it."""
    reset()
    from shared.utils.quiet_failures import heartbeat_line

    for _ in range(7):
        dropped("enrichment.tradfi.unrouted_source", "unrouted")
    dropped("enrichment.crypto.unrouted_source", "unrouted")

    line = heartbeat_line()
    assert "enrichment.tradfi.unrouted_source=7" in line
    assert line.startswith(" | suppressed:")


def test_the_heartbeat_stays_short_under_many_sites():
    """A heartbeat that prints fifty counters is a heartbeat nobody reads."""
    reset()
    from shared.utils.quiet_failures import heartbeat_line

    for i in range(20):
        dropped(f"site.{i}", "unrouted")
    line = heartbeat_line(top=3)
    assert line.count("=") == 3
    assert "(+17 more)" in line
