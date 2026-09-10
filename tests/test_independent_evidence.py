"""Gap 3: evidence is counted; independence is what is scarce.

Three mechanisms in this platform counted sources and called the total
confidence: corroboration counted outlets, consensus fusion counted agreeing
agents, correlation breadth counted supporting events. The first two were
repaired earlier in this audit -- provenance-aware corroboration after Jaccard
overlap was measured at 0.600 for an independent reword against 0.636 for
near-verbatim syndication, and averaging fusion for agents that agree because
they read the same topic. The third was not.

`breadth = log1p(n_support - 1) / log1p(49)` counts events. Measured over 24
hours of live clusters: **1,601 of 1,632 drew every supporting event from a
single source**, at an average of 3.1 events across 1.02 distinct sources. The
term was reporting how chatty one feed is and calling it corroboration.

The correlation layer could not have known better: the event payload written
into the 48-hour window carried type, domain, score, tags, region, entities and
headline, and **not the source**. The independence question was unanswerable at
the point where confidence is computed.
"""
import math

import pytest

from services.correlation.main import _independent_support


def _breadth(effective):
    return min(1.0, math.log1p(max(0.0, effective - 1.0)) / math.log1p(49))


def test_one_source_repeating_is_not_three_witnesses():
    same = [{"source": "coinbase_spot"}] * 3
    diff = [{"source": s} for s in ("coinbase_spot", "binance_futures", "ethereum_rpc")]
    assert _independent_support(same) < _independent_support(diff)


def test_genuinely_independent_evidence_keeps_its_full_weight():
    diff = [{"source": s} for s in ("a", "b", "c")]
    assert _independent_support(diff) == pytest.approx(3.0)
    assert _breadth(_independent_support(diff)) == pytest.approx(0.281, abs=0.01)


def test_the_live_median_cluster_loses_about_a_third_of_its_breadth():
    """3.1 events across 1.02 sources was the measured average."""
    before = _breadth(3)
    after = _breadth(_independent_support([{"source": "one"}] * 3))
    assert after < before
    assert 0.25 < (before - after) / before < 0.45


def test_repeats_within_a_source_still_count_for_something():
    """A second report is weaker evidence than the first and is not worthless."""
    one = _independent_support([{"source": "a"}])
    ten = _independent_support([{"source": "a"}] * 10)
    assert one < ten < 10.0


def test_an_unknown_source_falls_back_to_the_count():
    """Events stored before `source` was carried must not be downgraded."""
    assert _independent_support([{}, {}, {}]) == 3.0
    assert _independent_support([{"source": None}] * 3) == 3.0
    assert _independent_support([{"source": ""}] * 3) == 3.0


def test_no_evidence_is_no_support():
    assert _independent_support([]) == 0.0
    assert _independent_support(None) == 0.0


def test_a_mixed_cluster_sits_between_the_two():
    mixed = [{"source": "a"}, {"source": "a"}, {"source": "b"}]
    assert _independent_support([{"source": "a"}] * 3) < _independent_support(mixed)
    assert _independent_support(mixed) < _independent_support(
        [{"source": s} for s in ("a", "b", "c")]
    )


def test_the_source_reaches_the_correlation_window():
    """The layer could not ask the question before this was carried."""
    import pathlib

    src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "services" / "correlation" / "event_store.py"
    ).read_text(encoding="utf-8")
    assert '"source": getattr(event, "source", None)' in src


def test_the_cluster_records_what_it_was_derived_from():
    import pathlib

    src = (
        pathlib.Path(__file__).resolve().parents[1]
        / "services" / "correlation" / "main.py"
    ).read_text(encoding="utf-8")
    assert '"distinct_sources"' in src
    assert '"independent_support"' in src
