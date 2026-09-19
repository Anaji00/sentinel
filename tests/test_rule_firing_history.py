"""What the pruning curator is told about how often a rule fires.

`times_fired` is the only quantitative input to the decision about which rules
survive, and it came from `MetricsCollector`, whose counters are a
process-local defaultdict. They reset whenever the correlation service
restarts, which is every deploy.

Measured with the correlation service 53 seconds old:

    metric                              durable (30d)
    rule_aviation_activity_surge   3         1,606
    rule_maritime_chokepoint_ev    2         1,816
    SEMANTIC_001                   -       163,198
    HAWKES_EXCITATION              -       102,341

So a curator asked which rules to retire was reading a number three orders of
magnitude low, and could not see the two rules that fire most.
"""

from unittest.mock import AsyncMock

import pytest

from shared.utils.rule_feedback import firing_counts_from_history


class _Rows:
    """A db client returning whatever rows the test wants."""

    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    async def query(self, sql, *params):
        self.queries.append((sql, params))
        return self._rows


@pytest.mark.asyncio
async def test_firing_counts_come_from_the_durable_record():
    db = _Rows([
        {"rule_id": "SEMANTIC_001", "times_fired": 163198},
        {"rule_id": "HAWKES_EXCITATION", "times_fired": 102341},
        {"rule_id": "rule_maritime_chokepoint_evasion", "times_fired": 1816},
    ])
    counts = await firing_counts_from_history(db, days=30)

    assert counts["SEMANTIC_001"] == 163198
    assert counts["HAWKES_EXCITATION"] == 102341
    assert counts["rule_maritime_chokepoint_evasion"] == 1816

    sql, params = db.queries[0]
    assert "correlations" in sql, "the durable source is the correlations table"
    assert params == (30,), "the window must be parameterised, not interpolated"


@pytest.mark.asyncio
async def test_a_missing_database_returns_empty_rather_than_raising():
    """The caller falls back to the metric, so this must not throw."""
    assert await firing_counts_from_history(None) == {}


@pytest.mark.asyncio
async def test_a_failing_query_is_counted_not_swallowed_silently():
    class _Broken:
        async def query(self, *_a, **_k):
            raise RuntimeError("connection lost")

    from shared.utils.quiet_failures import reset, snapshot

    reset()
    assert await firing_counts_from_history(_Broken()) == {}
    sites = snapshot()
    assert any("firing_counts_from_history" in s for s in sites), (
        "a failure here silently degrades rule pruning and must be counted"
    )


@pytest.mark.asyncio
async def test_rows_without_a_rule_id_are_skipped():
    db = _Rows([
        {"rule_id": None, "times_fired": 99},
        {"rule_id": "", "times_fired": 5},
        {"rule_id": "real_rule", "times_fired": 7},
    ])
    assert await firing_counts_from_history(db) == {"real_rule": 7}


@pytest.mark.asyncio
async def test_labelled_metric_variants_are_not_counted_twice():
    """`collect_all` returns each counter twice, bare and per-service.

    Live, `firing_counts` returned four entries for two rules:
    `rule_aviation_activity_surge` and
    `rule_aviation_activity_surge{service="correlation"}`, each at 3. The bare
    name is already the cross-service sum, so treating the labelled variant as
    another rule id puts a phantom rule in a mapping the curator reads.
    """
    import shared.utils.rule_feedback as rf

    async def fake_collect_all(_redis):
        return {
            "correlation": {
                "correlation_rule_fired:rule_x": 3.0,
                'correlation_rule_fired:rule_x{service="correlation"}': 3.0,
            }
        }

    import shared.utils.metrics as metrics_mod
    original = metrics_mod.collect_all
    metrics_mod.collect_all = fake_collect_all
    try:
        counts = await rf.firing_counts(AsyncMock())
    finally:
        metrics_mod.collect_all = original

    assert counts == {"rule_x": 3}, f"labelled variant leaked: {counts}"
