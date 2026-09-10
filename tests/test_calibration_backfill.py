"""Gap 4: the loop was closed and starting from cold.

`record_outcome` fires when a scenario resolves and only then, so the fit
depends on the tracker being up at that instant -- and a restart loses every
sample that landed while it was down. Measured on this deployment: **562
resolved scenarios in the database, 0 calibration samples in Redis.**

Most of those cannot help, and that is worth saying rather than working around:
554 point at correlations whose `confidence_score` is null because they were
published before the engine recorded one. There is no published confidence to
calibrate against, and inventing one would be worse than the gap. Recent
clusters all carry it -- 1,014 of 1,014 SEMANTIC_001 clusters in the last 24
hours -- so the sweep becomes useful as those resolve.

Gap 4 asks for consequence rather than rarity: which score bands have preceded
something a person acted on. **The platform records no analyst action**, so
consequence in its true form is not measurable here and the nearest honest
proxy is whether the scenario confirmed. That limit is stated rather than
papered over with a table fitted on a substitute nobody chose.
"""
import pytest

from shared.utils.confidence_calibration import (
    BACKFILLED_KEY,
    backfill_from_resolved,
)


class _Raw:
    def __init__(self):
        self.sets = {}
        self.lists = {}

    async def sismember(self, key, val):
        return val in self.sets.get(key, set())

    async def sadd(self, key, val):
        self.sets.setdefault(key, set()).add(val)

    def pipeline(self):
        return self

    def lpush(self, key, val):
        self.lists.setdefault(key, []).insert(0, val)

    def ltrim(self, *a):
        pass

    async def execute(self):
        pass


class _Client:
    def __init__(self):
        self.raw = _Raw()


class _DB:
    def __init__(self, rows):
        self.rows = rows

    async def query(self, sql, *args):
        return self.rows


def _row(sid, status="confirmed", conf=0.8):
    return {"scenario_id": sid, "status": status, "raw_confidence": conf}


@pytest.mark.anyio
async def test_it_records_what_the_live_path_missed():
    c = _Client()
    out = await backfill_from_resolved(_DB([_row("s1"), _row("s2", "denied", 0.4)]), c)
    assert out["recorded"] == 2
    assert len(c.raw.lists["sentinel:calibration:correlation_outcomes"]) == 2


@pytest.mark.anyio
async def test_running_it_twice_does_not_double_count():
    """It runs every sweep, so this is the property that matters."""
    c = _Client()
    db = _DB([_row("s1"), _row("s2")])
    first = await backfill_from_resolved(db, c)
    second = await backfill_from_resolved(db, c)
    assert first["recorded"] == 2
    assert second["recorded"] == 0
    assert second["already_seen"] == 2
    assert len(c.raw.lists["sentinel:calibration:correlation_outcomes"]) == 2


@pytest.mark.anyio
async def test_a_resolution_with_no_published_confidence_is_counted_and_skipped():
    """554 of the 562 are this. Nothing is invented for them."""
    c = _Client()
    out = await backfill_from_resolved(_DB([_row("s1", conf=None), _row("s2")]), c)
    assert out["no_confidence"] == 1
    assert out["recorded"] == 1


@pytest.mark.anyio
async def test_the_outcome_recorded_matches_the_status():
    c = _Client()
    await backfill_from_resolved(_DB([_row("a", "confirmed", 0.9), _row("b", "denied", 0.9)]), c)
    import json

    pairs = [json.loads(x) for x in c.raw.lists["sentinel:calibration:correlation_outcomes"]]
    assert sorted(p[1] for p in pairs) == [0, 1]


@pytest.mark.anyio
async def test_a_missing_store_is_not_an_error():
    assert (await backfill_from_resolved(None, _Client()))["recorded"] == 0
    assert (await backfill_from_resolved(_DB([]), None))["recorded"] == 0


@pytest.mark.anyio
async def test_a_failing_query_returns_a_report_rather_than_raising():
    class _Broken:
        async def query(self, *a):
            raise RuntimeError("timescale down")

    out = await backfill_from_resolved(_Broken(), _Client())
    assert out["considered"] == 0


def test_the_sweep_calls_it():
    import inspect

    import services.reasoning.scenario_tracker as t

    assert "backfill_from_resolved" in inspect.getsource(t.ScenarioTracker.check_all), (
        "the backfill exists and nothing calls it"
    )


def test_the_dedup_set_is_separate_from_the_samples():
    assert BACKFILLED_KEY != "sentinel:calibration:correlation_outcomes"


def test_both_sweeps_run_before_the_expensive_loop():
    """Placement is the whole defect.

    These were at the end of `check_all`, and in sixty minutes of live running
    they fired zero times while two sweeps started. Each scenario costs up to
    ten database queries and the sweep carries a hundred against a 5.8 GB
    events table, so a pass does not finish inside the 1,800-second interval --
    anything after the loop is reached at the loop's cadence, which here meant
    never.

    Neither depends on the loop's results, so both belong in front of it.
    """
    import inspect

    import services.reasoning.scenario_tracker as t

    src = inspect.getsource(t.ScenarioTracker.check_all)
    loop_at = src.index("for scenario in active")
    assert src.index("_offer_open_questions") < loop_at
    assert src.index("backfill_from_resolved") < loop_at
