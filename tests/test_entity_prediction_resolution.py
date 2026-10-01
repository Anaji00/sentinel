"""A prediction that cannot be wrong is not a prediction.

Measured on the running deployment 2026-09-21:

    agent_name                |  n | resolved
    adversarial_wargamer      | 50 |        0
    quant_trading_engine      | 19 |       19
    macro_intelligence_engine |  5 |        5

The agents' own resolution loop scores a directional price call: it needs a
ticker, an entry price and a horizon. A next-target prediction -- "the entity
this cascade reaches next is RAGNAR" -- carries none of the three, so those
fifty rows were never offered to it and no deadline existed at which they could
be judged. Production and attribution were fixed in earlier passes; grading
never worked at all.

The claim is checkable, just not by that loop. It is true if an event names the
entity inside the horizon and false if none does.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.entity_prediction_resolver import (  # noqa: E402
    DEFAULT_HORIZON_HOURS,
    brier,
    resolve_entity_predictions,
)


class _DB:
    """Records what the resolver asked and what it wrote."""

    def __init__(self, due, hits=()):
        self._due = due
        self._hits = {h.upper() for h in hits}
        self.updates = []

    async def query(self, sql, *args):
        if "FROM agent_predictions" in sql:
            return list(self._due)
        if "FROM events" in sql:
            return [{"?column?": 1}] if str(args[0]).upper() in self._hits else []
        return []

    async def execute(self, sql, *args):
        self.updates.append(args)


def _row(id=1, target="RAGNAR", confidence=0.8, hours_ago=48):
    return {
        "id": id,
        "predicted_target": target,
        "confidence": confidence,
        "occurred_at": datetime.now(timezone.utc) - timedelta(hours=hours_ago),
        "agent_name": "adversarial_wargamer",
    }


# ── it can be right, and it can be wrong ─────────────────────────────────────


@pytest.mark.asyncio
async def test_a_prediction_borne_out_is_marked_correct():
    db = _DB([_row()], hits=["RAGNAR"])
    out = await resolve_entity_predictions(db)
    assert out == {"graded": 1, "hit": 1, "missed": 0}
    assert db.updates[0][1] is True


@pytest.mark.asyncio
async def test_a_prediction_nothing_bore_out_is_marked_wrong():
    """The half that matters: this is what makes it a forecast."""
    db = _DB([_row()], hits=[])
    out = await resolve_entity_predictions(db)
    assert out == {"graded": 1, "hit": 0, "missed": 1}
    assert db.updates[0][1] is False


@pytest.mark.asyncio
async def test_the_entity_is_matched_case_insensitively():
    db = _DB([_row(target="ragnar")], hits=["RAGNAR"])
    assert (await resolve_entity_predictions(db))["hit"] == 1


# ── scoring ──────────────────────────────────────────────────────────────────


def test_brier_rewards_a_confident_hit_and_punishes_a_confident_miss():
    assert brier(0.9, True) < brier(0.5, True)
    assert brier(0.9, False) > brier(0.5, False)


def test_a_missing_confidence_is_scored_as_uncertain_not_as_certain():
    """Scoring an unstated confidence as 0.0 punishes a missing field."""
    assert brier(None, True) == brier(0.5, True)
    assert brier("nonsense", False) == brier(0.5, False)


def test_an_out_of_range_confidence_does_not_produce_a_wild_score():
    assert 0.0 <= brier(7.0, True) <= 1.0
    assert 0.0 <= brier(-3.0, False) <= 1.0


def test_a_perfect_call_scores_zero():
    assert brier(1.0, True) == 0.0


# ── what it refuses to grade ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_prediction_inside_its_horizon_is_not_graded():
    """Pending is not wrong. The SQL filters it; the contract is asserted here."""
    sql_seen = []

    class _Peek(_DB):
        async def query(self, sql, *args):
            sql_seen.append(sql)
            return await super().query(sql, *args)

    await resolve_entity_predictions(_Peek([]), horizon_hours=DEFAULT_HORIZON_HOURS)
    due = sql_seen[0]
    assert "resolved_at IS NULL" in due
    assert "occurred_at < NOW() -" in due


@pytest.mark.asyncio
async def test_directional_predictions_are_left_to_their_own_resolver():
    """Rows with a ticker belong to the price path; grading both would double-count."""
    sql_seen = []

    class _Peek(_DB):
        async def query(self, sql, *args):
            sql_seen.append(sql)
            return await super().query(sql, *args)

    await resolve_entity_predictions(_Peek([]))
    assert "ticker IS NULL" in sql_seen[0]


@pytest.mark.asyncio
async def test_an_unnamed_target_is_not_graded():
    db = _DB([_row(target="unknown")], hits=[])
    assert (await resolve_entity_predictions(db))["graded"] == 0 or not db.updates


# ── failure is not evidence ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_grading_failure_leaves_the_row_unresolved():
    """A database error is not a refuted forecast."""

    class _Broken(_DB):
        async def execute(self, sql, *args):
            raise RuntimeError("write failed")

    db = _Broken([_row()], hits=["RAGNAR"])
    out = await resolve_entity_predictions(db)
    assert out["graded"] == 0


@pytest.mark.asyncio
async def test_no_database_is_zero_not_a_crash():
    assert await resolve_entity_predictions(None) == {"graded": 0, "hit": 0, "missed": 0}


# ── wired to something that runs ─────────────────────────────────────────────


def test_the_worker_that_writes_these_rows_also_grades_them():
    """`snapshot()` existed and reached nothing; this is that shape's home."""
    src = (ROOT / "services/telemetry-worker/main.py").read_text(encoding="utf-8")
    assert "resolve_entity_predictions" in src
    assert "_entity_prediction_loop" in src
    assert "resolver_task.cancel()" in src, "the loop must stop with the worker"


# ── the timestamp type this client actually hands back ───────────────────────


def test_an_iso_string_timestamp_is_coerced():
    """asyncpg refuses a string for a timestamptz argument.

    Live, the first sweep failed on all thirty due rows:

        invalid input for query argument $2:
        '2026-09-02T15:21:17.080732+00:00'
        (expected a datetime.date or datetime.datetime instance, got 'str')
    """
    from shared.utils.entity_prediction_resolver import _as_datetime

    got = _as_datetime("2026-09-02T15:21:17.080732+00:00")
    assert isinstance(got, datetime)
    assert got.year == 2026 and got.month == 9


def test_a_datetime_passes_through_unchanged():
    from shared.utils.entity_prediction_resolver import _as_datetime

    now = datetime.now(timezone.utc)
    assert _as_datetime(now) is now


def test_an_unreadable_timestamp_is_none_not_an_exception():
    from shared.utils.entity_prediction_resolver import _as_datetime

    assert _as_datetime("not a date") is None
    assert _as_datetime(None) is None
    assert _as_datetime("") is None


@pytest.mark.asyncio
async def test_the_query_receives_a_datetime_not_a_string():
    """What the live failure would have caught, had anything asserted it."""
    seen = []

    class _Capture(_DB):
        async def query(self, sql, *args):
            if "FROM events" in sql:
                seen.append(args[1])
            return await super().query(sql, *args)

    row = _row()
    row["occurred_at"] = "2026-09-02T15:21:17.080732+00:00"
    await resolve_entity_predictions(_Capture([row], hits=["RAGNAR"]))
    assert seen and isinstance(seen[0], datetime)


@pytest.mark.asyncio
async def test_a_row_with_no_usable_timestamp_is_left_unresolved():
    db = _DB([_row()], hits=["RAGNAR"])
    db._due[0]["occurred_at"] = "garbage"
    out = await resolve_entity_predictions(db)
    assert out["graded"] == 0 and not db.updates
