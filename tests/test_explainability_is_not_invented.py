"""The one endpoint whose entire output is a claim about how a number was made.

`/explain/event/{id}` backs `ExplainabilityModal` -- a panel titled "Factor
Attribution Waterfall" and "Score Derivation Timeline" that a person opens
precisely when they want to know whether to believe a score. Almost none of
what it showed was measured:

  * an event id that matched nothing returned a fully-populated explanation of
    an NVDA volatility anomaly that never happened;
  * the four waterfall bars carried fixed weights (0.40 / 0.30 / 0.20 / 0.10)
    against a streaming RRCF scorer that has no linear composite, and
    substituted 2.4, 0.35, 0.5 and 35.0 for whichever inputs were absent;
  * the four derivation steps were constants whose deltas were computed as
    fractions of the final score, so the arithmetic always reconciled while
    asserting of every event that its entity was on a top-tier watchlist and
    that three correlated prints had landed in sixty seconds.

The real derivation existed the whole time. `NormalizedEvent` carries
`anomaly_breakdown` and an ordered list of `ScoreAdjustment`, the scorer fills
both, and the events table had no column for either -- so the writer dropped
them, and the endpoint that exists to read them had nothing and invented the
rest. Migration 0024 is that column.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _DB:
    """An events table holding exactly the rows it is given."""

    def __init__(self, rows=None):
        self._rows = rows or []

    async def query(self, sql, *params):
        if not self._rows:
            return []
        return [r for r in self._rows if str(r["event_id"]) == str(params[0])]


SCORED_EVENT = {
    "event_id": "e-1",
    "type": "price_anomaly",
    "source": "collector-tradfi",
    "occurred_at": "2026-09-13T00:00:00+00:00",
    "primary_entity_name": "PLTR",
    "anomaly_score": 0.78,
    "financial_data": {"ticker": "PLTR"},
    "anomaly_breakdown": {
        "volatility_z_score": 3.0,
        "volume_z_score": 1.0,
        "spatial_score": 0.0,
        "coverage_basis": "percentile",
        "coverage_fraction": 0.91,
    },
    "score_adjustments": [
        {"reason": "volume_capitulation_x1.4", "delta": 0.08},
        {"reason": "earnings_window", "delta": -0.02},
    ],
}


# -- nothing to explain is an answer -----------------------------------------


async def test_an_unknown_event_is_not_explained():
    """It used to be explained in full, as an NVDA anomaly at 0.88."""
    from fastapi import HTTPException

    from services.api_gateway.routes.explain import explain_event_alert

    with pytest.raises(HTTPException) as raised:
        await explain_event_alert("no-such-event", db=_DB(), redis=None)
    assert raised.value.status_code == 404


async def test_no_database_is_not_a_licence_to_invent_one():
    """`db` is an optional dependency, so None was the commonest path here.

    503, not 404: "not found" and "could not look" are different answers, and
    only one of them is about the event.
    """
    from fastapi import HTTPException

    from services.api_gateway.routes.explain import explain_event_alert

    with pytest.raises(HTTPException) as raised:
        await explain_event_alert("e-1", db=None, redis=None)
    assert raised.value.status_code == 503


# -- the waterfall shows what was measured -----------------------------------


async def test_the_waterfall_is_built_from_the_recorded_breakdown():
    from services.api_gateway.routes.explain import explain_event_alert

    out = await explain_event_alert("e-1", db=_DB([SCORED_EVENT]), redis=None)
    factors = {f["factor_key"]: f for f in out["factor_attribution"]}

    # Three dimensions were recorded, so three bars -- not four, and not the
    # five the platform can measure.
    assert set(factors) == {"volatility_z_score", "volume_z_score", "spatial_score"}
    assert factors["volatility_z_score"]["raw_subscore"] == 3.0
    # 3.0 of 4.0 measured.
    assert factors["volatility_z_score"]["contribution_pct"] == 75.0
    assert factors["volume_z_score"]["contribution_pct"] == 25.0
    # A dimension that measured zero contributed zero, and says so.
    assert factors["spatial_score"]["contribution_pct"] == 0.0
    # No weights: there is no linear composite to publish coefficients for.
    assert "model_weight" not in factors["volatility_z_score"]


async def test_an_event_with_no_breakdown_gets_an_empty_waterfall():
    """Most events carry none, and four invented bars was the old answer."""
    from services.api_gateway.routes.explain import explain_event_alert

    bare = dict(SCORED_EVENT, anomaly_breakdown=None, score_adjustments=None)
    out = await explain_event_alert("e-1", db=_DB([bare]), redis=None)
    assert out["factor_attribution"] == []
    assert out["score_adjustments"] == []
    assert out["score_basis"] is None


async def test_the_hawkes_factor_is_no_longer_a_constant():
    """35.0 appeared on every event ever explained, on a platform that runs
    an actual Hawkes correlator."""
    from services.api_gateway.routes.explain import explain_event_alert

    out = await explain_event_alert("e-1", db=_DB([SCORED_EVENT]), redis=None)
    assert not any(
        f["raw_subscore"] == 35.0 for f in out["factor_attribution"]
    )
    source = (ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(
        encoding="utf-8"
    )
    body = source[source.index("async def explain_event_alert"):]
    body = body[: body.index("async def _observed_win_rate")]
    assert '"score": 35.0' not in body


# -- the timeline is the steps the scorer recorded ---------------------------


async def test_the_derivation_is_the_events_own_adjustments():
    """Real deltas, and a base that reconciles to the score on the row."""
    from services.api_gateway.routes.explain import explain_event_alert

    out = await explain_event_alert("e-1", db=_DB([SCORED_EVENT]), redis=None)
    steps = out["score_adjustments"]
    assert len(steps) == 3, "base plus the two recorded adjustments"
    assert [s["action"] for s in steps[1:]] == [
        "volume_capitulation_x1.4",
        "earnings_window",
    ]
    assert steps[1]["delta"] == 0.08
    assert steps[2]["delta"] == -0.02
    # The arithmetic closes on the score the row actually carries.
    assert steps[-1]["score_after"] == pytest.approx(0.78)
    assert steps[0]["delta"] == pytest.approx(0.72)


def test_the_fabricated_waterfall_is_gone_from_the_source():
    """The reasons were asserted of every event, true or not."""
    source = (ROOT / "services" / "api_gateway" / "routes" / "explain.py").read_text(
        encoding="utf-8"
    )
    body = source[source.index("async def explain_event_alert"):]
    body = body[: body.index("async def _observed_win_rate")]
    for invented in (
        '"action": "Base IsolationForest Anomaly Score"',
        '"action": "Watchlist Prior Alignment Boost"',
        '"reason": "Entity is active member of top tier watchlist"',
        '"summary": "High-frequency volume and volatility anomaly detected on NVDA"',
    ):
        assert invented not in body, f"still emitting {invented}"


def test_the_badge_counts_the_steps_it_is_given():
    """It said "4 STEPS" because the server always sent four."""
    modal = (
        ROOT / "frontend" / "src" / "components" / "ExplainabilityModal.tsx"
    ).read_text(encoding="utf-8")
    assert ">4 STEPS<" not in modal
    assert "score_adjustments || []).length} STEPS" in modal


# -- and the provenance has somewhere to live --------------------------------


def test_the_score_provenance_reaches_the_table():
    """Built on every scored event, dropped by the writer, missed by everyone.

    The endpoint above could not have read the real derivation before this:
    `anomaly_breakdown` and `score_adjustments` are on the model, filled by the
    tradfi path, and had no column -- so the only thing surviving to the events
    table was the single float at the end.
    """
    migrations = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")
    assert "0024_events_score_provenance" in migrations
    assert "ADD COLUMN IF NOT EXISTS anomaly_breakdown JSONB" in migrations
    assert "ADD COLUMN IF NOT EXISTS score_adjustments JSONB" in migrations

    writer = (ROOT / "services" / "enrichment" / "db_writer.py").read_text(
        encoding="utf-8"
    )
    assert "coordinates, corroboration, anomaly_breakdown, score_adjustments" in writer
    assert "_dump('anomaly_breakdown')" in writer
    assert "_dump('score_adjustments')" in writer


def test_a_list_of_models_survives_the_dump_helper():
    """`score_adjustments` is a list, and the helper only knew two shapes.

    `_dump` returned `val.model_dump() if hasattr(val, "model_dump") else val`.
    A list has no `model_dump`, so it would have reached asyncpg as a list of
    `ScoreAdjustment` objects and failed the encode -- taking the whole batch
    of events with it, the same way the correlation-id coercion note above it
    describes.
    """
    from shared.models.events import NormalizedEvent, ScoreAdjustment, Entity, EntityType
    from services.enrichment.db_writer import DBWriter
    from datetime import datetime, timezone

    event = NormalizedEvent(
        event_id="11111111-1111-1111-1111-111111111111",
        type="price_anomaly",
        occurred_at=datetime.now(timezone.utc),
        source="collector-tradfi",
        primary_entity=Entity(id="PLTR", type=EntityType.COMPANY, name="Palantir"),
        headline="x",
        anomaly_score=0.5,
        score_adjustments=[ScoreAdjustment(reason="volume_capitulation_x1.4", delta=0.08)],
    )
    row = DBWriter(None)._extract_tuple(event)

    # Addressed by column name, not by position.
    #
    # This read `row[-1]`, which was the adjustments only for as long as they
    # happened to be last in the INSERT. Adding the macro_data column in
    # migration 0025 appended one element and the assertion silently began
    # checking a different field -- a test pinned to a position rather than to
    # the thing it is about.
    columns = _insert_columns()
    assert len(columns) == len(row), (
        f"the INSERT names {len(columns)} bound columns and the tuple carries "
        f"{len(row)}; one of them was changed without the other"
    )
    adjustments = row[columns.index("score_adjustments")]
    assert adjustments == [{"reason": "volume_capitulation_x1.4", "delta": 0.08}], (
        "the adjustments must reach asyncpg as plain dicts"
    )


def _insert_columns() -> list:
    """The bound columns of db_writer's INSERT, in placeholder order.

    `coordinates` is in the column list and is supplied by a CASE expression
    rather than a placeholder, so it is not part of the value tuple and is
    dropped here.
    """
    import re
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "services" / "enrichment" / "db_writer.py"
    text = src.read_text(encoding="utf-8")
    block = text[text.index("INSERT INTO events"):]
    names = block.split("(", 1)[1].split(")", 1)[0]
    return [
        c.strip() for c in re.split(r",\s*", names.replace(chr(10), " "))
        if c.strip() and c.strip() != "coordinates"
    ]
