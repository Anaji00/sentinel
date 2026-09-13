"""The parts of the correlation path a scenario cannot reach on its own.

A scenario asks whether the platform sees a situation. These ask about the
machinery underneath it, in the places where a wrong answer produces a
plausible-looking finding rather than an error -- which is every place in this
file.

All of them come from re-reading the repairs in this pass rather than from
running them. Four were defects in those repairs.
"""
import asyncio
from datetime import datetime, timezone

import pytest

from tests.integration.market_scenarios import (
    Beat,
    InMemoryRedis,
    RecordingDB,
    _as_row,
    _event,
    cluster_for,
    run_scenario,
)

pytestmark = pytest.mark.anyio

HOUR = 60.0
DAY = 24 * HOUR


# ── The deep window ─────────────────────────────────────────────────────────


def _store_with(rows, redis=None):
    from services.correlation.event_store import EventStore

    return EventStore(redis_client=redis or InMemoryRedis(), db_client=RecordingDB(rows))


async def test_the_deep_window_applies_the_cache_s_own_anomaly_floor():
    """Otherwise one clause sees two different populations.

    `add_event` declines anything below RECENT_WINDOW_MIN_ANOMALY, so the
    cached half of a window has never held one. The database half has no such
    rule, so a clause asking for `min_anomaly: 0.0` would draw evidence from
    last week that it could not draw from yesterday -- and the older, weaker
    events would outrank nothing and simply appear.
    """
    from services.correlation.event_store import RECENT_WINDOW_MIN_ANOMALY

    now = datetime.now(timezone.utc)
    beat = Beat("options_flow", "ACME", 5 * DAY, RECENT_WINDOW_MIN_ANOMALY - 0.05,
                source="alpaca_options")
    store = _store_with([_as_row(_event(beat, now))])

    hits = await store.get_recent(["options_flow"], hours=168, min_anomaly=0.0)
    assert hits == [], (
        "A sub-floor event five days old was returned by a clause that could "
        "not have seen the same event yesterday."
    )


async def test_the_deep_window_reads_the_columns_the_rows_are_written_to():
    """`proximity_km` needs coordinates, and they live in two places.

    `db_writer` writes `latitude`/`longitude` and derives `coordinates` from
    them, so a row with only one of the pair has a NULL geography. Reading the
    geography alone made every deep-window hit coordinate-less, which a
    proximity join reads as "cannot be evaluated" and drops.
    """
    now = datetime.now(timezone.utc)
    beat = Beat("vessel_dark", "IMO9402304", 5 * DAY, 0.7, source="aisstream",
                latitude=26.57, longitude=56.25)
    store = _store_with([_as_row(_event(beat, now))])

    hits = await store.get_recent(["vessel_dark"], hours=168, min_anomaly=0.2)
    assert hits, "the deep window returned nothing"
    assert hits[0]["latitude"] == pytest.approx(26.57)
    assert hits[0]["longitude"] == pytest.approx(56.25)


async def test_a_slow_database_costs_the_older_days_and_nothing_else():
    """This read is inside the per-event rule loop.

    It is the only part of the correlation path that something outside the
    process can make slow, so a degraded database has to cost a seven-day
    clause its older five days rather than stall the consumer behind it.
    """
    from services.correlation.event_store import DEEP_WINDOW_TIMEOUT_SEC

    class _Slow(RecordingDB):
        async def query(self, sql, *args):
            if "FROM events" in sql:
                await asyncio.sleep(DEEP_WINDOW_TIMEOUT_SEC + 1.0)
            return []

    from services.correlation.event_store import EventStore

    now = datetime.now(timezone.utc)
    redis = InMemoryRedis()
    store = EventStore(redis_client=redis, db_client=_Slow())
    # One event inside the cached window, so there is something to return.
    await store.add_event(_event(
        Beat("options_flow", "ACME", 2 * HOUR, 0.6, source="alpaca_options"), now
    ))

    started = asyncio.get_event_loop().time()
    hits = await store.get_recent(["options_flow"], hours=168, min_anomaly=0.2)
    elapsed = asyncio.get_event_loop().time() - started

    assert len(hits) == 1, "the cached window was lost along with the deep one"
    assert elapsed < DEEP_WINDOW_TIMEOUT_SEC + 0.5, (
        f"the clause waited {elapsed:.1f}s on a database that never answered"
    )


async def test_a_failing_database_degrades_the_clause_rather_than_the_rule():
    from services.correlation.event_store import EventStore

    class _Broken(RecordingDB):
        async def query(self, sql, *args):
            raise RuntimeError("connection reset")

    now = datetime.now(timezone.utc)
    redis = InMemoryRedis()
    store = EventStore(redis_client=redis, db_client=_Broken())
    await store.add_event(_event(
        Beat("options_flow", "ACME", 2 * HOUR, 0.6, source="alpaca_options"), now
    ))

    hits = await store.get_recent(["options_flow"], hours=168, min_anomaly=0.2)
    assert len(hits) == 1


async def test_an_event_in_both_stores_is_counted_once():
    """The window is pruned every 250 writes, so the two halves overlap.

    Redis can still hold an event slightly older than 48 hours when the prune
    has not run, and the database half of the same clause covers everything
    older than 48 hours. Without a dedupe the cluster would cite one event
    twice and count it twice toward breadth.
    """
    now = datetime.now(timezone.utc)
    # Just past the cache horizon, and written to both stores.
    beat = Beat("options_flow", "ACME", 49 * HOUR, 0.62, source="alpaca_options")
    event = _event(beat, now)

    from services.correlation.event_store import EventStore

    redis = InMemoryRedis()
    store = EventStore(redis_client=redis, db_client=RecordingDB([_as_row(event)]))
    # Written directly, bypassing the prune that add_event would run.
    await redis.zadd(store.cache_key, {
        __import__("json").dumps({
            "event_id": event.event_id, "type": "options_flow", "domain": "tradfi",
            "source": "alpaca_options", "anomaly_score": 0.62, "tags": [],
            "region": None, "latitude": None, "longitude": None,
            "headline": "h", "summary": None, "named_entities": [],
            "entity_name": "ACME", "entity_type": "company", "entity_id": "ACME",
        }): event.occurred_at.timestamp()
    })

    hits = await store.get_recent(["options_flow"], hours=168, min_anomaly=0.2)
    assert len(hits) == 1, f"the same event came back {len(hits)} times"


async def test_a_clause_inside_the_cache_never_touches_the_database():
    """Most clauses are 48 hours or less; they must pay nothing for this."""
    now = datetime.now(timezone.utc)
    store = _store_with([])
    await store.add_event(_event(
        Beat("options_flow", "ACME", 2 * HOUR, 0.6, source="alpaca_options"), now
    ))

    await store.get_recent(["options_flow"], hours=48, min_anomaly=0.2)
    assert store._db.deep_reads == 0

    await store.get_recent(["options_flow"], hours=72, min_anomaly=0.2)
    assert store._db.deep_reads == 1


# ── Structure and confidence ────────────────────────────────────────────────


def test_a_rule_is_not_penalised_for_naming_more_alternatives():
    """Listing what a clause accepts must not make it score lower.

    `_structural_completeness` divided matched types by the full declared list,
    so widening the chokepoint clause from four listed types to five dropped
    its structure from 0.5 to 0.4 on identical evidence -- penalising exactly
    the change that made the rule able to see a ship.
    """
    from services.correlation.main import _structural_completeness as f

    assert f(1, 1, [2], [4]) == f(1, 1, [2], [5]) == f(1, 1, [2], [9])


def test_converging_on_subjects_scores_like_converging_on_types():
    """The gate accepts either, so the score has to as well.

    Three aircraft squawking anomalies in one corridor is a convergence of
    evidence rather than of kinds, and it is the only way a clause whose other
    declared types have no producer can ever fire.
    """
    from services.correlation.main import _structural_completeness as f

    two_types_one_subject = f(1, 1, [2], [5], [1])
    one_type_three_subjects = f(1, 1, [1], [5], [3])
    assert one_type_three_subjects == two_types_one_subject == 1.0


def test_structure_is_credited_only_for_what_the_rule_declared():
    """A rule matching half its clauses has demonstrated half its claim."""
    from services.correlation.main import _structural_completeness as f

    assert f(2, 2, [], []) == 1.0
    assert f(1, 2, [], []) == 0.5
    assert f(0, 2, [], []) == 0.0


def test_the_breadth_scale_tops_out_where_corroboration_actually_can():
    """Six independent sources, not fifty.

    The widest clause in the shipped set lists six event types, so six is the
    most sources a cluster can draw on. Normalised against fifty, the term
    holding 30% of the confidence scale gave a two-source cluster 0.05 of its
    possible 0.30 and the scale's top quarter was unoccupiable.
    """
    from services.correlation.main import (
        INDEPENDENT_SUPPORT_SATURATION,
        SHIPPED_RULES,
        _rule_confidence,
    )

    # Across every clause of a rule, not one of them: a cluster draws evidence
    # from all the clauses that matched, so the ceiling on its distinct sources
    # is the rule's whole vocabulary.
    from shared.models.events import POSITION_TELEMETRY_TYPES

    widest, widest_rule = max(
        (
            len(
                {t for c in (r.get("correlations") or [])
                 for t in (c.get("event_types") or [])}
                - POSITION_TELEMETRY_TYPES
            ),
            r["rule_id"],
        )
        for r in SHIPPED_RULES
    )
    assert INDEPENDENT_SUPPORT_SATURATION >= widest, (
        f"{widest_rule} can draw on {widest} event types, so a cluster from it "
        f"can reach {widest} independent sources -- above the saturation point "
        f"of {INDEPENDENT_SUPPORT_SATURATION}, where the breadth term has "
        f"already gone flat. Widening a rule moves this ceiling; raise the "
        f"constant deliberately or narrow the rule."
    )

    class _E:
        anomaly_score = 0.0
        corroboration = None

    saturated = [
        {"source": f"s{i}", "type": "x"}
        for i in range(int(INDEPENDENT_SUPPORT_SATURATION))
    ]
    from services.correlation.main import RULE_CONF_BREADTH_WEIGHT

    assert _rule_confidence(_E(), saturated, set()) == pytest.approx(
        RULE_CONF_BREADTH_WEIGHT, abs=0.01
    ), "a maximally corroborated cluster does not reach the top of the scale"


# ── Joins ───────────────────────────────────────────────────────────────────


def test_a_temporal_join_is_not_a_join_without_a_trigger_time():
    """`_apply_temporal_constraint` returns its input when the time is missing.

    Deliberately -- dropping every hit would turn a missing timestamp into a
    rule that never fires. But the clause then claimed a relationship nothing
    had checked, and skipped the entity fallback on the strength of it, so a
    cross-domain rule joined by sequence alone passed everything in its window.
    """
    from services.correlation.main import _join_is_usable

    clause = {"follows_trigger": True, "within_minutes": 240}

    class _Timed:
        occurred_at = datetime.now(timezone.utc)
        region = None
        tags = []
        named_entities = []
        primary_entity = None
        latitude = longitude = None

    class _Untimed(_Timed):
        occurred_at = None

    assert _join_is_usable(clause, _Timed()) is True
    assert _join_is_usable(clause, _Untimed()) is False


def test_a_temporal_join_must_be_bounded_and_directed():
    from services.correlation.main import _declares_temporal_join
    from shared.models.correlation_rules import TEMPORAL_JOIN_MAX_MINUTES

    assert _declares_temporal_join({"follows_trigger": True, "within_minutes": 240})
    assert _declares_temporal_join({"precedes_trigger": True, "within_minutes": 60})
    # A direction with no bound is the window the clause already had.
    assert not _declares_temporal_join({"follows_trigger": True})
    # A bound with no direction says "nearby, either side".
    assert not _declares_temporal_join({"within_minutes": 60})
    # And too wide a bound is not a constraint.
    assert not _declares_temporal_join({
        "follows_trigger": True,
        "within_minutes": TEMPORAL_JOIN_MAX_MINUTES + 1,
    })


def test_a_shared_category_is_not_a_shared_subject():
    """The asymmetry, at the level it is implemented.

    Two companies both tagged "regulatory" are in one news category. The
    overlap has to reach one side's identity -- its entity or its named
    entities -- or a tag vocabulary joins everything it classifies.
    """
    from services.correlation.main import _identity_tokens, _shares_a_subject, _subject_tokens

    class _Trigger:
        tags = ["PFE", "regulatory", "healthcare"]
        named_entities = ["PFIZER", "PFE"]
        primary_entity = None

    trigger_identity = _identity_tokens(_Trigger())
    trigger_subjects = _subject_tokens(_Trigger())

    same_category = {"tags": ["DUK", "regulatory"], "named_entities": ["DUKE ENERGY"],
                     "entity_id": "DUK", "entity_name": "DUK"}
    same_subject = {"tags": ["tradfi", "equity_block", "pfe"], "named_entities": [],
                    "entity_id": "PFE", "entity_name": "PFE"}

    assert not _shares_a_subject(trigger_identity, trigger_subjects, same_category)
    assert _shares_a_subject(trigger_identity, trigger_subjects, same_subject)


def test_the_domain_and_type_vocabulary_cannot_join_anything():
    """Every tradfi event carries the tag "tradfi"."""
    from services.correlation.main import _fold

    assert _fold(["tradfi", "equity_block", "maritime", "crypto"]) == set()
    assert _fold(["tradfi", "ba"]) == {"ba"}


def test_proximity_joins_on_distance_and_drops_what_it_cannot_measure():
    from services.correlation.main import _apply_join_requirement, _km_between

    # Hormuz to Malacca is most of a hemisphere; Hormuz to Hormuz is not.
    assert _km_between((26.57, 56.25), (26.4, 56.4)) < 30
    assert _km_between((26.57, 56.25), (1.3, 103.8)) > 5000

    class _Trigger:
        latitude, longitude = 26.57, 56.25
        region = None
        tags = []
        named_entities = []
        primary_entity = None
        occurred_at = None
        type = None

    hits = [
        {"event_id": "near", "latitude": 26.4, "longitude": 56.4, "type": "vessel_dark"},
        {"event_id": "far", "latitude": 1.3, "longitude": 103.8, "type": "vessel_dark"},
        {"event_id": "nowhere", "latitude": None, "longitude": None, "type": "vessel_dark"},
    ]
    kept = _apply_join_requirement(
        hits, {"proximity_km": 50}, {"correlations": []}, _Trigger()
    )
    assert [h["event_id"] for h in kept] == ["near"]


# ── The evaluator as a whole ────────────────────────────────────────────────


async def test_a_finding_never_cites_its_own_trigger():
    """Self-corroboration would make every single-clause rule tautological."""
    from tests.integration import scenarios as S

    for scenario in [s for s in S.ALL_SCENARIOS if s.expect_rule][:6]:
        clusters, store, trigger = await run_scenario(scenario)
        for cluster in clusters:
            assert trigger.event_id not in (cluster.supporting_event_ids or []), (
                f"{scenario.name}: {cluster.rule_id} cites the trigger as its own "
                f"supporting evidence"
            )


async def test_the_same_scenario_twice_produces_the_same_rules():
    """A finding that depends on iteration order is not reproducible.

    Evidence is selected round-robin across types from a dict, and the window
    is read from a sorted set whose ties are broken by insertion. Neither is
    guaranteed to be stable unless it is.
    """
    from tests.integration import scenarios as S

    first, _, _ = await run_scenario(S.CPI_SURPRISE)
    second, _, _ = await run_scenario(S.CPI_SURPRISE)
    assert sorted(c.rule_id for c in first) == sorted(c.rule_id for c in second)


async def test_more_corroboration_never_lowers_confidence():
    """Monotonicity, on the one term that is supposed to reward evidence.

    `_independent_support` counts distinct sources fully and repeats within a
    source sub-linearly, so an extra event from a feed that already reported
    must add something and cannot subtract.
    """
    import dataclasses

    from tests.integration import scenarios as S

    base = S.CPI_SURPRISE
    richer = dataclasses.replace(
        base,
        name=base.name + " (with one more corroborating feed)",
        evidence=list(base.evidence) + [
            Beat("equity_block", "QQQ", 18, 0.6,
                 source="finnhub_equities",
                 headline="QQQ block on the print",
                 named_entities=["US-CPI"]),
        ],
    )

    thin, _, _ = await run_scenario(base)
    thick, _, _ = await run_scenario(richer)

    a = cluster_for(thin, base.expect_rule)
    b = cluster_for(thick, base.expect_rule)
    assert a and b
    assert b.confidence_score >= a.confidence_score, (
        f"adding an independent corroborating source lowered confidence from "
        f"{a.confidence_score:.3f} to {b.confidence_score:.3f}"
    )


async def test_evidence_below_the_clause_floor_is_not_cited():
    """The floor is what stops a rule resting on noise it already judged."""
    import dataclasses

    from tests.integration import scenarios as S

    weakened = dataclasses.replace(
        S.EARNINGS_SURPRISE,
        name="Earnings surprise with evidence under the clause floor",
        evidence=[dataclasses.replace(b, anomaly=0.21) for b in S.EARNINGS_SURPRISE.evidence],
    )
    clusters, _, _ = await run_scenario(weakened)
    assert cluster_for(clusters, "rule_earnings_surprise_flow") is None, (
        "the clause declares min_anomaly 0.30 and cited evidence at 0.21"
    )
