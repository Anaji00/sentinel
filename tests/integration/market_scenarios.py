"""Real market situations, expressed as event sequences the platform must see.

Every other correlation test in this repository asserts a mechanism: that a
window is filtered, that a join is applied, that a clause is counted. None of
them asks the question an analyst would ask -- *given what actually happened in
the market that week, does this platform say anything about it?*

The scenarios in `scenarios.py` are drawn from situations that recur often
enough to be worth naming: a Form 4 sale followed by put buying followed by a
gap down; an exchange liquidation cascade that reaches the listed crypto
proxies; a CPI print that moves rates, equities and gold within the hour; a
tanker going dark in the Strait of Hormuz. These are not exotic. If the
platform cannot see them it cannot see anything, and a test that fails here is
a gap in the product rather than a gap in the test.

This module is the harness: a faithful in-memory stand-in for the two pieces of
infrastructure the correlation path touches, so the *real* rule evaluator, the
*real* event store and the *real* domain resolution run unmodified. Nothing
here doubles a decision. The Redis double implements the four commands
`EventStore` actually issues, with Redis's own semantics for each; the database
double holds the events table and applies the real query's own WHERE clause to
it, so the deep-window read that serves any clause older than the cache is
exercised rather than stubbed.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.events import (  # noqa: E402
    Entity,
    EntityType,
    EventType,
    FinancialData,
    MacroReleaseData,
    NormalizedEvent,
    SupplyChainData,
)


# ── Infrastructure doubles ──────────────────────────────────────────────────
#
# Not mocks. A mock asserts that a call was made; these two execute the call.
# The distinction matters because every bug this harness is meant to catch
# lives in what the real code does with a real reply -- a mock returning a
# canned list of hits would pass a rule that queries the wrong window.


class _Pipeline:
    """Redis pipeline semantics: queue commands, apply them on execute()."""

    def __init__(self, redis: "InMemoryRedis") -> None:
        self._redis = redis
        self._queued: List[tuple] = []

    def zremrangebyscore(self, key: str, lo: Any, hi: Any) -> "_Pipeline":
        self._queued.append(("zremrangebyscore", key, lo, hi))
        return self

    async def execute(self) -> List[Any]:
        out = []
        for cmd, key, lo, hi in self._queued:
            if cmd == "zremrangebyscore":
                out.append(self._redis._zremrangebyscore(key, lo, hi))
        self._queued.clear()
        return out


class _Raw:
    """The `.raw` handle EventStore reaches through for native commands."""

    def __init__(self, redis: "InMemoryRedis") -> None:
        self._redis = redis

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self._redis)

    async def zrange(
        self,
        key: str,
        start: Any,
        end: Any,
        desc: bool = False,
        byscore: bool = False,
        offset: int = 0,
        num: Optional[int] = None,
        withscores: bool = False,
    ):
        """ZRANGE ... BYSCORE REV LIMIT, with Redis's argument order.

        Under REV, `start` is the high bound and `end` the low one -- the
        reversal that makes `zrange(key, "+inf", cutoff, desc=True)` mean "from
        now back to the cutoff". Getting this backwards in the double would
        make every window empty and every scenario fail for the wrong reason,
        so it is implemented the way the server does it rather than the way it
        reads.
        """
        if not byscore:
            raise NotImplementedError("EventStore only issues BYSCORE range reads")
        hi, lo = (start, end) if desc else (end, start)
        lo_f = float("-inf") if lo in ("-inf", None) else float(lo)
        hi_f = float("inf") if hi in ("+inf", None) else float(hi)

        members = [
            (member, score)
            for member, score in self._redis._zsets.get(key, {}).items()
            if lo_f <= score <= hi_f
        ]
        members.sort(key=lambda ms: ms[1], reverse=desc)
        window = members[offset:] if num is None else members[offset: offset + num]
        return window if withscores else [m for m, _ in window]


class InMemoryRedis:
    """Sorted sets, in a dict, with the commands EventStore issues."""

    def __init__(self) -> None:
        self._zsets: Dict[str, Dict[str, float]] = {}
        self.raw = _Raw(self)

    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        z = self._zsets.setdefault(key, {})
        added = sum(1 for m in mapping if m not in z)
        z.update(mapping)
        return added

    def _zremrangebyscore(self, key: str, lo: Any, hi: Any) -> int:
        z = self._zsets.get(key)
        if not z:
            return 0
        lo_f = float("-inf") if lo in ("-inf", None) else float(lo)
        hi_f = float("inf") if hi in ("+inf", None) else float(hi)
        doomed = [m for m, s in z.items() if lo_f <= s <= hi_f]
        for m in doomed:
            del z[m]
        return len(doomed)

    def cardinality(self, key: str = "events:recent_window") -> int:
        return len(self._zsets.get(key, {}))

    def members(self, key: str = "events:recent_window") -> List[Dict[str, Any]]:
        return [json.loads(m) for m in self._zsets.get(key, {})]


class RecordingDB:
    """An events table that answers the one query the correlation path reads.

    The Redis window holds 48 hours; the events hypertable holds 90 days, and
    `EventStore._deep_window` is what lets a clause asking for seven days see
    the five it has evicted. A double returning `[]` would make that path
    untestable and would make every long-window rule look correct.

    The WHERE clause is interpreted rather than parsed. `_deep_window` builds
    it from a fixed vocabulary of nine predicates, and each one is matched
    here against the exact string that generates it -- so a predicate that
    changes shape in production raises `UnsupportedPredicate` rather than
    being silently ignored, which is the failure mode that makes hand-written
    SQL doubles worthless.
    """

    class UnsupportedPredicate(RuntimeError):
        pass

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.executed: List[tuple] = []
        self.rows: List[Dict[str, Any]] = list(rows or [])
        self.deep_reads = 0

    async def execute(self, sql: str, *args) -> None:
        self.executed.append((sql, args))

    async def execute_many(self, sql: str, rows) -> None:
        self.executed.append((sql, tuple(rows)))

    async def query(self, sql: str, *args) -> List[Dict[str, Any]]:
        self.executed.append((sql, args))
        if "FROM events" not in sql:
            return []
        self.deep_reads += 1
        return self._select(sql, args)

    # ── the interpreter ─────────────────────────────────────────────────────

    def _select(self, sql: str, args) -> List[Dict[str, Any]]:
        where = sql.split("WHERE", 1)[1].split("ORDER BY", 1)[0]
        predicates = [p.strip() for p in where.split(" AND ")]
        rows = [r for r in self.rows if all(self._holds(p, r, args) for p in predicates)]
        rows.sort(key=lambda r: (r["anomaly_score"], r["occurred_at_epoch"]), reverse=True)
        limit = int(sql.split("LIMIT", 1)[1].strip())
        out = [dict(r) for r in rows[:limit]]
        # Derived columns, answered rather than omitted. A double that returns
        # the stored row and not the projection makes a query look correct when
        # the code reading it will find the column absent.
        if "AS move_pct" in sql:
            for row in out:
                row["move_pct"] = _row_move_pct(row)
        return out

    _PLACEHOLDER = re.compile(r"\$(\d+)")

    def _arg(self, predicate: str, args):
        match = self._PLACEHOLDER.search(predicate)
        if not match:
            raise self.UnsupportedPredicate(f"{predicate!r} binds no parameter")
        return args[int(match.group(1)) - 1]

    def _holds(self, predicate: str, row: Dict[str, Any], args) -> bool:
        if predicate.startswith("occurred_at >="):
            return row["occurred_at_epoch"] >= self._arg(predicate, args).timestamp()
        if predicate.startswith("occurred_at <"):
            return row["occurred_at_epoch"] < self._arg(predicate, args).timestamp()
        if predicate.startswith("COALESCE(anomaly_score, 0) >="):
            return float(row["anomaly_score"]) >= float(self._arg(predicate, args))
        if predicate.startswith("NOT (type = ANY("):
            return row["type"] not in self._arg(predicate, args)
        if predicate.startswith("type = ANY("):
            return row["type"] in self._arg(predicate, args)
        if predicate.startswith("UPPER(primary_entity_id) ="):
            return str(row.get("entity_id") or "").upper() == self._arg(predicate, args)
        if predicate.startswith("region ="):
            return row.get("region") == self._arg(predicate, args)
        if predicate.startswith("tags &&"):
            return bool(set(self._arg(predicate, args)) & set(row.get("tags") or []))
        if predicate.startswith("event_id::text <>"):
            return row["event_id"] != self._arg(predicate, args)
        if predicate.startswith("ABS(COALESCE("):
            # The magnitude filter, over the same payloads the real COALESCE
            # walks. No price on this event is not "flat": excluded, as in
            # Postgres, where the COALESCE is NULL and the comparison is not
            # true.
            move = _row_move_pct(row)
            if move is None:
                return False
            return abs(move) >= float(self._arg(predicate, args))
        raise self.UnsupportedPredicate(
            f"{predicate!r} is not a predicate this double knows how to apply. "
            f"_deep_window has grown a filter the scenario tests are silently "
            f"ignoring; teach it here or the long-window rules are untested."
        )


# ── Scenario description ────────────────────────────────────────────────────


@dataclass
class Beat:
    """One observation in a scenario, positioned relative to the trigger.

    `minutes_before` is how far ahead of the trigger this happened. The
    scenarios are written in the order a person would narrate them, and the
    offsets are what turn that narration into the ordering the sequence rules
    actually test.
    """

    event_type: str
    entity_id: str
    minutes_before: float
    anomaly: float
    entity_name: Optional[str] = None
    entity_type: EntityType = EntityType.COMPANY
    source: str = "scenario"
    headline: Optional[str] = None
    region: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    named_entities: List[str] = field(default_factory=list)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    # How far the price moved, in percent, for the beats where that is part of
    # the situation being described.
    #
    # A scenario had no way to say "and the freight rate moved 4%", so a rule
    # written to require a move -- which is what a rule named for a repricing
    # should require -- excluded every beat in the suite. The situations always
    # included the move; only the harness could not express it.
    move_pct: Optional[float] = None


@dataclass
class Scenario:
    """A real situation, its evidence, and what the platform should conclude.

    `expect_rule` is the rule that should fire. `why` is the reason a person
    would give for expecting it, and it is quoted in the assertion message so a
    failure reads as "the platform missed this" rather than "assert 0 == 1".
    """

    name: str
    domain: str
    why: str
    trigger: Beat
    evidence: List[Beat]
    expect_rule: str
    # The floor below which the finding does not reach anyone.
    #
    # Not the rule's declared tier. The engine publishes min(declared, earned),
    # so asserting that a particular scenario earns exactly what its rule
    # declares is asserting something the platform has never promised -- the
    # declaration is a ceiling on severity, not a floor. What it must promise
    # is that a textbook instance of a named pattern surfaces: ALERT and WATCH
    # are the tiers a finding goes to be ignored in.
    #
    # That the declared tier is *reachable at all* is a separate and stronger
    # property, tested once over the whole rule set rather than per scenario.
    expect_min_tier: str = "ELEVATED"
    # Evidence that must appear in the cluster, by event type. A rule can fire
    # on one clause while the situation needs several, and "it fired" is a
    # weaker claim than "it fired on the right evidence".
    expect_evidence_types: Sequence[str] = ()
    expect_min_domains: int = 1


# Which payload a domain puts its move in. The same mapping `event_move_pct`
# reads, from the writing side.
_MOVE_PAYLOAD_BY_TYPE = {
    "supply_chain_metric": ("supply_chain_data", "change_14d_pct", SupplyChainData),
    "macro_release": ("macro_data", "surprise_pct", MacroReleaseData),
}


def _move_payload(beat: Beat) -> dict:
    """The domain payload carrying this beat's move, if it has one.

    Built through the real model, so a payload that cannot be constructed here
    is one the platform could not have produced either. `SupplyChainData`
    requires an index name, which is the sort of thing a hand-built fixture
    quietly omits and a real enricher never does.
    """
    if beat.move_pct is None:
        return {}
    name, field, model = _MOVE_PAYLOAD_BY_TYPE.get(
        beat.event_type, ("financial_data", "change_pct_bar", FinancialData)
    )
    payload = {field: beat.move_pct}
    for required, value in (
        ("index_name", beat.entity_id),
        ("indicator", beat.entity_id),
        ("ticker", beat.entity_id),
    ):
        if required in model.model_fields and required not in payload:
            payload[required] = value
    return {name: model(**payload)}


def _event(beat: Beat, trigger_at: datetime) -> NormalizedEvent:
    """A NormalizedEvent for one beat, validated by the real model."""
    return NormalizedEvent(
        type=EventType(beat.event_type),
        occurred_at=trigger_at - timedelta(minutes=beat.minutes_before),
        source=beat.source,
        primary_entity=Entity(
            id=beat.entity_id,
            type=beat.entity_type,
            name=beat.entity_name or beat.entity_id,
        ),
        region=beat.region,
        latitude=beat.latitude,
        longitude=beat.longitude,
        headline=beat.headline or f"{beat.event_type} on {beat.entity_id}",
        anomaly_score=beat.anomaly,
        **_move_payload(beat),
        tags=list(beat.tags),
        named_entities=list(beat.named_entities),
    )


def _row_move_pct(row: Dict[str, Any]) -> Optional[float]:
    """What `move_pct_sql()` computes in Postgres, over the same mapping.

    Reads `PAYLOAD_MOVE_FIELDS` rather than restating which payload carries a
    move, so a domain added there is covered here without a second edit. None
    when no payload carries one -- in Postgres the COALESCE is NULL and the
    comparison is not true, which excludes the row rather than treating it as
    flat.
    """
    from shared.models.events import PAYLOAD_MOVE_FIELDS

    for payload_name, fields in PAYLOAD_MOVE_FIELDS:
        payload = row.get(payload_name) or {}
        for field in fields:
            value = payload.get(field)
            if value is None:
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number == number:
                return number
    return None


def _as_row(event: NormalizedEvent) -> Dict[str, Any]:
    """One event as the `events` table stores it.

    Including the domain payload columns. This used to mirror the column list
    `_deep_window` selected, which made the double correct only for as long as
    that query stayed the same -- and the query grew a derived column, computed
    from payloads this row did not carry, so every long-window row came back
    with no move and the magnitude filter excluded all of them.
    """
    pe = event.primary_entity
    payloads = {}
    for name in ("financial_data", "crypto_data", "supply_chain_data", "macro_data"):
        value = getattr(event, name, None)
        if value is not None:
            payloads[name] = (
                value.model_dump() if hasattr(value, "model_dump") else dict(value)
            )
    return {
        **payloads,
        "event_id": event.event_id,
        "type": event.type.value,
        "source": event.source,
        "anomaly_score": float(event.anomaly_score or 0.0),
        "tags": list(event.tags or []),
        "region": event.region,
        "headline": event.headline,
        "summary": event.summary,
        "named_entities": list(event.named_entities or []),
        "entity_id": pe.id if pe else None,
        "entity_name": pe.name if pe else None,
        "entity_type": pe.type.value if pe else None,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "occurred_at_epoch": event.occurred_at.timestamp(),
    }


async def load_scenario(scenario: Scenario, *, now: Optional[datetime] = None):
    """Writes a scenario's evidence into a real EventStore. Returns (store, trigger).

    Both stores, because the platform has both: every event is written to the
    events hypertable by db_writer and the recent ones are additionally cached
    in Redis. A scenario whose evidence went only into the cache would make a
    48-hour store look like a seven-day one.

    The trigger is deliberately *not* written: `get_recent` would otherwise
    return the trigger as its own corroboration, which is the mistake
    `exclude_event_id` exists to prevent and which would make every single-
    clause rule self-satisfying.
    """
    from services.correlation.event_store import EventStore

    # The trigger is placed far enough back that evidence *after* it is still
    # in the past. The store refuses any event more than five minutes ahead of
    # now -- a future timestamp is never evicted by a sliding window and would
    # be served as the newest evidence forever -- so a spillover clause, whose
    # whole claim is that its evidence follows the trigger, would otherwise
    # have every one of its events silently declined.
    latest_after = max([-b.minutes_before for b in scenario.evidence] + [0.0])
    trigger_at = (now or datetime.now(timezone.utc)) - timedelta(
        minutes=latest_after + 10
    )
    events = [_event(beat, trigger_at) for beat in scenario.evidence]
    store = EventStore(
        redis_client=InMemoryRedis(),
        db_client=RecordingDB([_as_row(e) for e in events]),
    )
    for event in events:
        await store.add_event(event)
    return store, _event(scenario.trigger, trigger_at)


def install_shipped_rules() -> None:
    """Loads exactly what the build ships into the evaluator's cache.

    The service populates this from Redis at startup and reconciles it against
    `SHIPPED_RULES`; a test that hand-builds a rule proves only that the
    evaluator can evaluate a rule someone wrote for it. These scenarios run
    against the rules that are actually deployed.
    """
    from services.correlation.main import (
        SHIPPED_RULES,
        _dynamic_rules_cache,
    )

    _dynamic_rules_cache.clear()
    for rule in SHIPPED_RULES:
        _dynamic_rules_cache[rule["rule_id"]] = rule


async def run_scenario(scenario: Scenario):
    """Loads the evidence, fires the trigger, returns the clusters produced."""
    from services.correlation.main import evaluate_dynamic_rules

    install_shipped_rules()
    store, trigger = await load_scenario(scenario)
    clusters = await evaluate_dynamic_rules(trigger, store)
    return clusters, store, trigger


def cluster_for(clusters, rule_id: str):
    for c in clusters:
        if c.rule_id == rule_id:
            return c
    return None


def describe(clusters) -> str:
    """What the platform did say, for a failure message that can be acted on."""
    if not clusters:
        return "nothing at all"
    return "; ".join(
        f"{c.rule_id} [{c.alert_tier.value if hasattr(c.alert_tier, 'value') else c.alert_tier}] "
        f"conf={c.confidence_score:.2f} evidence={len(c.supporting_event_ids)}"
        for c in clusters
    )


def evidence_types(store, cluster) -> set:
    """The event types of the evidence a cluster actually drew on.

    Resolved against both stores. Evidence beyond the cache's 48 hours comes
    back from the events table, and looking only in Redis would report a
    long-window rule as resting on nothing.
    """
    ids = set(cluster.supporting_event_ids or [])
    known = {m["event_id"]: m["type"] for m in store._redis.members()}
    known.update({r["event_id"]: r["type"] for r in store._db.rows})
    return {t for eid, t in known.items() if eid in ids}


__all__ = [
    "Beat",
    "InMemoryRedis",
    "RecordingDB",
    "Scenario",
    "cluster_for",
    "describe",
    "evidence_types",
    "install_shipped_rules",
    "load_scenario",
    "run_scenario",
]
