"""Round 3: correlation. The deployed rules, against evidence built from their own definitions.

For each of the shipped rules a generator reads its clauses and builds the smallest
evidence that should make it fire, with seeded jitter in the times and scores. Then it
breaks that evidence one way at a time: wrong order, wrong subject, outside the window,
a single type where two are needed, a move too small, a trigger scoring too low, and
position telemetry posing as a finding. A rule that still fires on broken evidence is
publishing something it should not. A rule that will not fire on its own textbook case
is a rule nobody will ever see.

The doubles are the repository's own (tests/integration/market_scenarios.py): a Redis
that runs the four commands the store issues, and an events table that applies the real
query's WHERE clause. Nothing is published and nothing in the live system is touched.
"""
from __future__ import annotations

import asyncio
import copy
from typing import Any, Dict, List, Optional, Tuple

from shared.models.correlation_rules import CONVERGENCE_TYPE_TARGET
from shared.models.events import POSITION_TELEMETRY_TYPES, EventType

from tests.integration.market_scenarios import Beat, Scenario, run_scenario
from tests.stress.core import DEFECT, NOTE, PASS, Ledger, rng, upgrade_redis_double

ENTITY = "ZZTEST"
OTHER = ["ZZOTHER1", "ZZOTHER2", "ZZOTHER3"]
REGION = "TESTREGION"
TAG = "exposes:ZZTEST"
VALID = {e.value for e in EventType}


def _triggers(rule) -> List[str]:
    spec = rule["trigger_event_type"]
    return [t for t in ([spec] if isinstance(spec, str) else list(spec or [])) if t in VALID]


def _types(clause) -> List[str]:
    return [t for t in clause["event_types"] if t in VALID and t not in POSITION_TELEMETRY_TYPES]


def _admits(clause, t: str) -> bool:
    """Whether evidence of type t can satisfy this clause's selection at all."""
    return not (clause.get("min_abs_move_pct") and t in NO_MOVE_TYPES)


# Types whose payload carries no move field (shared.models.events.PAYLOAD_MOVE_FIELDS), found by probing.
NO_MOVE_TYPES: set = {"macro_release"}    # confirmed by the alternatives check below, which also reports it


def build(rule: Dict[str, Any], r, flaw: Optional[str] = None) -> Tuple[Scenario, str]:
    """A scenario that should fire `rule`, broken in the way `flaw` names (or intact)."""
    trig_types = _triggers(rule)
    trig_type = trig_types[0]
    cond_min = float(rule.get("conditions", {}).get("min_anomaly", 0.0))
    needs_region = any(c.get("region") is True for c in rule["correlations"]) or bool(rule.get("conditions", {}).get("region"))
    needs_tags = any(c.get("shared_tags") for c in rule["correlations"])
    trig_anom = min(0.9, max(cond_min, 0.2) + r.uniform(0.15, 0.3))
    if flaw == "trigger_too_weak":
        trig_anom = max(0.0, cond_min - 0.05)
    trigger = Beat(trig_type, ENTITY, 0, trig_anom, region=REGION if needs_region else None, tags=[TAG] if needs_tags else [],
                   source="stress", headline=f"{trig_type} on {ENTITY}")
    evidence: List[Beat] = []
    clauses = rule["correlations"]
    skip_clause = r.randrange(len(clauses)) if flaw == "drop_clause" else None
    for ci, clause in enumerate(clauses):
        if ci == skip_clause:
            continue
        types = [t for t in _types(clause) if _admits(clause, t)]
        want = min(CONVERGENCE_TYPE_TARGET, len(types))
        if flaw == "single_type" and len(types) > 1:
            want = 1
        chosen = r.sample(types, want)
        window_min = float(clause.get("hours", 48)) * 60
        within = clause.get("within_minutes")
        for k, t in enumerate(chosen):
            inside = min(window_min * 0.5, float(within) * 0.5 if within else window_min)
            minutes = r.uniform(0.2, 1.0) * inside
            if clause.get("precedes_trigger") is not True and r.random() < 0.3:
                minutes = -min(minutes, 3.0)          # shortly after the trigger, when order does not matter
            ent = ENTITY if clause.get("same_entity") else (OTHER[k % len(OTHER)] if clause.get("other_entity") else ENTITY)
            anom = min(0.95, max(float(clause.get("min_anomaly", 0)), 0.2) + r.uniform(0.1, 0.3))
            reg = REGION if clause.get("region") is True else None
            tags = [TAG] if clause.get("shared_tags") else []
            move = (float(clause["min_abs_move_pct"]) + r.uniform(0.5, 3)) * r.choice([-1, 1]) if clause.get("min_abs_move_pct") else None
            if flaw == "wrong_order" and clause.get("precedes_trigger"):
                minutes = -r.uniform(1, 10)
            if flaw == "outside_window":
                minutes = window_min * 1.3 + 30
            if flaw == "wrong_subject" and clause.get("same_entity"):
                ent = "ZZSTRANGER"
            if flaw == "wrong_region" and clause.get("region") is True:
                reg = "ELSEWHERE"
            if flaw == "no_shared_tag" and clause.get("shared_tags"):
                tags = ["exposes:ZZSTRANGER"]
                ent = "ZZSTRANGER"          # a shared entity name is itself a shared subject
            if flaw == "move_too_small" and clause.get("min_abs_move_pct"):
                move = float(clause["min_abs_move_pct"]) * 0.3
            if flaw == "too_quiet":
                anom = max(0.0, float(clause.get("min_anomaly", 0)) - 0.15)
            if flaw == "same_subject_when_other_required" and clause.get("other_entity"):
                ent = ENTITY
            evidence.append(Beat(t, ent, minutes, anom, region=reg, tags=tags, move_pct=move, source="stress", headline=f"{t} on {ent}"))
    return Scenario(name=f"{rule['rule_id']} [{flaw or 'intact'}]", domain="stress", why="generated from the rule's own clauses",
                    trigger=trigger, evidence=evidence, expect_rule=rule["rule_id"]), flaw or "intact"


def _fires(sc: Scenario) -> Tuple[bool, str]:
    clusters, store, trig = asyncio.run(run_scenario(sc))
    hit = [c for c in clusters if c.rule_id == sc.expect_rule]
    detail = "; ".join(f"{c.rule_id}[{getattr(c.alert_tier, 'value', c.alert_tier)}]" for c in clusters) or "nothing"
    return bool(hit), detail


# A flaw only applies to a rule that has the feature it breaks.
APPLIES = {
    "drop_clause": lambda rule: len(rule["correlations"]) > 1,
    "single_type": lambda rule: len(rule["correlations"]) == 1 and len(_types(rule["correlations"][0])) > 1,
    "wrong_order": lambda rule: any(c.get("precedes_trigger") for c in rule["correlations"]),
    "outside_window": lambda rule: True,
    "wrong_subject": lambda rule: any(c.get("same_entity") for c in rule["correlations"]),
    "wrong_region": lambda rule: any(c.get("region") is True for c in rule["correlations"]),
    "no_shared_tag": lambda rule: any(c.get("shared_tags") for c in rule["correlations"]),
    "move_too_small": lambda rule: any(c.get("min_abs_move_pct") for c in rule["correlations"]),
    "too_quiet": lambda rule: True,
    "trigger_too_weak": lambda rule: float(rule.get("conditions", {}).get("min_anomaly", 0)) > 0.05,
    "same_subject_when_other_required": lambda rule: any(c.get("other_entity") for c in rule["correlations"]),
}


# Flaws that break some clauses and leave others intact. When two clauses list the same type, the intact
# clause's evidence can legitimately answer the broken one, so the mutant proves nothing about the engine.
PARTIAL_FLAWS = {"drop_clause", "wrong_order", "wrong_subject", "wrong_region", "no_shared_tag", "move_too_small", "same_subject_when_other_required"}


def overlapping_types(rule) -> set:
    seen: set = set()
    shared: set = set()
    for clause in rule["correlations"]:
        types = set(_types(clause))
        shared |= seen & types
        seen |= types
    return shared


def run(seed: int, jitter_runs: int = 4) -> Ledger:
    from services.correlation.main import SHIPPED_RULES

    L = Ledger("3. Correlation", seed)
    upgrade_redis_double()
    rules = [rr for rr in SHIPPED_RULES if _triggers(rr)]

    # Textbook evidence, jittered: every rule must fire on the case built from its own definition.
    cases = [(rule, j) for rule in rules for j in range(jitter_runs)]
    outcomes: Dict[str, List[str]] = {}

    def fires_on_textbook(c):
        rule, j = c
        sc, _ = build(rule, rng(seed, f"{rule['rule_id']}:{j}"))
        ok, detail = _fires(sc)
        outcomes.setdefault(rule["rule_id"], []).append(detail)
        return None if ok else f"did not fire; platform said: {detail}"
    L.family("every shipped rule fires on evidence built from its own definition", cases, fires_on_textbook, lambda c: f"{c[0]['rule_id']} run {c[1]}")

    # Broken evidence: each flaw, applied to each rule that has the feature it breaks.
    skipped: dict = {}
    for flaw, applies in APPLIES.items():
        subset = [rule for rule in rules if applies(rule)]
        if flaw in PARTIAL_FLAWS:
            for rule in subset:
                if overlapping_types(rule):
                    skipped.setdefault(rule["rule_id"], overlapping_types(rule))
            subset = [rule for rule in subset if not overlapping_types(rule)]
        if not subset:
            continue

        def must_not_fire(rule, flaw=flaw):
            sc, _ = build(rule, rng(seed, f"{rule['rule_id']}:{flaw}"), flaw)
            ok, detail = _fires(sc)
            return f"still fired with {flaw}: {detail}" if ok else None
        L.family(f"broken evidence does not fire: {flaw.replace('_', ' ')}", subset, must_not_fire, lambda rule: rule["rule_id"])

    if skipped:
        L.record("rules whose clauses list the same event type", NOTE,
                 "; ".join(f"{rid} shares {sorted(types)}" for rid, types in skipped.items()) +
                 ". One clause's evidence can then answer another's, using two distinct events, so such a rule is a weaker conjunction than its name says. "
                 "Partial-break checks cannot be applied to it.", len(skipped))

    # Position telemetry is not corroboration, however loudly it scores.
    def telemetry(rule):
        sc, _ = build(rule, rng(seed, f"{rule['rule_id']}:tele"))
        junk = [Beat(t, ENTITY, 5.0, 0.95, region=REGION, tags=[TAG], source="stress") for t in sorted(POSITION_TELEMETRY_TYPES) if t in VALID]
        sc2 = Scenario(sc.name, "stress", "telemetry only", sc.trigger, junk, sc.expect_rule)
        ok, detail = _fires(sc2)
        return f"fired on position fixes alone: {detail}" if ok else None
    L.family("position fixes alone never fire a rule", rules, telemetry, lambda rule: rule["rule_id"])

    # Noise with no rule evidence: random events, random entities. Anything fired must have real support.
    def noise(i):
        rr = rng(seed, f"noise{i}")
        all_types = [t for t in VALID if t not in POSITION_TELEMETRY_TYPES]
        beats = [Beat(rr.choice(all_types), rr.choice([ENTITY] + OTHER), rr.uniform(0, 600), rr.uniform(0, 0.5), source="stress",
                      region=rr.choice([None, REGION, "ELSEWHERE"])) for _ in range(40)]
        trig = Beat(rr.choice(all_types), ENTITY, 0, rr.uniform(0.0, 0.4), source="stress")
        sc = Scenario("noise", "stress", "noise", trig, beats, "none")
        clusters, store, t = asyncio.run(run_scenario(sc))
        bad = [c for c in clusters if len(c.supporting_event_ids or []) == 0]
        return f"fired {[c.rule_id for c in bad]} with no supporting events" if bad else None
    L.family("low-scoring random noise never produces a finding without support", range(40), noise, lambda i: f"noise batch {i}")

    # Boundaries and malformed triggers.
    informed = next((x for x in rules if x["rule_id"] == "rule_informed_trading_sequence"), None)
    if informed:
        base_sc, _ = build(informed, rng(seed, "bound"))
        def at_boundary(minutes_fraction):
            sc = copy.deepcopy(base_sc)
            for b in sc.evidence:
                if b.event_type == "options_flow":
                    b.minutes_before = 4320 * minutes_fraction
                    b.anomaly = 0.5
            ok, _ = _fires(sc)
            return ok
        inside, edge, over = at_boundary(0.9), at_boundary(0.999), at_boundary(1.2)
        L.record("options leg: 4,320-minute limit", PASS if inside and not over else DEFECT,
                 f"90% of the limit fires={inside}, 99.9% fires={edge}, 120% fires={over}", 3)

    # Every alternative a clause lists must be able to satisfy that clause. A type that its own
    # selection excludes is dead text: it makes the rule look wider than it is.
    from services.correlation import main as engine
    from tests.integration.market_scenarios import install_shipped_rules, load_scenario

    async def admitted(clause, t, rule):
        beat = Beat(t, ENTITY, 20.0, 0.9, region=REGION if clause.get("region") is True else None,
                    tags=[TAG] if clause.get("shared_tags") else [], source="stress",
                    move_pct=(float(clause["min_abs_move_pct"]) + 2.0) if clause.get("min_abs_move_pct") else None)
        trig = Beat(_triggers(rule)[0], ENTITY, 0, 0.9, region=REGION if clause.get("region") is True else None, tags=[TAG] if clause.get("shared_tags") else [], source="stress")
        store, trigger = await load_scenario(Scenario("a", "stress", "a", trig, [beat], rule["rule_id"]))
        te = trigger.occurred_at.timestamp()
        hits = await store.get_recent([t], exclude_event_id=str(trigger.event_id), hours=clause.get("hours", 48), min_anomaly=clause.get("min_anomaly", 0.0),
                                      min_abs_move_pct=clause.get("min_abs_move_pct"), region=(REGION if clause.get("region") is True else clause.get("region")),
                                      entity_id=(ENTITY if clause.get("same_entity") else None), exclude_types=sorted(POSITION_TELEMETRY_TYPES), **engine._window_bounds(clause, te))
        hits = engine._apply_temporal_constraint(hits, clause, te)
        hits = engine._apply_join_requirement(hits, clause, rule, trigger)
        if clause.get("other_entity"):
            hits = [h for h in hits if str(h.get("entity_id") or "").upper() != ENTITY]
        return bool(hits)

    install_shipped_rules()
    probes = [(rule, ci, t) for rule in rules for ci, clause in enumerate(rule["correlations"]) for t in _types(clause) if not clause.get("other_entity")]

    def alt_ok(c):
        rule, ci, t = c
        return None if asyncio.run(admitted(rule["correlations"][ci], t, rule)) else "this declared alternative can never satisfy its own clause"
    L.family("every event type a clause lists can satisfy that clause", probes, alt_ok, lambda c: f"{c[0]['rule_id']} clause {c[1]}: {c[2]}")
    for rule, ci, t in probes:
        if rule["correlations"][ci].get("min_abs_move_pct") and not asyncio.run(admitted(rule["correlations"][ci], t, rule)):
            NO_MOVE_TYPES.add(t)

    # One event is one observation. If it can answer two clauses, a rule asking for two things
    # fires on a single fact.
    def single_event_two_clauses(rule):
        clauses = rule["correlations"]
        shared = set(_types(clauses[0]))
        for c in clauses[1:]:
            shared &= set(_types(c))
        shared = {t for t in shared if all(_admits(c, t) for c in clauses)}
        if not shared:
            return None
        t = sorted(shared)[0]
        need_region = any(c.get("region") is True for c in clauses)
        need_tags = any(c.get("shared_tags") for c in clauses)
        move = max([float(c.get("min_abs_move_pct", 0)) for c in clauses] + [0.0])
        anom = max([float(c.get("min_anomaly", 0)) for c in clauses] + [0.2]) + 0.2
        trig = Beat(_triggers(rule)[0], ENTITY, 0, 0.9, region=REGION if need_region else None, tags=[TAG] if need_tags else [], source="stress")
        one = Beat(t, ENTITY, 20.0, anom, region=REGION if need_region else None, tags=[TAG] if need_tags else [], source="stress", move_pct=(move + 2.0) if move else None)
        ok, detail = _fires(Scenario("one", "stress", "one", trig, [one], rule["rule_id"]))
        return f"fired on ONE {t} event that answers {len(clauses)} clauses: {detail}" if ok else None
    multi = [rule for rule in rules if len(rule["correlations"]) > 1]
    L.family("a single event cannot answer two clauses of the same rule", multi, single_event_two_clauses, lambda rule: rule["rule_id"])

    # Flood: one rule triggered over and over against one shared store, as in production.
    # The platform should damp a rule that will not stop, not announce each one at full volume.
    from shared.models.events import Entity
    import uuid

    flood_rule = next((x for x in rules if x["rule_id"] == "rule_insider_filing_flow"), rules[0])
    evidence: List[Beat] = []
    first_trigger = None
    N = 90
    for i in range(N):
        sc, _ = build(flood_rule, rng(seed, f"flood{i}"))
        for b in sc.evidence:
            if b.entity_id == ENTITY:
                b.entity_id = f"ZZFLOOD{i}"
            b.tags = [f"exposes:ZZFLOOD{i}" for _ in b.tags]
        evidence.extend(sc.evidence)
        first_trigger = first_trigger or sc.trigger
    master = Scenario("flood", "stress", "flood", first_trigger, evidence, flood_rule["rule_id"])
    install_shipped_rules()
    store, trigger = asyncio.run(load_scenario(master))
    evaluate_dynamic_rules = engine.evaluate_dynamic_rules

    async def storm():
        out = []
        for i in range(N):
            ev = trigger.model_copy(update={"event_id": str(uuid.uuid4()), "primary_entity": Entity(id=f"ZZFLOOD{i}", type=trigger.primary_entity.type, name=f"ZZFLOOD{i}"),
                                            "tags": [f"exposes:ZZFLOOD{i}" for _ in trigger.tags]})
            got = [c for c in await evaluate_dynamic_rules(ev, store) if c.rule_id == flood_rule["rule_id"]]
            out.append(str(getattr(got[0].alert_tier, "value", got[0].alert_tier)) + f"@{got[0].confidence_score:.2f}" if got else "held")
        return out
    calls = {"n": 0}
    original = engine._tier_after_frequency

    async def counted(*a, **k):
        calls["n"] += 1
        return await original(*a, **k)
    engine._tier_after_frequency = counted
    try:
        seq_full = asyncio.run(storm())
    finally:
        engine._tier_after_frequency = original
    seq = [x.split("@")[0] for x in seq_full]
    fired = [t for t in seq if t != "held"]
    conf = [float(x.split("@")[1]) for x in seq_full if "@" in x]
    runs = []
    for t in seq:
        if runs and runs[-1][0] == t:
            runs[-1][1] += 1
        else:
            runs.append([t, 1])
    shape = " then ".join(f"{t}x{n}" for t, n in runs)
    damped = len(set(seq)) > 1
    L.record(f"flood of {N} triggers on one rule in one store", PASS if damped else NOTE,
             f"{len(fired)} of {N} published. Tier sequence: {shape}. Confidence {conf[0]:.2f} on the first, {conf[-1]:.2f} on the last. "
             f"The hourly budget (ELEVATED: {engine.TIER_HOURLY_BUDGET[engine.AlertTier.ELEVATED]}) was consulted {calls['n']} times for {N} triggers." +
             ("" if damped else " Nothing was damped or held: every trigger was announced at the same tier."), N)
    return L
