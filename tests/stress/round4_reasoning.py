"""Round 4: reasoning, agents and forecasting skill.

The model is the one part of this system that cannot be reasoned about from its code,
so the stress here is on everything around it: what is allowed to reach it, what is
done with what comes back, and whether the system's own report card can tell skill from
luck. Model calls are stubbed. A stub that misbehaves on purpose (returns nothing,
raises, returns a scenario that cannot be saved) shows what the pipeline does next.
"""
from __future__ import annotations

import asyncio
import math
import random
from datetime import date
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from shared.models.correlation_rules import CONVERGENCE_TYPE_TARGET  # noqa: F401  (documented bar)
from shared.utils import forecast_skill as fs
from shared.utils import stock_evidence as se

from tests.stress.core import DEFECT, NOTE, PASS, Ledger, rng, upgrade_redis_double


# ── A: can the report card tell skill from luck? ──────────────────────────

def _calls(r: random.Random, kind: str, p_up: float, n: int) -> List[Tuple[bool, bool, Optional[float]]]:
    out = []
    for _ in range(n):
        went_up = r.random() < p_up
        if kind == "coin":
            call, p = r.random() < 0.5, 0.5
        elif kind == "always_down":
            call, p = False, 0.6
        elif kind == "always_up":
            call, p = True, 0.6
        elif kind.startswith("skilled"):
            hit = float(kind.split("_")[1])
            call, p = (went_up if r.random() < hit else not went_up), hit
        elif kind == "anti":
            call, p = (went_up if r.random() < 0.40 else not went_up), 0.6
        elif kind == "overconfident":
            call, p = r.random() < 0.5, 0.9
        else:
            raise ValueError(kind)
        out.append((call, went_up, p))
    return out


def grader_power(seed: int, trials: int = 300) -> Tuple[Dict[Tuple[str, float, int], Dict[str, float]], Ledger]:
    L = Ledger("4. Reasoning and forecasting", seed)
    r = rng(seed, "r4grader")
    table: Dict[Tuple[str, float, int], Dict[str, float]] = {}
    for kind in ("coin", "always_down", "always_up", "skilled_0.55", "skilled_0.60", "skilled_0.65", "anti", "overconfident"):
        for p_up in (0.3, 0.5, 0.7):
            for n in (30, 100, 300):
                sk = wr = 0
                bss: List[float] = []
                for _ in range(trials):
                    res = fs.skill(_calls(r, kind, p_up, n))
                    sk += res["verdict"] == "skill"
                    wr += res["verdict"] == "worse than chance"
                    if res["bss"] is not None:
                        bss.append(res["bss"])
                table[(kind, p_up, n)] = {"skill": sk / trials, "worse": wr / trials, "bss": sum(bss) / len(bss) if bss else float("nan")}
    return table, L


def part_a(L: Ledger, seed: int) -> Dict[Tuple[str, float, int], Dict[str, float]]:
    table, _ = grader_power(seed)
    nulls = ("coin", "always_down", "always_up", "overconfident")
    fpr = {k: max(v["skill"] for (kind, p, n), v in table.items() if kind == k) for k in nulls}
    worst = max(fpr.values())
    L.record("report card: false alarms on forecasters with no skill", PASS if worst <= 0.06 else DEFECT,
             "share of 300 simulated forecasters per setting that were wrongly called skilled (worst across markets and sample sizes): "
             + ", ".join(f"{k} {v:.1%}" for k, v in fpr.items()) + ". A real hit rate does not earn skill if it only follows the market's lean.", 300 * len(nulls) * 9)
    power = {n: table[("skilled_0.60", 0.5, n)]["skill"] for n in (30, 100, 300)}
    power55 = {n: table[("skilled_0.55", 0.5, n)]["skill"] for n in (30, 100, 300)}
    L.record("report card: power to detect real skill", NOTE,
             "a forecaster who is right 60% of the time is recognised in " + ", ".join(f"{p:.0%} of runs at n={n}" for n, p in power.items()) +
             "; at 55%: " + ", ".join(f"{p:.0%} at n={n}" for n, p in power55.items()) + ". Fewer than 20 resolved calls are never graded. Skill takes months of calls to see.", 300 * 6)
    anti = table[("anti", 0.5, 300)]
    L.record("report card: forecasters who are wrong more often than right",
             PASS if anti["worse"] >= 0.5 else NOTE, f"a 40%-right forecaster is called worse than chance in {anti['worse']:.0%} of runs at n=300 and {table[('anti', 0.5, 100)]['worse']:.0%} at n=100", 300)
    oc = table[("overconfident", 0.5, 300)]["bss"]
    L.record("report card: stated confidence is graded, not just direction", PASS if oc < -0.5 else DEFECT,
             f"a coin-flipper who claims 90% confidence scores a Brier skill of {oc:.2f} (below zero is worse than stating the base rate)", 300)
    # Edge inputs
    edge = [[], [(True, True, 0.6)], [(True, True, None)] * 25, [(True, True, float("nan"))] * 25, [(True, True, 1.5)] * 25, [(True, False, 0.6)] * 25,
            [(True, True, 0.6)] * 10 + [(False, False, 0.6)] * 10]

    def edge_ok(calls):
        res = fs.skill(calls)
        for k in ("hit", "chance_hit", "heidke", "bss", "z"):
            v = res.get(k)
            if v is not None and not math.isfinite(v):
                return f"{k}={v}"
        return None
    L.family("skill grading survives empty, tiny, one-sided and nonsense inputs", edge, edge_ok, lambda c: f"{len(c)} calls")
    return table


# ── B: who gets the scarce inference ──────────────────────────────────────

def part_b(L: Ledger, seed: int) -> None:
    from services.reasoning.main import _reasoning_priority

    r = rng(seed, "r4prio")

    def cl(tier="ELEVATED", conf=0.6, support=6, domains=2, rule="rule_x", cov=None):
        metrics = {"domain_count": domains}
        if cov is not None:
            metrics["evidence_coverage"] = cov
        return SimpleNamespace(alert_tier=SimpleNamespace(value=tier), confidence_score=conf, supporting_event_ids=[f"e{i}" for i in range(support)],
                               metrics_summary=metrics, rule_id=rule)

    def prio(**k):
        return _reasoning_priority((cl(**k), None))

    tiers = ["WATCH", "ALERT", "ELEVATED", "INTELLIGENCE", "CRITICAL"]
    L.family("priority rises with tier", [(a, b) for a, b in zip(tiers, tiers[1:])], lambda c: None if prio(tier=c[1]) >= prio(tier=c[0]) else "higher tier ranked lower", lambda c: f"{c[0]}->{c[1]}")
    L.family("priority rises with confidence, evidence and cross-domain spread", range(60), lambda i: (
        None if (prio(conf=0.3 + i / 100) <= prio(conf=0.31 + i / 100) and prio(support=2 + i % 8) <= prio(support=3 + i % 8) and prio(domains=1 + i % 3) <= prio(domains=2 + i % 3)) else "ordering violated"), lambda i: f"case {i}")
    L.family("a cluster citing no events gets no inference", [0], lambda n: None if prio(support=n, tier="CRITICAL", conf=1.0, domains=5) == 0.0 else "evidence-free cluster still competes", lambda n: f"{n} events")
    nasty = [float("nan"), float("inf"), -float("inf"), -3.0, 7.0, None, "0.8", "abc"]
    L.family("hostile confidence values stay bounded", nasty, lambda v: None if 0 <= (_reasoning_priority((cl(conf=v), None))) <= 1 else "priority outside 0..1", safe_repr_local)
    L.family("priority is deterministic and order-independent", range(30), lambda i: (
        lambda batch: None if [round(_reasoning_priority((c, None)), 6) for c in sorted(batch, key=lambda c: -_reasoning_priority((c, None)))] ==
        [round(_reasoning_priority((c, None)), 6) for c in sorted(random.Random(i).sample(batch, len(batch)), key=lambda c: -_reasoning_priority((c, None)))] else "ordering depends on arrival")(
            [cl(tier=r.choice(tiers), conf=r.random(), support=r.randint(0, 10), domains=r.randint(1, 4)) for _ in range(40)]), lambda i: f"batch {i}")
    cold, warm = prio(tier="CRITICAL", conf=0.9, domains=3, cov=0.1), prio(tier="ELEVATED", conf=0.6, domains=1, cov=1.0)
    L.record("a cold detector on a critical cross-domain cluster still outranks a warm ordinary one", PASS if cold > warm else NOTE, f"critical/cold {cold:.3f} vs elevated/warm {warm:.3f}", 1)


def safe_repr_local(x: Any) -> str:
    return repr(x)[:30]


# ── C: the path from cluster to published scenario ────────────────────────

class _Order:
    n = 0

    @classmethod
    def tick(cls) -> int:
        cls.n += 1
        return cls.n


class _FakeDB:
    def __init__(self, has_row: bool = True) -> None:
        self.has_row = has_row

    async def query(self, sql, *a):
        return [{"?column?": 1}] if self.has_row and "correlations" in sql else []

    async def execute(self, *a, **k):
        return None


class _FakeRedis:
    def __init__(self):
        self.store: Dict[str, Any] = {}

    @property
    def raw(self):
        return self

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v, *a, **kw):
        self.store[k] = v
        return True

    async def incr(self, k):
        self.store[k] = int(self.store.get(k, 0)) + 1
        return self.store[k]

    def __getattr__(self, name):
        async def f(*a, **k):
            return None
        return f


class _Producer:
    def __init__(self, fail: bool = False) -> None:
        self.sent: List[Tuple[int, str]] = []
        self.fail = fail

    async def send(self, topic, payload, key=None):
        if self.fail:
            raise ConnectionError("broker down")
        self.sent.append((_Order.tick(), topic))


def _scenario(cid):
    return SimpleNamespace(scenario_id="s-1", correlation_id=cid, headline="stub scenario", significance="x", confidence_rationale="x",
                           confidence_overall=70, model_dump=lambda: {"scenario_id": "s-1"})


def part_c(L: Ledger, seed: int) -> None:
    import services.reasoning.main as rm
    from tests.integration.market_scenarios import run_scenario
    from tests.stress.round3_correlation import build
    from services.correlation.main import SHIPPED_RULES

    upgrade_redis_double()
    rule = next(x for x in SHIPPED_RULES if x["rule_id"] == "rule_informed_trading_sequence")
    sc, _ = build(rule, rng(seed, "r4c"))
    clusters, _, _ = asyncio.run(run_scenario(sc))
    base = next(c for c in clusters if c.rule_id == rule["rule_id"])

    state: Dict[str, Any] = {}
    real_save, real_feedback = rm._save_scenario, rm.apply_autonomous_feedback

    async def run(tier="CRITICAL", has_row=True, gen="ok", save=True, producer_fail=False, force=False, retired=False):
        calls = {"generated": 0}

        async def generate(cluster, context, patterns):
            calls["generated"] += 1
            if gen == "none":
                return None
            if gen == "raise":
                raise RuntimeError("model timed out")
            return _scenario(cluster.correlation_id)

        async def save(db, scenario):
            state["saved_at"] = _Order.tick()
            return save_result

        async def feedback(*a, **k):
            return None

        async def retired_rules(_r):
            return {rm.rule_family(base.rule_name)} if retired and hasattr(rm, "rule_family") else ({base.rule_name} if retired else set())
        save_result = save
        rm._save_scenario, rm.apply_autonomous_feedback = save_wrapper(save, save_result), feedback
        orig_retired = rm.retired_rules
        rm.retired_rules = retired_rules
        producer = _Producer(producer_fail)
        cluster = base.model_copy(update={"alert_tier": type(base.alert_tier)[tier]})
        try:
            ctx = SimpleNamespace(build=lambda c: asyncio.sleep(0, result={}))
            lib = SimpleNamespace(find_similar=lambda t, r: asyncio.sleep(0, result=[]), outcome_base_rate=lambda r: asyncio.sleep(0, result=None))
            err = None
            try:
                out = await rm.process_cluster(cluster, _FakeDB(has_row), _FakeRedis(), producer, ctx, SimpleNamespace(generate=generate), lib, force=force)
            except Exception as exc:
                out, err = None, f"{type(exc).__name__}: {exc}"
            return {"generated": calls["generated"], "sent": producer.sent, "saved_at": state.pop("saved_at", None), "out": out, "err": err}
        finally:
            rm._save_scenario, rm.apply_autonomous_feedback, rm.retired_rules = real_save, real_feedback, orig_retired

    def save_wrapper(fn, _unused):
        async def inner(db, scenario):
            return await fn(db, scenario)
        return inner

    # Tier gating
    def gate(tier):
        res = asyncio.run(run(tier=tier, save=True))
        warranted = rm.tier_warrants_synthesis(tier)
        if warranted and not res["generated"]:
            return f"{tier} should have been synthesised but was not"
        if not warranted and res["generated"]:
            return f"{tier} spent a model call it should not have"
        return None
    L.family("only the tiers worth an inference reach the model", ["WATCH", "ALERT", "ELEVATED", "INTELLIGENCE", "CRITICAL"], gate, str)

    # Scenarios replace the saver; verify order via a recording saver.
    async def go(**kw):
        return await run(**kw)

    r_ok = asyncio.run(go())
    L.record("saved before it is announced", PASS if r_ok["saved_at"] is not None and r_ok["sent"] and r_ok["saved_at"] < r_ok["sent"][0][0] else DEFECT,
             f"save at step {r_ok['saved_at']}, publish at step {r_ok['sent'][0][0] if r_ok['sent'] else None}", 1)

    async def failing_save():
        calls = {"generated": 0}
        orig = rm._save_scenario

        async def nope(db, scenario):
            return False
        rm._save_scenario = nope
        try:
            producer = _Producer()

            async def generate(c, x, p):
                calls["generated"] += 1
                return _scenario(c.correlation_id)
            ctx = SimpleNamespace(build=lambda c: asyncio.sleep(0, result={}))
            lib = SimpleNamespace(find_similar=lambda t, r: asyncio.sleep(0, result=[]), outcome_base_rate=lambda r: asyncio.sleep(0, result=None))
            out = await rm.process_cluster(base.model_copy(update={"alert_tier": type(base.alert_tier)["CRITICAL"]}), _FakeDB(True), _FakeRedis(), producer, ctx, SimpleNamespace(generate=generate), lib)
            return out, producer.sent
        finally:
            rm._save_scenario = orig
    out, sent = asyncio.run(failing_save())
    L.record("a scenario the database refused is never announced", PASS if out is None and not sent else DEFECT, f"returned {out}, published {len(sent)}", 1)

    r_missing = asyncio.run(go(has_row=False))
    L.record("no stored correlation, no inference", PASS if r_missing["generated"] == 0 else DEFECT, f"model called {r_missing['generated']} times for a cluster with no correlations row", 1)
    r_forced = asyncio.run(go(has_row=False, force=True))
    L.record("a person's request skips the tier gates but not the stored-row check", PASS if r_forced["generated"] == 0 else DEFECT, f"forced, no row: model called {r_forced['generated']} times", 1)
    r_force_low = asyncio.run(go(tier="WATCH", force=True))
    L.record("a person's request is served even at the lowest tier", PASS if r_force_low["generated"] == 1 else DEFECT, f"forced WATCH: model called {r_force_low['generated']} times", 1)
    r_none = asyncio.run(go(gen="none"))
    L.record("a model that returns nothing publishes nothing", PASS if not r_none["sent"] and r_none["out"] is None else DEFECT, f"published {len(r_none['sent'])}", 1)
    r_raise = asyncio.run(go(gen="raise"))
    L.record("a model that times out", NOTE if r_raise["err"] else PASS,
             ("process_cluster lets the error escape: " + r_raise["err"] + ". The caller must contain it and count it; see the next check.") if r_raise["err"] else "contained inside process_cluster", 1)
    r_pf = asyncio.run(go(producer_fail=True))
    L.record("scenario saved but the broker is down", NOTE if r_pf["err"] else PASS,
             ("saved, then the publish raised (" + r_pf["err"] + "): the scenario exists in the database but was never announced, and "
              "the live-feed publish and feedback steps are skipped") if r_pf["err"] else "handled", 1)
    r_ret = asyncio.run(go(tier="CRITICAL", retired=True))
    L.record("a rule graded as having no edge no longer takes an inference", PASS if r_ret["generated"] == 0 else NOTE, f"model called {r_ret['generated']} times for a retired rule", 1)
    import inspect
    src = inspect.getsource(rm)
    acct = src[src.index("def _account_for_synthesis"): src.index("def _account_for_synthesis") + 2500]
    counted = "task.exception()" in acct and "_errors += 1" in acct and "Topics.DLQ" in acct
    L.record("a failed synthesis is counted and sent to the dead-letter queue", PASS if counted else DEFECT,
             "the completion callback retrieves the exception, counts it and dead-letters the payload" if counted else "no accounting found in the completion callback", 1)


# ── D: the agent's evidence logic ─────────────────────────────────────────

def part_d(L: Ledger, seed: int) -> None:
    card = [{"id": f"E{i}", "tag": "t", "text": "x"} for i in range(1, 8)]
    wild = ["E1 and e2", "see E99", ["E3"], {"k": "E4"}, "E1E2", "E 5", None, 7, "Е1", "E007", "E1" * 500, "e1, E1, E1", "E-1", "<E3>"]
    L.family("citations are read out of anything the model writes", [wild], lambda w: None if isinstance(se.citations(w), list) and all(c == c.upper() for c in se.citations(w)) else "bad list", lambda w: "wild list")
    L.family("a citation to evidence that does not exist is dropped", ["E99", "E0", "E8", "E1000"], lambda c: None if se.known_citations(se.citations([c]), card) == [] else "kept a citation to nothing", str)
    vals = [float("nan"), float("inf"), -float("inf"), -1.0, 0.0, 0.6, 0.61, 1.0, 5.0]
    L.family("conviction is capped, finite and not negative", vals, lambda v: None if 0 <= se.capped_conviction(v) <= se.MAX_FORECAST_CONVICTION else f"returned {se.capped_conviction(v)}", repr)
    ranks = [None, 0.0, 0.5, 1.0, 0.74, 0.76, 0.25, float("nan")]
    L.family("the no-model call only speaks when both signals lean the same way", [(a, b) for a in ranks for b in ranks],
             lambda c: (lambda d: None if d in (None, "up", "down") and not (d == "up" and c[0] is not None and c[1] is not None and c[0] - c[1] < se.EVIDENCE_MIN_EDGE) else "called on too small a lean")(se.evidence_direction(*c)), repr, note=True)
    # Calendar: the report-by-next-session skip must understand closed days.
    days = [(date(2026, 11, 25), date(2026, 11, 27), "Wednesday before Thanksgiving: next session is Friday"),
            (date(2026, 7, 2), date(2026, 7, 6), "Thursday before the observed Independence Day holiday: next session is Monday"),
            (date(2026, 12, 24), date(2026, 12, 28), "Christmas Eve 2026: Christmas Friday is closed, next session is Monday"),
            (date(2026, 11, 5), date(2026, 11, 6), "an ordinary Thursday"), (date(2026, 11, 6), date(2026, 11, 9), "an ordinary Friday")]
    L.family("the next trading session skips exchange holidays", days, lambda d: None if se.next_session(d[0]) == d[1] else f"said {se.next_session(d[0])}, wanted {d[1]}", lambda d: d[2])
    # An earnings call on the next real session must suppress today's call.
    L.family("a report on the next real session is seen", [(date(2026, 11, 27), date(2026, 11, 25)), (date(2026, 7, 6), date(2026, 7, 2))],
             lambda c: None if se.reports_by_next_session(c[0].isoformat(), c[1]) else f"report {c[0]} not seen from {c[1]}", lambda c: f"report {c[0]}")
    L.family("report dates in odd shapes do not break the skip", ["", None, "garbage", "2026-13-45", "2026-11-06T09:00:00Z", 20261106, "06/11/2026"],
             lambda v: None if se.reports_by_next_session(v, date(2026, 11, 5)) in (True, False) else "non-bool", repr)
    # Macro stats: unknown is not zero.
    short = [100.0, 99.0, 98.0, 97.0, 96.0, 70.0]
    ms = se.macro_stats("CL=F", short)
    L.record("a huge move with too little history", NOTE if ms and ms["z"] == 0.0 else PASS,
             f"a {abs(ms['pct']) * 100:.0f}% move on a 6-close history reads z={ms['z']}: with fewer than 20 prior sessions the size in standard deviations is unknown, not zero" if ms else "none", 1)
    L.family("macro statistics survive flat, zero and reversed closes", [[100.0] * 30, [0.0] * 5, [], [100.0], [5.0, 0.0, 4.0, 3.0], [float("nan")] * 4, [1e-12, 1e12, 5.0]],
             lambda c: (lambda s: None if s is None or all(math.isfinite(v) for v in s.values()) else f"non-finite {s}")(se.macro_stats("CL=F", c)), lambda c: f"{len(c)} closes")


# ── E: a synthetic trading day through the whole chain ────────────────────

def part_e(L: Ledger, seed: int) -> str:
    from services.correlation.main import SHIPPED_RULES
    from services.reasoning.main import _reasoning_priority, tier_warrants_synthesis
    from tests.integration.market_scenarios import run_scenario
    from tests.stress.round3_correlation import APPLIES, _triggers, build

    upgrade_redis_double()
    r = rng(seed, "r4e")
    rules = [x for x in SHIPPED_RULES if _triggers(x)]
    rows = []
    for i in range(240):
        rule = r.choice(rules)
        from tests.stress.round3_correlation import PARTIAL_FLAWS, overlapping_types
        flaw = r.choice([None, None, None] + [f for f, ok in APPLIES.items() if ok(rule) and not (f in PARTIAL_FLAWS and overlapping_types(rule))])
        sc, _ = build(rule, rng(seed, f"day{i}"), flaw)
        clusters, _, _ = asyncio.run(run_scenario(sc))
        fired = [c for c in clusters if c.rule_id == rule["rule_id"]]
        rows.append((rule["rule_id"], flaw, fired[0] if fired else None))
    intact = [x for x in rows if x[1] is None]
    broken = [x for x in rows if x[1] is not None]
    fired_intact = sum(1 for x in intact if x[2])
    fired_broken = sum(1 for x in broken if x[2])
    clusters = [(x[2], None) for x in rows if x[2]]
    ranked = sorted(clusters, key=lambda it: -_reasoning_priority(it))
    budget = 36
    taken = [it for it in ranked if tier_warrants_synthesis(it[0].alert_tier)][:budget]
    by_rule: Dict[str, int] = {}
    for c, _ in taken:
        by_rule[c.rule_id] = by_rule.get(c.rule_id, 0) + 1
    tiers: Dict[str, int] = {}
    for c, _ in clusters:
        tiers[c.alert_tier.value] = tiers.get(c.alert_tier.value, 0) + 1
    eligible = [c for c, _ in clusters if tier_warrants_synthesis(c.alert_tier)]
    multi = [c for c, _ in taken if (c.metrics_summary or {}).get("domain_count", 1) > 1]
    L.record("a synthetic day: 240 situations through collection-shaped events, rules and ranking",
             PASS if fired_intact == len(intact) and fired_broken == 0 else DEFECT,
             f"{len(intact)} intact situations: {fired_intact} produced a finding. {len(broken)} deliberately broken situations: {fired_broken} produced a finding. "
             f"Findings by tier: {tiers}. Only {len(eligible)} of {len(clusters)} are in a tier that earns a model-written scenario at all. Of the {len(taken)} "
             f"admitted against a budget of {budget} an hour, {len(multi)} span more than one domain. Share of the inference budget by rule: "
             + ", ".join(f"{k.replace('rule_', '')} {v}" for k, v in sorted(by_rule.items(), key=lambda kv: -kv[1])[:6]), len(rows))
    missed = [(x[0], x[1]) for x in intact if not x[2]][:3]
    leaked = [(x[0], x[1]) for x in broken if x[2]][:3]
    if missed or leaked:
        L.record("day: what was missed and what leaked", DEFECT if leaked or missed else PASS, f"missed intact: {missed}; fired on broken: {leaked}", len(rows), [str(m) for m in missed + leaked])
    return "ok"


def run(seed: int) -> Ledger:
    L = Ledger("4. Reasoning and forecasting", seed)
    part_a(L, seed)
    part_b(L, seed)
    part_c(L, seed)
    part_d(L, seed)
    part_e(L, seed)
    return L
