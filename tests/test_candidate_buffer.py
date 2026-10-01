"""The scarce slot should go to the best candidate, not the luckiest one.

Measured on the running deployment 2026-09-20, ten minutes on the fast tier:

    messages processed                    ~1,970
    survive the agents' own cheap gates      ~700
    reach the admission bar                    44   6% of candidates
      held back by the percentile              37
      claimed a slot                            7
    generate calls at the model server          5

The percentile bar works, and it was being applied to six per cent of the
population. The other ninety-four per cent died at `is_available()`: five
agents queue for one slot, the peek yields to whoever asked earliest, and
anything arriving while another agent held the head of the queue was dropped.
Ranking applied after a timing lottery is not ranking.

This does not add capacity. The same number of inferences run; a different set
of candidates gets them.
"""

import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.candidate_buffer import (  # noqa: E402
    DEFERRED_KEY,
    CandidateBuffer,
    strip_deferred,
)


def _msg(name, **extra):
    m = {"event_id": name}
    m.update(extra)
    return m


# ── it keeps the best, not the newest ────────────────────────────────────────


def test_the_best_candidate_is_the_one_returned():
    buf = CandidateBuffer("t")
    buf.offer(_msg("weak"), 0.20)
    buf.offer(_msg("strong"), 0.95)
    buf.offer(_msg("middling"), 0.60)
    assert buf.take_best()["event_id"] == "strong"


def test_arrival_order_breaks_ties_not_score():
    buf = CandidateBuffer("t")
    buf.offer(_msg("first"), 0.5)
    buf.offer(_msg("second"), 0.5)
    assert buf.take_best()["event_id"] == "first"


def test_draining_returns_candidates_in_merit_order():
    buf = CandidateBuffer("t")
    for name, score in (("a", 0.1), ("b", 0.9), ("c", 0.5)):
        buf.offer(_msg(name), score)
    assert [buf.take_best()["event_id"] for _ in range(3)] == ["b", "c", "a"]


def test_an_unscored_candidate_does_not_outrank_a_measured_one():
    buf = CandidateBuffer("t")
    buf.offer(_msg("unscored"), None)
    buf.offer(_msg("measured"), 0.3)
    assert buf.take_best()["event_id"] == "measured"


def test_an_unparseable_score_is_zero_not_an_error():
    buf = CandidateBuffer("t")
    buf.offer(_msg("junk"), "not a number")
    buf.offer(_msg("real"), 0.01)
    assert buf.take_best()["event_id"] == "real"


# ── bounded, and honest about what it drops ──────────────────────────────────


def test_the_buffer_is_bounded():
    buf = CandidateBuffer("t", max_items=4)
    for i in range(50):
        buf.offer(_msg(f"m{i}"), i / 100.0)
    assert len(buf) == 4


def test_a_burst_of_noise_cannot_evict_a_real_finding():
    """The whole point: memory bounded without losing the thing worth running."""
    buf = CandidateBuffer("t", max_items=4)
    buf.offer(_msg("finding"), 0.99)
    for i in range(100):
        buf.offer(_msg(f"noise{i}"), 0.01)
    assert buf.take_best()["event_id"] == "finding"


def test_eviction_is_counted():
    """A discarded observation is a decision, not an absence."""
    buf = CandidateBuffer("t", max_items=2)
    for i in range(10):
        buf.offer(_msg(f"m{i}"), 0.5)
    assert buf.evicted == 8
    assert buf.offered == 10


def test_offer_reports_whether_the_candidate_was_kept():
    buf = CandidateBuffer("t", max_items=1)
    assert buf.offer(_msg("good"), 0.9) is True
    assert buf.offer(_msg("worse"), 0.1) is False, "a losing offer must say so"


# ── stale candidates are not run ─────────────────────────────────────────────


def test_a_candidate_that_aged_out_is_not_returned():
    buf = CandidateBuffer("t", max_age_sec=0.0)
    buf.offer(_msg("old"), 0.9)
    time.sleep(0.01)
    assert buf.take_best() is None
    assert buf.expired == 1


def test_expiry_and_eviction_are_counted_separately():
    """One is the buffer being full; the other is the world moving on."""
    buf = CandidateBuffer("t", max_items=1, max_age_sec=0.0)
    buf.offer(_msg("a"), 0.9)
    buf.offer(_msg("b"), 0.1)
    time.sleep(0.01)
    buf.take_best()
    assert buf.evicted == 1 and buf.expired == 1


def test_an_empty_buffer_returns_nothing():
    assert CandidateBuffer("t").take_best() is None


# ── a drained candidate does not come back round ─────────────────────────────


def test_a_drained_candidate_is_marked():
    buf = CandidateBuffer("t")
    buf.offer(_msg("x"), 0.5)
    assert buf.take_best()[DEFERRED_KEY] is True


def test_a_marked_candidate_is_refused_re_entry():
    """Otherwise one message could hold the head position indefinitely."""
    buf = CandidateBuffer("t")
    drained = {"event_id": "x", DEFERRED_KEY: True}
    assert buf.offer(drained, 0.99) is False
    assert len(buf) == 0


def test_the_marker_is_strippable():
    assert DEFERRED_KEY not in strip_deferred({"a": 1, DEFERRED_KEY: True})
    assert strip_deferred({"a": 1}) == {"a": 1}


def test_a_non_dict_is_refused_rather_than_raising():
    assert CandidateBuffer("t").offer("not a message", 0.5) is False


# ── what the agent tier does with it ─────────────────────────────────────────


def test_the_peek_sites_defer_rather_than_discard():
    """Every `is_available()` peek that used to end in a bare return."""
    agents = ROOT / "services" / "agents"
    for name in (
        "knowledge_graph_engine", "adversarial_wargamer", "macro_intelligence_engine",
        "quant_trading_engine", "stock_correlation_agent",
    ):
        src = (agents / f"{name}.py").read_text(encoding="utf-8")
        assert "self._inference_budget.is_available()" not in src, (
            f"{name} still discards the candidate it cannot run"
        )
        assert "capacity_or_defer(message)" in src, name


def test_the_drain_goes_through_the_normal_dispatch_path():
    """Staleness, the telemetry denylist, the type filter and dedup must all
    re-apply -- nothing is bypassed to get a candidate in front of the model."""
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    drain = src[src.index("async def _candidate_drain_loop"):]
    drain = drain[: drain.index("\n    async def ", 10)]
    assert "await self._dispatch(best)" in drain
    assert "self._inference_budget.is_available()" in drain, (
        "the drain must check capacity before spending a candidate"
    )


def test_a_deferred_message_proceeds_without_being_re_offered():
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    helper = src[src.index("async def capacity_or_defer"):]
    helper = helper[: helper.index("\n    async def ", 10)]
    assert "if message.get(DEFERRED_KEY):" in helper
    assert "return True" in helper


def test_every_defer_site_names_a_variable_in_its_own_scope():
    """The swap was made across eight call sites and one was not `message`.

    `_run_macro_review_now(self, trigger_event=None)` has no `message`, so a
    bulk replacement put a NameError on a path that only runs when the macro
    review fires -- which is why it reached the deployment rather than the
    suite. Read from the syntax tree so the check cannot be fooled by a name
    that merely appears nearby.
    """
    import ast
    import re

    agents = ROOT / "services" / "agents"
    bad = []
    for path in sorted(agents.glob("*.py")):
        src = path.read_text(encoding="utf-8")
        if "capacity_or_defer(" not in src:
            continue
        for node in ast.walk(ast.parse(src)):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            params = {a.arg for a in node.args.args} | {a.arg for a in node.args.kwonlyargs}
            body = ast.get_source_segment(src, node) or ""
            for name in re.findall(r"capacity_or_defer\((\w+)\)", body):
                if name not in params:
                    bad.append(f"{path.name}:{node.name} -> {name}")
    assert not bad, f"capacity_or_defer called with a name not in scope: {bad}"


def test_the_buffer_reports_itself_on_the_heartbeat():
    """A deferred candidate and a discarded one look identical from outside
    unless something publishes the difference. `snapshot()` existed and reached
    nothing, which is the defect shape this module was written to remove."""
    src = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert '"candidates": self._candidates.snapshot()' in src


def test_the_snapshot_names_what_a_reader_needs():
    buf = CandidateBuffer("t", max_items=1)
    buf.offer(_msg("a"), 0.9)
    buf.offer(_msg("b"), 0.1)
    buf.take_best()
    snap = buf.snapshot()
    for key in ("held", "offered", "drained", "evicted", "expired", "best_score"):
        assert key in snap, key
    assert snap["offered"] == 2 and snap["evicted"] == 1 and snap["drained"] == 1


# ── no domain can be crowded out by another's volume ─────────────────────────


def _dm(domain, name, **extra):
    d = {"primary_domain": domain, "event_id": name}
    d.update(extra)
    return d


def test_a_flood_cannot_evict_a_quiet_domain_entirely():
    """The live situation, measured over six hours.

        1,177 events cleared the 0.8 admission bar
          887 of them (75%) were aisstream

    Not because AIS over-scores: it clears the bar on 1.73% of its events,
    against 26.8% for okx_swap, 25% for telegram and 20% for reddit. It wins on
    volume -- 51,110 events in the window. A single global ranking hands every
    slot to whichever collector is busiest.
    """
    buf = CandidateBuffer("t", max_items=8)
    for i in range(200):
        buf.offer(_dm("maritime", f"ais{i}"), 0.95)
    buf.offer(_dm("macro", "freight"), 0.58)
    assert "macro" in buf.snapshot()["domains"]


def test_a_detector_below_every_other_ceiling_still_reaches_the_model():
    """macro_freight peaked at 0.583 in six hours; aisstream routinely hits 0.99."""
    buf = CandidateBuffer("t", max_items=8)
    for i in range(50):
        buf.offer(_dm("maritime", f"ais{i}"), 0.99)
    buf.offer(_dm("macro", "freight"), 0.583)
    drained = [buf.take_best() for _ in range(2)]
    assert "freight" in [d["event_id"] for d in drained if d]


def test_within_a_domain_the_best_still_wins():
    """Fairness across domains must not become indifference inside one."""
    buf = CandidateBuffer("t")
    buf.offer(_dm("maritime", "weak"), 0.10)
    buf.offer(_dm("maritime", "strong"), 0.95)
    assert buf.take_best()["event_id"] == "strong"


def test_the_least_recently_drained_domain_goes_first():
    buf = CandidateBuffer("t")
    buf.offer(_dm("a", "a1"), 0.9)
    buf.offer(_dm("a", "a2"), 0.8)
    buf.offer(_dm("b", "b1"), 0.1)
    assert [buf.take_best()["event_id"] for _ in range(3)] == ["a1", "b1", "a2"]


def test_domainless_messages_behave_exactly_as_before():
    """Everything without a domain shares one, so ordering is pure score."""
    buf = CandidateBuffer("t")
    for name, score in (("a", 0.1), ("b", 0.9), ("c", 0.5)):
        buf.offer(_msg(name), score)
    assert [buf.take_best()["event_id"] for _ in range(3)] == ["b", "c", "a"]


def test_the_domain_is_read_from_whatever_the_message_carries():
    from shared.utils.candidate_buffer import UNKNOWN_DOMAIN, domain_of

    assert domain_of({"primary_domain": "Maritime"}) == "maritime"
    assert domain_of({"domain": "Macro"}) == "macro"
    assert domain_of({"asset_class": "Crypto"}) == "crypto"
    assert domain_of({"source": "aisstream"}) == "aisstream"
    assert domain_of({}) == UNKNOWN_DOMAIN
    assert domain_of("not a dict") == UNKNOWN_DOMAIN


def test_eviction_takes_from_the_crowded_domain_not_the_lowest_score():
    buf = CandidateBuffer("t", max_items=3)
    buf.offer(_dm("maritime", "m1"), 0.9)
    buf.offer(_dm("maritime", "m2"), 0.8)
    buf.offer(_dm("macro", "freight"), 0.2)
    buf.offer(_dm("maritime", "m3"), 0.95)
    held = {i[3]["event_id"] for i in buf._items}
    assert "freight" in held, "the weakest overall, but the only one of its kind"


def test_the_snapshot_reports_which_domains_are_held():
    """32 AIS events and nothing else must not read as a healthy full buffer."""
    buf = CandidateBuffer("t")
    buf.offer(_dm("maritime", "m"), 0.9)
    buf.offer(_dm("macro", "f"), 0.2)
    assert buf.snapshot()["domains"] == ["macro", "maritime"]


def test_an_expired_candidate_does_not_win_its_domain_a_turn():
    buf = CandidateBuffer("t", max_age_sec=0.0)
    buf.offer(_dm("macro", "stale"), 0.9)
    time.sleep(0.01)
    assert buf.take_best() is None
    assert buf.expired == 1


# ── the domain marker the messages actually carry ────────────────────────────


def test_the_payload_block_identifies_the_domain():
    """`primary_domain` is declared on the event model and set by nothing.

    Sampled live off enriched.events 2026-09-21: None on every message. So the
    fallback to `source` ran every time, and three crypto RPC collectors
    competed as three separate domains instead of one.
    """
    from shared.utils.candidate_buffer import domain_of

    assert domain_of({"source": "aisstream", "vessel_data": {"mmsi": 1}}) == "maritime"
    assert domain_of({"source": "ethereum_rpc", "crypto_data": {"tx": 1}}) == "crypto"
    assert domain_of({"source": "opensky", "flight_data": {"icao": "x"}}) == "aviation"
    assert domain_of({"source": "finnhub", "financial_data": {"px": 1}}) == "financial"


def test_three_crypto_collectors_are_one_domain():
    from shared.utils.candidate_buffer import domain_of

    seen = {
        domain_of({"source": s, "crypto_data": {"tx": 1}})
        for s in ("arbitrum_rpc", "base_rpc", "ethereum_rpc")
    }
    assert seen == {"crypto"}


def test_an_explicit_domain_still_wins_over_the_payload():
    from shared.utils.candidate_buffer import domain_of

    assert domain_of({"primary_domain": "macro", "vessel_data": {"mmsi": 1}}) == "macro"


def test_an_empty_payload_block_does_not_claim_a_domain():
    """A key present and empty is not evidence of anything."""
    from shared.utils.candidate_buffer import domain_of

    assert domain_of({"source": "x", "vessel_data": {}}) == "x"
    assert domain_of({"source": "x", "vessel_data": None}) == "x"


# ── an equities platform, weighted as one ────────────────────────────────────


def test_crypto_takes_fewer_turns_than_an_unweighted_domain():
    """The platform is an equities platform with a crypto sidecar.

    Measured before the cut: 176,094 of 240,789 events over six hours were
    crypto, at a mean anomaly of 0.02-0.07, and the sidecar set the agenda for
    every stage downstream. Crypto still reaches the model; it does not go
    first.
    """
    from collections import Counter
    from shared.utils.candidate_buffer import domain_of

    buf = CandidateBuffer("t", max_items=60)
    for i in range(30):
        buf.offer({"crypto_data": {"t": i}, "event_id": f"c{i}"}, 0.99)
        buf.offer({"financial_data": {"p": i}, "event_id": f"eq{i}"}, 0.55)
    share = Counter(domain_of(buf.take_best()) for _ in range(12))
    assert share["financial"] > share["crypto"], share
    assert share["crypto"] > 0, "deprioritised, not locked out"


def test_the_highest_scoring_crypto_candidate_still_wins_its_own_turn():
    """Weighting is across domains; within one, score still decides."""
    buf = CandidateBuffer("t")
    buf.offer({"crypto_data": {"t": 1}, "event_id": "weak"}, 0.10)
    buf.offer({"crypto_data": {"t": 2}, "event_id": "strong"}, 0.95)
    assert buf.take_best()["event_id"] == "strong"


def test_an_unlisted_domain_costs_one_turn():
    from shared.utils.candidate_buffer import DEFAULT_TURN_COST, DOMAIN_TURN_COST

    assert DOMAIN_TURN_COST.get("financial", DEFAULT_TURN_COST) == 1
    assert DOMAIN_TURN_COST["crypto"] > DEFAULT_TURN_COST


def test_a_domain_appearing_late_is_not_handed_every_turn_it_missed():
    """Virtual time starts at the front of the queue, not at zero."""
    from shared.utils.candidate_buffer import domain_of

    buf = CandidateBuffer("t", max_items=60)
    for i in range(10):
        buf.offer({"financial_data": {"p": i}, "event_id": f"eq{i}"}, 0.9)
    for _ in range(5):
        buf.take_best()
    buf.offer({"vessel_data": {"m": 1}, "event_id": "late"}, 0.1)
    served = [domain_of(buf.take_best()) for _ in range(4)]
    assert served.count("maritime") == 1, served
