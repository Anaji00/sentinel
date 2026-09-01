"""
tests/test_scenario_quality_and_tradfi_scoring.py

Two problems that made output unusable rather than absent.

1. Scenario framing. The prompt led with `Rule Fired: {rule_name}` and carried a
   section headed "RECENT GEOPOLITICAL HEADLINES", with nothing anywhere saying
   what the entities actually were. A 1.5B model will not infer that "ADAUSDT"
   is a perpetual futures pair, so it wrote what it was framed to write:
   "Geopolitical Cascade Alert in 'Adausdt'" -- a crypto ticker read as a place.

2. Tradfi saturation. Every boost was `min(1.0, anomaly * k)`. Multipliers
   compound: the block-trade path applies 1.15, 1.2, 1.3, 1.1 and 1.4 to one
   score -- 3.17x -- so anything above about 0.32 clamped to exactly 1.0.
   Measured over three days, 330 of 426 tradfi anomalies (77.5%) sat on the
   ceiling, which makes the strongest signal indistinguishable from a merely
   notable one.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GENERATOR = ROOT / "services/reasoning/scenario_generator.py"


def _load(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def generator():
    return _load("reasoning_scenario_generator_q", "services/reasoning/scenario_generator.py")


@pytest.fixture(scope="module")
def tradfi():
    return _load("enrichment_tradfi_scoring", "services/enrichment/enrichers/tradfi.py")


class _Cluster:
    def __init__(self, names):
        self.entity_names = names
        self.entity_ids = []


# -- the model must be told what it is looking at -----------------------------

def test_a_crypto_cluster_is_named_as_crypto(generator):
    line = generator._subject_line(_Cluster(["ADAUSDT", "DOGEUSDT"]), [{"crypto_data": {"pair": "ADA"}}])
    assert "ADAUSDT" in line
    assert "crypto" in line.lower()


def test_a_ticker_is_explicitly_not_a_place(generator):
    """The failure this guards: "Geopolitical Cascade Alert in 'Adausdt'"."""
    line = generator._subject_line(_Cluster(["ADAUSDT"]), [{"crypto_data": {"p": 1}}])
    assert "not a country" in line


def test_a_vessel_is_not_told_it_has_no_location(generator):
    """A ship genuinely has a position; suppressing that removes the signal."""
    line = generator._subject_line(_Cluster(["BINTANG LIBERTY 2"]), [{"vessel_data": {"imo": "9427843"}}])
    assert "vessel" in line.lower()
    assert "not a country" not in line


def test_an_equity_cluster_is_named_as_equities(generator):
    line = generator._subject_line(_Cluster(["NVDA"]), [{"financial_data": {"ticker": "NVDA"}}])
    assert "equit" in line.lower()


def test_an_unknown_cluster_says_so_rather_than_guessing(generator):
    line = generator._subject_line(_Cluster([]), [])
    assert "unstated type" in line
    assert "unnamed entities" in line


def test_the_prompt_leads_with_the_subject():
    src = GENERATOR.read_text(encoding="utf-8")
    prompt = src[src.index('=== SUBJECT ==='):]
    assert prompt.index("=== SUBJECT ===") < prompt.index("=== ANOMALY CLUSTER ===")


def test_the_detector_name_is_framed_as_a_label_not_a_finding():
    src = GENERATOR.read_text(encoding="utf-8")
    assert "Rule Fired:" not in src, "the bare label led the model to write to it"
    assert "not a conclusion" in src


def test_the_news_section_is_not_framed_as_geopolitical():
    # Comments stripped: the file names the old header when explaining why it
    # was changed, and matching that is asserting against the explanation.
    src = re.sub(r"^\s*#.*$", "", GENERATOR.read_text(encoding="utf-8"), flags=re.M)
    assert "RECENT GEOPOLITICAL HEADLINES" not in src
    assert "RECENT NEWS CONTEXT" in src


def test_the_task_forbids_inventing_geography():
    src = GENERATOR.read_text(encoding="utf-8")
    task = src[src.index("=== TASK ==="):]
    assert "no geography may be inferred" in task
    # The rule is that a mechanism must be anchored in the evidence. The wording
    # was tightened after a live scenario came back with the field's own
    # description in place of an analysis -- "causal mechanism explaining signal
    # convergence" -- so the rule now demands a quoted number or name and says
    # explicitly that describing the field will be rejected.
    assert "must quote a number or a name that appears above" in task
    assert "is a description of the field and will be rejected" in task


# -- tradfi scores must not saturate ------------------------------------------

def test_no_multiplicative_boost_remains():
    src = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    code = re.sub(r"^\s*#.*$", "", src, flags=re.M)
    triple = chr(34) * 3
    code = re.sub(triple + ".*?" + triple, "", code, flags=re.S)
    assert not re.search(r"anomaly = min\(1\.0, anomaly \* [0-9.]+\)", code)


def test_a_lift_never_exceeds_one(tradfi):
    for base in (0.0, 0.35, 0.5, 0.9, 1.0):
        for weight in (0.1, 0.5, 1.0, 5.0):
            assert 0.0 <= tradfi._lift(base, weight) <= 1.0


def test_a_lift_is_monotonic_in_the_starting_score(tradfi):
    """Two events differing only in base score must keep their order."""
    lifted = [tradfi._lift(b, 0.3) for b in (0.2, 0.4, 0.6, 0.8)]
    assert lifted == sorted(lifted)
    assert len(set(lifted)) == 4


def test_the_full_block_trade_stack_does_not_reach_the_ceiling(tradfi):
    """1.15 x 1.2 x 1.3 x 1.1 x 1.4 clamped everything above 0.32 to exactly 1."""
    results = []
    for start in (0.35, 0.5, 0.7, 0.9):
        score = start
        for weight in (0.15, 0.20, 0.30, 0.10, 0.40):
            score = tradfi._lift(score, weight)
        results.append(score)
    assert all(r < 1.0 for r in results), "the stack still saturates"
    assert results == sorted(results), "ordering was lost"
    assert len(set(results)) == 4, "distinct inputs collapsed to one score"


def test_a_lift_raises_the_score(tradfi):
    """It must still be a boost, not a no-op."""
    assert tradfi._lift(0.5, 0.2) > 0.5


def test_junk_inputs_leave_the_score_untouched(tradfi):
    assert tradfi._lift(0.5, 0) == 0.5
    assert tradfi._lift(0.5, -1) == 0.5
    assert tradfi._lift("x", 0.2) == "x"


# -- adjustments share one allowance ------------------------------------------

def test_sequential_lifts_share_a_budget():
    """Each lift taking a share of *remaining* headroom compounds toward 1.0.

    Measured after the multiplier fix: equity_block scores formed a reasonable
    curve from 0.4 to 0.9 -- 102, 233, 328, 357, 319 per decile -- and then
    piled 1,177 events into the top decile alone, 47% of the sample. A ranking
    that puts nearly half its population in one bucket is not ranking that half.
    """
    from services.enrichment.enrichers.tradfi import _lift, MAX_TOTAL_LIFT_SHARE

    score, spent = 0.60, 0.0
    for weight in (0.15, 0.15, 0.2, 0.3, 0.1, 0.35):
        score = _lift(score, weight, spent)
        spent += weight

    unbudgeted = 0.60
    for weight in (0.15, 0.15, 0.2, 0.3, 0.1, 0.35):
        unbudgeted = _lift(unbudgeted, weight, 0.0)

    assert score < unbudgeted, "the budget did not restrain the accumulation"
    assert score < 0.90, "a fully-boosted event still crowds the ceiling"


def test_lifts_still_order_events():
    """The budget must bound the total, not flatten the ranking."""
    from services.enrichment.enrichers.tradfi import _lift

    one = _lift(0.60, 0.15, 0.0)
    two = _lift(one, 0.15, 0.15)
    assert 0.60 < one < two


def test_the_budget_is_threaded_through_every_call_site():
    """A lift that forgets to pass `spent` silently opts out of the budget."""
    source = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert source.count("_lift(anomaly,") == source.count("lift_spent +=")


def test_the_allowance_is_configurable():
    source = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert "TRADFI_MAX_TOTAL_LIFT" in source


def test_every_function_using_the_lift_budget_initialises_it():
    """A missing initializer is a NameError on every event through that path.

    _enrich_insider referenced lift_spent twelve times without declaring it.
    The caller swallows per-event failures, so the path went silent while the
    heartbeat still read errors=0 -- writes simply stopped, with nothing in the
    logs to say why.
    """
    import ast

    source = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    missing = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            body = ast.get_source_segment(source, node) or ""
            if body.count("lift_spent") and "lift_spent = 0.0" not in body:
                missing.append(node.name)
    assert not missing, f"lift_spent used without initialisation in: {missing}"


# ── Guards must announce when a type change disarms them ─────────────────────
#
# Turning signals from strings into ResolutionSignal objects blinded both the
# template-echo check and the hypothesis-discrimination check. Each kept
# passing while checking nothing, for 35 minutes and 22 wasted inferences,
# because both fell through silently on a type they did not recognise.
#
# The guards are fixed for that specific type. The class is not fixable by
# enumeration -- the next shape change is unknown -- so what these pin is that
# an unrecognised type is now audible rather than silent.

def test_the_template_guard_warns_when_it_cannot_inspect_a_type(caplog):
    import logging

    from services.reasoning.scenario_generator import _echoes_the_template

    class Opaque:
        """A shape the guard's walk does not recognise."""

    class Draft:
        headline = Opaque()
        significance = ""
        hypotheses = []

    with caplog.at_level(logging.WARNING):
        _echoes_the_template(Draft())

    assert any("cannot inspect" in r.message for r in caplog.records), (
        "an unrecognised field type must not pass the guard in silence"
    )


def test_the_signature_guard_warns_when_it_falls_back_to_str(caplog):
    import logging

    from services.reasoning.scenario_generator import _signal_signature

    class Opaque:
        def __str__(self):
            return "threshold=5.0"

    with caplog.at_level(logging.WARNING):
        _signal_signature(Opaque())

    assert any("falling back to str()" in r.message for r in caplog.records)


def test_known_signal_shapes_do_not_warn(caplog):
    """The warning has to mean something, so the supported shapes stay quiet."""
    import logging

    from services.reasoning.scenario_generator import _signal_signature

    with caplog.at_level(logging.WARNING):
        _signal_signature({"entity": "NVDA", "observable": "guidance"})

    assert not [r for r in caplog.records if "falling back" in r.message]


# ── Headlines have to name their subject, and the right one ────────────────
#
# Over 24 hours, 10 of 83 scenario headlines carried a concrete identifier. The
# rest read "Suspicious Crypto Transfer Activity" -- true of thousands of events
# and therefore about none of them.
#
# The first fix took entities[0] and put it in front. Verified live during
# market hours, that produced headlines that were not vague but wrong:
#
#     VRH6823: Cryptocurrency Market Shift...     (an aircraft callsign)
#     QQQ: Potential Cybersecurity Threat...      (an ETF)
#
# A cluster spanning crypto, aviation and cyber holds entities from all three,
# and the first is not the subject -- it is merely first. An ungrounded headline
# is imprecise; a wrongly grounded one asserts something false. The entity now
# has to appear in the scenario's own reasoning to be attached, which makes this
# the repair of an omission rather than a guess.

def test_an_omitted_subject_is_restored():
    from services.reasoning.scenario_generator import _ground_headline

    out = _ground_headline(
        "Suspicious Crypto Transfer Activity",
        ["0x5594abc123def4567890"],
        "Flows through 0x5594abc123def4567890 cluster around one counterparty.",
    )
    assert out.startswith("0x5594abc1")
    assert "Suspicious Crypto Transfer Activity" in out


def test_an_entity_the_scenario_never_discusses_is_not_attached():
    """The live regression. An aircraft callsign on a crypto headline is worse
    than no callsign at all."""
    from services.reasoning.scenario_generator import _ground_headline

    original = "Cryptocurrency Market Shift: Bitcoin and Ethereum Prices"
    out = _ground_headline(original, ["VRH6823"], "Bitcoin and Ethereum moved sharply.")
    assert out == original


def test_the_discussed_entity_wins_over_the_first_one():
    """Multi-domain clusters are the normal case, not the exception."""
    from services.reasoning.scenario_generator import _ground_headline

    out = _ground_headline(
        "Cryptocurrency Market Shift",
        ["VRH6823", "CYF106", "0x28c6abc"],
        "Accumulation through 0x28c6abc precedes the move.",
    )
    assert out.startswith("0x28c6abc:")


def test_a_headline_that_already_names_its_subject_is_untouched():
    from services.reasoning.scenario_generator import _ground_headline

    original = "Crypto Asset Anomaly Alert: ADAUSDT Market Movement"
    assert _ground_headline(original, ["ADAUSDT"], "ADAUSDT fell.") == original


def test_the_match_is_case_insensitive():
    from services.reasoning.scenario_generator import _ground_headline

    original = "Unusual flow through 0xAbCd1234"
    assert _ground_headline(original, ["0xabcd1234"], "body") == original


def test_nothing_is_invented_when_the_cluster_names_nobody():
    from services.reasoning.scenario_generator import _ground_headline

    original = "Multi-domain Correlated Signals Detected in Black Sea"
    assert _ground_headline(original, [], "body") == original


def test_long_identifiers_are_truncated_rather_than_dropped():
    from services.reasoning.scenario_generator import _ground_headline

    addr = "0x28c6c06298d514db089934071355e5743bf21d60"
    out = _ground_headline("Transfer activity", [addr], f"Funds left {addr} in size.")
    assert len(out) < 60
    assert "0x28c6c062" in out


def test_a_readable_name_is_kept_whole():
    from services.reasoning.scenario_generator import _ground_headline

    out = _ground_headline(
        "Sanctioned Vessel Alert", ["BINTANG LIBERTY 3"],
        "BINTANG LIBERTY 3 went dark near the strait.",
    )
    assert out.startswith("BINTANG LIBERTY 3:")


def test_an_empty_headline_is_not_given_a_subject():
    from services.reasoning.scenario_generator import _ground_headline

    assert _ground_headline("", ["ADAUSDT"], "ADAUSDT fell.") == ""


def test_the_body_text_gathers_what_the_model_reasoned_about():
    from services.reasoning.scenario_generator import _scenario_body_text

    class H:
        label = "Accumulation"
        mechanism = "Funds move to 0x28c6abc"
        beneficiaries = ["a desk"]

    class Draft:
        significance = "Notable flow"
        hypotheses = [H()]

    body = _scenario_body_text(Draft())
    assert "0x28c6abc" in body and "Notable flow" in body and "a desk" in body
