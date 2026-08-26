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
    assert "must reference a signal that actually appears" in task


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
