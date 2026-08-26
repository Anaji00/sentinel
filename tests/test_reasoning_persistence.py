"""
tests/test_reasoning_persistence.py

The reasoning tier never persisted a single scenario.

Zero rows for the life of the deployment while the correlation topic grew past
299,000 messages. Five defects in series, each hidden by the one in front of it,
all silent -- the service logged "Broadcasted Scenario to Kafka" and reported
errors=0 throughout:

  1. Starvation. Reasoning and the five agents-fast agents both run
     qwen2.5:1.5b and shared one inference-budget key. The agents re-claimed it
     before it could expire; sampled every ten seconds it was never free.
     Reasoning sheds whenever the slot is busy, so it never called the model.

  2. Truncation. The generator asks for 1024 tokens; the client capped small
     models at 512. The response was cut mid-object and the extractor needs a
     closing brace, so every scenario failed to parse -- reported as "No valid
     JSON found", blaming formatting rather than the token budget.

  3. Serialisation: json.dumps of Pydantic models raised "Object of type
     ScenarioHypothesis is not JSON serializable".

  4. Identity: scenario_id "scn_a2754df3" against a uuid column.

  5. Double encoding: the pool jsonb codec already runs json.dumps, so a
     pre-serialised string landed as a jsonb string rather than an array.
"""

import json
import re
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ollama import OllamaClient  # noqa: E402

MAIN = ROOT / "services/reasoning/main.py"
GENERATOR = ROOT / "services/reasoning/scenario_generator.py"
OLLAMA = ROOT / "shared/utils/ollama.py"


def _code(path):
    """Source with comments and docstrings stripped."""
    text = path.read_text(encoding="utf-8")
    triple = chr(34) * 3
    text = re.sub(triple + ".*?" + triple, "", text, flags=re.S)
    return re.sub(r"^\s*#.*$", "", text, flags=re.M)


# -- 1. reasoning must get a turn at the model --------------------------------

def test_reasoning_has_a_reserved_lane():
    assert 'lane="reasoning"' in _code(MAIN), "reasoning competes with the swarm again"


def test_a_lane_produces_a_distinct_budget_key():
    from shared.utils.inference_budget import InferenceBudget
    shared = InferenceBudget(None, "qwen2.5:1.5b")
    reserved = InferenceBudget(None, "qwen2.5:1.5b", lane="reasoning")
    assert shared._key != reserved._key
    assert "reasoning" in reserved._key


def test_the_default_stays_shared():
    """Peers competing for one model should still share a slot."""
    from shared.utils.inference_budget import InferenceBudget
    assert InferenceBudget(None, "qwen2.5:3b")._key == InferenceBudget(None, "qwen2.5:3b")._key


# -- 2. the token budget must clear the schema --------------------------------

def test_a_caller_that_declares_a_large_schema_is_not_halved():
    code = _code(OLLAMA)
    assert "min(num_predict or 384, 512)" not in code
    assert "SMALL_MODEL_MAX_TOKENS" in code


def test_the_ceiling_clears_a_measured_scenario():
    from shared.utils.ollama import SMALL_MODEL_MAX_TOKENS
    from services.reasoning.scenario_generator import SCENARIO_TOKEN_BUDGET
    assert SCENARIO_TOKEN_BUDGET > 1024
    assert SMALL_MODEL_MAX_TOKENS >= SCENARIO_TOKEN_BUDGET


def test_truncation_is_diagnosed_as_truncation():
    code = _code(OLLAMA)
    assert 'done_reason") == "length"' in code
    assert "last_truncated" in code


# -- the parser must salvage what the model did produce -----------------------

def test_a_complete_object_is_untouched():
    raw = '{"headline":"A","hypotheses":[{"label":"h1","probability":50}]}'
    assert OllamaClient._extract_json(raw)["headline"] == "A"


def test_an_object_cut_mid_element_keeps_the_complete_ones():
    raw = ('{"headline":"A","significance":"B","hypotheses":'
           '[{"label":"h1","probability":50},{"label":"h2","probab')
    out = OllamaClient._extract_json(raw)
    assert out["headline"] == "A"
    assert len(out["hypotheses"]) == 1, "the half-written hypothesis must be dropped"


def test_a_cut_inside_a_string_still_yields_the_earlier_fields():
    raw = '{"headline":"Cross-Domain Convergence","significance":"Vessel dark event near'
    assert OllamaClient._extract_json(raw)["headline"] == "Cross-Domain Convergence"


def test_braces_inside_strings_do_not_confuse_the_repair():
    raw = '{"headline":"uses { and } inside","hypotheses":[{"label":"x","probability":1}'
    assert OllamaClient._extract_json(raw)["headline"] == "uses { and } inside"


def test_a_nested_array_is_never_mistaken_for_the_whole_object():
    raw = '{"headline":"A","hypotheses":[{"label":"h1","probability":50}],'
    assert "headline" in OllamaClient._extract_json(raw)


def test_the_extractor_always_returns_a_dict_or_none():
    for raw in ('{"a":1}', '[{"a":1}]', 'nonsense', ''):
        out = OllamaClient._extract_json(raw)
        assert out is None or isinstance(out, dict)


def test_nothing_is_invented_by_the_repair():
    raw = '{"headline":"A","hypotheses":[{"label":"h1","probability":50},{"label":"h2'
    out = OllamaClient._extract_json(raw)
    assert out["hypotheses"] == [{"label": "h1", "probability": 50}]


# -- 3, 4, 5: persistence -----------------------------------------------------

def test_pydantic_hypotheses_are_converted_before_insert():
    from services.reasoning.main import _jsonable
    from pydantic import BaseModel

    class H(BaseModel):
        label: str
        probability: int

    out = _jsonable([H(label="a", probability=40)])
    assert out == [{"label": "a", "probability": 40}]
    json.dumps(out)


def test_jsonable_handles_plain_data_unchanged():
    from services.reasoning.main import _jsonable
    assert _jsonable({"x": 1}) == {"x": 1}
    assert _jsonable("text") == "text"


def test_the_scenario_id_is_a_real_uuid():
    code = _code(GENERATOR)
    assert "scn_" not in code, "the short id was rejected by the uuid column"
    assert "scenario_id=str(uuid.uuid4())" in code


def test_hypotheses_are_not_double_encoded():
    code = _code(MAIN)
    assert "json.dumps(_jsonable(scenario.hypotheses))" not in code
    assert "_jsonable(scenario.hypotheses)," in code
