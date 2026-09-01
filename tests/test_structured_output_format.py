"""
tests/test_structured_output_format.py

Three inferences per advisory, ninety seconds each, all discarded.

With the budget made fair the agents finally got turns, and the work they did
was then thrown away at the last step. Reproduced directly against the live
model, twice:

    prompt : "Produce a trading advisory for XRPUSDT. RSI 61, EMA20 above
              EMA50, ATR 0.9%. Return raw JSON matching the schema."
    output : {"symbol":"XRPUSDT","rsi":61,"ema20_above_ema50":true,
              "atr_percentage":0.9}

    loc=('market_regime',)            type=missing  Field required
    loc=('general_hedging_strategy',) type=missing  Field required

A flat echo of the inputs. Those two are the only fields in
FinancialAdviceBrief without a default, which is why the count is exactly two
and why the same pair appears on every attempt.

The schema was already being sent -- as prose, in the prompt, under "JSON
SCHEMA:". A 1.5B model reads that and ignores it. `format="json"` does not help
either: it compels *valid* JSON, which the model was already producing, and says
nothing about shape. So the retry ladder spent three inferences restating a
request the model had never been constrained by, on a host that affords very few.

Ollama 0.23.3 accepts a JSON Schema in `format` and builds a decoding grammar
from it, which is the difference between asking and requiring. The same
`schema_dict` already computed for the prompt is now passed there on the first
attempt.

First attempt only, deliberately. If Ollama cannot build a grammar for some
schema it answers with an error, and attempts two and three then behave exactly
as all three do today -- so this can improve on current behaviour and cannot
fall below it. That property matters more than usual here: four separate
attempts to measure this change against the live server were defeated by
queueing behind the reasoning tier, so it ships verified by construction and by
the schema-failure rate in the logs, not by a benchmark.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODULE = ROOT / "shared" / "utils" / "ollama.py"


def _source() -> str:
    return MODULE.read_text(encoding="utf-8")


def _infer_method() -> ast.AsyncFunctionDef:
    tree = ast.parse(_source())
    return next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "infer"
    )


# -- the constraint is actually sent -------------------------------------------

def test_the_schema_is_passed_as_the_response_format():
    """Not merely pasted into the prompt, where it was already being ignored."""
    assert 'format=(schema_dict if attempt == 0 else "json")' in _source()


def test_the_first_attempt_is_constrained_and_the_rest_are_not():
    """A schema Ollama cannot compile must not cost all three attempts."""
    call = next(
        n for n in ast.walk(_infer_method())
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "_call_ollama"
    )
    fmt = next(k.value for k in call.keywords if k.arg == "format")
    assert isinstance(fmt, ast.IfExp), "format is unconditional; a bad schema now costs every attempt"
    assert isinstance(fmt.orelse, ast.Constant) and fmt.orelse.value == "json"


def test_the_schema_is_still_described_in_the_prompt():
    """Belt and braces: the grammar constrains shape, the prose still tells the
    model what the fields mean."""
    source = _source()
    assert "JSON SCHEMA:" in source
    assert "Return ONLY a raw JSON object conforming to this schema." in source


def test_the_same_schema_object_serves_both():
    """Two derivations of one schema would drift, and the prompt and the grammar
    disagreeing is worse than either alone."""
    source = _source()
    assert source.count("schema.model_json_schema()") == 1
    assert "schema_json = json.dumps(schema_dict" in source


def test_the_call_accepts_a_mapping_not_only_a_string():
    assert "format: Optional[Union[str, dict]] = None" in _source()


def test_the_payload_sends_whatever_format_it_was_given():
    """A dict must reach the server as a JSON object, not str()'d into nonsense."""
    source = _source()
    assert 'payload["format"] = format' in source


# -- the failure it exists to fix ----------------------------------------------

def test_the_failing_schema_has_exactly_two_required_fields():
    """The measured error count was 2, and this is why. If a default is later
    added to either, the reproduction above stops describing this code."""
    from services.agents.quant_trading_engine import FinancialAdviceBrief

    required = [
        name for name, field in FinancialAdviceBrief.model_fields.items()
        if field.is_required()
    ]
    assert sorted(required) == ["general_hedging_strategy", "market_regime"]


def test_a_flat_echo_of_the_inputs_does_not_validate():
    """The exact output the model returned, kept as a regression case."""
    from pydantic import ValidationError

    from services.agents.quant_trading_engine import FinancialAdviceBrief

    echoed = '{"symbol":"XRPUSDT","rsi":61,"ema20_above_ema50":true,"atr_percentage":0.9}'
    with pytest.raises(ValidationError) as caught:
        FinancialAdviceBrief.model_validate_json(echoed)

    missing = {e["loc"][0] for e in caught.value.errors() if e["type"] == "missing"}
    assert missing == {"market_regime", "general_hedging_strategy"}


def test_the_schema_ollama_receives_is_serialisable():
    """A grammar cannot be built from something that will not serialise, and the
    request would fail before generating a token."""
    import json

    from services.agents.quant_trading_engine import FinancialAdviceBrief

    schema = FinancialAdviceBrief.model_json_schema()
    schema.pop("title", None)
    json.dumps(schema)                      # raises if not
    assert schema.get("type") == "object"
    assert "properties" in schema


def test_nested_models_are_reachable_from_the_schema():
    """These schemas are not flat -- plays, allocations and metrics are nested
    models -- so the grammar depends on $defs surviving."""
    from services.agents.quant_trading_engine import FinancialAdviceBrief

    schema = FinancialAdviceBrief.model_json_schema()
    assert "$defs" in schema, "nested models no longer resolve; the grammar would be wrong"


# -- what the first real prediction exposed ------------------------------------

@pytest.mark.parametrize(
    "given,expected",
    [
        (55.0, 0.55),      # the measured value
        (0.55, 0.55),      # already a probability, untouched
        (100, 1.0),
        (1.0, 1.0),        # the boundary belongs to the probability reading
        (0.0, 0.0),
        (150, 1.0),        # nonsense clamps rather than propagating
        (-5, 0.0),
    ],
)
def test_conviction_is_stored_as_a_probability(given, expected):
    """The first prediction this system recorded carried conviction=55.0.

    Every consumer reads 0-1: the wargamer divides by 100 before recording, and
    the quant engine's own tiering tests `< 0.6` and `< 0.8`. So 55.0 was not
    just weighted 55x -- it cleared every threshold, and a model saying "55%
    confident" was handed the widest risk-reward tier available.
    """
    from services.agents.base import AgentPrediction

    pred = AgentPrediction(
        agent_name="quant_trading_engine", ticker="DOTUSDT",
        direction="up", conviction=given,
    )
    assert pred.conviction == pytest.approx(expected)


def test_an_unparseable_conviction_does_not_raise():
    """record_prediction swallows exceptions and returns "", so raising here
    would convert a recoverable value into a silently missing prediction."""
    from services.agents.base import AgentPrediction

    AgentPrediction(
        agent_name="a", ticker="X", direction="up", conviction=float("nan"),
    )


def test_the_bound_reaches_the_model_through_the_schema():
    """The normaliser is the safety net. The bound in the schema is what Ollama
    decodes against, and is the only thing that prevents the wrong scale being
    generated in the first place."""
    from services.agents.quant_trading_engine import TradingSignal

    prop = TradingSignal.model_json_schema()["properties"]["conviction_score"]
    assert prop.get("minimum") == 0.0
    assert prop.get("maximum") == 1.0


def test_the_normaliser_lives_on_the_shared_contract():
    """Not on the one caller that happened to expose it: the next recorder to be
    written would otherwise arrive with the same bug."""
    source = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
    assert 'field_validator("conviction"' in source
