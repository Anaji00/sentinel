"""
tests/test_scenario_resolution.py

231 scenarios. All 231 still `hypothesis`. Not one confirmed or denied, ever.

    SELECT status, COUNT(*) FROM scenarios GROUP BY 1;
      hypothesis | 231

The tracker was not idle. It ran on schedule for the life of the service:

    [reasoning.tracker] Checking 100 active scenarios for resolution signals
    [reasoning.tracker] Checking 100 active scenarios for resolution signals

and matched nothing, every time, because the query it runs cannot execute:

    OR tags && $3::varchar[]

`events.tags` and `events.named_entities` are `text[]`. Postgres has no
`text[] && varchar[]` operator, so the statement raised on every call:

    ERROR: operator does not exist: text[] && character varying[]

The exception was caught and logged at `debug`. At INFO -- which is what the
service runs at -- a query that could not run and a signal that did not match
produced exactly the same output: silence. `_match_signals` returned `[]` for
every signal of every hypothesis of every scenario, so no watch signal ever
fired, no deny signal ever fired, the Bayesian recalibrator was never invoked,
and no scenario could reach CONFIRM_THRESHOLD or DENY_THRESHOLD.

This is why the confidence distribution could not be calibrated. 135 of the
scenarios sit at exactly 85 and the open item called for "calibration against
outcomes" -- but there were no outcomes, and there was no mechanism by which
there could ever be one. The clustering was the visible symptom; this was the
cause standing behind it.

The cast is one word. What made it survive is the log level.

Fixing it exposed the next problem immediately: 185 confirmed, none denied.
Matching any keyword over four characters fires on 89% of signals -- measured,
303 of them against 48 hours of events -- so watch and deny signals for the same
scenario both hit and the recalibrator nets out positive on noise. Matching is
now conjunctive, which drops that to 2.3%. That is a floor rather than a
solution: at 2.3% these signals are still barely checkable, because they are
model-generated prose rather than statements with an entity and a threshold.
Under-resolving is the honest failure here and over-confirming is not, since
these outcomes feed the confidence calibration and the pattern library.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TRACKER = ROOT / "services" / "reasoning" / "scenario_tracker.py"


def _source() -> str:
    return TRACKER.read_text(encoding="utf-8")


# -- the cast ------------------------------------------------------------------

def test_the_arrays_are_compared_as_text():
    """text[] op text[] exists; text[] op varchar[] does not.

    The operator has since changed from `&&` (overlap) to `@>` (contains), when
    matching was tightened from any-keyword to all-keywords -- see
    test_the_match_is_conjunctive below. The cast is the part this pins.
    """
    source = _source()
    assert "OR tags @> $3::text[]" in source
    assert "OR named_entities @> $4::text[]" in source


def test_no_varchar_cast_remains_anywhere():
    """Both columns are text[]; either cast alone re-breaks the whole match."""
    assert "::varchar[]" not in _source()


@pytest.mark.parametrize("column", ["tags", "named_entities"])
def test_each_array_column_is_cast_to_its_own_type(column):
    """Read from the SQL, not from a comment that happens to name the column."""
    source = _source()
    line = next(
        ln for ln in source.splitlines()
        if f"{column} @>" in ln and not ln.strip().startswith("#")
    )
    assert "::text[]" in line, f"{column} is compared against the wrong array type"


def test_the_match_is_conjunctive():
    """Once matching ran at all, it confirmed 185 scenarios and denied none.

    Measured over 303 distinct watch signals against 48 hours of events: any
    keyword matched 271 of them (89%), all keywords matched 7 (2.3%). A rule
    that fires on 89% of signals is not evidence -- the same wording appears in
    a scenario's watch and deny signals, both hit, and the recalibrator nets out
    positive on noise.
    """
    source = _source()
    assert "headline ILIKE ALL($2)" in source
    assert "OR tags @> $3::text[]" in source
    # Deliberately not asserting the absence of "&&" or "ILIKE ANY": the module
    # explains the old operators in prose, and a test that fails on its own
    # explanation is a trap this audit already sprang three times.


# -- the silence that let it survive -------------------------------------------

def test_a_failed_query_is_not_reported_as_no_match():
    """The defect is one word; the reason it lasted is this handler.

    A query that cannot execute and a signal that genuinely did not match were
    indistinguishable in the logs, so the tracker looked like it was working and
    simply finding nothing -- which is also what it would look like if there
    were no news. Nothing distinguished the two for the life of the service.
    """
    tree = ast.parse(_source())
    matcher = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_match_signals"
    )
    handlers = [n for n in ast.walk(matcher) if isinstance(n, ast.ExceptHandler)]
    assert handlers, "the query is no longer guarded at all"

    for handler in handlers:
        levels = {
            node.func.attr
            for node in ast.walk(handler)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "logger"
        }
        assert "debug" not in levels, "a broken query is still logged at debug"
        assert levels & {"warning", "error", "exception"}, "the failure is not surfaced"


def test_the_matcher_still_degrades_rather_than_raising():
    """One unmatchable signal must not abort the scenario's other signals, nor
    the tracker loop. Surfacing the failure is not the same as propagating it."""
    tree = ast.parse(_source())
    matcher = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_match_signals"
    )
    for handler in [n for n in ast.walk(matcher) if isinstance(n, ast.ExceptHandler)]:
        assert not any(isinstance(n, ast.Raise) for n in ast.walk(handler))


# -- the resolution path this unblocks ------------------------------------------

def test_a_matched_signal_reaches_the_recalibrator():
    """Nothing downstream of the match was ever exercised: with `[]` returned
    every time, the Bayesian recalibrator was unreachable code in production."""
    source = _source()
    assert "if watch_hits_by_index or deny_hits_by_index:" in source
    assert "DynamicBayesianCalibrator.recalibrate_hypotheses(" in source


def test_both_outcomes_are_reachable():
    source = _source()
    assert "ScenarioStatus.CONFIRMED" in source
    assert "ScenarioStatus.DENIED" in source


def test_the_query_still_bounds_its_own_cost():
    """The N+1 guard predates this and must survive it: one query per signal,
    capped, over a bounded window."""
    source = _source()
    assert "signals[:10]" in source
    assert "LIMIT 1" in source
    assert "timedelta(hours = 48)" in source or "timedelta(hours=48)" in source


def test_keywords_are_lowercased_for_the_array_containment():
    """`@>` is exact-match, unlike the ILIKE branch beside it. Mixed-case
    keywords would match the headline and never the tags."""
    source = _source()
    assert "w.lower()" in source


# -- signals with a shape ------------------------------------------------------

def test_a_signal_must_name_an_entity():
    """Free text is why nothing resolved honestly.

    "Significant price movements on cryptocurrency exchanges" contains words
    that appear in tens of thousands of position-telemetry headlines, so any-
    keyword matching fired on 89% of signals and all-keyword matching on 2.3%.
    Neither number is about the world; both are about the sentence. `entity` is
    what turns a search into a lookup.
    """
    from services.reasoning.scenario_generator import GeneratedSignal
    from shared.models.events import ResolutionSignal

    # Strict for the decoder...
    schema = GeneratedSignal.model_json_schema()["properties"]["entity"]
    assert schema.get("minLength") == 1

    # ...permissive for the table, or several hundred stored scenarios whose
    # signals are bare sentences would stop loading entirely.
    assert not ResolutionSignal.model_fields["entity"].is_required()


def test_the_comparator_is_constrained_by_the_schema():
    """A Literal becomes an enum in the JSON schema, and the schema is passed to
    Ollama as `format` -- so this reaches the decoder rather than the prompt."""
    from services.reasoning.scenario_generator import HypothesisOutput

    comparator = (
        HypothesisOutput.model_json_schema()["$defs"]["GeneratedSignal"]
        ["properties"]["comparator"]
    )
    assert comparator["enum"] == ["above", "below", "occurs", "absent"]


def test_a_threshold_is_optional_but_typed():
    """Not every observable has a number. One that claims to must carry it as a
    number, not inside a sentence."""
    from shared.models.events import ResolutionSignal

    assert ResolutionSignal(entity="NVDA", observable="halt").threshold is None
    assert ResolutionSignal(
        entity="NVDA", observable="block volume", comparator="above",
        threshold=5.0, unit="%",
    ).as_text() == "NVDA: block volume above 5.0%"


@pytest.mark.parametrize(
    "signal,entity",
    [
        ({"entity": "NVDA", "observable": "block volume"}, "NVDA"),
        ("a bare sentence with no entity", ""),
        (None, ""),
    ],
)
def test_both_signal_shapes_are_readable(signal, entity):
    """Hundreds of scenarios are already stored as plain strings. Rewriting
    their hypotheses would mean inventing entities they never named."""
    from services.reasoning.scenario_tracker import _signal_parts

    assert _signal_parts(signal)[0] == entity


def test_a_named_entity_is_matched_by_lookup_not_by_scan():
    """The point of requiring an entity: an indexed equality instead of an
    ILIKE across every headline in the window."""
    source = _source()
    assert "upper(primary_entity_id) = upper($2)" in source
    assert "_match_on_entity" in source


def test_the_persisted_model_and_the_generator_agree_on_the_type():
    """Zero scenarios generated for thirty-five minutes, every run failing with

        hypotheses.0.watch_signals.0
          Input should be a valid string
          input_value={'entity': 'ETHUSDT', ..., 'threshold': 50.0, 'unit': '%'}

    The model was producing exactly the right structure -- constrained decoding
    working on the first try -- and ScenarioHypothesis still declared List[str],
    so every generation was thrown away at the last step. Changing an output
    schema without following it into the model it is mapped onto.
    """
    from shared.models.events import ResolutionSignal, ScenarioHypothesis
    from services.reasoning.scenario_generator import GeneratedSignal

    field = ScenarioHypothesis.model_fields["watch_signals"]
    assert "ResolutionSignal" in str(field.annotation), "the persisted type is out of step again"
    assert issubclass(GeneratedSignal, ResolutionSignal), "the generator emits a type the table cannot hold"


def test_a_generated_hypothesis_survives_persistence():
    """The round trip the outage broke, end to end."""
    from shared.models.events import ScenarioHypothesis
    from services.reasoning.scenario_generator import HypothesisOutput

    generated = HypothesisOutput(
        label="cascade", probability=45, mechanism="because",
        watch_signals=[{"entity": "ETHUSDT", "observable": "price move",
                        "comparator": "above", "threshold": 50.0, "unit": "%"}],
        deny_signals=[], time_horizon="24h",
    )
    stored = ScenarioHypothesis(
        label=generated.label, probability=generated.probability,
        mechanism=generated.mechanism, beneficiaries=[],
        watch_signals=[s.model_dump() for s in generated.watch_signals],
        deny_signals=[], time_horizon=generated.time_horizon,
    )
    assert stored.watch_signals[0].entity == "ETHUSDT"
    assert stored.watch_signals[0].as_text() == "ETHUSDT: price move above 50.0%"


def test_the_signal_lists_are_bounded():
    """A structured signal costs several times what a sentence did, and this
    schema was already truncating at the token ceiling -- which surfaces as a
    missing required field, not as a truncation.

    The bound must exist and stay small; the exact number is a tuned value the
    code invites re-measuring ("Raising this needs the same A/B"). Asserting
    `== 2` made running that A/B fail the suite, which teaches the next person
    to edit the test rather than restore the setting.
    """
    from services.reasoning.scenario_generator import HypothesisOutput

    props = HypothesisOutput.model_json_schema()["properties"]
    for field in ("watch_signals", "deny_signals"):
        bound = props[field].get("maxItems")
        assert bound is not None, f"{field} is unbounded again"
        assert 1 <= bound <= 4, f"{field} bounded at {bound}; the token ceiling is close"
