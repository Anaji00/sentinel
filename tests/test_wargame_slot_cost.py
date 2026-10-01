"""
tests/test_wargame_slot_cost.py

Zero wargames completed, zero errors logged, ninety minutes of live traffic.

    adversarial_wargamer | processed=132 errors=0 rate=0.06/s
    WARGAME SIMULATION started: 3
    WARGAME SKIPPED (personas empty): 3
    WARGAME COMPLETED: 0

Both gates in front of the expensive path were working: 3 of 132 messages were
worth simulating and capacity was available for them. All three then died at the
same place -- "All persona turns returned empty" -- and the agent recorded
nothing. Zero predictions in the system traced back to here.

Two faults, one visible:

  * InferenceShed is a BaseException, deliberately, so that the ten inference
    call sites wrapped in `except Exception` cannot swallow a shed and carry on
    as though a model had answered. _execute_persona_turn was one of those
    sites, and its `except Exception` fallback -- a "PASS" move -- was therefore
    unreachable. gather(return_exceptions=True) collected three sheds, the
    isinstance filter dropped all three, and `moves` was empty. errors stayed 0,
    which is why this read as a quiet agent rather than a broken one.

  * The deeper one: a wargame is an all-or-nothing four-slot operation that
    asked for its slots as four independent races. Sharing one slot with radar,
    the graph engine and quant, losing all four is ordinary. A partial win was
    worth nothing -- arbitration needs its own slot regardless -- so every
    outcome short of four wins threw the work away, along with the Neo4j query
    it had already paid for.

The personas are now one structured call: two slots instead of four, and the
expensive step is atomic. What is given up is three independent samplings of the
model. That is a real loss, and a smaller one than never running.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.inference_budget import InferenceShed  # noqa: E402

MODULE = ROOT / "services" / "agents" / "adversarial_wargamer.py"


def _source() -> str:
    return MODULE.read_text(encoding="utf-8")


# -- the exception that was being missed ---------------------------------------

def test_a_shed_is_not_an_exception():
    """The property the persona turn's fallback depended on, and did not have."""
    assert issubclass(InferenceShed, BaseException)
    assert not issubclass(InferenceShed, Exception)


def test_a_bare_except_exception_cannot_catch_a_shed():
    """Stated as behaviour, because reading it off the class hierarchy is
    exactly the step that was skipped."""
    caught = False
    try:
        try:
            raise InferenceShed("wargamer", "model")
        except Exception:  # noqa: BLE001 - the bug, reproduced
            caught = True
    except InferenceShed:
        pass
    assert not caught, "a shed was swallowed by except Exception"


# -- the slot cost -------------------------------------------------------------

def test_the_whole_wargame_is_one_call():
    """Four races became two, and two became one.

    The argument this file was written to make -- an all-or-nothing operation
    must not ask for its slots as independent races -- was applied to the three
    persona turns and left the board/arbitration pair untouched. Measured
    2026-09-20 over the agent's whole history: 685 persona boards completed,
    30 arbitrations, 0 predictions recorded. 655 paid-for boards were discarded
    because the second call sheds independently of the first.
    """
    tree = ast.parse(_source())
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)}

    assert "_execute_wargame" in names
    assert "_execute_persona_turn" not in names, "the per-persona call still exists"
    assert "_execute_persona_board" not in names, "the board is still a separate claim"


def test_the_personas_are_not_gathered_concurrently():
    source = _source()
    assert "asyncio.gather" not in source, "concurrent persona claims are back"


def test_the_wargame_makes_exactly_one_inference_call():
    """Anything more re-creates the race this file exists to close."""
    tree = ast.parse(_source())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_execute_with_telemetry"
    ]
    assert len(calls) == 1, (
        f"{len(calls)} inference call sites; a second one sheds independently "
        "of the first and discards whatever the first paid for"
    )


def test_every_persona_is_still_played():
    """Cutting the cost must not quietly cut the adversaries."""
    source = _source()
    for persona in ("State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender"):
        assert persona in source


def test_the_outcome_carries_one_move_per_persona():
    from services.agents.adversarial_wargamer import (
        SimulationMove,
        WargameSimulationOutput,
    )

    board = WargameSimulationOutput(
        primary_vulnerability_isolated="v",
        cascade_failure_probability=50,
        predicted_next_target_entity_id="NVDA",
        remediation_recommendation="r",
        moves=[
        SimulationMove(
            persona_name=name,
            proposed_counter_action="act",
            target_entity_id="NVDA",
            strategic_rationale="because",
        )
            for name in ("State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender")
        ],
    )
    assert len(board.moves) == 3
    assert {m.persona_name for m in board.moves} == {
        "State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender"
    }


def test_an_empty_board_is_refused_by_the_schema():
    """The earlier decision was right for a board-only call and wrong for this one.

    When the board was its own inference, an empty `moves` meant the model had
    declined, and forcing a minimum would have made it invent one. Combined,
    the model is answering both halves in a single response: it filled the
    required scalars and returned `moves: []` twice running, and because Ollama
    builds its grammar from the schema, a field with a default is one it may
    legally omit. Raising num_predict to 1024 changed nothing -- the budget was
    never the constraint, the schema was.

    An empty array now means half the task was skipped, not that anything was
    declined. Code minting a placeholder move stays forbidden; see
    test_no_placeholder_move_is_fabricated.
    """
    import pydantic
    from services.agents.adversarial_wargamer import WargameSimulationOutput

    with pytest.raises(pydantic.ValidationError):
        WargameSimulationOutput(
            primary_vulnerability_isolated="v",
            cascade_failure_probability=0,
            predicted_next_target_entity_id="NONE",
            remediation_recommendation="r",
        )


# -- a declined wargame must stay declined -------------------------------------

def test_no_placeholder_move_is_fabricated():
    """The old fallback minted a "PASS" move on failure.

    Reachable or not, it was the wrong answer: three placeholder moves still
    satisfy `if not moves`, still reach arbitration, still get published, and
    still record a prediction -- an invented opinion indistinguishable
    downstream from a reasoned one. A skipped wargame is visible in the logs; a
    fabricated one is not.
    """
    source = _source()
    assert "Fallback default move" not in source
    assert 'proposed_counter_action="PASS"' not in source


def test_a_model_failure_returns_none_rather_than_a_placeholder():
    """A model error is not a wargame. handle() reads it as "skip"."""
    tree = ast.parse(_source())
    board = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "handle"
    )
    handlers = [n for n in ast.walk(board) if isinstance(n, ast.ExceptHandler)]
    generic = [
        h for h in handlers
        if isinstance(h.type, ast.Name) and h.type.id == "Exception"
    ]
    assert generic, "the board no longer handles model failure"
    for handler in generic:
        returns = [n for n in ast.walk(handler) if isinstance(n, ast.Return)]
        assert returns, "the generic handler falls through instead of returning"
        for node in returns:
            assert isinstance(node.value, ast.Constant) and node.value.value is None, (
                "a value is returned in place of a real board"
            )


def test_a_shed_propagates_out_of_the_wargame():
    """The dispatch loop distinguishes a shed from an error; absorbing one here
    would report declined work as completed-with-nothing."""
    tree = ast.parse(_source())
    board = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "handle"
    )
    shed = [
        h for h in ast.walk(board)
        if isinstance(h, ast.ExceptHandler)
        and isinstance(h.type, ast.Name) and h.type.id == "InferenceShed"
    ]
    assert shed, "InferenceShed is not handled explicitly"
    assert any(isinstance(n, ast.Raise) for n in ast.walk(shed[0])), (
        "a shed is caught and not re-raised"
    )


# -- the gates in front of it stay ---------------------------------------------

def test_capacity_is_checked_before_context_is_built():
    """The Neo4j subgraph query must not be paid for work that cannot run."""
    source = _source()
    gate = source.index("self.capacity_or_defer(message)")
    context = source.index("_fetch_subgraph_context(entity_ids)")
    assert gate < context, "context is built before capacity is checked"


@pytest.mark.parametrize(
    "message,worth",
    [
        ({"alert_tier": "CRITICAL"}, True),
        ({"alert_tier": "WATCH"}, False),
        ({"confidence_score": 0.9}, True),
        ({"confidence_score": 0.1}, False),
        ({"type": "vessel_position"}, False),
    ],
)
def test_the_significance_gate_still_holds(message, worth):
    from services.agents.adversarial_wargamer import _is_worth_simulating

    assert _is_worth_simulating(message) is worth


# -- the target has to name something ------------------------------------------

def test_the_predicted_target_is_validated_against_the_entities_given():
    """First combined run, live 2026-09-20 13:03:51:

        predicted_next_target_entity_id:
            '50 events across 2 domains (aviation, maritime)'

    The cluster's own summary, echoed into an identity field. The prompt already
    forbids inventing a target; the model did it anyway, which is the case a
    prompt instruction cannot cover. Recorded, that puts a sentence where every
    consumer reads a ticker -- the defect this audit logged as a bulletin whose
    ticker was "CPB ($21.53)".
    """
    source = _source()
    assert "target_is_named" in source, "nothing checks the target names a real entity"
    assert "known = {str(e).strip().upper() for e in entity_ids}" in source
    # The prediction and the bulletin must both be gated on it.
    assert "if target_is_named and" in source, "a prediction can still be recorded on prose"
    assert "ticker=target if target_is_named else None" in source, (
        "a bulletin can still carry a prose ticker, and consensus fuses by ticker"
    )


def test_the_combined_call_is_given_room_for_both_halves():
    """`moves: []` on the first run: the grammar's required scalars were filled
    and the array had no budget left."""
    source = _source()
    assert "num_predict=1024" in source, (
        "the combined schema is still sized for an arbitration alone"
    )


def test_a_model_authored_probability_cannot_become_certainty():
    """The first prediction this agent ever recorded came back at conviction 1.0.

    `cascade_failure_probability` is an integer the model writes; it said 100.
    Conviction reaches the consensus engine's Subjective Logic fusion, where an
    opinion at exactly 1.0 drives uncertainty to zero -- so one model saying
    "100%" outweighs every measured opinion beside it. The platform already
    refuses certainty elsewhere (FALLBACK_MAX_SCORE, RULE_CONF_CEILING); neither
    reached an agent bulletin.
    """
    from services.agents.adversarial_wargamer import _MAX_MODEL_CONVICTION

    assert 0.0 < _MAX_MODEL_CONVICTION < 1.0
    source = _source()
    assert "min(1.0, synthesis.cascade_failure_probability" not in source, (
        "a model-authored 100 still becomes certainty"
    )
    assert source.count("min(_MAX_MODEL_CONVICTION, synthesis.cascade_failure_probability") == 2, (
        "the prediction and the bulletin must both be bounded"
    )
