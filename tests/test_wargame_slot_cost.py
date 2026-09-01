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

def test_the_personas_are_one_call_not_three():
    """Four races for an all-or-nothing four-slot operation."""
    tree = ast.parse(_source())
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)}

    assert "_execute_persona_board" in names
    assert "_execute_persona_turn" not in names, "the per-persona call still exists"


def test_the_personas_are_not_gathered_concurrently():
    source = _source()
    assert "asyncio.gather" not in source, "concurrent persona claims are back"


def test_the_wargame_makes_at_most_two_inference_calls():
    """One persona board, one arbitration. Anything more re-creates the race."""
    tree = ast.parse(_source())
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_execute_with_telemetry"
    ]
    assert len(calls) == 2, f"{len(calls)} inference call sites; the budget affords two"


def test_every_persona_is_still_played():
    """Cutting the cost must not quietly cut the adversaries."""
    source = _source()
    for persona in ("State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender"):
        assert persona in source


def test_the_board_carries_one_move_per_persona():
    from services.agents.adversarial_wargamer import SimulationBoard, SimulationMove

    board = SimulationBoard(moves=[
        SimulationMove(
            persona_name=name,
            proposed_counter_action="act",
            target_entity_id="NVDA",
            strategic_rationale="because",
        )
        for name in ("State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender")
    ])
    assert len(board.moves) == 3
    assert {m.persona_name for m in board.moves} == {
        "State_Saboteur", "Financial_Short_Seller", "Asymmetric_Defender"
    }


def test_an_empty_board_is_representable():
    """The schema must not force the model to invent moves it does not have."""
    from services.agents.adversarial_wargamer import SimulationBoard

    assert SimulationBoard().moves == []


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


def test_the_board_returns_none_rather_than_a_placeholder_on_failure():
    """A model error is not a wargame. The caller reads None as "skip"."""
    tree = ast.parse(_source())
    board = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_execute_persona_board"
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


def test_a_shed_propagates_out_of_the_board():
    """The dispatch loop distinguishes a shed from an error; absorbing one here
    would report declined work as completed-with-nothing."""
    tree = ast.parse(_source())
    board = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "_execute_persona_board"
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
    gate = source.index("self._inference_budget.is_available()")
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
