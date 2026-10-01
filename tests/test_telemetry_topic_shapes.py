"""Topics.TELEMETRY carries two shapes and only one of them is telemetry.

`_execute_with_telemetry` in the agent base class sends an inference record:
status, task_id, prompt lengths, latency, output payload. `rule_agent` sends
domain events on the same topic:

    {"agent": "rule_synthesizer", "event": "rule_created",
     "rule_id": ..., "rule_name": ..., "timestamp": ...}

The worker treated every non-prediction message as an inference record, so a
rule creation became this, measured on the live table 2026-09-20:

    agent_name       | task_id | status  | sys_len | latency_ms | output_payload
    rule_synthesizer | unknown | unknown |         |            |

Seven rows recording nothing but "rule_synthesizer, at some time". The rule_id
and rule_name were discarded because the INSERT has no column for them -- and
those rows are the record of the rule synthesiser *succeeding*, which is the
one output that agent exists to produce.

Handled at this boundary rather than in the producer: the flattening belongs to
the worker, and any producer on this topic can hit it.
"""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location(
    "telemetry_worker_main", ROOT / "services/telemetry-worker/main.py"
)
tw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tw)


def _event(**over):
    d = {
        "agent": "rule_synthesizer",
        "event": "rule_created",
        "rule_id": "rule_chokepoint_spike",
        "rule_name": "Chokepoint Spike",
        "timestamp": "2026-09-20T23:00:00Z",
    }
    d.update(over)
    return d


def _inference(**over):
    d = {
        "agent": "quant_trading_engine",
        "status": "COMPLETE",
        "task_id": "evt-1",
        "system_prompt_length": 100,
        "user_prompt_length": 200,
        "latency_ms": 1234.0,
        "output_payload": {"k": 1},
    }
    d.update(over)
    return d


# ── the two shapes are told apart ────────────────────────────────────────────


def test_an_event_is_not_an_inference_record():
    assert tw._is_domain_event(_event())
    assert not tw._is_domain_event(_inference())


def test_an_event_keeps_its_name_in_the_status():
    assert tw._event_status(_event()) == "EVENT:rule_created"
    assert tw._event_status(_event(event="rule_deprecated")) == "EVENT:rule_deprecated"


def test_an_event_is_joinable_back_to_its_subject():
    """`task_id=unknown` on every row made them impossible to correlate."""
    assert tw._event_task_id(_event()) == "rule_chokepoint_spike"


def test_an_event_falls_back_through_the_ids_it_might_carry():
    for key in ("event_id", "correlation_id", "trace_id"):
        d = _event(rule_id=None)
        d[key] = "xyz"
        assert tw._event_task_id(d) == "xyz", key


def test_the_whole_event_survives_rather_than_being_discarded():
    payload = tw._telemetry_payload(_event())
    assert payload["rule_name"] == "Chokepoint Spike"
    assert payload["rule_id"] == "rule_chokepoint_spike"


# ── inference records are untouched ──────────────────────────────────────────


def test_an_inference_record_keeps_its_own_status_and_task():
    d = _inference()
    assert (d.get("status") or tw._event_status(d)) == "COMPLETE"
    assert (d.get("task_id") or tw._event_task_id(d)) == "evt-1"


def test_an_inference_payload_is_the_output_not_the_envelope():
    assert tw._telemetry_payload(_inference()) == {"k": 1}


def test_an_inference_record_with_an_explicit_null_payload_stays_null():
    d = _inference(output_payload=None)
    assert tw._telemetry_payload(d) is None


# ── the payload must be an object, not JSON text ─────────────────────────────


def test_the_payload_is_handed_over_as_an_object_not_as_json_text():
    """`json.dumps` here stored a jsonb *string*; the codec encodes already.

    Measured on the live table 2026-09-21:

        rows_with_payload  4483
        can_read_a_key        0     output_payload->>'agent' on every row
        proper_objects        0     jsonb_typeof(output_payload) = 'object'

    Every model output this platform has stored was unqueryable. The same
    defect and the same remedy are documented twenty lines above for
    agent_predictions' two jsonb columns, where the fix was applied -- those
    now store proper arrays.
    """
    assert isinstance(tw._telemetry_payload(_inference()), dict)
    assert isinstance(tw._telemetry_payload(_event()), dict)


def test_the_insert_does_not_re_encode_what_the_codec_encodes():
    src = (ROOT / "services/telemetry-worker/main.py").read_text(encoding="utf-8")
    import ast

    # From the syntax tree: the docstring explaining this fix names json.dumps,
    # and a text search cannot tell an explanation from a call.
    tree = ast.parse(src)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_telemetry_payload"
    )
    calls = {
        ast.unparse(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)
    }
    assert "json.dumps" not in calls, (
        "the pool's jsonb codec already encodes; dumping here stores a string"
    )


def test_a_message_that_is_neither_still_writes_a_row():
    """Unknown is still the honest answer for a shape nobody recognises."""
    d = {"agent": "somebody"}
    assert tw._event_status(d) == "unknown"
    assert tw._event_task_id(d) == "unknown"
    assert tw._telemetry_payload(d) is None


# ── the producers that made this visible ─────────────────────────────────────


def test_rule_agent_still_publishes_the_events():
    """The fix preserves them; it does not silence the producer."""
    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    assert '"event": "rule_created"' in src
    assert '"event": "rule_deprecated"' in src


def test_the_insert_no_longer_flattens_a_missing_status_to_unknown():
    src = (ROOT / "services/telemetry-worker/main.py").read_text(encoding="utf-8")
    assert 'data.get("status", "unknown")' not in src
    assert 'data.get("status") or _event_status(data)' in src
