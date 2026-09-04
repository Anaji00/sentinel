"""jsonb parameters must be passed as objects, never as pre-serialised strings.

The connection pool registers a jsonb codec whose encoder is json.dumps
(shared/db/__init__.py). Anything already turned into a string by the caller is
therefore encoded twice and stored as a jsonb *string* rather than as the array
or object it represents.

Measured on the live database before the fix: `hypotheses` came back as jsonb
type "string" on 324 of 672 scenarios, and every populated `confidence_history`
was an array whose single element was a string. `jsonb_array_elements` fails
outright on the first and `entry->>'confidence'` returns null on the second, so
the scenario tracker's Bayesian confidence updates were being written where
nothing -- including the tracker itself -- could read them back.

The explicit `::jsonb` cast in the SQL is what made this survive an earlier fix
to the insert path: the cast looks like it settles the encoding question, and it
does not. It casts what the codec produced, which is already a JSON string.
"""

import asyncio
import inspect
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class _CapturingDb:
    """Records the parameters an execute() would send to asyncpg."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def execute(self, sql: str, *params):
        self.calls.append((sql, params))
        return None

    async def query(self, sql: str, *params):
        return []


def _encode_like_asyncpg(value):
    """What the registered codec does to a parameter bound to a jsonb column."""
    return json.dumps(value)


def test_the_codec_is_still_json_dumps():
    """The premise. If the pool stops pre-encoding, this file is obsolete."""
    source = (ROOT / "shared/db/__init__.py").read_text(encoding="utf-8")
    assert "encoder=json.dumps" in source, (
        "the jsonb codec no longer encodes with json.dumps; re-check whether "
        "passing objects is still correct"
    )


def test_passing_a_string_double_encodes_and_an_object_does_not():
    """The mechanism, demonstrated rather than asserted from memory."""
    entries = [{"ts": "2026-09-02T00:00:00+00:00", "confidence": 50, "notes": "x"}]

    as_object = json.loads(_encode_like_asyncpg(entries))
    assert isinstance(as_object, list), "an object round-trips to an array"
    assert as_object[0]["confidence"] == 50

    as_string = json.loads(_encode_like_asyncpg(json.dumps(entries)))
    assert isinstance(as_string, str), (
        "a pre-serialised string round-trips to a jsonb string, which is the bug"
    )


def test_the_tracker_sends_objects_for_both_jsonb_parameters():
    """The regression itself, against the real _update_scenario."""
    from services.reasoning.scenario_tracker import ScenarioTracker

    tracker = ScenarioTracker.__new__(ScenarioTracker)
    tracker._db = _CapturingDb()

    hypotheses = [{"label": "H1", "probability": 45}]
    asyncio.run(
        tracker._update_scenario(
            scenario_id="00000000-0000-0000-0000-000000000000",
            new_confidence=61,
            new_status=None,
            notes="watch hit",
            hypotheses=hypotheses,
        )
    )

    assert tracker._db.calls, "_update_scenario issued no write at all"
    _sql, params = tracker._db.calls[-1]

    offenders = [
        p for p in params
        if isinstance(p, str) and p.lstrip()[:1] in "[{"
    ]
    assert not offenders, (
        "a pre-serialised JSON string was passed as a parameter and will be "
        f"double-encoded: {offenders}"
    )

    # And the payloads are actually present, so the test cannot pass by the
    # tracker having quietly stopped writing them.
    assert any(isinstance(p, list) and p and isinstance(p[0], dict) for p in params), (
        "no structured payload was sent; the write may have lost its content"
    )


def test_no_jsonb_parameter_in_the_tracker_is_pre_serialised():
    """Both call sites, and any added later, read from the source.

    The behavioural test above exercises one branch of four; _update_scenario
    picks its SQL from new_status and whether hypotheses were supplied.
    """
    source = (ROOT / "services/reasoning/scenario_tracker.py").read_text(encoding="utf-8")
    body = inspect.cleandoc(source[source.index("async def _update_scenario"):])
    executable = [
        line for line in body.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    joined = "\n".join(executable)
    assert "json.dumps" not in joined, (
        "_update_scenario serialises a jsonb parameter itself; the pool already does"
    )
