"""
tests/test_dlq_payload_storage.py

Every dead letter was stored in a form the replay path was right to refuse.

The replay was investigated once and left open: a batch of nine rows came back
as six new dead letters, the same event_id in each, wrapped one encoding layer
deeper. What was checked and cleared at the time was correct --
`decode_dlq_payload` returns a proper event for a stored row, the replay refuses
anything that is not a dict or list, and the producer signature is what the
caller assumes. The mechanism was upstream of all three.

    INSERT INTO failed_events (..., raw_payload, ...)
    VALUES ($1, $2, $3, $4, $5)
    ... original_topic, error_msg, json.dumps(raw_data), ...

The pool registers a jsonb codec whose encoder is already `json.dumps`, so the
pre-serialised string is encoded a second time and lands as a jsonb *string*.
Measured: all 20,772 rows in failed_events report `jsonb_typeof = 'string'`.

The guard added later stopped the bad republishes -- 0 unresolved rows have a
replay_count above zero -- but nothing made the payloads replayable. This is the
same defect this codebase already recorded for `scenarios.hypotheses`: one
codec, two callers that did not know about it.
"""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKER = ROOT / "services" / "dlq-worker" / "main.py"
DB = ROOT / "shared" / "db" / "__init__.py"


def test_the_pool_encodes_jsonb_itself():
    """The premise. If this changes, the call site below must change with it."""
    src = DB.read_text(encoding="utf-8")
    codec = src.index("set_type_codec")
    body = src[codec : codec + 300]
    assert "'jsonb'" in body
    assert "encoder=json.dumps" in body


def test_the_dlq_insert_does_not_pre_serialise():
    """A document, not a string containing a document."""
    src = WORKER.read_text(encoding="utf-8")
    insert = src.index("INSERT INTO failed_events")
    # The parameter tuple follows the closing triple-quote of the statement.
    tail = src[insert : insert + 2200]
    call = tail[tail.index('"""', tail.index("VALUES")) :]
    assert "json.dumps(raw_data)" not in call, (
        "the jsonb codec encodes this already; pre-serialising stores a string"
    )
    assert "raw_data," in call


def test_a_round_trip_through_the_codec_survives():
    """What the pool does to the value, done here without a database."""
    import json

    from shared.utils.dlq_payload import decode_dlq_payload

    event = {"event_id": "d0463069-0000-0000-0000-000000000000", "type": "headline"}

    # Correct: the codec encodes the document once.
    stored_ok = json.loads(json.dumps(event))
    assert decode_dlq_payload(stored_ok) == event

    # The defect: pre-serialised, then encoded again by the codec.
    stored_bad = json.loads(json.dumps(json.dumps(event)))
    assert isinstance(stored_bad, str), "this is what the table actually held"
    # decode_dlq_payload rescues this particular shape, which is why the
    # mechanism stayed hidden: the decoder looked correct in isolation.
    assert decode_dlq_payload(stored_bad) == event


def test_the_replay_still_refuses_a_bare_string():
    """The guard stays. It was never the bug, and it is the last line of defence."""
    src = WORKER.read_text(encoding="utf-8")
    assert "if not isinstance(payload, (dict, list)):" in src
    assert "republishing it would put a bare string on the topic" in src
