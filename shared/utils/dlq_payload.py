"""
shared/utils/dlq_payload.py

One way to put a failed Kafka message into the dead-letter queue, and one way
to get it back out.

Every producer wrote `{"raw": str(msg.value)}`, and `msg.value` is bytes. In
Python `str(b'{"a":1}')` is not the JSON -- it is the *repr* of the bytes
object, the six characters `b'{"a"` and so on, quotes and prefix included. The
dlq-worker then tried `json.loads` on that, which cannot succeed, and stored the
repr as a JSON string.

The consequence was total and silent. Measured on the live table: all 8,914
outstanding rows across all six topics hold `jsonb_typeof = 'string'`, and the
string is `"b'{\\"event_id\\": ...}'"`. Replaying one publishes that string onto
the topic, where the consumer does `payload.get(...)` on a `str` and fails --
so the replay path was complete, correct, operator-gated, hash-chained, and
incapable of restoring a single event. Worse, the new failure is stored the same
way, so each attempt adds an encoding layer: a row replayed twice reads
`"b'\\"b\\\\'{...".

`encode_dlq_payload` is what every producer should call. `decode_dlq_payload`
reads both the correct form and the legacy repr, so the history already in the
table becomes replayable rather than being written off.
"""

from __future__ import annotations

import ast
import json
import logging
from typing import Any

logger = logging.getLogger("shared.dlq_payload")


def encode_dlq_payload(value: Any) -> Any:
    """The message body, in a form that can be published again.

    Bytes are decoded and parsed. A dict comes back unchanged. Anything that
    cannot be parsed is returned as a plain string rather than a repr, so the
    failure is legible instead of being wrapped in Python syntax.
    """
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return {"_undecodable": repr(bytes(value)[:512])}
    elif isinstance(value, str):
        text = value
    else:
        return value

    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text


def decode_dlq_payload(stored: Any) -> Any:
    """The body a replay should publish, from whatever the table holds.

    Three forms, because three were written:

      dict   -- what `encode_dlq_payload` produces. Returned as-is.
      str    -- a JSON document, or the legacy `b'...'` repr, possibly nested
                several layers deep from repeated replays.
      other  -- returned unchanged; the caller decides.

    The legacy form is unwrapped with `ast.literal_eval`, which evaluates a
    Python literal and nothing else -- it cannot call a function or import a
    module, which is what makes it safe to point at data from a queue.
    """
    seen = 0
    value = stored
    # Bounded: a payload replayed repeatedly nests, and this must terminate
    # whatever the table holds.
    while seen < 8:
        seen += 1
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, (bytes, bytearray)):
            try:
                value = bytes(value).decode("utf-8")
                continue
            except UnicodeDecodeError:
                return stored
        if not isinstance(value, str):
            return value

        text = value.strip()

        # A JSON document.
        if text[:1] in "{[" or text[:1] == '"':
            try:
                parsed = json.loads(text)
            except ValueError:
                return value
            if parsed is value:
                return value
            value = parsed
            continue

        # The legacy repr of a bytes object: b'...' or b"...".
        if text[:2] in ("b'", 'b"'):
            try:
                unwrapped = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                return value
            value = unwrapped
            continue

        return value

    logger.warning("DLQ payload is nested more than 8 layers deep; returning as-is.")
    return value
