"""
tests/test_log_formatter_keeps_tracebacks.py

`exc_info=True` must actually emit the traceback.

SentinelConsoleFormatter overrides format() and builds its line by hand. The
base logging.Formatter appends formatException(record.exc_info) after the
message; this override returned before doing so, so all 54 `exc_info=True` call
sites in the tree logged their message and discarded the stack.

The cost is not theoretical, and it is worse than a missing traceback. Several
exception types carry an empty str() -- asyncpg's QueryCanceledError is one --
so a handler written as

    logger.error("SEC Form 4 error: %s", e, exc_info=True)

printed `SEC Form 4 error: ` with nothing after the colon and nothing below it.
Measured on the live deployment: 51 occurrences of "Signal match query failed
for 'X': " and a once-a-minute "SEC Form 4 error: ", none of which identified
the exception, because the one mechanism that would have named it was being
dropped at the formatter.

A log line that reports a failure without naming it is the same defect as a
handler that swallows one, and this repository already has a test for the
second shape.
"""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.logging import SentinelConsoleFormatter  # noqa: E402


class _Silent(Exception):
    """An exception whose str() is empty, like QueryCanceledError."""

    def __str__(self) -> str:
        return ""


def _record_with_traceback(**kwargs) -> logging.LogRecord:
    try:
        raise _Silent()
    except _Silent:
        return logging.LogRecord(
            name="probe",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="SEC Form 4 error: %s",
            args=("",),
            exc_info=sys.exc_info(),
            **kwargs,
        )


def test_the_traceback_is_emitted():
    formatted = SentinelConsoleFormatter().format(_record_with_traceback())
    assert "Traceback (most recent call last)" in formatted


def test_the_exception_type_survives_an_empty_str():
    """The whole point: naming the failure when the message cannot."""
    formatted = SentinelConsoleFormatter().format(_record_with_traceback())
    assert "_Silent" in formatted, (
        "an exception with an empty str() must still be identifiable from the "
        "log line, or the handler reports that something failed and nothing else"
    )


def test_the_traceback_is_rendered_once_and_cached():
    """Two handlers on one record must not re-render the stack."""
    record = _record_with_traceback()
    formatter = SentinelConsoleFormatter()
    first = formatter.format(record)
    assert record.exc_text, "the rendered traceback should be cached on the record"
    second = formatter.format(record)
    assert first.count("Traceback (most recent call last)") == 1
    assert second.count("Traceback (most recent call last)") == 1


def test_a_record_without_exc_info_is_unchanged():
    """The ordinary path must not grow a blank line."""
    record = logging.LogRecord(
        name="probe", level=logging.INFO, pathname=__file__, lineno=1,
        msg="all quiet", args=(), exc_info=None,
    )
    formatted = SentinelConsoleFormatter().format(record)
    assert "Traceback" not in formatted
    assert not formatted.endswith("\n")
