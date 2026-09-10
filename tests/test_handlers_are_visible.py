"""A handler that fires and produces nothing is how this codebase fails.

Counted at the start of this pass across `services/` and `shared/`: 957
exception handlers, 122 swallowing silently with a bare `pass` and 218 logging
only at DEBUG, against a deployment that runs at INFO and emits no DEBUG lines
at all. Roughly 340 -- 36% -- were guaranteed to produce nothing observable.

Every significant defect this audit found failed into one of them: the backtest
cache write that could not create a task on a worker thread, the graph backfill
that never ran, the 13F deduplication, the counterparty degree that stayed zero
because three Redis commands were missing from a test double, the movement score
that fell back to its base on every filing. In each case the code reported
success and did nothing.

Raising them all to WARNING is not the fix -- most are genuinely recoverable and
the log would become unreadable, which is its own way of hiding things. A count
is what was missing, and `shared/utils/quiet_failures.swallowed` supplies it:
always counted, DEBUG every time, escalated to WARNING on the first occurrence,
at powers of ten, and at most once per interval after that.

These tests are the ratchet. They do not demand zero; they demand that the
number does not grow back.
"""
import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

# Where `pass` is the correct and complete answer rather than a swallowed
# failure. Counting these would fire on every clean shutdown and teach the
# reader to skip the line.
ALLOWED_SILENT = {
    "asyncio.CancelledError",
    "(asyncio.CancelledError, Exception)",
    "asyncio.TimeoutError",
    "_QuoteCacheMiss",
    "(asyncio.QueueEmpty, asyncio.QueueFull)",
    "ImportError",
    "ModuleNotFoundError",
    "(ImportError, ModuleNotFoundError)",
}

# The measured remainder, so a regression is a failing test rather than a slow
# drift. Both were far higher when this was first counted.
MAX_UNEXPLAINED_SILENT = 0
MAX_DEBUG_ONLY = 218


def _handlers():
    for base in ("services", "shared"):
        for f in (ROOT / base).rglob("*.py"):
            if "__pycache__" in str(f):
                continue
            try:
                tree = ast.parse(f.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler):
                    yield f.relative_to(ROOT), node


def _called_names(node):
    return {
        getattr(c.func, "id", None) or getattr(c.func, "attr", None)
        for c in ast.walk(node)
        if isinstance(c, ast.Call)
    }


def test_no_handler_swallows_silently_without_saying_why():
    offenders = []
    for path, node in _handlers():
        if not (len(node.body) == 1 and isinstance(node.body[0], ast.Pass)):
            continue
        kind = ast.unparse(node.type) if node.type else "<bare>"
        if kind in ALLOWED_SILENT:
            continue
        offenders.append(f"{path}:{node.lineno}: except {kind}: pass")

    assert len(offenders) <= MAX_UNEXPLAINED_SILENT, (
        "handlers that fire and produce nothing observable:\n  "
        + "\n  ".join(sorted(offenders))
        + "\n\nUse shared.utils.quiet_failures.swallowed(site, exc, logger) so the "
          "failure is counted, or add the exception type to ALLOWED_SILENT with "
          "a reason if `pass` really is the complete answer."
    )


def test_a_bare_except_is_never_silent():
    """`except:` catches BaseException, including the shed this codebase raises."""
    for path, node in _handlers():
        if node.type is None:
            assert not (len(node.body) == 1 and isinstance(node.body[0], ast.Pass)), (
                f"{path}:{node.lineno}: a bare `except: pass` catches KeyboardInterrupt "
                "and InferenceShed and reports nothing."
            )


def test_debug_only_handlers_do_not_grow():
    """The deployment runs at INFO, so a DEBUG-only handler is invisible.

    Not zero, deliberately. Many of these log a line and take a working
    fallback, and converting 218 of them wholesale is a larger and riskier
    change than replacing a lone `pass`. What must not happen is the number
    growing again.
    """
    count = 0
    for _path, node in _handlers():
        names = _called_names(node)
        if "swallowed" in names or "dropped" in names:
            continue
        if names and names <= {"debug", "getLogger"} and "debug" in names:
            count += 1
    assert count <= MAX_DEBUG_ONLY, (
        f"{count} handlers log only at DEBUG, up from {MAX_DEBUG_ONLY}. "
        "The deployment emits no DEBUG lines, so these fire invisibly."
    )


def test_the_counter_is_actually_wired_to_something():
    """A count nobody reads is the defect this module was built to end.

    `snapshot()` existed from the day quiet_failures.py was written and had no
    callers, so the counters counted into a dictionary nothing opened.
    """
    from shared.utils.quiet_failures import heartbeat_line

    agents = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")
    enrich = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert "quiet_heartbeat_line()" in agents
    assert "quiet_heartbeat_line()" in enrich
    assert callable(heartbeat_line)


def test_conversions_actually_landed():
    counted = sum(
        1 for _p, node in _handlers()
        if {"swallowed", "dropped"} & _called_names(node)
    )
    assert counted >= 110, f"only {counted} handlers report their suppression"
