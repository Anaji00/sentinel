"""A failed rates evaluation must not consume the hour it failed in.

The dedup key was marked for a full hour immediately after the check and before
any work, so a crash still counted as a reading. Observed: the first evaluation
ever to get past the yield guard died two seconds later on an unguarded format
string, and the engine then declined to try again for an hour -- while the very
inputs it had been missing arrived four minutes after the crash.

The claim is still taken up front, so a burst of macro ticks does not all
evaluate the same hour concurrently. It is just short, and only a run that
produced a brief extends it to the hour.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.macro_intelligence_engine import (  # noqa: E402
    RATES_RETRY_COOLDOWN_SEC,
)

SOURCE = (ROOT / "services/agents/macro_intelligence_engine.py").read_text(encoding="utf-8")
FUNC = SOURCE[SOURCE.index("async def _process_rates_and_macro_regime"):]
FUNC = FUNC[:FUNC.index("# ── SUB-ENGINE 2")]


def _marks():
    return re.findall(r"mark_processed\(dedup_key,\s*window_seconds=([A-Za-z_0-9]+)\)", FUNC)


def test_the_hour_is_claimed_briefly_before_the_work():
    """Concurrency protection, which is what the dedup is actually for."""
    marks = _marks()
    assert marks, "the rates path no longer marks its dedup key at all"
    assert marks[0] == "RATES_RETRY_COOLDOWN_SEC", marks


def test_the_full_hour_is_only_taken_after_a_brief_exists():
    marks = _marks()
    assert len(marks) == 2, f"expected a claim and a confirmation, got {marks}"
    # The hour is now named rather than inlined: five dedup windows across the
    # agents were bare literals with nothing stating why one subject deserves
    # ten minutes and another an hour.
    assert marks[1] == "DEDUP_WINDOW_SLOW_SEC", marks


def test_the_confirmation_sits_on_the_success_path():
    """Not in the except block, and not before the inference."""
    # Anchored on the mark, not on the literal: is_recently_processed takes
    # window_seconds=3600 too, and matching that made this test read the
    # dedup *check* as the confirmation.
    confirm = FUNC.index("mark_processed(dedup_key, window_seconds=DEDUP_WINDOW_SLOW_SEC)")
    inference = FUNC.index("_execute_with_telemetry")
    failure = FUNC.index("except (SchemaViolationError, InferenceError)")
    assert inference < confirm < failure


def test_the_cooldown_is_shorter_than_the_hour_it_replaces():
    assert 0 < RATES_RETRY_COOLDOWN_SEC < 3600


def test_the_cooldown_is_long_enough_to_stop_a_tick_burst():
    """Macro ticks arrive every 30 seconds; a cooldown below that would let
    each one re-enter the expensive path."""
    assert RATES_RETRY_COOLDOWN_SEC >= 60
