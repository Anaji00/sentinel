"""
tests/test_recent_window_floor.py

One key held 86% of Redis and could never be evicted.

    events:recent_window   zset   321,535 members   196 MB   ttl=-1

Under volatile-lru a key with no TTL is unreclaimable, so this one forced
everything else out and writes began failing across the platform:

    command not allowed when used memory > 'maxmemory'

which took the supervisor's dispatch down with it, refused the aircraft index
the dark-flight detector scans, and emptied the macro cache. Several of those
were investigated separately as defects in the components that read them.

The window itself is legitimate -- rules genuinely look back forty-eight hours,
and shortening it would break correlation. What was not legitimate was its
contents. The lowest `min_anomaly` any rule requests is 0.20, so every event
stored below that is unreachable by every rule in the system, and it was being
kept for two days regardless. flight_position averages 0.107 and vessel_static
is exactly 0.000; between them they are thousands of members an hour that no
query could ever return.

The floor's relationship to the rules is the thing worth pinning. A rule written
below it would silently match nothing, which is the failure this file exists to
turn into a test failure instead.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.correlation.event_store import RECENT_WINDOW_MIN_ANOMALY  # noqa: E402

SOURCE = (ROOT / "services" / "correlation" / "event_store.py").read_text(encoding="utf-8")
MAIN = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")


def _rule_thresholds():
    """Every min_anomaly the static rules declare."""
    return [float(m) for m in re.findall(r'min_anomaly"?:\s*([0-9.]+)', MAIN)]


def test_the_floor_sits_below_every_rule_threshold():
    """The invariant. If a rule is ever written below the floor it will match
    nothing, silently, because its inputs were never stored."""
    thresholds = _rule_thresholds()
    assert thresholds, "no rule thresholds found to check against"
    assert RECENT_WINDOW_MIN_ANOMALY < min(thresholds), (
        f"floor {RECENT_WINDOW_MIN_ANOMALY} is not below the lowest rule "
        f"threshold {min(thresholds)}; that rule can never match"
    )


def test_the_floor_leaves_margin():
    """Flush against the lowest rule leaves no room for a slightly more
    permissive one to be added later."""
    assert min(_rule_thresholds()) - RECENT_WINDOW_MIN_ANOMALY >= 0.02


def test_routine_telemetry_is_excluded():
    """The bulk of the window: position pings no rule can retrieve."""
    for observed in (0.0, 0.107, 0.113):     # vessel_static, flight_position
        assert observed < RECENT_WINDOW_MIN_ANOMALY


def test_real_signal_is_retained():
    """The floor must not eat anything a rule would have matched."""
    for observed in (0.205, 0.667, 0.82, 1.0):  # vessels, equity blocks, anomalies
        assert observed >= RECENT_WINDOW_MIN_ANOMALY


def test_the_guard_runs_before_the_write():
    """Filtering after the zadd would cost the memory it exists to save."""
    assert SOURCE.index("RECENT_WINDOW_MIN_ANOMALY:") < SOURCE.index("zadd(self.cache_key") \
        or SOURCE.index("< RECENT_WINDOW_MIN_ANOMALY") < SOURCE.index("zadd(self.cache_key")


def test_a_missing_score_does_not_crash_the_store():
    """anomaly_score arrives None on some paths, and an exception here would
    lose the event entirely rather than merely skip it."""
    assert "(event.anomaly_score or 0.0)" in SOURCE


def test_long_text_is_truncated_at_write():
    """Both fields are read back so neither can be dropped, but the consumer
    already cuts them to 200 and storing them in full multiplied a structure
    with three hundred thousand members."""
    assert '(event.headline or "")[:160]' in SOURCE
    assert '[:200] or None' in SOURCE


def test_the_window_itself_is_unchanged():
    """Rules genuinely look back forty-eight hours. This changes what goes in,
    not how long it stays."""
    assert "48 * 3600" in SOURCE
