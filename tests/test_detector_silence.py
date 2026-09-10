"""A detector that stops firing must not be invisible to the detector monitor.

The score-diversity loop reports detectors whose output has stopped varying. Its
query is `GROUP BY type ... HAVING COUNT(*) >= N` over a one-hour window, so a
detector emitting *nothing* produces no row -- it is not among the "N detectors
checked, none flat", it is simply absent. The component built to notice a
detector going wrong could not notice the one failure mode that produces no
output at all, which is also the cheapest failure mode there is.

Measured when this was written: `prediction_market_trade` had run at 1.45/hour
for a week and been silent nine hours -- thirteen times its own mean
interarrival -- and every sweep reported all clear. The cause turned out to be
real: the collector was polling a watch list that was 82% off-domain.

Silence is judged against each detector's own cadence, for the reason source
freshness already judges feeds that way: an hourly poller quiet for 55 minutes
is normal and a tick feed quiet for the same 55 minutes is dead.
"""
import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")


def _fn(name):
    tree = ast.parse(SRC)
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
            return n
    return None


def test_the_silence_check_exists_and_is_reached():
    assert _fn("_report_silent_detectors") is not None
    diversity = _fn("_score_diversity_loop")
    assert diversity is not None
    called = {
        getattr(c.func, "id", None)
        for c in ast.walk(diversity)
        if isinstance(c, ast.Call)
    }
    assert "_report_silent_detectors" in called, (
        "the silence check exists and nothing calls it, which is the shape "
        "this audit has found eight times"
    )


def test_it_measures_against_the_detector_s_own_rate():
    """One global threshold cannot describe a tick feed and an hourly poller."""
    fn = _fn("_report_silent_detectors")
    src = ast.unparse(fn)
    assert "per_hour" in src and "minutes_silent" in src
    assert "DETECTOR_SILENCE_MULTIPLE" in src


def test_a_closed_market_is_not_reported_as_a_fault():
    """Equities do not trade overnight; saying so nightly teaches the reader to skip it."""
    from services.enrichment import main as m

    assert "equity_block" in m.DETECTOR_SILENCE_MARKET_HOURS_TYPES
    assert "options_flow" in m.DETECTOR_SILENCE_MARKET_HOURS_TYPES
    # Crypto and flights run around the clock and must stay checked.
    assert "crypto_transfer" not in m.DETECTOR_SILENCE_MARKET_HOURS_TYPES
    assert "flight_position" not in m.DETECTOR_SILENCE_MARKET_HOURS_TYPES


def test_the_session_lookup_uses_a_name_that_exists():
    """The first version imported `is_market_open`, which does not exist.

    A broad `except` would have defaulted it to "open", so the exemption above
    would never have applied and every equity detector would have been reported
    silent all night -- an import error swallowed into a plausible default.
    """
    import shared.utils.market_session as ms

    assert not hasattr(ms, "is_market_open")
    assert hasattr(ms, "current_session") and hasattr(ms, "Session")
    src = ast.unparse(_fn("_report_silent_detectors"))
    assert "current_session" in src
    assert "is_market_open" not in src


def test_a_detector_below_the_rate_floor_is_left_alone():
    """Below a real cadence the arithmetic is noise, not a finding."""
    from services.enrichment import main as m

    assert m.DETECTOR_SILENCE_MIN_RATE_PER_HOUR > 0


def test_both_conditions_are_required():
    """Unusual for this detector, and long enough to be worth reading."""
    from services.enrichment import main as m

    assert m.DETECTOR_SILENCE_MIN_MINUTES >= 5.0, (
        "without an absolute floor, a detector running at 892/hour is flagged "
        "for an ordinary four-minute pause, which is 59x its four-second cadence"
    )


@pytest.mark.parametrize(
    "rate_per_hour,minutes_silent,should_report",
    [
        (1.45, 540, True),    # the measured prediction_market_trade case: 13x
        (891.0, 4, False),    # flight_position, 4x -- ordinary burstiness
        (117.0, 7, False),    # equity_block during a session
        (0.2, 6000, False),   # below the rate floor: no cadence to be late against
        (5.6, 27, False),     # ransomware, 2.5x
    ],
)
def test_the_multiple_matches_the_measurements_it_was_set_from(
    rate_per_hour, minutes_silent, should_report
):
    from services.enrichment import main as m

    if rate_per_hour < m.DETECTOR_SILENCE_MIN_RATE_PER_HOUR:
        reported = False
    elif minutes_silent < m.DETECTOR_SILENCE_MIN_MINUTES:
        # The multiple alone is wrong at high rates: flight_position's mean gap
        # is four seconds, so an ordinary four-minute pause is 59x its cadence.
        reported = False
    else:
        expected_gap = 60.0 / rate_per_hour
        reported = (minutes_silent / expected_gap) >= m.DETECTOR_SILENCE_MULTIPLE
    assert reported is should_report, (
        f"{rate_per_hour}/hr silent {minutes_silent}min -> "
        f"{minutes_silent / max(1e-9, 60.0 / rate_per_hour):.1f}x"
    )
