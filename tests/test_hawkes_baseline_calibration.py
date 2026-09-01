"""
tests/test_hawkes_baseline_calibration.py

"crypto-domain intensity (230,093,397.3x baseline)".

That line went out in a tier-4 correlation description, repeatedly, from a
freshly built system. It is not a large measurement; it is not a measurement at
all. The excitation ratio divides current intensity by a baseline, and the
baselines were constants written before the system had ever run:

    self._baselines = {"crypto": 0.01, "tradfi": 0.01, ...}   # events/second

Crypto actually arrives at roughly 4.7 events/second on this deployment -- about
470 times the seeded guess -- so every ratio computed against it was wrong by
orders of magnitude before any excitation was added. The seed was a magic
number with no owner, no units stated at the call site, and no test that would
have noticed it drifting away from reality.

A ratio means "how much busier than usual". Only the deployment knows what
usual is, so the baseline is now measured rather than assumed, and the reported
ratio is capped -- past two orders of magnitude the figure has stopped being a
measurement and become a division artefact.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from shared.utils.streaming_detectors import (  # noqa: E402
    BASELINE_MIN_OBSERVATIONS,
    BASELINE_WINDOW_SEC,
    MAX_EXCITATION_RATIO,
    HawkesIntensityTracker,
)

T0 = 1_000_000.0


# The baseline is a rate measured over BASELINE_WINDOW_SEC windows, and
# BASELINE_MIN_OBSERVATIONS of them must elapse before it is trusted over the
# seed. Tests must therefore feed *time*, not just a count of arrivals.
WARMUP_SEC = BASELINE_WINDOW_SEC * (BASELINE_MIN_OBSERVATIONS + 1)


def _feed(tracker, domain, rate_per_sec, seconds=WARMUP_SEC, start=T0):
    """Drives a steady stream for `seconds` of simulated time."""
    count = max(1, int(rate_per_sec * seconds))
    for i in range(count):
        tracker.record_event(domain, start + i / rate_per_sec)
    return start + count / rate_per_sec


# -- the baseline is measured, not assumed ------------------------------------

def test_the_baseline_learns_the_real_arrival_rate():
    """Crypto arrives ~4.7/s here; the seed said 0.01/s."""
    t = HawkesIntensityTracker()
    _feed(t, "crypto", 4.7)
    assert t._effective_baseline("crypto") == pytest.approx(4.7, rel=0.05)


def test_the_ratio_is_no_longer_astronomical():
    """The number that shipped was 2.3e8. Anything of that order is a defect."""
    t = HawkesIntensityTracker()
    end = _feed(t, "crypto", 4.7)
    assert t.get_excitation_ratio("crypto", end) < 50.0


def test_a_steady_stream_is_not_reported_as_excited():
    """Excitation means busier than usual. A domain running at its own normal
    rate is, by definition, not."""
    t = HawkesIntensityTracker()
    end = _feed(t, "tradfi", 2.0)
    assert t.get_excitation_ratio("tradfi", end) < 20.0


def test_two_steady_streams_at_different_rates_both_read_as_normal():
    """Excitation is relative to each domain's own normal, so a domain running
    steadily at 10/s and one running steadily at 0.5/s are equally unexcited.

    An earlier version of this test asserted the sparse stream should read as
    more excited, which is the assumption the whole fix removes: it is what a
    fixed baseline would have said.
    """
    calm = HawkesIntensityTracker()
    calm_end = _feed(calm, "crypto", 10.0)

    sparse = HawkesIntensityTracker()
    sparse_end = _feed(sparse, "crypto", 0.5)

    assert calm.get_excitation_ratio("crypto", calm_end) < 20.0
    assert sparse.get_excitation_ratio("crypto", sparse_end) < 20.0


def test_a_burst_above_an_established_rate_registers():
    """The detector must still detect. A domain that has settled at a slow rate
    and then bursts is genuinely excited, and must read higher than it did
    while it was steady."""
    t = HawkesIntensityTracker()
    steady_end = _feed(t, "crypto", 0.5)
    steady = t.get_excitation_ratio("crypto", steady_end)

    # Twenty arrivals in a second, against a rate of one every two seconds.
    for i in range(20):
        t.record_event("crypto", steady_end + i * 0.05)
    burst = t.get_excitation_ratio("crypto", steady_end + 1.0)

    assert burst > steady


def test_two_of_seven_domains_still_cannot_register_excitation():
    """Was five of seven, while the table was invented.

    The coefficients have since been measured -- scripts/estimate_excitation.py,
    a joint Poisson fit over ten days -- and five of forty-nine ordered pairs
    survived being positive, significant and stable across halves. Four of those
    are self-excitation, so news, maritime and cyber gained an inbound term by
    exciting themselves.

    Aviation and prediction still have none, and that is a measurement rather
    than an omission: aviation produced no stable inbound coefficient from any
    source, and prediction has 217 events in the whole window. The guard matters
    exactly as much as before -- a domain with no path reports its baseline
    restated, whatever arrives.
    """
    t = HawkesIntensityTracker()
    for measured in ("crypto", "tradfi", "news", "maritime", "cyber"):
        assert t.has_excitation_path(measured) is True
    for blind in ("prediction", "aviation"):
        assert t.has_excitation_path(blind) is False


def test_a_blind_domain_reports_exactly_one_however_busy():
    """aviation, not cyber -- cyber now has a measured self-excitation term."""
    t = HawkesIntensityTracker()
    end = _feed(t, "aviation", 20.0)
    assert t.get_excitation_ratio("aviation", end) == pytest.approx(1.0)


# -- the seed is used only until there is something better --------------------

def test_the_seed_governs_until_the_rate_is_established():
    """One arrival is not an estimate of a rate."""
    t = HawkesIntensityTracker()
    t.record_event("crypto", T0)
    t.record_event("crypto", T0 + 1)
    assert t._effective_baseline("crypto") == 0.01


def test_the_measurement_takes_over_once_established():
    t = HawkesIntensityTracker()
    _feed(t, "crypto", 4.7)
    assert t._effective_baseline("crypto") != 0.01


# -- an outage is not a quiet period ------------------------------------------

def test_a_long_gap_does_not_collapse_the_baseline():
    """A restart or a sleeping laptop leaves an hours-long gap. Folding it in
    would drag the baseline toward zero and make the first event afterwards look
    infinitely excited -- which is how this class of bug returns."""
    t = HawkesIntensityTracker()
    _feed(t, "crypto", 4.7)
    learned = t._effective_baseline("crypto")

    t.record_event("crypto", T0 + 100_000)          # an 8-hour outage
    assert t._effective_baseline("crypto") == pytest.approx(learned, rel=0.1)


# -- the reported figure is bounded -------------------------------------------

def test_the_ratio_is_capped():
    """Past two orders of magnitude the figure invites belief it has not
    earned. A cap invites a question instead."""
    t = HawkesIntensityTracker()
    for i in range(400):
        t.record_event("prediction", T0 + i * 0.001)   # a pathological burst
    assert t.get_excitation_ratio("prediction", T0 + 1.0) <= MAX_EXCITATION_RATIO


def test_record_event_reports_the_same_bounded_ratio():
    """Both entry points must agree; the description is built from this one."""
    t = HawkesIntensityTracker()
    for i in range(400):
        state = t.record_event("prediction", T0 + i * 0.001)
    assert state["excitation_ratio"] <= MAX_EXCITATION_RATIO


def test_an_unseen_domain_does_not_raise():
    t = HawkesIntensityTracker()
    assert t.get_excitation_ratio("does_not_exist", T0) >= 0.0


# -- forecasts are only made where the model can represent them ---------------

def test_no_forecast_is_published_for_a_domain_that_cannot_move():
    """"prediction-domain anomaly intensity is forecast Nx above baseline" is a
    sentence about a quantity that is structurally pinned to 1.0."""
    source = (ROOT / "services/correlation/hawkes_correlator.py").read_text(encoding="utf-8")
    assert "if not self._tracker.has_excitation_path(target):" in source


def test_no_ratio_is_published_against_a_seeded_guess():
    """"prediction-domain intensity (100.0x baseline)" was the cap announcing it
    gave up, dressed as a finding. A low-volume domain divided by its seed."""
    t = HawkesIntensityTracker()
    t.record_event("prediction", T0)
    assert t.is_baseline_established("prediction") is False

    _feed(t, "prediction", 2.0)
    assert t.is_baseline_established("prediction") is True

    source = (ROOT / "services/correlation/hawkes_correlator.py").read_text(encoding="utf-8")
    assert "if not self._tracker.is_baseline_established(source):" in source


def test_the_excitation_matrix_records_why_it_is_incomplete():
    """A documented negative result, so the next attempt is not a repeat.

    Estimating these coefficients from bin-level activity correlation produced
    21 pairs above |r| = 0.15 and every one was an artifact: symmetric to three
    decimals (co-occurrence, not direction), rising with lag (an influence that
    strengthens as it ages), and constrained by closure (shares sum to 1, and
    news is 62% of the average bin). Filling the matrix from it would have
    produced confident coefficients and no knowledge.
    """
    source = (ROOT / "shared/utils/streaming_detectors.py").read_text(encoding="utf-8")
    assert "Why the empty cells are still empty" in source
    assert "Symmetry." in source and "Closure." in source


def test_the_guard_still_stands_while_the_matrix_is_incomplete():
    """The honest half: decline to forecast what the model cannot represent."""
    t = HawkesIntensityTracker()
    assert t.has_excitation_path("prediction") is False
    correlator = (ROOT / "services/correlation/hawkes_correlator.py").read_text(encoding="utf-8")
    assert "if not self._tracker.has_excitation_path(target):" in correlator
