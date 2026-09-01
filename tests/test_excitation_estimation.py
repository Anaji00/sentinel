"""
tests/test_excitation_estimation.py

The excitation table was invented, and every cell that could be checked was
wrong by one to two orders of magnitude.

    crypto -> tradfi     0.3   invented    0.0019  measured
    crypto -> crypto     0.5   invented    0.1044  measured
    tradfi -> tradfi     0.4   invented    unstable across halves
    news   -> tradfi     0.1   invented    no excitation
    prediction -> *      0.15  invented    source has 217 events

Three approaches were tried before the one that worked, and the failures are
what shaped it.

Bin-level activity correlation gave twenty-one confident numbers: shares sum to
one, so every domain anti-correlates with the busiest by arithmetic, and several
pairs showed excitation rising with lag.

Entity-level lead-lag, the prescribed replacement, cannot run: aviation,
maritime and prediction share zero entities with tradfi.

A window-and-control design accepted 33 of 49 pairs, including tradfi->maritime
at 0.203 and crypto->aviation at 0.182. Stock trades do not excite vessel
positions. Restricting to co-active minutes removes the outage confound but not
the burst one -- an event happening is evidence the pipeline is busy, busy is
system-wide, and bursts decay, so the decay test passed them. Matching controls
on third-party load then accepted zero of 49, because for news and tradfi every
minute lies within an hour of an event: at these arrival rates there is no
uncontaminated control period anywhere in the data, and more data makes that
worse.

So the estimator uses no windows. It fits all seven domains jointly by Poisson
maximum likelihood, which needs no unexcited baseline because it never compares
two populations -- each coefficient is a partial effect with the other six
domains' recent activity already in the model.

Five of forty-nine pairs survived. Four are self-excitation, which is the effect
this data can actually see. The honest summary is "this deployment shows almost
no cross-domain excitation", not "here are the cross-domain coefficients".
"""

import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.streaming_detectors import HawkesIntensityTracker  # noqa: E402

SCRIPT = ROOT / "scripts" / "estimate_excitation.py"


def _script() -> str:
    return SCRIPT.read_text(encoding="utf-8")


# -- the table now says only what was measured ---------------------------------

def test_every_coefficient_is_one_that_survived_the_checks():
    measured = {
        ("crypto", "crypto"): 0.1044,
        ("cyber", "cyber"): 0.0957,
        ("maritime", "maritime"): 0.0950,
        ("news", "news"): 0.0783,
        ("crypto", "tradfi"): 0.0019,
    }
    assert HawkesIntensityTracker.DEFAULT_EXCITATION == measured


@pytest.mark.parametrize(
    "pair",
    [("tradfi", "crypto"), ("tradfi", "tradfi"), ("news", "tradfi"),
     ("news", "crypto"), ("maritime", "tradfi"), ("cyber", "tradfi"),
     ("prediction", "tradfi"), ("prediction", "crypto")],
)
def test_the_invented_coefficients_are_gone(pair):
    """Each of these was a number somebody chose. None survived measurement."""
    assert pair not in HawkesIntensityTracker.DEFAULT_EXCITATION


def test_a_domain_with_no_measured_path_still_refuses_to_forecast():
    """The guard is what makes an empty cell safe. Aviation and prediction have
    no inbound coefficient, so their intensity is their baseline restated."""
    tracker = HawkesIntensityTracker()
    assert tracker.has_excitation_path("aviation") is False
    assert tracker.has_excitation_path("prediction") is False


@pytest.mark.parametrize("domain", ["crypto", "tradfi", "news", "maritime", "cyber"])
def test_a_measured_domain_can_register_excitation(domain):
    tracker = HawkesIntensityTracker()
    assert tracker.has_excitation_path(domain) is True


def test_cross_domain_excitation_is_not_overstated():
    """Only one cross-domain term survived, and it is small. A table that
    implied strong coupling between domains would be the old table again."""
    cross = {
        pair: value for pair, value in HawkesIntensityTracker.DEFAULT_EXCITATION.items()
        if pair[0] != pair[1]
    }
    assert list(cross) == [("crypto", "tradfi")]
    assert cross[("crypto", "tradfi")] < 0.01


def test_the_table_records_where_it_came_from():
    source = (ROOT / "shared/utils/streaming_detectors.py").read_text(encoding="utf-8")
    assert "scripts/estimate_excitation.py" in source
    assert "Re-run the script rather than editing these by hand" in source


# -- the estimator's own guards ------------------------------------------------

def test_history_excludes_the_current_bin():
    """Including it credits a domain with exciting itself in the bin it arrived
    in, and every self-coefficient becomes 1."""
    from scripts.estimate_excitation import _history

    counts = np.array([[0.0], [5.0], [0.0], [0.0]])
    history = _history(counts, beta=0.1)
    assert history[0, 0] == 0.0
    assert history[1, 0] == 0.0, "the arrival is exciting its own bin"
    assert history[2, 0] > 0.0
    assert history[3, 0] < history[2, 0], "the kernel does not decay"


def test_history_decays_at_the_configured_rate():
    from scripts.estimate_excitation import _history

    counts = np.zeros((5, 1))
    counts[0, 0] = 1.0
    history = _history(counts, beta=0.1)
    ratio = history[3, 0] / history[2, 0]
    assert ratio == pytest.approx(np.exp(-0.1), rel=1e-6)


def test_a_known_excitation_is_recovered():
    """A synthetic series where domain A genuinely drives domain B."""
    from scripts.estimate_excitation import estimate

    rng = np.random.default_rng(7)
    span = 20_000
    a = rng.poisson(2.0, span).astype(float)

    decay, alpha, baseline = float(np.exp(-0.1)), 0.35, 1.0
    history, b = 0.0, np.zeros(span)
    for t in range(1, span):
        history = decay * (history + a[t - 1])
        b[t] = rng.poisson(baseline + alpha * history)

    series = {
        "a": {t: v for t, v in enumerate(a) if v},
        "b": {t: v for t, v in enumerate(b) if v},
    }
    accepted = {
        (s, d): value for s, d, value, _ in estimate(series, beta=0.1) if value is not None
    }
    assert ("a", "b") in accepted, "a real excitation was not detected"
    assert accepted[("a", "b")] == pytest.approx(alpha, rel=0.2)


def test_independent_series_produce_no_coefficient():
    """The check that matters: two unrelated domains must yield nothing. The
    window design gave 33 of 49 pairs a number on data like this."""
    from scripts.estimate_excitation import estimate

    rng = np.random.default_rng(11)
    span = 20_000
    a = rng.poisson(2.0, span).astype(float)
    b = rng.poisson(2.0, span).astype(float)

    series = {
        "a": {t: v for t, v in enumerate(a) if v},
        "b": {t: v for t, v in enumerate(b) if v},
    }
    cross = [
        (s, d, value) for s, d, value, _ in estimate(series, beta=0.1)
        if value is not None and s != d
    ]
    assert not cross, f"excitation invented between independent series: {cross}"


def test_the_estimator_states_its_own_rejected_designs():
    """Three approaches failed before this one. A reader who does not know that
    will reach for the simplest of them again."""
    source = _script()
    for reason in ("sum to 1", "share zero entities", "no uncontaminated control"):
        assert reason in source


def test_the_guards_are_all_present():
    source = _script()
    for guard in ("MIN_MAGNITUDE", "MIN_SOURCE_EVENTS", "STABILITY_TOLERANCE", "SIGNIFICANCE"):
        assert guard in source


def test_the_fit_uses_an_identity_link():
    """A log link fits exp(sum of alphas); the coefficients then stop being the
    additive excitation the consuming model applies."""
    source = _script()
    assert "params[0] + design @ params[1:]" in source
    assert "np.exp(params" not in source
