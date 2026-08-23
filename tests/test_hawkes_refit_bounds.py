"""Keeps the Hawkes refit from stopping the correlation engine.

The maximum-likelihood fit is O(n^2) in events and was handed the entire 7-day
history, which only grows. Measured on this host: 800 events take 16s, 3,200
take 218s, and the 23,055 it was actually given works out at roughly three
hours. It ran synchronously on the event loop, so for those hours the service
consumed nothing, logged nothing and committed nothing -- 252,000 messages of
backlog while the CPU sat pinned at 93%, with a refit due again every six hours.

Two constraints hold it in place: the input is capped, and the fit runs off the
loop. The algorithm itself is untouched -- same gradient ascent, same tolerance,
same learning rate.
"""
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services/correlation/hawkes_correlator.py").read_text(encoding="utf-8")

from services.correlation.hawkes_correlator import HawkesMLE  # noqa: E402


def test_the_fit_runs_off_the_event_loop():
    """Synchronously, this blocked every consumer in the service."""
    assert "await asyncio.to_thread(self._mle.fit" in SRC, (
        "the MLE fit is called inline and will stall the correlation consumer"
    )
    assert not re.search(r"^\s*result = self\._mle\.fit\(", SRC, re.M)


def test_the_input_is_capped_before_fitting():
    """Uncapped, cost grows quadratically with a history that never stops growing."""
    m = re.search(r"MAX_EVENTS_PER_DOMAIN_FOR_FIT\s*=\s*(\d+)", SRC)
    assert m, "no cap on events per domain"
    cap = int(m.group(1))
    assert 50 <= cap <= 1000, f"cap {cap} is outside a defensible range"
    assert "timestamps[-self.MAX_EVENTS_PER_DOMAIN_FOR_FIT:]" in SRC, "the cap is declared but not applied"


def test_the_cap_keeps_the_fit_within_its_refit_interval():
    """A fit that outlasts its own schedule can never finish before the next starts."""
    cap = int(re.search(r"MAX_EVENTS_PER_DOMAIN_FOR_FIT\s*=\s*(\d+)", SRC).group(1))
    interval = eval(re.search(r"REFIT_INTERVAL\s*=\s*([0-9]+(?:\s*\*\s*[0-9]+)*)", SRC).group(1).strip())
    domains = len(HawkesMLE().domain_idx)
    # Measured: ~86s at 2,000 events, scaling quadratically.
    estimated = 86.0 * ((cap * domains) / 2000.0) ** 2
    assert estimated < interval / 10, (
        f"a fit of ~{estimated:.0f}s is too large a share of a {interval}s interval"
    )


def test_the_most_recent_events_are_kept_not_the_oldest():
    """The excitation kernel decays as exp(-beta*dt); recent events carry the signal."""
    assert "[-self.MAX_EVENTS_PER_DOMAIN_FOR_FIT:]" in SRC, "keeps the oldest events instead of the newest"


@pytest.mark.parametrize("per_domain", [40, 80])
def test_capped_fits_still_produce_usable_parameters(per_domain):
    """Trimming must not degrade the fit into nonsense."""
    import random
    random.seed(11)
    domains = list(HawkesMLE().domain_idx.keys())
    streams = {d: sorted(random.uniform(0, 86400 * 7) for _ in range(per_domain)) for d in domains}
    result = HawkesMLE().fit(streams)
    rho = result.get("spectral_radius")
    assert rho is not None and 0.0 <= rho <= 1.0, f"unstable spectral radius {rho}"
    assert result.get("mu"), "no baseline intensities produced"
