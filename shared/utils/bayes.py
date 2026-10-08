"""Beta-binomial shrinkage for hit rates measured on small samples.

A hit rate from five scored predictions is mostly noise, and the alternatives
used before -- trust the raw rate above a sample floor, a flat 0.55 below it --
either believe a lucky streak or ignore the record entirely. The posterior mean
under a Beta prior does neither: with a prior worth `strength` observations
centred on a reference rate, the estimate starts at the reference and moves to
the measured rate as evidence accumulates (Efron & Morris; standard empirical
Bayes for proportions).
"""
from __future__ import annotations

from typing import Optional

DEFAULT_PRIOR_STRENGTH = 20.0


def posterior_hit_rate(
    hits: int,
    trials: int,
    prior_mean: float = 0.5,
    strength: float = DEFAULT_PRIOR_STRENGTH,
) -> float:
    """Posterior mean of a Beta(prior_mean*s, (1-prior_mean)*s) after the record."""
    prior_mean = min(0.99, max(0.01, float(prior_mean)))
    a = prior_mean * strength
    b = (1.0 - prior_mean) * strength
    hits = max(0, int(hits))
    trials = max(hits, int(trials))
    return (hits + a) / (trials + a + b)


def prob_rate_above(
    hits: int,
    trials: int,
    threshold: float = 0.5,
    prior_mean: float = 0.5,
    strength: float = DEFAULT_PRIOR_STRENGTH,
) -> Optional[float]:
    """P(true rate > threshold | record), or None without scipy."""
    try:
        from scipy.stats import beta as beta_dist
    except ImportError:
        return None
    prior_mean = min(0.99, max(0.01, float(prior_mean)))
    a = prior_mean * strength + max(0, int(hits))
    b = (1.0 - prior_mean) * strength + max(0, int(trials) - int(hits))
    return float(beta_dist.sf(threshold, a, b))


__all__ = ["DEFAULT_PRIOR_STRENGTH", "posterior_hit_rate", "prob_rate_above"]
