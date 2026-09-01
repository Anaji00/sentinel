"""
scripts/estimate_excitation.py

Estimates Hawkes cross-domain excitation from the event stream, or refuses to.

Five of seven domains have carried no coefficient because three approaches were
tried before this one, and each failed for a reason worth keeping.

  1. Bin-level activity correlation produced twenty-one confident numbers and no
     knowledge. Shares of activity sum to 1, so every domain anti-correlates
     with the busiest one by arithmetic necessity -- that is where
     news->crypto = -0.817 came from. Several pairs also showed excitation
     *rising* with lag (aviation->crypto: 0.509 at five minutes, 0.611 at
     thirty), and an influence that strengthens as it ages is not an influence.

  2. Event-level lead-lag on matched entities, the prescribed replacement,
     cannot run at all. Measured over fourteen days: aviation, maritime and
     prediction share zero entities with tradfi, and crypto shares 9 of 87,902.
     These paths are not entity-mediated -- a Suez closure moves crude through a
     channel that names no vessel.

  3. A window-and-control design, written here first, accepted 33 of 49 ordered
     pairs including tradfi->maritime at 0.203 and crypto->aviation at 0.182. A
     stock trade does not excite vessel positions. Restricting to co-active
     minutes removes the *outage* confound but not the *burst* one: an event
     happening at all is evidence the pipeline is in a busy stretch, and busy
     stretches are system-wide. Bursts decay too, so the decay test waved them
     through.

     Matching controls on third-party load then accepted zero of 49 -- and that
     is the useful part. For news and tradfi every minute lies within an hour of
     an event, so no uncontaminated control period exists anywhere in the data.
     At these arrival rates the counterfactual does not exist, and more data
     makes it worse rather than better.

So no window design can identify these parameters. This one uses no windows. It
fits the discrete-time form of the multivariate Hawkes process --

    E[N_d(t)] = mu_d + SUM_d'  alpha_{d',d} * w_{d'}(t)
    w_{d'}(t) = SUM_{s<t}  exp(-beta (t - s)) * N_{d'}(s)

-- by Poisson maximum likelihood over one-minute bins, with every source domain
in the same regression. That is what makes it work where the window design could
not: each alpha is a *partial* effect, fitted while the other six domains'
recent activity is in the model, so "the whole system was busy" becomes a term
rather than a confound. It needs no unexcited baseline because it never compares
two populations.

numpy and scipy only. `tick` would do this and is not installed; taking on a
dependency to fit a handful of coefficients is not the trade this codebase makes.

Every coefficient must survive three checks before it is reported:

  positive      an excitation that lowers the target's rate is not excitation
  significant   likelihood ratio against the same model without that term
  stable        fitted on the first half of the window and re-checked on the
                second, rejected if the sign flips or the magnitude moves by
                more than half

    python scripts/estimate_excitation.py
    python scripts/estimate_excitation.py --days 14 --beta 0.1
"""

import argparse
import asyncio
import collections
import functools
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import minimize
from scipy.stats import chi2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Decay rate per minute. Matches HawkesIntensityTracker.DEFAULT_DECAY, so a
# coefficient estimated here means the same thing in the model that consumes it.
DEFAULT_BETA = 0.1

# A domain counts as collecting at minute m if it produced anything within this
# many minutes either side. Gaps shorter than this are ordinary quiet, not an
# outage -- and an outage modelled as low intensity would be fitted as
# suppression by whatever else happened to be quiet at the same time.
ACTIVE_HALF_WINDOW = 30

MIN_EVENTS = 500
SIGNIFICANCE = 0.001
STABILITY_TOLERANCE = 0.5

# Below this an 'excitation' is a rounding artifact wearing a coefficient.
# The first run reported aviation->news at 0.0000 and aviation->cyber at
# 0.0001 as accepted: statistically distinguishable from zero on 800,000
# events, and meaningless in a model that multiplies them by an intensity.
MIN_MAGNITUDE = 0.001

# The source needs its own history, not just the target. prediction has 217
# events in 136 minutes and produced prediction->tradfi = 0.4163, the largest
# coefficient in the table, from a column that is zero in 99.9% of rows. The
# stability check passed it because both halves were equally thin.
MIN_SOURCE_EVENTS = 2000

DOMAIN_SQL = """
SELECT
  CASE
    WHEN type LIKE 'vessel%' THEN 'maritime'
    WHEN type LIKE 'flight%' THEN 'aviation'
    WHEN type IN ('headline','social_signal','narrative_cluster') THEN 'news'
    WHEN type IN ('bgp_anomaly','breach_detected','infra_exposed','ransomware','vulnerability') THEN 'cyber'
    WHEN type LIKE 'crypto%' THEN 'crypto'
    WHEN type IN ('prediction_market_trade','prediction_market') THEN 'prediction'
    ELSE 'tradfi'
  END AS domain,
  FLOOR(EXTRACT(EPOCH FROM occurred_at) / 60)::bigint AS minute,
  COUNT(*) AS n
FROM events
WHERE occurred_at > NOW() - ($1 || ' days')::interval
GROUP BY 1, 2
"""


def _counts_matrix(series: dict, domains: list) -> np.ndarray:
    """Dense (minutes x domains) count matrix over the observed span."""
    all_minutes = [m for counts in series.values() for m in counts]
    lo, hi = min(all_minutes), max(all_minutes)
    counts = np.zeros((hi - lo + 1, len(domains)), dtype=np.float64)
    for j, domain in enumerate(domains):
        for minute, n in series[domain].items():
            counts[minute - lo, j] = n
    return counts


def _history(counts: np.ndarray, beta: float) -> np.ndarray:
    """Exponentially weighted history, strictly excluding the current minute.

    w[t] = exp(-beta) * (w[t-1] + counts[t-1]) is the exponential kernel written
    as a recursion. Strictly past, or a domain is credited with exciting itself
    in the same bin it arrived in, and every self-coefficient becomes 1.
    """
    decay = float(np.exp(-beta))
    history = np.zeros_like(counts)
    for t in range(1, counts.shape[0]):
        history[t] = decay * (history[t - 1] + counts[t - 1])
    return history


def _active(counts_col: np.ndarray) -> np.ndarray:
    """Minutes where this domain was demonstrably collecting."""
    present = (counts_col > 0).astype(np.float64)
    window = np.ones(2 * ACTIVE_HALF_WINDOW + 1)
    return np.convolve(present, window, mode="same") > 0


def _neg_log_likelihood(params, design, observed) -> float:
    """Poisson NLL with an identity link, which is what a Hawkes intensity is.

    A log link would fit exp(sum of alphas), and the coefficients would stop
    being the additive excitation the model consuming them applies.
    """
    rate = np.maximum(params[0] + design @ params[1:], 1e-12)
    return float(np.sum(rate) - np.sum(observed * np.log(rate)))


def _fit(design: np.ndarray, observed: np.ndarray) -> np.ndarray:
    start = np.concatenate(([max(observed.mean(), 1e-6)], np.full(design.shape[1], 1e-3)))
    result = minimize(
        _neg_log_likelihood, start, args=(design, observed),
        method="L-BFGS-B",
        bounds=[(1e-9, None)] * (design.shape[1] + 1),
        options={"maxiter": 500},
    )
    return result.x


def estimate(series: dict, beta: float) -> list:
    domains = sorted(series)
    counts = _counts_matrix(series, domains)
    history = _history(counts, beta)
    active = np.column_stack([_active(counts[:, j]) for j in range(len(domains))])

    source_totals = counts.sum(axis=0)

    results = []
    for j, target in enumerate(domains):
        rows = active[:, j]
        if rows.sum() < MIN_EVENTS or counts[rows, j].sum() < MIN_EVENTS:
            for source in domains:
                results.append((source, target, None, "target has too little data"))
            continue

        design = history[rows]
        observed = counts[rows, j]
        params = _fit(design, observed)
        ll_full = -_neg_log_likelihood(params, design, observed)

        half = design.shape[0] // 2
        first = _fit(design[:half], observed[:half])
        second = _fit(design[half:], observed[half:])

        for i, source in enumerate(domains):
            alpha = params[1 + i]

            keep = [k for k in range(len(domains)) if k != i]
            reduced = design[:, keep]
            params_reduced = _fit(reduced, observed)
            ll_reduced = -_neg_log_likelihood(params_reduced, reduced, observed)
            p_value = float(chi2.sf(max(2.0 * (ll_full - ll_reduced), 0.0), df=1))

            a, b = first[1 + i], second[1 + i]
            larger = max(abs(a), abs(b), 1e-12)
            stable = (a > 0) == (b > 0) and abs(a - b) / larger <= STABILITY_TOLERANCE

            if source_totals[i] < MIN_SOURCE_EVENTS:
                results.append((source, target, None,
                                f"source has only {int(source_totals[i]):,} events"))
            elif alpha <= 1e-6:
                results.append((source, target, None, "no excitation"))
            elif alpha < MIN_MAGNITUDE:
                results.append((source, target, None, f"below the noise floor ({alpha:.5f})"))
            elif p_value > SIGNIFICANCE:
                results.append((source, target, None, f"not significant (p={p_value:.3f})"))
            elif not stable:
                results.append((source, target, None, f"unstable across halves ({a:.4f} vs {b:.4f})"))
            else:
                results.append((source, target, float(alpha), "accepted"))
    return results


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--beta", type=float, default=DEFAULT_BETA)
    args = parser.parse_args()

    from shared.db import get_timescale

    db = await get_timescale()
    rows = await db.query(DOMAIN_SQL, str(args.days))

    series = collections.defaultdict(dict)
    for row in rows:
        series[row["domain"]][int(row["minute"])] = int(row["n"])

    print(f"{len(rows)} (domain, minute) buckets over {args.days} days, beta={args.beta}")
    for domain, counts in sorted(series.items()):
        print(f"  {domain:12} {sum(counts.values()):>9,} events in {len(counts):>6,} minutes")
    print()

    # Off the event loop. CLAUDE.md: "Never block active event loops. Ensure
    # explicit thread-pool offloading for blocking CPU-bound ... calls." The fit
    # walks ~14,400 one-minute bins per domain and then runs L-BFGS-B once per
    # ordered pair plus a reduced model for each -- tens of seconds of pure
    # arithmetic, during which the asyncpg pool cannot service its own
    # heartbeats.
    results = await asyncio.get_running_loop().run_in_executor(
        None, functools.partial(estimate, series, args.beta)
    )
    accepted = [r for r in results if r[2] is not None]

    for source, target, alpha, verdict in results:
        shown = f"{alpha:+.4f}" if alpha is not None else "   --   "
        print(f"  {source + ' -> ' + target:26} {shown:>10}  {verdict}")

    print()
    print(f"{len(accepted)} of {len(results)} ordered pairs survived all three checks.")
    if accepted:
        print()
        for source, target, alpha, _ in sorted(accepted, key=lambda r: -r[2]):
            print(f'    ("{source}", "{target}"): {round(alpha, 4)},')
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
