"""Three-level empirical-Bayes shrinkage for signal cells (family x horizon x size band), for the registered phase 2 test.

Level 0 is a global prior of zero excess return. Level 1 is the family at that horizon, pooled across size bands and shrunk toward zero.
Level 2 is the size-band cell, shrunk toward its level 1 posterior mean instead of toward zero. Prior strengths are not chosen by hand: they
come from the data by method of moments (between-cell variance against within-cell variance). The effective sample size of a cell is its
own N plus its parent's N scaled by sigma^2 / (sigma^2 + tau^2), so a cell with few episodes borrows only as much as its siblings agree.

Shadow tool: the live gate (shared/utils/tradable_cells.py) is untouched. Pure; no I/O.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Dict, List, Mapping, Sequence, Tuple

RELEASE_P = 0.95
PRIOR_FLOOR = 0.05
RELEASE_N_EFF = 30.0


def _phi(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


@dataclass
class CellPosterior:
    key: Tuple[str, str, str]
    n: int
    raw_mean: float
    post_mean: float
    post_sd: float
    p_positive: float
    n_eff: float
    parent_mean: float
    release_candidate: bool = False


def _moments(xs: Sequence[float]) -> Tuple[int, float, float]:
    n = len(xs)
    if n == 0:
        return 0, 0.0, 0.0
    m = sum(xs) / n
    v = sum((x - m) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
    return n, m, v


def shrink(cells: Mapping[Tuple[str, str, str], Sequence[float]], cost: float = 0.0, min_cell_n: int = 5) -> List[CellPosterior]:
    """Posterior for every (family, horizon, band) cell from its list of aligned excess returns (fractions, direction already applied).

    `cost` is subtracted from every observation (a round-trip cost as a fraction). Cells under `min_cell_n` are skipped."""
    groups: Dict[Tuple[str, str], Dict[str, List[float]]] = {}
    for (fam, hor, band), xs in cells.items():
        if len(xs) >= min_cell_n:
            groups.setdefault((fam, hor), {})[band] = [x - cost for x in xs]
    if not groups:
        return []
    # within-cell variance, pooled over every cell with at least two observations
    all_var = [(len(xs) - 1, _moments(xs)[2]) for g in groups.values() for xs in g.values() if len(xs) > 1]
    sigma2 = sum(dof * v for dof, v in all_var) / max(1, sum(dof for dof, _ in all_var)) or 1e-12
    # level 1: group means across (family, horizon) shrunk toward zero
    g_mean = {k: statistics.fmean([x for xs in g.values() for x in xs]) for k, g in groups.items()}
    g_n = {k: sum(len(xs) for xs in g.values()) for k, g in groups.items()}
    # Method-of-moments variances are clipped at a floor, not at zero: a sampling accident that makes them zero would claim certainty that
    # every family is exactly null. The floor caps a prior's strength at 20 times a typical cell.
    floor1 = PRIOR_FLOOR * sigma2 / max(1.0, statistics.fmean(g_n.values()))
    tau1 = max(floor1, statistics.pvariance(list(g_mean.values())) - statistics.fmean([sigma2 / n for n in g_n.values()])) if len(g_mean) > 1 else floor1
    out: List[CellPosterior] = []
    for key, g in groups.items():
        k1 = sigma2 / tau1
        parent = g_n[key] / (g_n[key] + k1) * g_mean[key]
        parent_var = sigma2 / (g_n[key] + k1)                    # how sure the family estimate is: a cell cannot be surer than its parent
        band_means = [statistics.fmean(xs) for xs in g.values()]
        band_n = [len(xs) for xs in g.values()]
        tau2 = max(PRIOR_FLOOR * sigma2 / max(1.0, statistics.fmean(band_n)), (statistics.pvariance(band_means) if len(band_means) > 1 else 0.0) - statistics.fmean([sigma2 / n for n in band_n]))
        k2 = sigma2 / tau2
        for band, xs in g.items():
            n, m, _ = _moments(xs)
            post = (n * m + k2 * parent) / (n + k2)
            sd = math.sqrt(sigma2 / (n + k2) + (k2 / (n + k2)) ** 2 * parent_var)
            n_eff = n + (g_n[key] - n) / (1.0 + tau2 / sigma2)
            out.append(CellPosterior((key[0], key[1], band), n, m, post, sd, _phi(post / sd) if sd > 0 else 0.5, n_eff, parent))
    return out


def benjamini_hochberg(pvalues: Sequence[float], q: float = 0.05) -> List[bool]:
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    cutoff = -1
    for rank, i in enumerate(order, start=1):
        if pvalues[i] <= q * rank / m:
            cutoff = rank
    keep = [False] * m
    for rank, i in enumerate(order, start=1):
        keep[i] = rank <= cutoff
    return keep


def release_candidates(posteriors: List[CellPosterior], q: float = 0.05) -> List[CellPosterior]:
    """Mark the cells the registered rule would let out of shadow: posterior probability of a positive net return of 0.95 or more,
    effective N of 30 or more, and a pass of the false-discovery step over all cells (p = 1 - posterior probability)."""
    keep = benjamini_hochberg([1.0 - p.p_positive for p in posteriors], q) if posteriors else []
    for p, fdr in zip(posteriors, keep):
        p.release_candidate = bool(p.p_positive >= RELEASE_P and p.n_eff >= RELEASE_N_EFF and fdr)
    return [p for p in posteriors if p.release_candidate]


__all__ = ["CellPosterior", "RELEASE_N_EFF", "RELEASE_P", "benjamini_hochberg", "release_candidates", "shrink"]
