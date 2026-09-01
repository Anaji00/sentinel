"""
services/correlation/peer_graph.py

Derives PEER_OF edges, so contagion has something to travel along.

"Bad earnings at X moves peer Y" was not a rule that could be written here. The
graph holds 146,993 entities and 1,450 earnings events, and its relationships
are RELATED_TO, LOCATED_IN and REGISTERED_IN -- none of which mean "is a
comparable of". An earnings surprise had no edge to propagate down, so the
question "who else does this hit" had no answer at all.

Two sources of evidence, and the distinction matters:

  Structural   Shared sector, industry or index membership. Cheap, stable, and
               wrong on its own -- two S&P financials can be a regional bank and
               a card network, and an earnings miss at one says nothing about
               the other.

  Realised     Correlation of hourly returns over a common window. This is the
               part that knows whether they actually move together, and it is
               the part that can be measured rather than asserted.

A peer needs realised co-movement. Structure corroborates it and raises the
recorded strength; structure alone does not create an edge. That ordering is
deliberate: sector membership is a taxonomy someone chose, and this system has
already been burned by treating a chosen number as a measured one -- the Hawkes
excitation table, the macro yield defaults, the conviction scale.

Refusal is the normal outcome. Only 20 tickers currently carry twenty or more
hourly bars, and a correlation computed over four observations is a coincidence
with a decimal point. Pairs without a window get no edge and no apology.
"""

import logging
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("correlation.peer_graph")

# Bars required before a correlation means anything. Twenty hourly bars is
# already thin; below it the estimate is dominated by whichever two moves
# happened to land in the window.
MIN_OVERLAP_BARS = 20

# |r| a pair must clear to be called peers. Deliberately high: the question is
# not "are these weakly related" but "would a shock to one plausibly reach the
# other", and a 0.3 correlation answers neither.
MIN_ABS_CORRELATION = 0.60

# Two-sided p-value ceiling. With twenty bars, r=0.60 sits around p=0.005, so
# this rejects rather than rubber-stamps.
MAX_P_VALUE = 0.01

# Bars are not observations. The macro series carry ~105 hourly bars each and
# only 14-17 distinct closing prices between them -- the price repeats, so most
# returns are exactly zero and the variance lives in a handful of moves. Two
# series that are flat except for the same few jumps correlate at exactly 1.000,
# and the first live run published six pairs at |r| = 1.0 for precisely that
# reason. A perfect correlation is nearly always an artifact.
#
# What matters is the number of bars that actually moved, not the number
# recorded. n here is the effective sample, and the p-value is computed from it.
MIN_MOVING_BARS = 15

# The fraction of a window that must carry distinct prices before the series is
# usable at all.
#
# MIN_MOVING_BARS filters bars where nothing moved. It cannot tell a synchronised
# market from a synchronised *measurement*, and the difference matters: the macro
# tickers carry 102 hourly bars with 15-18 distinct closes, because a collector
# defect republished one frozen quote for days. The few bars that do move are the
# moments the stale snapshot refreshed -- and it refreshed for all twelve
# instruments in the same poll cycle.
#
# Correlating those transitions produced ten edges between |0.916| and |0.98|,
# including gold against the Nasdaq at +0.967. That is not a market structure;
# it is one poll loop seen twelve times.
#
# A series this stale cannot be repaired by filtering within it, so it is refused
# whole. As genuine bars accumulate and the frozen history rolls out of the
# window, the same series becomes usable on its own merits.
MIN_DISTINCT_RATIO = 0.40

# How much shared structure adds to the recorded strength. Corroboration, not
# evidence -- it cannot lift a pair over MIN_ABS_CORRELATION on its own.
STRUCTURAL_BONUS = 0.10


@dataclass(frozen=True)
class PeerEdge:
    """One measured comparable relationship."""

    source: str
    target: str
    correlation: float
    p_value: float
    overlap_bars: int
    shared_sector: Optional[str]
    shared_index: Optional[str]
    strength: float

    @property
    def is_inverse(self) -> bool:
        """Peers that move against each other are still peers.

        A hedge or a share-shift pair -- one airline's loss is another's gain --
        transmits an earnings surprise just as reliably as a co-mover, in the
        opposite direction. Discarding the sign here would make the edge useless
        for saying *which way* contagion runs.
        """
        return self.correlation < 0

    def as_proposal(self) -> dict:
        """The governed graph proposal the supervisor consumes."""
        return {
            "entity_id": self.source,
            "action": "LINK_ENTITY",
            "data": {
                "target_id": self.target,
                "source_label": "Company",
                "target_label": "Company",
                "relation_type": "PEER_OF",
                "weight": round(abs(self.correlation), 4),
                "confidence": round(self.strength, 4),
                "properties": {
                    "coefficient": round(self.correlation, 4),
                    "p_value": round(self.p_value, 6),
                    "method": "pearson_returns",
                    "window": f"{self.overlap_bars}_bars",
                    "shared_sector": self.shared_sector,
                    "shared_index": self.shared_index,
                    "inverse": self.is_inverse,
                },
            },
        }


def simple_returns(prices: Sequence[float]) -> List[float]:
    """Bar-over-bar returns, skipping non-positive prices.

    Returns rather than levels, because two instruments that both drift upward
    correlate on levels whatever their behaviour -- the classic spurious
    regression, and the reason a naive price correlation calls every equity a
    peer of every other.
    """
    out = []
    for previous, current in zip(prices, prices[1:]):
        if previous and previous > 0 and current and current > 0:
            out.append((current - previous) / previous)
    return out


def pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[Tuple[float, float]]:
    """(r, two-sided p) or None when the sample cannot support either.

    Written out rather than imported: this module runs inside the correlation
    service, which does not carry scipy, and the t-approximation is exact enough
    for a threshold test at n>=20.
    """
    n = min(len(xs), len(ys))
    if n < MIN_OVERLAP_BARS:
        return None

    xs, ys = list(xs[:n]), list(ys[:n])
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]

    var_x = sum(d * d for d in dx)
    var_y = sum(d * d for d in dy)
    if var_x <= 0 or var_y <= 0:
        # A flat series has no correlation with anything. Returning 0 would be a
        # claim; returning None says the question does not apply.
        return None

    r = sum(a * b for a, b in zip(dx, dy)) / math.sqrt(var_x * var_y)
    r = max(-1.0, min(1.0, r))

    if abs(r) >= 1.0:
        return r, 0.0

    t = abs(r) * math.sqrt((n - 2) / (1.0 - r * r))
    return r, _two_sided_p(t, n - 2)


def _two_sided_p(t: float, df: int) -> float:
    """Student's t survival function, doubled.

    Uses the incomplete beta identity rather than an approximation, so the
    threshold at n=20 is a real one.
    """
    if df <= 0:
        return 1.0
    x = df / (df + t * t)
    return max(0.0, min(1.0, _incomplete_beta(x, df / 2.0, 0.5)))


def _incomplete_beta(x: float, a: float, b: float) -> float:
    """Regularised incomplete beta, by continued fraction."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0

    log_beta = (
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log(1.0 - x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return math.exp(log_beta) * _beta_cf(x, a, b) / a
    return 1.0 - math.exp(log_beta) * _beta_cf(1.0 - x, b, a) / b


def _beta_cf(x: float, a: float, b: float, iterations: int = 200) -> float:
    tiny = 1e-30
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    if abs(d) < tiny:
        d = tiny
    d = 1.0 / d
    h = d

    for m in range(1, iterations + 1):
        m2 = 2 * m
        numerator = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + numerator * d
        c = 1.0 + numerator / c
        if abs(d) < tiny:
            d = tiny
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        h *= d * c

        numerator = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + numerator * d
        c = 1.0 + numerator / c
        if abs(d) < tiny:
            d = tiny
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 3e-7:
            break
    return h


def _shared(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a and b and str(a).strip().lower() == str(b).strip().lower():
        return str(a).strip()
    return None


def _shared_index(a: Sequence[str], b: Sequence[str]) -> Optional[str]:
    common = {str(x).strip().upper() for x in (a or []) if x} & {
        str(x).strip().upper() for x in (b or []) if x
    }
    return sorted(common)[0] if common else None


def derive_peers(
    price_series: Dict[str, Sequence[float]],
    reference: Optional[Dict[str, dict]] = None,
) -> List[PeerEdge]:
    """Every pair whose returns actually co-move, with structure as corroboration.

    `price_series` maps ticker -> bars, oldest first. `reference` optionally maps
    ticker -> {"sector": str, "index_membership": [str]}; it is absent today
    because the reference-data refresh has never run, and the function is built
    to work without it rather than to wait for it.
    """
    reference = reference or {}
    returns = {
        ticker: simple_returns(bars)
        for ticker, bars in price_series.items()
        if bars and len(bars) > MIN_OVERLAP_BARS
    }
    # A series that barely moves cannot be a leg of a measured relationship,
    # however many bars it has.
    returns = {
        ticker: series for ticker, series in returns.items()
        if sum(1 for r in series if r != 0.0) >= MIN_MOVING_BARS
    }

    # And a series that is mostly a repeated print is not a price history,
    # whatever survives the bar-level filter.
    stale = []
    for ticker in list(returns):
        bars = price_series.get(ticker) or []
        if not bars:
            continue
        ratio = len(set(bars)) / len(bars)
        if ratio < MIN_DISTINCT_RATIO:
            stale.append(f"{ticker} ({ratio:.0%})")
            returns.pop(ticker, None)
    if stale:
        logger.info(
            "Peer derivation refused %s stale series, below %.0f%% distinct "
            "prices: %s. A repeated print is not an observation.",
            len(stale), MIN_DISTINCT_RATIO * 100, ", ".join(sorted(stale)[:8]),
        )

    tickers = sorted(returns)
    edges: List[PeerEdge] = []
    considered = 0

    for i, a in enumerate(tickers):
        for b in tickers[i + 1:]:
            considered += 1
            overlap = min(len(returns[a]), len(returns[b]))
            xs, ys = returns[a][-overlap:], returns[b][-overlap:]

            # Only bars where at least one leg moved carry information. A stale
            # feed repeating its last print produces zeros, and zeros agree with
            # each other perfectly.
            moving = [(x, y) for x, y in zip(xs, ys) if x != 0.0 or y != 0.0]
            if len(moving) < MIN_MOVING_BARS:
                continue

            result = pearson([m[0] for m in moving], [m[1] for m in moving])
            if result is None:
                continue

            r, p = result
            if abs(r) < MIN_ABS_CORRELATION or p > MAX_P_VALUE:
                continue

            ref_a, ref_b = reference.get(a, {}), reference.get(b, {})
            sector = _shared(ref_a.get("sector"), ref_b.get("sector"))
            index = _shared_index(
                ref_a.get("index_membership", []), ref_b.get("index_membership", [])
            )

            strength = min(1.0, abs(r) + (STRUCTURAL_BONUS if (sector or index) else 0.0))
            edges.append(
                PeerEdge(
                    source=a, target=b, correlation=r, p_value=p,
                    overlap_bars=len(moving), shared_sector=sector,
                    shared_index=index, strength=strength,
                )
            )

    logger.info(
        "Peer derivation: %s tickers with a usable window, %s pairs considered, "
        "%s peers (%s inverse). Pairs below |r|=%.2f or above p=%.3f get no edge.",
        len(tickers), considered, len(edges),
        sum(1 for e in edges if e.is_inverse), MIN_ABS_CORRELATION, MAX_P_VALUE,
    )
    return edges


def contagion_candidates(edges: Sequence[PeerEdge], ticker: str) -> List[PeerEdge]:
    """Who an event at `ticker` plausibly reaches, strongest first.

    This is the query the whole module exists to answer. An earnings surprise at
    one name is only actionable if something can say which comparables it should
    be read against, and the inverse peers belong in the answer -- they move too,
    the other way.
    """
    target = str(ticker or "").strip().upper()
    if not target:
        return []
    touching = [
        e for e in edges
        if e.source.upper() == target or e.target.upper() == target
    ]
    return sorted(touching, key=lambda e: e.strength, reverse=True)
