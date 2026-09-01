"""
tests/test_peer_graph.py

"Bad earnings at X moves peer Y" was not a rule that could be written.

The graph holds 146,993 entities and 1,450 earnings events, and its
relationships are RELATED_TO, LOCATED_IN and REGISTERED_IN -- none of which mean
"is a comparable of". An earnings surprise had no edge to travel down, so the
question "who else does this hit" had no answer at all.

Two sources of evidence, ordered deliberately. Realised co-movement of returns
decides a peer; shared sector or index corroborates it and raises the recorded
strength. Structure alone creates nothing -- two S&P financials can be a
regional bank and a card network, and sector membership is a taxonomy somebody
chose. This system has been burned three times by treating a chosen number as a
measured one: the Hawkes excitation table, the macro yield defaults, and the
conviction scale.

Refusal is the normal outcome. Twenty of the platform's tickers currently carry
twenty or more hourly bars, and a correlation over four observations is a
coincidence with a decimal point.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.correlation.peer_graph import (  # noqa: E402
    MIN_ABS_CORRELATION, MIN_OVERLAP_BARS, PeerEdge,
    contagion_candidates, derive_peers, pearson, simple_returns,
)


def _walk(seed: int, n: int = 60, start: float = 100.0) -> list:
    import random

    rng = random.Random(seed)
    out = [start]
    for _ in range(n):
        out.append(out[-1] * (1 + rng.gauss(0, 0.01)))
    return out


def _mirror(prices: list, factor: float = -0.95) -> list:
    out = [200.0]
    for previous, current in zip(prices, prices[1:]):
        out.append(out[-1] * (1 + factor * (current - previous) / previous))
    return out


def _twin(prices: list) -> list:
    """A near-perfect co-mover: r rounds to 1.0."""
    return [p * 1.0001 for p in prices]


def _noisy_twin(prices: list, seed: int = 99, noise: float = 0.004) -> list:
    """Strongly correlated but not perfectly, so the structural bonus has
    headroom to show. A twin at r=1.0 is already at the cap and would hide it."""
    import random

    rng = random.Random(seed)
    out = [prices[0]]
    for previous, current in zip(prices, prices[1:]):
        drift = (current - previous) / previous
        out.append(out[-1] * (1 + drift + rng.gauss(0, noise)))
    return out


# -- returns, not levels -------------------------------------------------------

def test_the_measurement_is_on_returns():
    """Two instruments that merely drift upward correlate on levels whatever
    their behaviour -- the spurious regression that would call every equity a
    peer of every other."""
    prices = [100.0, 110.0, 121.0]
    assert simple_returns(prices) == pytest.approx([0.1, 0.1])


def test_non_positive_prices_are_skipped():
    assert simple_returns([100.0, 0.0, 50.0, 55.0]) == pytest.approx([0.1])


# -- what counts as a peer -----------------------------------------------------

def test_co_movers_become_peers():
    base = _walk(3)
    edges = derive_peers({"AAA": base, "BBB": _twin(base)})
    assert len(edges) == 1
    assert edges[0].correlation > MIN_ABS_CORRELATION
    assert edges[0].is_inverse is False


def test_an_inverse_pair_is_still_a_peer():
    """A hedge, or a share-shift pair where one name's loss is another's gain,
    transmits a surprise as reliably as a co-mover -- in the other direction.
    Discarding the sign makes the edge useless for saying which way."""
    base = _walk(5)
    edges = derive_peers({"AAA": base, "CCC": _mirror(base)})
    assert len(edges) == 1
    assert edges[0].is_inverse is True
    assert edges[0].correlation < 0


def test_unrelated_series_produce_no_edge():
    """The check that matters. An earlier estimator in this codebase accepted 33
    of 49 pairs on data like this."""
    assert derive_peers({"AAA": _walk(11), "DDD": _walk(29)}) == []


def test_a_short_window_produces_nothing():
    """Twenty hourly bars is already thin; below it the estimate is whichever two
    moves happened to land in the window."""
    short = _walk(7, n=MIN_OVERLAP_BARS - 5)
    assert derive_peers({"AAA": short, "BBB": _twin(short)}) == []


def test_a_flat_series_correlates_with_nothing():
    """Zero variance has no correlation with anything. Returning 0.0 would be a
    claim; there is no answer to give."""
    assert pearson([0.0] * 40, [0.01] * 40) is None


# -- structure corroborates, it does not create --------------------------------

def test_shared_sector_does_not_create_an_edge():
    reference = {"AAA": {"sector": "Tech"}, "DDD": {"sector": "Tech"}}
    assert derive_peers({"AAA": _walk(11), "DDD": _walk(29)}, reference) == []


def test_shared_structure_is_recorded_when_the_pair_co_moves():
    base = _walk(13)
    reference = {
        "AAA": {"sector": "Semis", "index_membership": ["SPX"]},
        "BBB": {"sector": "Semis", "index_membership": ["SPX"]},
    }
    edges = derive_peers({"AAA": base, "BBB": _noisy_twin(base)}, reference)
    assert edges[0].shared_sector == "Semis"
    assert edges[0].shared_index == "SPX"
    assert abs(edges[0].correlation) < 1.0, "a perfect twin is already at the cap"
    assert edges[0].strength > abs(edges[0].correlation)


def test_strength_never_exceeds_one():
    base = _walk(17)
    reference = {"AAA": {"sector": "X"}, "BBB": {"sector": "X"}}
    edges = derive_peers({"AAA": base, "BBB": _twin(base)}, reference)
    assert edges[0].strength <= 1.0


def test_missing_reference_data_is_not_an_obstacle():
    """Sector and index membership are 0% populated today, because the
    reference-data refresh has never run. This is built to work without it
    rather than to wait for it."""
    base = _walk(19)
    edges = derive_peers({"AAA": base, "BBB": _twin(base)})
    assert edges and edges[0].shared_sector is None


# -- the query the module exists for -------------------------------------------

def test_contagion_candidates_rank_by_strength():
    base = _walk(23)
    edges = derive_peers({"AAA": base, "BBB": _twin(base), "CCC": _mirror(base)})
    ranked = contagion_candidates(edges, "AAA")
    assert ranked, "an earnings surprise at AAA reaches nobody"
    assert ranked == sorted(ranked, key=lambda e: e.strength, reverse=True)


def test_contagion_includes_inverse_peers():
    base = _walk(31)
    edges = derive_peers({"AAA": base, "CCC": _mirror(base)})
    assert any(e.is_inverse for e in contagion_candidates(edges, "AAA"))


def test_a_shock_reaches_either_end_of_the_edge():
    """The edge is stored once; the query must not depend on which name was
    alphabetically first when it was derived."""
    base = _walk(37)
    edges = derive_peers({"AAA": base, "BBB": _twin(base)})
    assert contagion_candidates(edges, "BBB")


def test_an_unknown_ticker_reaches_nobody():
    assert contagion_candidates([], "NVDA") == []
    assert contagion_candidates([], "") == []


# -- the proposal the supervisor will accept -----------------------------------

def test_the_proposal_uses_a_registered_predicate():
    """An unregistered predicate is dropped by is_valid_predicate() without a
    word, so every edge would be discarded on the way to the graph."""
    from shared.models.ontology import is_valid_predicate

    edge = PeerEdge("AAA", "BBB", 0.9, 0.001, 40, None, None, 0.9)
    assert is_valid_predicate(edge.as_proposal()["data"]["relation_type"])


def test_the_proposal_carries_the_signed_coefficient():
    """weight is abs() for ranking; the sign lives in coefficient, and losing it
    erases the difference between a co-mover and a hedge."""
    edge = PeerEdge("AAA", "CCC", -0.88, 0.001, 40, None, None, 0.88)
    data = edge.as_proposal()["data"]
    assert data["weight"] == 0.88
    assert data["properties"]["coefficient"] == -0.88
    assert data["properties"]["inverse"] is True


def test_the_proposal_records_how_it_was_measured():
    """A coefficient with no provenance is what the excitation table used to be."""
    edge = PeerEdge("AAA", "BBB", 0.9, 0.0004, 41, "Semis", "SPX", 1.0)
    props = edge.as_proposal()["data"]["properties"]
    assert props["method"] == "pearson_returns"
    assert props["window"] == "41_bars"
    assert props["p_value"] == 0.0004


def test_the_confidence_survives_the_supervisors_normaliser():
    """Strength is already on 0-1, so the boundary normaliser must pass it
    through rather than rescale it as a percentage."""
    from services.agents.supervisor import _as_unit_interval

    edge = PeerEdge("AAA", "BBB", 0.9, 0.001, 40, None, None, 0.9)
    confidence = edge.as_proposal()["data"]["confidence"]
    assert _as_unit_interval(confidence) == pytest.approx(confidence)


# -- a stale series is refused whole ------------------------------------------
#
# Found live, during market hours, on the first pass that produced real edges.
# Ten peers came back between |0.916| and |0.98| -- gold against the Nasdaq at
# +0.967, crude against everything at -0.947 -- which is not a market structure.
#
# The macro tickers carry 102 hourly bars with 15-18 distinct closes, because a
# collector defect republished one frozen quote for days. MIN_MOVING_BARS drops
# the flat bars, but what survives is the moments the stale snapshot refreshed,
# and it refreshed for all twelve instruments in the same poll cycle. The filter
# was correlating one poll loop seen twelve times.
#
# No amount of filtering *within* such a series recovers it, so it is refused
# whole and becomes usable again once genuine bars displace the frozen history.

def _mostly_frozen(levels: int = 17, n: int = 102, start: float = 100.0) -> list:
    """102 bars carrying `levels` distinct prices -- the live macro shape."""
    per = n // levels
    out = []
    for i in range(levels):
        out += [start + i] * per
    return out + [out[-1]] * (n - len(out))


def test_a_mostly_frozen_series_is_refused():
    a = _mostly_frozen()
    b = [p * 0.5 for p in a]
    assert derive_peers({"GC=F": a, "NQ=F": b}) == []


def test_synchronised_snapshot_refreshes_do_not_become_peers():
    """The exact failure: twelve instruments whose only moves are simultaneous,
    because one poll loop updated all of them at once."""
    base = _mostly_frozen()
    series = {f"T{i}": [p * (1 + i * 0.1) for p in base] for i in range(6)}
    assert derive_peers(series) == []


def test_a_live_series_is_not_caught_by_the_stale_guard():
    """The guard must not reject real prices. A genuine walk has a distinct
    value in nearly every bar."""
    live = _walk(41, n=101)
    assert len(set(live)) / len(live) > 0.9
    edges = derive_peers({"AAA": live, "BBB": _noisy_twin(live)})
    assert edges, "a real pair must still be measurable"


def test_the_ratio_bar_sits_between_the_two_populations():
    """17 distinct in 102 is 17%; a live series is above 90%. The threshold
    only has to separate those, not be precise."""
    from services.correlation.peer_graph import MIN_DISTINCT_RATIO

    stale = _mostly_frozen()
    assert len(set(stale)) / len(stale) < MIN_DISTINCT_RATIO
    assert MIN_DISTINCT_RATIO < 0.9
