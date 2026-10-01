"""A short uppercase string from the graph is not an instrument.

The graph-derived half of `build_candidate_pairs` gated only on length, so
anything the knowledge graph called an entity became a candidate for Granger
causality and cointegration. Measured on the live candidate list 2026-09-21:

    Candidate pairs name 23 symbol(s) with no usable price history:
    APATE, ARZANA, BTC-USD, CVX, DXY, EURUSD, EVENT1, EVENT2, EVENT3,
    EVENT_1, EVENT_2, EVENT_3, GD, LMT, MEA, MEA1304, MISHELL, OXY, QQQ, RTX

EVENT1..EVENT_3 are nodes the graph holds as `event_1`, `event2` and `event(s)`
-- model output stored as entities -- uppercased by the query's
`toUpper(coalesce(a.name, a.id))`. MEA1304 is a flight callsign. ARZANA and
MISHELL are vessels.

The predicates disagree about what a symbol is, and only one of them is right
for this job:

    symbol     looks_like_ticker   asset_class
    NVDA       True                equity
    QQQ        True                index
    CL=F       False               commodity     <- a real instrument, rejected
    BTC-USD    False               crypto        <- a real instrument, rejected
    EVENT1     False               None
    MEA1304    False               None
    APATE      True                equity        <- a vessel, accepted

`asset_class` admits the instruments the shape predicates reject and rejects
the placeholders. It is not perfect: APATE is five uppercase letters and
classifies as an equity. It has no price bars, so it still appears in the
engine's own "no usable price history" report rather than silently consuming
an evaluation.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.equities import asset_class  # noqa: E402

SRC = (ROOT / "services/correlation/statistical_discovery.py").read_text(encoding="utf-8")


def _admits(a: str, b: str) -> bool:
    """The gate the engine applies to a graph-derived pair."""
    return asset_class(a) is not None and asset_class(b) is not None


# ── what must not reach a correlation test ───────────────────────────────────


def test_model_output_stored_as_an_entity_is_refused():
    for placeholder in ("EVENT1", "EVENT2", "EVENT3", "EVENT_1", "EVENT_2", "EVENT_3"):
        assert not _admits(placeholder, "NVDA"), placeholder


def test_a_flight_callsign_is_refused():
    assert not _admits("MEA1304", "SPY")


def test_a_vessel_name_is_refused():
    for vessel in ("ARZANA", "MISHELL"):
        assert not _admits(vessel, "XOM"), vessel


# ── what must still reach one ────────────────────────────────────────────────


def test_equities_still_pair():
    assert _admits("NVDA", "TSM")


def test_the_instruments_the_shape_predicates_reject_still_pair():
    """CL=F and BTC-USD are why `looks_like_ticker` is the wrong gate here."""
    assert _admits("XOM", "CL=F")
    assert _admits("BTC-USD", "QQQ")
    assert asset_class("CL=F") == "commodity"
    assert asset_class("BTC-USD") == "crypto"


# ── the limit of the gate, stated rather than hidden ─────────────────────────


def test_a_vessel_shaped_like_a_ticker_still_gets_through():
    """APATE is a ship. Five uppercase letters is a ticker to any predicate.

    Recorded so the gate is not read as complete. It has no bars, so it lands
    in the engine's missing-history report instead of being tested.
    """
    assert _admits("APATE", "NVDA")


# ── the shipped code ─────────────────────────────────────────────────────────


def test_the_gate_is_applied_to_graph_derived_pairs():
    assert "asset_class(src) is None or asset_class(tgt) is None" in SRC


def test_the_refusal_is_counted_not_silent():
    assert "correlation.candidate_not_an_instrument" in SRC


def test_the_variant_is_a_kind_not_a_symbol():
    """Keying on the symbol would make each graph entity its own first firing."""
    block = SRC[SRC.index("correlation.candidate_not_an_instrument"):]
    block = block[: block.index("continue")]
    assert '"ticker_shaped"' in block and '"not_a_symbol"' in block
    assert "variant=src" not in block and "variant=tgt" not in block


def test_the_engine_still_reports_what_it_cannot_test():
    """Dropping a real instrument silently would trade one gap for another."""
    assert "no usable price history" in SRC
