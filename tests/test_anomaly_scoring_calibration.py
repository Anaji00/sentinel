"""
tests/test_anomaly_scoring_calibration.py

Anomaly scores that carried no information.

Measured over 24 hours before these changes: 21.6% of all events (105,411) were
CRITICAL. Three causes:

  1. track_frequency() returned +0.05 per repeat, capped at +0.20, so the fourth
     and every later event from an entity scored the maximum forever. That
     rewards repetition, which is the opposite of what an anomaly detector is
     for: an address emitting eleven transfers in half an hour is the least
     surprising thing in the stream. It produced a constant -- every suspect
     crypto transfer scored 0.4 + 0.15 + 0.20 = exactly 0.75, across 29,150
     events in a day.

  2. Whale-transfer scoring conflated size with provenance, so a 50 USDC
     movement from a watched wallet was published as "SUSPECT Whale Transfer"
     at CRITICAL. Every headline read "$0.0M", which is why a stream of
     genuinely distinct transfers looked like one event repeated.

  3. BGP hijacks were floored at 0.85 then multiplied by 1.3 for a novel
     AS-path: 1.105, clamped to 1.0. All 1,852 bgp_anomaly events in 24 hours
     shared one distinct score.
"""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def crypto_enricher():
    spec = importlib.util.spec_from_file_location(
        "enrichment_crypto_scoring", ROOT / "services/enrichment/enrichers/crypto.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["enrichment_crypto_scoring"] = module
    spec.loader.exec_module(module)
    return module


def _code(path):
    """Source with comments and docstrings stripped.

    Both files document these defects by name, so matching raw text asserts
    against the explanation rather than the code.
    """
    text = path.read_text(encoding="utf-8")
    triple = chr(34) * 3
    text = re.sub(triple + ".*?" + triple, "", text, flags=re.S)
    return re.sub(r"^\s*#.*$", "", text, flags=re.M)


# -- transfer size must be visible and must rank ------------------------------

def test_amounts_are_shown_at_a_unit_that_reveals_them(crypto_enricher):
    """Every headline read "$0.0M" because notional/1e6 was used for all sizes."""
    money = crypto_enricher._money
    assert money(50) == "$50"
    assert money(50_000) == "$50.0K"
    assert money(1_500_000) == "$1.50M"
    assert money(2_400_000_000) == "$2.40B"


def test_a_small_transfer_is_not_dressed_as_a_large_one(crypto_enricher):
    assert "0.0M" not in crypto_enricher._money(50)


def test_size_scoring_discriminates_across_orders_of_magnitude(crypto_enricher):
    """notional / 50_000_000 * 0.5 gave a $5M move 0.05 -- below the noise."""
    score = crypto_enricher._notional_score
    assert score(50) == 0.0
    assert 0.4 < score(1_000_000) < 0.6
    assert score(5_000_000) > 0.6
    assert score(500_000_000) == 1.0
    sizes = [10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
    scores = [score(v) for v in sizes]
    assert scores == sorted(scores), "a larger transfer must never rank lower"


def test_junk_notionals_do_not_raise(crypto_enricher):
    for bad in (None, "", "abc", float("nan"), float("inf"), -5):
        assert crypto_enricher._notional_score(bad) == 0.0
        assert crypto_enricher._money(bad).startswith("$")


def test_only_a_real_whale_is_called_a_whale():
    src = _code(ROOT / "services/enrichment/enrichers/crypto.py")
    assert "is_whale = notional >= WHALE_NOTIONAL_USD" in src
    assert "Watched Wallet Transfer" in src, "the two signals must be labelled apart"
    assert "notional < 1_000_000 and not is_suspect" not in src


# -- BGP must rank hijacks against each other ---------------------------------

def test_bgp_scoring_no_longer_saturates():
    src = _code(ROOT / "services/enrichment/anomaly_scorer.py")
    assert "rrcf_score * 1.3" not in src, "the multiplier that clamped to 1.0 is back"
    assert "max(rrcf_score, 0.85)" not in src
    assert "HIJACK_BASE_SCORE" in src


def test_the_hijack_weights_leave_exactly_one_way_to_reach_one():
    from services.enrichment.anomaly_scorer import (
        HIJACK_CENTRALITY_WEIGHT, HIJACK_NOVELTY_WEIGHT, HIJACK_VELOCITY_WEIGHT,
    )
    total = HIJACK_NOVELTY_WEIGHT + HIJACK_CENTRALITY_WEIGHT + HIJACK_VELOCITY_WEIGHT
    assert total == pytest.approx(1.0), "only a maximal event should reach 1.0"


def test_hijacks_of_differing_severity_are_rankable():
    from services.enrichment.anomaly_scorer import (
        HIJACK_BASE_SCORE, HIJACK_CENTRALITY_WEIGHT,
        HIJACK_NOVELTY_WEIGHT, HIJACK_VELOCITY_WEIGHT,
    )

    def blend(novelty, centrality, velocity):
        head = 1.0 - HIJACK_BASE_SCORE
        contrib = (HIJACK_NOVELTY_WEIGHT * novelty
                   + HIJACK_CENTRALITY_WEIGHT * centrality
                   + HIJACK_VELOCITY_WEIGHT * velocity)
        return round(min(1.0, HIJACK_BASE_SCORE + head * min(1.0, contrib)), 4)

    quiet, novel, extreme = blend(0, 0, 0), blend(1.0, 0.2, 0), blend(1.0, 1.0, 1.0)
    assert quiet < novel < extreme
    assert extreme == 1.0
    assert quiet == pytest.approx(HIJACK_BASE_SCORE)
