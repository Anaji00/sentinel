"""
tests/test_correlation_payload_sanity.py

Alerts that assert things their evidence never contained.

Four defects found by reading what the correlations table actually held, rather
than by reading the code that writes it:

  1. "Cyber Aviation Chokepoint Disruption", alert tier CRITICAL, triggered by
     AS31216 -- and "Correlated with 50 events across 1 domains (vessel)". The
     rule accepts any one of four correlation types, one of which is
     vessel_position, so it fired on fifty vessel position fixes with no
     aviation event anywhere in the evidence. Vessels are always somewhere;
     that is not corroboration of a BGP hijack.

  2. "Market Anomaly Cluster (0Xd9695C855Ea4477C3290Dec8Adc8E3F6C5B1C30E)" --
     a cluster of wallet transfers labelled a market anomaly. Coins moving
     between addresses is not a price event.

  3. That same address, mangled. key.title() turned
     0xd9695c855ea4477c3290dec8adc8e3f6c5b1c30e into
     0Xd9695C855Ea4477C3290Dec8Adc8E3F6C5B1C30E: a string that matches nothing,
     cannot be pasted into a block explorer, and reads as a different address.

  4. entity_ids listing the same wallet twice, because the trigger entity also
     appeared in its own supporting events. entity_names was already
     deduplicated; entity_ids was not.

All four share a shape: a constant in the payload asserting more than the
variable evidence supports.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from services.correlation.cascade import (  # noqa: E402
    _classify_cluster,
    _looks_like_identifier,
)
from services.correlation.soft_correlator import POSITION_TELEMETRY_TYPES  # noqa: E402


# -- 1. routine telemetry is not corroboration --------------------------------

def test_position_telemetry_is_excluded_from_rule_evidence():
    """The CRITICAL alert whose only evidence was fifty position fixes."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "POSITION_TELEMETRY_TYPES" in source
    assert 'if str(h.get("type", "")) not in POSITION_TELEMETRY_TYPES' in source


def test_findings_about_vessels_are_still_correlated():
    """A dark or spoofed vessel IS a finding. Only routine fixes are excluded."""
    for finding in ("vessel_dark", "vessel_spoof", "vessel_sts", "flight_dark", "flight_anomaly"):
        assert finding not in POSITION_TELEMETRY_TYPES


def test_the_headline_states_which_domains_matched():
    """A detector's name may assert a combination the match never required."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "[matched: {', '.join(sorted(domains_triggered))" in source


# -- 2. on-chain movement is not a market event -------------------------------

def test_a_wallet_transfer_cluster_is_not_a_market_anomaly():
    assert _classify_cluster({"crypto_transfer"}, False)[1] == "On-Chain Activity Cluster"


def test_a_crypto_trade_cluster_still_is_one():
    """The distinction is movement versus price, not crypto versus not-crypto."""
    assert _classify_cluster({"crypto_trade"}, False)[1] == "Market Anomaly Cluster"


# -- 3. identifiers are quoted, never restyled --------------------------------

def test_a_wallet_address_is_recognised_as_an_identifier():
    assert _looks_like_identifier("0xd9695c855ea4477c3290dec8adc8e3f6c5b1c30e") is True


def test_an_instrument_symbol_is_recognised_as_an_identifier():
    assert _looks_like_identifier("SI=F") is True


@pytest.mark.parametrize("place", ["black sea", "strait of malacca", "suez canal"])
def test_a_region_is_not_an_identifier(place):
    """Regions read better title-cased; only machine identifiers are protected."""
    assert _looks_like_identifier(place) is False


def test_the_cascade_no_longer_titlecases_a_key_blindly():
    source = (ROOT / "services/correlation/cascade.py").read_text(encoding="utf-8")
    # Interpolations, not prose: the comment above display_key explains the bug
    # and names key.title(), and an earlier version of this test failed on its
    # own explanation.
    assert "{key.title()}" not in source, "a key is still interpolated title-cased"
    assert "display_key = key if _looks_like_identifier(key) else key.title()" in source
    assert "{display_key}" in source


# -- 4. one subject, counted once ---------------------------------------------

def test_entity_ids_are_deduplicated():
    """The trigger entity also appears in its own supporting events, so a
    cluster listed the same wallet twice and read as two subjects."""
    source = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
    assert "entity_ids=list(dict.fromkeys(" in source
