"""
tests/test_wargame_targets_are_entities.py

Wargame predictions were stored against things that are not entities.

The wargamer checks its predicted target against the entities it was given,
and that list carried whatever the cluster put in entity_names: live on
1 October, predictions were stored against "Market Anomaly Cluster (Flashpoint:
66.2/100)" and "financial_market". And the check only gated the wargamer's own
record_prediction call -- the telemetry worker stored every wargame payload as
a prediction regardless, so the check was bypassed by the path that writes.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.adversarial_wargamer import _names_an_entity


def test_identifiers_are_entities():
    for t in ("MSFT", "US10Y", "SEA STAR", "PGT69CB", "Wallet_0x4d5f", "GOLDEN STAR", "BRK.B",
              "MMSI:564743000"):
        assert _names_an_entity(t), t


def test_descriptions_and_categories_are_not():
    for t in ("Market Anomaly Cluster (Flashpoint: 66.2/100)", "financial_market",
              "maritime", "GLOBAL_SYS", "", "NONE", "50 events across 2 domains (aviation, maritime)",
              "AVIATION ALERT: SHI7340", "aviation_alert:SHI7340", "quant_discovery_apg_macro",
              "on-chain_activity_cluster"):
        assert not _names_an_entity(t), t


def test_the_flag_travels_and_the_worker_honours_it():
    w = (ROOT / "services" / "agents" / "adversarial_wargamer.py").read_text(encoding="utf-8")
    assert 'output["prediction_recordable"] = target_is_named' in w
    assert "and _names_an_entity(target)" in w
    t = (ROOT / "services" / "telemetry-worker" / "main.py").read_text(encoding="utf-8")
    assert 'data.get("prediction_recordable") is False' in t
