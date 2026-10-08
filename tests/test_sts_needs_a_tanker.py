"""A ship-to-ship transfer is reported only when it can move oil or sanctions.

221 of 287 STS events in the 24 hours to 4 October sat in Taiwan's territorial
waters and the Singapore approach: fishing boats rafted together and hulls with
no AIS type, stopped side by side at anchor. The geometry test cannot tell those
from a transfer, so the pair now has to include a tanker or a flagged hull.
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.maritime_behaviour import is_tanker, sts_is_reportable  # noqa: E402


def test_two_fishing_boats_are_not_a_transfer():
    a = {"vessel_type": "Fishing", "name": "FWU LIANG 888", "flags": []}
    assert not sts_is_reportable(a, {})


def test_two_untyped_hulls_are_not_a_transfer():
    assert not sts_is_reportable({"vessel_type": "Unknown(0)"}, {"vessel_type": "Unknown"})


def test_a_tanker_on_either_side_is_reportable():
    assert sts_is_reportable({"vessel_type": "CargoVessel"}, {"vessel_type": "Tanker"})
    assert sts_is_reportable({"vessel_type": "Tanker"}, {})


def test_a_flagged_hull_is_reportable_whatever_its_type():
    assert sts_is_reportable({"vessel_type": "Fishing", "flags": ["sanctioned_ofac"]}, {})


def test_a_tanker_is_known_by_name_when_the_type_is_missing():
    assert is_tanker("Unknown(0)", "PACIFIC CRUDE")
    assert not is_tanker("Unknown(0)", "SMOOTH VENTURE")


def test_the_enricher_applies_the_gate_and_tags_crude():
    src = (ROOT / "services/enrichment/enrichers/maritime.py").read_text(encoding="utf-8")
    loop = src[src.index("own_types = {row[2]: row[12] for row in results}"):]
    loop = loop[:loop.index("behaviour_events = ")]
    assert "if not sts_is_reportable(mine, theirs):" in loop
    assert 'vessel_exposure_tags(region, "Tanker") if tanker else None' in loop
