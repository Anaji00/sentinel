"""
tests/test_eight_k_items_and_prospectus_noise.py

Filings, scored by what happened rather than diluted by what did not.

In a measured week 765 of 1,189 SEC filings were 424B2 pricing supplements --
banks' structured notes -- each at a flat 0.2, and a window of each issuer's
latest fifteen forms could hold nothing else. Five material 8-K items the score
table did not name fell to its 0.70 default, while the exhibit item attached to
most 8-Ks lifted every Regulation FD filing to that same 0.70.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.enrichers.tradfi import _eight_k_item_score


def test_a_restatement_outranks_scheduled_results():
    assert _eight_k_item_score(["4.02"]) > _eight_k_item_score(["2.02"])
    assert _eight_k_item_score(["1.05"]) >= 0.85
    assert _eight_k_item_score(["2.04"]) >= 0.85


def test_the_exhibit_item_does_not_lift_a_routine_filing():
    assert _eight_k_item_score(["7.01", "9.01"]) == _eight_k_item_score(["7.01"])
    assert _eight_k_item_score(["5.07", "9.01"]) < 0.5


def test_material_items_still_compound_and_cap():
    assert _eight_k_item_score(["1.01", "5.02", "9.01"]) > _eight_k_item_score(["5.02"])
    assert _eight_k_item_score(["1.03", "4.02", "2.04", "5.01"]) <= 0.99


def test_the_collector_skips_424b2_before_counting_its_window():
    src = (ROOT / "services" / "collector-filings" / "main.py").read_text(encoding="utf-8")
    loop = src.split("considered = 0")[1]
    assert loop.index('startswith("424B2")') < loop.index("considered += 1")
    assert "count = min(15, len(forms))" not in src
