"""Two scores that were constants standing where a measurement belonged.

A ranking tier, a percentile gate and an "most anomalous today" query are all
order statistics. Computed over a field that takes three values, they return
whichever record the tie-break happened to favour. Both paths here fed such a
field:

  * every 8-K carried 0.85 regardless of what it disclosed, while the SEC item
    codes that say what it disclosed were parsed, attached to the event and read
    by nothing;
  * every ransomware incident carried 0.95, 0.75 or 0.45, two of which describe
    the victim's sector rather than the incident.
"""
import importlib
import sys
import types

import pytest

tradfi = importlib.import_module("services.enrichment.enrichers.tradfi")


class TestEightKItemScoring:
    def test_a_bankruptcy_outranks_a_regulation_fd_disclosure(self):
        bankruptcy = tradfi._filing_form_score("8-K", True, ["1.03"])
        reg_fd = tradfi._filing_form_score("8-K", True, ["7.01"])
        assert bankruptcy > reg_fd
        # Not a hair apart: these are different kinds of news.
        assert bankruptcy - reg_fd > 0.3

    def test_every_named_item_gets_its_own_score(self):
        scores = {
            code: tradfi._filing_form_score("8-K", True, [code])
            for code in tradfi._EIGHT_K_ITEM_SCORES
        }
        # The defect was one value for all of them.
        assert len(set(scores.values())) > 10, scores

    def test_labelled_items_score_the_same_as_bare_codes(self):
        bare = tradfi._filing_form_score("8-K", True, ["5.02"])
        labelled = tradfi._filing_form_score(
            "8-K", True, ["5.02: Departure of Directors or Principal Officers"]
        )
        assert bare == labelled

    def test_several_material_items_outrank_one(self):
        one = tradfi._filing_form_score("8-K", True, ["1.01"])
        several = tradfi._filing_form_score("8-K", True, ["1.01", "5.02", "2.01"])
        assert several > one

    def test_the_bonus_cannot_run_away(self):
        many = tradfi._filing_form_score("8-K", True, ["1.03"] * 20)
        assert many <= 0.99

    def test_an_unknown_item_is_unclassified_not_routine(self):
        unknown = tradfi._filing_form_score("8-K", True, ["9.99"])
        routine = tradfi._filing_form_score("8-K", True, ["8.01"])
        assert unknown > routine
        assert unknown < tradfi._filing_form_score("8-K", True, ["1.03"])

    def test_an_eight_k_with_no_items_keeps_the_form_level_score(self):
        # No discriminator is present, so the absence must not be scored as if
        # it were information.
        assert tradfi._filing_form_score("8-K", True, []) == tradfi._EIGHT_K_NO_ITEMS
        assert tradfi._filing_form_score("8-K", True, None) == tradfi._EIGHT_K_NO_ITEMS

    def test_non_8k_forms_are_unaffected_by_the_items_argument(self):
        assert tradfi._filing_form_score("10-Q", False, ["1.03"]) == \
               tradfi._filing_form_score("10-Q", False)


class TestRansomwareScoring:
    def test_the_categorical_ladder_is_a_floor_not_the_answer(self):
        cyber = importlib.import_module("services.enrichment.enrichers.cyber")
        # The floor share exists precisely so the detector can separate incidents
        # inside a band without demoting one out of it.
        assert 0.0 < cyber.RANSOMWARE_FLOOR_SHARE < 1.0

    def test_ransomware_is_scored_against_its_own_detector(self):
        # Its feature vector is five live quantities; the KEV vector sharing the
        # cyber domain is one severity and four padding zeros. Pooling them would
        # compare a victim-sector flag against a zero that means nothing.
        from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
        assert "ransomware" in DynamicAnomalyScorer._OWN_DETECTOR_EVENT_TYPES
