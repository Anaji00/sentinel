"""The repair decisions, tested without a database.

Three populations of stored events carry extraction output that the producing
code no longer emits. The causes are fixed; these are the rows written before
the fix, and they are what every historical query and every baseline reads.

A data repair is the one kind of change that cannot be rolled back by editing
code, so the judgement each phase makes is separated from the I/O and tested
here: what gets cleared, what gets kept, and what gets left alone because it
cannot be judged.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "repair_under_test", ROOT / "scripts" / "repair_event_extraction_artifacts.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def repair():
    return _load()


class TestSanctionsFlags:
    def test_a_flag_raised_by_a_four_letter_surname_is_cleared(self, repair):
        # "maria" matched a container ship. `sanctioned_ofac` drives the
        # CRITICAL alert tier, so this is not a cosmetic row.
        flags = ["sanctioned_ofac", "sanctioned_kw:maria", "vessel"]
        result = repair._repaired_sanctions(flags, ["sanctioned_ofac", "maritime"])
        assert result is not None
        new_flags, new_tags = result
        assert "sanctioned_ofac" not in new_flags
        assert not any(f.startswith("sanctioned_kw:") for f in new_flags)
        assert "vessel" in new_flags
        assert new_tags == ["maritime"]

    def test_a_flag_raised_by_a_real_sanctioned_party_is_kept(self, repair):
        flags = ["sanctioned_ofac", "sanctioned_kw:sovcomflot"]
        assert repair._repaired_sanctions(flags, ["sanctioned_ofac"]) is None

    def test_a_short_but_curated_keyword_is_kept(self, repair):
        # "irgc" is four characters and unambiguous. The length rule is about
        # synced aliases, not about curated terms.
        flags = ["sanctioned_ofac", "sanctioned_kw:irgc"]
        assert repair._repaired_sanctions(flags, []) is None

    def test_one_good_keyword_saves_the_row(self, repair):
        flags = [
            "sanctioned_ofac",
            "sanctioned_kw:lily",
            "sanctioned_kw:sovcomflot",
        ]
        assert repair._repaired_sanctions(flags, []) is None

    def test_a_fuzzy_match_is_judged_the_same_way(self, repair):
        assert repair._repaired_sanctions(
            ["sanctioned_ofac", "sanctioned_fuzzy:star"], []
        ) is not None
        assert repair._repaired_sanctions(
            ["sanctioned_ofac", "sanctioned_fuzzy:sovcomflot"], []
        ) is None

    def test_a_flag_with_no_recorded_cause_is_left_alone(self, repair):
        # Not repairable and not safely clearable: without the keyword there is
        # no way to re-judge it, and guessing would delete a true positive.
        assert repair._repaired_sanctions(["sanctioned_ofac"], []) is None


class TestMangledAutonomousSystemNames:
    def test_a_list_becomes_its_first_origin_plus_context(self, repair):
        primary, co = repair._split_mangled_as("AS[1299, 3257, 6939]")
        assert primary == "AS1299"
        assert co == ["AS3257", "AS6939"]

    def test_a_single_element_list_has_no_co_origins(self, repair):
        assert repair._split_mangled_as("AS[15169]") == ("AS15169", [])

    def test_prefixed_numbers_are_accepted(self, repair):
        primary, co = repair._split_mangled_as("AS['AS1299', 'AS3257']")
        assert primary == "AS1299"
        assert co == ["AS3257"]

    def test_a_real_as_name_is_not_touched(self, repair):
        assert repair._split_mangled_as("AS1299") is None
        assert repair._split_mangled_as("ASN Bank") is None
        assert repair._split_mangled_as("") is None
        assert repair._split_mangled_as(None) is None

    def test_a_bracketed_name_with_no_numbers_is_not_repairable(self, repair):
        # Better to leave a row unrepaired than to invent an AS number for it.
        assert repair._split_mangled_as("AS[unknown]") is None


class TestNamedEntityTokens:
    """The filter the named_entities phase applies, at its source."""

    def test_scaffolding_and_numbers_are_rejected(self):
        from shared.utils.entity_resolution import is_plausible_entity_name
        for junk in ("0.80", "GLOBAL CONTEXT", "1,299", "- -", "", "UNKNOWN", "2024-01-01"):
            assert not is_plausible_entity_name(junk), junk

    def test_real_subjects_survive(self):
        from shared.utils.entity_resolution import is_plausible_entity_name
        for name in ("AAPL", "Apple Inc.", "3M", "7-Eleven", "Gazprom PJSC", "AS1299", "X"):
            assert is_plausible_entity_name(name), name
