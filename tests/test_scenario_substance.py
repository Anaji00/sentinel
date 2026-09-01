"""
tests/test_scenario_substance.py

A scenario that passes every schema check and contains no analysis.

Observed in production, from the live reasoning service:

    headline:    "Multi-Domain Signals Indicate Potential Maritime Security Threat"
    probabilities: 64 / 27 / 9          (three hypotheses, summing to 100)
    mechanism:   "Causal mechanism explaining signal convergence (...)"
    mechanism:   "Alternative causal explanation (...)"
    mechanism:   "Tail-risk / high-impact alternative (...)"
    deny_signals: ["Publicly reported shipping movements"]   -- on all three
    beneficiaries: ["MARINA ARIEL", "INSIDE"]

Every mechanism is the prompt's own placeholder text. Every deny_signal is
identical, so no observation could separate the hypotheses. "INSIDE" is a
fragment of the word "Insider". The prompt had supplied a filled-in-looking
template and a 1.5B model copied it rather than reasoning into it.

It satisfied the schema, normalised its probabilities, cleared the critique
gate, and was persisted -- indistinguishable from real analysis to anything
downstream.

That is worse than producing nothing. An empty table says the system found
nothing. A well-formed scenario containing no analysis says it found something,
and an analyst reading the table has no way to tell which they are looking at.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from services.reasoning.scenario_generator import (  # noqa: E402
    _discriminates_between_hypotheses,
    _echoes_the_template,
)


class _H:
    def __init__(self, label="Insider Threat", mechanism="Options bought hours before the AIS gap",
                 beneficiaries=None, watch_signals=None, deny_signals=None):
        self.label = label
        self.mechanism = mechanism
        self.beneficiaries = beneficiaries if beneficiaries is not None else ["Charterer"]
        self.watch_signals = watch_signals if watch_signals is not None else ["MMSI reappears"]
        self.deny_signals = deny_signals if deny_signals is not None else ["Port call filed"]


class _S:
    def __init__(self, headline="Vessel went dark before an options spike",
                 significance="A 6-hour AIS gap preceded unusual call volume.",
                 hypotheses=None):
        self.headline = headline
        self.significance = significance
        self.hypotheses = hypotheses if hypotheses is not None else [_H(), _H(label="Equipment fault")]


# -- the observed failure -----------------------------------------------------

def test_the_scenario_that_shipped_is_now_rejected():
    """Reconstructed from the row that reached the scenarios table."""
    bad = _S(hypotheses=[
        _H(label="Baseline Hypothesis: Insider Threat",
           mechanism="Causal mechanism explaining signal convergence (insider options, dark net activity)",
           beneficiaries=["MARINA ARIEL", "INSIDE"],
           deny_signals=["Publicly reported shipping movements"]),
        _H(label="Alternative Hypothesis: Operational Disruption",
           mechanism="Alternative causal explanation (malware, cyber attack)",
           deny_signals=["Publicly reported shipping movements"]),
        _H(label="High-Impact Hypothesis: Cyber-Physical Attack",
           mechanism="Tail-risk / high-impact alternative (cyber attack, physical disruption)",
           deny_signals=["Publicly reported shipping movements"]),
    ])
    assert _echoes_the_template(bad) is True


@pytest.mark.parametrize(
    "echoed",
    [
        "Causal mechanism explaining signal convergence",
        "Alternative causal explanation",
        "Tail-risk / high-impact alternative",
        "Concrete observable confirming indicator",
    ],
)
def test_each_template_phrase_is_caught(echoed):
    assert _echoes_the_template(_S(hypotheses=[_H(mechanism=echoed)])) is True


def test_an_unfilled_slot_is_caught():
    """The new prompt uses <...> slots; returning one unfilled is not an answer."""
    assert _echoes_the_template(_S(hypotheses=[_H(mechanism="<how the signals cause one another>")])) is True


def test_echoes_in_a_list_field_are_caught():
    assert _echoes_the_template(_S(hypotheses=[_H(watch_signals=["Confirming indicator"])])) is True


def test_an_echoed_headline_is_caught():
    assert _echoes_the_template(_S(headline="Concise high-impact intelligence judgment (max 150 chars)")) is True


# -- real analysis is not rejected --------------------------------------------

def test_genuine_analysis_passes():
    """The guard must not eat good output.

    Real mechanisms name real signals, which is exactly what the template
    placeholders never do.
    """
    good = _S(hypotheses=[
        _H(label="Positioning ahead of the print",
           mechanism="$4.2M of call volume in NVDA arrived 18 hours before earnings, against a 30-day average of $0.3M.",
           beneficiaries=["Buyer of the 24 Aug calls"],
           watch_signals=["Open interest holds through the print"],
           deny_signals=["Position closed before the close"]),
        _H(label="Index rebalancing",
           mechanism="NVDA weight changes on the quarterly reconstitution, which mechanically forces size.",
           beneficiaries=["Index tracking funds"],
           watch_signals=["Matching flow in peer index members"],
           deny_signals=["No comparable flow in peers"]),
    ])
    assert _echoes_the_template(good) is False


def test_none_is_not_an_echo():
    assert _echoes_the_template(None) is False


# -- hypotheses must be separable by observation ------------------------------

def test_identical_signals_across_hypotheses_are_rejected():
    """A deny signal that refutes all three refutes none of them.

    Competing hypotheses exist so that evidence can separate them; identical
    signals mean no observation ever will.
    """
    same = _S(hypotheses=[
        _H(label="A", deny_signals=["Publicly reported shipping movements"], watch_signals=["MMSI changes"]),
        _H(label="B", deny_signals=["Publicly reported shipping movements"], watch_signals=["MMSI changes"]),
        _H(label="C", deny_signals=["Publicly reported shipping movements"], watch_signals=["MMSI changes"]),
    ])
    assert _discriminates_between_hypotheses(same) is False


def test_distinct_signals_are_accepted():
    distinct = _S(hypotheses=[
        _H(label="A", deny_signals=["Port call filed"], watch_signals=["MMSI reappears"]),
        _H(label="B", deny_signals=["Maintenance record published"], watch_signals=["Speed drops below 4kn"]),
    ])
    assert _discriminates_between_hypotheses(distinct) is True


def test_a_single_hypothesis_cannot_fail_discrimination():
    """Nothing to separate it from; a different check handles thin drafts."""
    assert _discriminates_between_hypotheses(_S(hypotheses=[_H()])) is True


# -- the prompt no longer offers text worth copying ---------------------------

def test_the_prompt_uses_slots_not_sample_values():
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    # The old sample values are what the model copied; none may remain in a
    # value position in the prompt template.
    for copied in (
        '"mechanism": "Causal mechanism explaining signal convergence"',
        '"mechanism": "Alternative causal explanation"',
        '"mechanism": "Tail-risk / high-impact alternative"',
        '"beneficiaries": ["key_actor"]',
    ):
        assert copied not in source, f"the prompt still offers {copied} to copy"
    assert '"mechanism": "<how the signals above cause one another' in source
    assert "ANGLE BRACKETS ARE SLOTS TO FILL" in source


def test_synthesis_discards_rather_than_publishes():
    source = (ROOT / "services" / "reasoning" / "scenario_generator.py").read_text(encoding="utf-8")
    assert "if _echoes_the_template(output):" in source
    assert "if not _discriminates_between_hypotheses(output):" in source


# -- a scenario nothing could refute ------------------------------------------

def test_a_scenario_with_no_watch_signals_is_capped():
    """Measured: one scenario in nine came back with three hypotheses --
    "Baseline / Alternative / High-impact Maritime Traffic" -- and not a single
    watch signal between them.

    The tracker will sweep that scenario every thirty minutes for as long as it
    lives and can never confirm or deny it, because nothing was named to look
    for. A claim that cannot be wrong is not a forecast, and it must not carry
    the confidence of one.

    Capped rather than rejected, following the rule the other ceilings follow:
    the hypotheses may still be worth reading.
    """
    from services.reasoning.scenario_generator import (
        UNFALSIFIABLE_CEILING, _supported_confidence,
    )

    class _H:
        def __init__(self, probability, watch):
            self.probability = probability
            self.watch_signals = watch

    class _O:
        def __init__(self, hypotheses):
            self.confidence_overall = 90
            self.hypotheses = hypotheses
            self.confidence_rationale = "a" * 200

    barren = _O([_H(60, []), _H(20, []), _H(20, [])])
    assert _supported_confidence(barren) == UNFALSIFIABLE_CEILING


def test_one_checkable_hypothesis_is_enough_to_lift_the_cap():
    """The cap is for scenarios with nothing to check anywhere, not for a single
    weak hypothesis among several."""
    from services.reasoning.scenario_generator import _supported_confidence

    class _H:
        def __init__(self, probability, watch):
            self.probability = probability
            self.watch_signals = watch

    class _O:
        def __init__(self, hypotheses):
            self.confidence_overall = 90
            self.hypotheses = hypotheses
            self.confidence_rationale = "a" * 200

    assert _supported_confidence(_O([_H(60, ["signal"]), _H(20, [])])) == 90


def test_the_cap_is_the_lowest_of_them():
    """Ranked deliberately: the others describe a weak argument, this one
    describes a claim that cannot be wrong."""
    from services.reasoning.scenario_generator import (
        SINGLE_HYPOTHESIS_CEILING, UNARGUED_CEILING,
        UNFALSIFIABLE_CEILING, UNSEPARATED_CEILING,
    )

    assert UNFALSIFIABLE_CEILING < min(
        SINGLE_HYPOTHESIS_CEILING, UNARGUED_CEILING, UNSEPARATED_CEILING
    )


def test_the_json_example_stays_valid_json():
    """Rule 1 tells the model to return only valid JSON, and the example below
    it is what a 1.5B model actually copies.

    Adding `// comments` and a `"_watch_signals_note"` field to that block to
    explain the watch-signal requirement would have taught it to emit both --
    invalid JSON, and a field no schema declares. Instructions belong in the
    numbered rules; the example belongs to the parser.
    """
    import re

    source = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    block = source[source.index("OUTPUT SCHEMA:"):source.index('"time_horizon": "immediate')]
    assert "//" not in block, "a comment is inside the JSON the model copies"
    assert "_note" not in block, "a pseudo-field is inside the JSON the model copies"


def test_the_watch_signal_requirement_is_stated_in_the_rules():
    """One scenario in nine came back with three hypotheses and no signals."""
    source = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    rules = source[source.index("1. Return ONLY valid JSON"):source.index("OUTPUT SCHEMA:")]
    assert "at least one watch_signal" in rules
    assert "Never a category" in rules
