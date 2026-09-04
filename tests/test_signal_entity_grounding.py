"""A signal must name something the cluster actually contains.

Measured across 48 hours of stored scenarios: 353 distinct watch-signal
entities, and 201 of them -- 57% -- named something this platform has never
observed. They fall into a few shapes, all of them the model writing rather than
reading:

  invented placeholders   XYZ Corp, Exchange A, Vessel X, JKL Wallet
  forbidden categories    INSIDER, CYBER THREAT, Stablecoin Usage
  mangled tickers         "PIP R" for PIPR, ADBES
  observables as entities AIS call volume

The tracker resolves a signal by indexed entity lookup, so every one of these is
a sweep that can never match, and the scenario carrying them can never be
confirmed or denied through them.

The system prompt already states the rule -- "Never a category" -- and is
ignored more than half the time. A rule worth stating to the model is worth
enforcing on its output.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.reasoning.scenario_generator import (  # noqa: E402
    UNFALSIFIABLE_CEILING, _known_entity_tokens, _prune_unresolvable_signals,
    _supported_confidence,
)


class _Sig:
    def __init__(self, entity):
        self.entity = entity
        self.observable = "volume"
        self.comparator = "above"
        self.threshold = 5.0
        self.unit = "%"


class _Hyp:
    def __init__(self, watch=(), deny=(), probability=50):
        self.probability = probability
        self.watch_signals = list(watch)
        self.deny_signals = list(deny)


class _Out:
    def __init__(self, hypotheses):
        self.hypotheses = hypotheses
        self.confidence_overall = 90
        self.confidence_rationale = "a" * 200


class _Cluster:
    entity_ids = ["AZO", "DELL", "PIPR"]
    entity_names = ["AutoZone"]


def _known():
    return _known_entity_tokens(_Cluster(), [{"entity_id": "SNDK"}])


def test_the_cluster_entities_are_collected():
    known = _known()
    assert {"azo", "dell", "pipr", "autozone", "sndk"} <= known


def test_a_real_entity_survives():
    out = _Out([_Hyp(watch=[_Sig("AZO")], deny=[_Sig("DELL")])])
    removed = _prune_unresolvable_signals(out, _known())
    assert removed == 0
    assert len(out.hypotheses[0].watch_signals) == 1


def test_the_mangled_ticker_is_dropped():
    """PIPR arrived as "PIP R" in a live scenario, in beneficiaries and as a
    watch entity the tracker would sweep for forever."""
    out = _Out([_Hyp(watch=[_Sig("PIP R")])])
    assert _prune_unresolvable_signals(out, _known()) == 1
    assert out.hypotheses[0].watch_signals == []


def test_invented_placeholders_are_dropped():
    for name in ("XYZ Corp", "Exchange A", "Vessel X", "JKL Wallet", "CVE-123456789"):
        out = _Out([_Hyp(watch=[_Sig(name)])])
        assert _prune_unresolvable_signals(out, _known()) == 1, name


def test_categories_are_dropped():
    for name in ("INSIDER", "CYBER THREAT", "Stablecoin Usage", "AIS call volume"):
        out = _Out([_Hyp(watch=[_Sig(name)])])
        assert _prune_unresolvable_signals(out, _known()) == 1, name


def test_deny_signals_are_pruned_too():
    out = _Out([_Hyp(watch=[_Sig("AZO")], deny=[_Sig("Exchange A")])])
    assert _prune_unresolvable_signals(out, _known()) == 1
    assert out.hypotheses[0].deny_signals == []


def test_a_hypothesis_stripped_bare_is_priced_by_the_existing_ceiling():
    """Pruning does not invent a judgement; it lets the ceilings do their work."""
    out = _Out([_Hyp(watch=[_Sig("XYZ Corp")]), _Hyp(watch=[_Sig("Exchange A")])])
    _prune_unresolvable_signals(out, _known())
    assert _supported_confidence(out) <= UNFALSIFIABLE_CEILING


def test_nothing_is_pruned_when_the_cluster_has_no_entities():
    """Without a reference set, everything would look unresolvable; refusing to
    guess is better than stripping every signal off every scenario."""
    out = _Out([_Hyp(watch=[_Sig("AZO")])])
    assert _prune_unresolvable_signals(out, set()) == 0
    assert len(out.hypotheses[0].watch_signals) == 1


def test_matching_is_case_insensitive_but_not_substring():
    """"AS" inside "increased" is how an autonomous-system number once ended up
    in front of a maritime headline."""
    out = _Out([_Hyp(watch=[_Sig("azo")])])
    assert _prune_unresolvable_signals(out, _known()) == 0

    out = _Out([_Hyp(watch=[_Sig("ZO")])])
    assert _prune_unresolvable_signals(out, _known()) == 1


def test_the_pruning_is_actually_wired_into_generation():
    """The helper above is exercised directly, so deleting the call site would
    leave every test in this file passing while nothing pruned anything."""
    source = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
    calls = [
        line for line in source.splitlines()
        if "_prune_unresolvable_signals(" in line
        and not line.strip().startswith(("#", "def "))
    ]
    assert calls, "_prune_unresolvable_signals is defined and never invoked"
    assert "_known_entity_tokens(cluster, raw_events)" in source
