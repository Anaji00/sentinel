"""Kinematic scoring goes to its own domain's detector, not to a guess.

`AnomalyScorer` constructs eight independent RRCF detectors, one per domain.
The kinematic path chose between two of them by looking at the entity id:

    domain = "aviation" if sample_entity.startswith("icao")
                           or "adsb" in sample_entity else "maritime"

An aviation entity is a bare ICAO24 hex code. Read from the live table:
`78927f`, `89916c`, `885963`. None of them starts with "icao" and none
contains "adsb", so every aviation batch resolved to "maritime" and the
aviation detector -- constructed, configured, never removed -- has scored
nothing since it was added.

It did not fail quietly. Maritime builds an 8-feature vector and aviation a
6-feature one, so every aviation batch raised

    operands could not be broadcast together with shapes (6,) (8,)

which the enricher caught and turned into a floor score for every aircraft in
the batch. Observed in the running deployment at roughly one failure per
second across the aviation stream.

The shape mismatch is the lucky half. Had the two domains happened to agree on
a feature count, aviation would have been fed into the maritime detector's
trees and nothing would have raised at all.
"""
from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCORER = ROOT / "services" / "enrichment" / "anomaly_scorer.py"
AVIATION = ROOT / "services" / "enrichment" / "enrichers" / "aviation.py"
MARITIME = ROOT / "services" / "enrichment" / "enrichers" / "maritime.py"


def _code(path: Path) -> str:
    """Source with comments and docstrings removed.

    Prose explaining this defect quotes the defect, and a check that cannot
    tell an explanation from an occurrence is the trap this audit has walked
    into repeatedly.
    """
    text = path.read_text(encoding="utf-8")
    text = re.sub(r'"""(?:.|\n)*?"""', "", text)
    text = re.sub(r"^\s*#.*$", "", text, flags=re.M)
    return text


@pytest.mark.parametrize(
    "path, expected",
    [
        pytest.param(AVIATION, "aviation", id="aviation"),
        pytest.param(MARITIME, "maritime", id="maritime"),
    ],
)
def test_each_enricher_names_its_own_domain(path: Path, expected: str) -> None:
    """The caller knows which domain it is. Nothing else can tell."""
    code = _code(path)
    assert "score_kinematic_event_batch(" in code, f"{path.name} no longer scores kinematics"
    call = code.split("score_kinematic_event_batch(", 1)[1]
    call = call[: call.index(")")]
    assert f'domain="{expected}"' in call or f"domain='{expected}'" in call, (
        f"{path.name} does not tell the scorer which domain it is, so the scorer "
        f"has to guess from an entity id -- which is how every aircraft ended up "
        f"in the maritime detector"
    )


def test_an_icao24_code_is_not_recognisable_as_aviation() -> None:
    """The premise of the old guess, stated as a test.

    These are real primary_entity_id values for `flight_position` rows. If a
    future guess is reintroduced, this says why it cannot work.
    """
    live_icao24 = ["78927f", "89916c", "885963", "899121"]
    for code in live_icao24:
        assert not code.lower().startswith("icao")
        assert "adsb" not in code.lower()


def test_the_scorer_refuses_rather_than_borrowing_another_detector() -> None:
    """Falling back mixes two populations in one tree.

    The previous line was `self._rrcf_detectors.get(domain) or
    self._rrcf_detectors.get("maritime")`, which is the same defect expressed
    as a default: an unknown domain silently became maritime.
    """
    code = _code(SCORER)
    assert 'self._rrcf_detectors.get(domain) or' not in code, (
        "an unknown domain must not borrow the maritime detector"
    )
    assert "No RRCF detector for kinematic domain" in code, (
        "a missing detector must be refused and said, not substituted"
    )


def test_the_domain_is_a_parameter_of_the_batch_scorer() -> None:
    tree = ast.parse(SCORER.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "score_kinematic_event_batch":
            names = [a.arg for a in node.args.args] + [a.arg for a in node.args.kwonlyargs]
            assert "domain" in names, "the caller cannot say which domain this is"
            return
    raise AssertionError("score_kinematic_event_batch not found")


def test_every_constructed_detector_is_reachable() -> None:
    """A detector nobody can route to is a detector that scores nothing.

    Eight are constructed. This does not assert all eight are used by the
    kinematic path -- only the two are -- but it does assert that the two the
    kinematic path names both exist, which is what the guess got wrong.
    """
    code = _code(SCORER)
    block = code.split("_rrcf_detectors: Dict[str, RRCFDetector] = {", 1)[1]
    block = block[: block.index("}")]
    declared = set(re.findall(r'"([a-z]+)":', block))
    assert {"aviation", "maritime"} <= declared, (
        f"the kinematic path routes to aviation and maritime; declared: {sorted(declared)}"
    )


def test_every_detector_says_which_one_it_is() -> None:
    """The tree-reset warning could not be acted on.

    `RRCFDetector` had no `name` attribute and the warning read
    `getattr(self, "name", "unnamed")`, so all eight domain detectors logged
    as 'unnamed'. Observed in the running deployment:

        RRCF tree reset (1 total) in detector 'unnamed': ... A reset tree
        contributes nothing until it refills, so the forest is running below
        its configured size.

    That message is written to be acted on, and an operator reading it could
    not tell whether maritime, crypto or cyber had degraded.
    """
    detectors = ROOT / "shared" / "utils" / "streaming_detectors.py"
    code = _code(detectors)
    assert "self.name = name" in code, "a detector cannot say which one it is"
    assert 'getattr(self, "name", "unnamed")' not in code, (
        "the fallback was the only path; nothing ever set a name"
    )

    # Parsed, not matched. `RRCFDetector\(([^)]*)\)` stops at the first
    # closing paren, which in one of these constructions belongs to a nested
    # `getattr(...)` -- so the regex read a truncated argument list and
    # reported a named detector as unnamed. A call site is a tree, not a
    # string.
    tree = ast.parse(SCORER.read_text(encoding="utf-8"))
    constructions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "RRCFDetector"
    ]
    assert constructions, "no detectors constructed"
    unnamed = [
        node.lineno
        for node in constructions
        if not any(kw.arg == "name" for kw in node.keywords)
    ]
    assert not unnamed, f"detector(s) constructed without a name at line(s) {unnamed}"
