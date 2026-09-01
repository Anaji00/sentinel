"""
tests/test_graph_node_identity.py

139,047 wallet nodes spelled `0X...` beside 6,366 spelled `0x...`.

The graph supervisor was corrected to canonicalise every node id it writes, and
it does. The knowledge-graph engine, which *produces* the proposals the
supervisor consumes, went on calling `.upper()` first. Measured on the live
graph:

    MATCH (n) WHERE n.name STARTS WITH '0X'  ->  139,047
    MATCH (n) WHERE n.name STARTS WITH '0x'  ->    6,366

    "0xaa8ba7d4611437141192e7ceced531bc0a133efb" present as BOTH
    ["0XAA8BA7D4611437141192E7CECED531BC0A133EFB",
     "0xaa8ba7d4611437141192e7ceced531bc0a133efb"]

The first version of this file asserted the wrong reason and failed, which is
the useful part. WALLET *lower-cases* -- EIP-55 checksum casing is a validation
artifact, not identity -- so had these nodes been labelled `Wallet`, the rule
would have undone the `.upper()` and there would be no defect.

They are labelled `Entity`. That maps to UNKNOWN, and UNKNOWN preserves the
string exactly as given, which is the correct answer when we cannot say what
kind of identifier we hold. The generic label is what made the mangling
permanent: neither the pre-mangling producer nor the preserving rule does this
alone, which is why correcting the writer did not correct it.

The rule now lives on the model, with both writers importing it, because when it
lived beside one writer the other one went on doing something else.
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.models.events import EntityType, canonical_entity_id, graph_node_id  # noqa: E402

WALLET = "0xAA8BA7D4611437141192E7CECED531BC0A133EFB"


# -- the rule itself -----------------------------------------------------------

def test_a_wallet_keeps_its_own_casing_rule():
    assert graph_node_id(WALLET, "Wallet") == WALLET.lower()


def test_a_known_label_recovers_from_upper_casing():
    """Why the defect is not simply "the producer upper-cased"."""
    assert graph_node_id(WALLET.upper(), "Wallet") == graph_node_id(WALLET, "Wallet")


def test_a_generic_label_cannot_recover_from_it():
    """The actual defect: UNKNOWN preserves, so the mangling becomes the
    identity. All 139,047 corrupted nodes carry the label `Entity`."""
    assert graph_node_id(WALLET.upper(), "Entity") != graph_node_id(WALLET, "Entity")
    assert graph_node_id(WALLET, "Entity") == WALLET


def test_the_graph_agrees_with_the_database():
    """A join across the two stores is the whole point of canonicalising."""
    assert graph_node_id(WALLET, "Wallet") == canonical_entity_id(WALLET, EntityType.WALLET)


@pytest.mark.parametrize(
    "label,raw,expected",
    [
        ("Company", "  nvidia corp  ", "NVIDIA CORP"),
        ("Instrument", "SI=F ($59.06)", "SI=F"),
        ("Wallet", WALLET, WALLET.lower()),
        ("Vessel", "MMSI:311000123", "311000123"),
        # The venue mints these; we quote them back exactly as issued.
        ("PredictionMarket", "Will-X-Happen", "Will-X-Happen"),
    ],
)
def test_each_label_gets_its_own_rule(label, raw, expected):
    assert graph_node_id(raw, label) == expected


def test_an_unmapped_label_preserves_the_string():
    """The safe answer when we cannot say what kind of identifier we hold. It
    must not fall back to upper-casing, which is what it replaced."""
    assert graph_node_id("0xAbC", "SomethingNew") == "0xAbC"
    assert graph_node_id("0xAbC", "Entity") == "0xAbC"


@pytest.mark.parametrize("label", [None, "", "  "])
def test_a_missing_label_does_not_raise(label):
    assert graph_node_id("NVDA", label) == "NVDA"


def test_the_rule_is_idempotent():
    """Both writers apply it, so it runs more than once on the same value."""
    once = graph_node_id(WALLET, "Wallet")
    assert graph_node_id(once, "Wallet") == once


# -- and that both writers actually use it -------------------------------------

def _calls_upper_on_an_identifier(path: Path) -> list:
    """`.upper()` applied to something that looks like an entity id."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "upper":
            continue
        target = node.func.value
        name = None
        if isinstance(target, ast.Name):
            name = target.id
        elif isinstance(target, ast.Attribute):
            name = target.attr
        if name and any(k in name.lower() for k in ("entity", "subject", "object", "id", "name")):
            found.append((name, node.lineno))
    return found


def test_the_producer_no_longer_upper_cases_identifiers():
    """The supervisor was fixed and the corruption continued, because the
    supervisor is not where it started."""
    offenders = _calls_upper_on_an_identifier(
        ROOT / "services/agents/knowledge_graph_engine.py"
    )
    assert not offenders, f"identifiers upper-cased before canonicalisation: {offenders}"


def test_the_supervisor_no_longer_upper_cases_identifiers():
    offenders = _calls_upper_on_an_identifier(ROOT / "services/agents/supervisor.py")
    assert not offenders, f"identifiers upper-cased before canonicalisation: {offenders}"


@pytest.mark.parametrize(
    "module",
    ["services/agents/knowledge_graph_engine.py", "services/agents/supervisor.py"],
)
def test_both_writers_import_the_shared_rule(module):
    source = (ROOT / module).read_text(encoding="utf-8")
    assert "from shared.models.events import" in source
    assert "graph_node_id" in source


def test_the_rule_has_one_definition():
    """It had one caller and one copy; the second writer is why it moved."""
    hits = [
        path for path in (ROOT / "services").rglob("*.py")
        if "def graph_node_id" in path.read_text(encoding="utf-8")
    ]
    assert not hits, f"a second copy of the rule exists: {hits}"


def test_the_label_map_moved_with_it():
    """A rule split from its lookup table is two rules."""
    from shared.models.events import _LABEL_TO_ENTITY_TYPE

    assert _LABEL_TO_ENTITY_TYPE["wallet"] is EntityType.WALLET
    supervisor = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    assert "_LABEL_TO_ENTITY_TYPE = {" not in supervisor


# -- the repair ----------------------------------------------------------------

def test_the_repair_selects_by_corruption_not_by_type():
    """136,838 of the 139,047 needed only a rename; routing all of them through
    the merge path would have cost hours to achieve the same thing. An earlier
    backfill in this system selected 1.4M rows by type when 5,728 actually
    differed."""
    script = (ROOT / "scripts/repair_graph_entity_casing.py").read_text(encoding="utf-8")
    assert "NOT EXISTS { MATCH (l:Entity {name: toLower(u.name)}) }" in script
    assert "apoc.refactor.mergeNodes" in script


def test_the_repair_is_idempotent():
    """Both phases select on the corruption itself, so a second run is a no-op."""
    script = (ROOT / "scripts/repair_graph_entity_casing.py").read_text(encoding="utf-8")
    assert "STARTS WITH $prefix" in script
    assert "--dry-run" in script


def test_the_repair_transfers_relationships_rather_than_dropping_them():
    """A merge that deletes the duplicate without rewiring its edges silently
    loses everything the wrong-cased node was connected to."""
    script = (ROOT / "scripts/repair_graph_entity_casing.py").read_text(encoding="utf-8")
    assert "mergeRels: true" in script


# -- a name bound in a sibling branch is not bound in this one ------------------

def test_the_tag_branch_uses_the_label_it_binds():
    """`cannot access local variable 'source_label'`, on every tag proposal.

    The tag handler binds `label` and its Cypher uses `{label}`, but the
    parameter it passed named `source_label` -- which is bound only inside the
    two link-handling branches of the same method. Python does not object until
    the branch runs, and when it did the exception surfaced as "Neo4j commit
    failed", which reads like the database refused the write rather than like
    the write was never built.
    """
    import ast

    tree = ast.parse((ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8"))
    method = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.AsyncFunctionDef) and n.name == "execute_proposal"
    )

    # Every branch that calls graph_node_id must bind the label name it passes.
    bound = {
        t.id
        for node in ast.walk(method)
        if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name)
    }
    used = {
        node.args[1].id
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "graph_node_id"
        and len(node.args) > 1
        and isinstance(node.args[1], ast.Name)
    }
    assert used <= bound, f"graph_node_id is passed names this method never binds: {used - bound}"


def test_the_tag_cypher_and_its_parameter_agree():
    """The MERGE interpolates one label; the id must be spelled for that label."""
    source = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    assert 'graph_node_id(entity_id, label), "new_tags": tags' in source
    assert 'graph_node_id(entity_id, source_label), "new_tags"' not in source
