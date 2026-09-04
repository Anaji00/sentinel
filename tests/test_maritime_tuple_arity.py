"""
tests/test_maritime_tuple_arity.py

Threading one field through this enricher broke it three times.

    Batch enrichment failed for topic events.raw.maritime:
    too many values to unpack (expected 11)   ... then (expected 13)

vessel_position last fired at 18:30:38 and produced nothing for ten and a half
hours -- 24,564 events the day before, zero after. The batch handler catches per
topic and carries on, so the service stayed up, the heartbeat stayed green, and
vessel_static kept flowing on a separate path.

Three distinct sites, each in a different syntactic form:

  * `for (...) in zip(parsed, ...)`            -- updated
  * `for idx, ((...), a, b) in enumerate(zip(parsed, ...))`  -- missed
  * `for (_, _, mmsi, _, ...) in results`      -- missed again

And one that did not raise at all: `results` never carried nav_code, so
`is_restricted_nav_status(nav_code)` read whatever the previous loop had left
bound -- the last vessel's status applied to every vessel in this one. Python
does not complain and a stale integer is a plausible one, so that flag was
wrong rather than absent.

Two regex-based versions of this test passed while the bug was live, because
each pattern matched only the forms its author happened to think of. That is the
vacuous pass this file now exists to prevent, so it walks the AST instead: every
`for` loop whose iterable mentions the collection, whatever shape the target is.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PATH = ROOT / "services" / "enrichment" / "enrichers" / "maritime.py"
TREE = ast.parse(PATH.read_text(encoding="utf-8"))


def _append_arity(name: str) -> int:
    """How many fields the collection is built with."""
    for node in ast.walk(TREE):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "append"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == name
                and node.args
                and isinstance(node.args[0], ast.Tuple)):
            return len(node.args[0].elts)
    raise AssertionError(f"{name}.append((...)) not found")


def _mentions(node, name: str) -> bool:
    return any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node))


def _tuple_targets(target):
    """Every tuple pattern in a loop target, including nested ones."""
    return [n for n in ast.walk(target) if isinstance(n, ast.Tuple)]


def _unpack_arities(name: str):
    """(line, arity) for every destructuring of rows from `name`."""
    found = []
    for node in ast.walk(TREE):
        if not isinstance(node, (ast.For, ast.AsyncFor)):
            continue
        if not _mentions(node.iter, name):
            continue
        for tup in _tuple_targets(node.target):
            # The row pattern is the one sized like a row, not the outer
            # (idx, row) wrapper.
            if len(tup.elts) > 3:
                found.append((tup.lineno, len(tup.elts)))
    return found


def test_every_parsed_unpack_matches_the_tuple_it_unpacks():
    expected = _append_arity("parsed")
    bad = [(line, n) for line, n in _unpack_arities("parsed") if n != expected]
    assert not bad, f"parsed is built with {expected} fields, unpacked with {bad}"


def test_every_results_unpack_matches_the_tuple_it_unpacks():
    expected = _append_arity("results")
    bad = [(line, n) for line, n in _unpack_arities("results") if n != expected]
    assert not bad, f"results is built with {expected} fields, unpacked with {bad}"


def test_the_walk_finds_every_known_site():
    """Two earlier versions of this test passed while the bug was live, because
    their pattern matched only some forms. If this count drops, the guard has
    gone blind again rather than the code having got simpler."""
    assert len(_unpack_arities("parsed")) >= 3, "parsed sites under-counted"
    assert len(_unpack_arities("results")) >= 2, "results sites under-counted"


def test_nav_code_is_carried_rather_than_inherited():
    """A field read from the enclosing scope does not raise; it goes stale."""
    source = PATH.read_text(encoding="utf-8")
    header = source.split(") in results:")[0].split("for (raw, meta, mmsi")[-1]
    assert "nav_code" in header


def test_nav_code_reaches_the_scoring_loop():
    source = PATH.read_text(encoding="utf-8")
    assert "is_restricted_nav_status(nav_code)" in source
    assert 'nav_code = pos.get("NavigationalStatus")' in source
