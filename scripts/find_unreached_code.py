"""
scripts/find_unreached_code.py

Finds functions that are defined and never referenced.

This audit kept finding the same defect and finding it one instance at a time:

    release()                frees the inference slot early -- never called
    try_acquire(score=...)   a parameter that decided admission -- never read
    RadarAgent               the one agent that reliably won a slot, and the
                             only one not wired to publish_bulletin
    get_entity_centrality()  weights cluster severity by graph centrality --
                             never called, so that weighting does not happen

None of these were broken. Each was written, several were tested, and none ran.
That is harder to notice than a bug, because everything that *does* run keeps
working and the missing behaviour has no failure to report.

    $ python scripts/find_unreached_code.py
    $ python scripts/find_unreached_code.py --include-decorated

READ THIS BEFORE ACTING ON THE OUTPUT. It is a lead list, not a defect list.
Three consecutive findings from the first run of this script were false alarms:

  * `install_redaction()` has no callers -- but `RedactingFilter`, the thing it
    installs, is attached directly in shared/utils/logging.py. Redaction works.
  * `fetch_and_sync_ofac_sdn_list()` has no callers -- but `_ofac_sync_loop()`
    in the enrichment service duplicates its body and runs on startup. The live
    automaton holds 39,085 keywords.
  * `get_entity_centrality()` looked broken for a second reason -- a lookup on
    `{id: ...}` when the graph is keyed on `name` -- until the graph showed both
    properties present on 146,970 of 146,993 nodes.

So each candidate needs its own check: a duplicate implementation elsewhere, a
dynamic dispatch, a decorator, a registry keyed by string. What the script gives
you is the short list worth checking, which is the part that does not scale by
hand across 833 functions.
"""

import argparse
import ast
import collections
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
SKIP_DIRS = {"__pycache__", ".git", "node_modules", "frontend", ".next", "dist"}


def _modules(include_tests: bool = True):
    for path in ROOT.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if not include_tests and "tests" in path.parts:
            continue
        yield path


def _definitions(include_decorated: bool):
    """name -> [(path, lineno)], for functions defined in production code.

    Decorated functions are excluded by default: a FastAPI route, a pydantic
    validator and a pytest fixture are all called by their decorator rather than
    by name, and including them buries the real candidates. The first run of
    this script reported 142 orphans, of which roughly a hundred were routes.
    """
    found = {}
    for path in _modules(include_tests=False):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("__"):
                continue
            if node.decorator_list and not include_decorated:
                continue
            found.setdefault(node.name, []).append(
                (str(path.relative_to(ROOT)), node.lineno)
            )
    return found


def _references():
    """How often each name is used, counting the whole tree including tests.

    Attribute access counts as well as calls: `self._budget.finish` passed as a
    callback is a use, and a name that appears only in its own `def` line does
    not register here at all -- which is exactly the signal being looked for.
    """
    counts = collections.Counter()
    literals = collections.Counter()
    for path in _modules():
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    counts[func.id] += 1
                elif isinstance(func, ast.Attribute):
                    counts[func.attr] += 1
            elif isinstance(node, ast.Name):
                counts[node.id] += 1
            elif isinstance(node, ast.Attribute):
                counts[node.attr] += 1
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                # getattr(obj, "name"), registries, Celery-style task names.
                literals[node.value] += 1
    return counts, literals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-decorated", action="store_true",
        help="also report decorated functions (routes, validators, fixtures)",
    )
    args = parser.parse_args()

    definitions = _definitions(args.include_decorated)
    counts, literals = _references()

    unreached = [
        (name, sites)
        for name, sites in definitions.items()
        if counts.get(name, 0) == 0 and literals.get(name, 0) == 0
    ]
    unreached.sort(key=lambda item: item[1][0])

    print(f"{len(definitions)} functions considered, {len(unreached)} never referenced\n")
    for name, sites in unreached:
        path, line = sites[0]
        extra = f"  (+{len(sites) - 1} more definitions)" if len(sites) > 1 else ""
        print(f"  {path}:{line}  {name}{extra}")

    if unreached:
        print("\nEach of these needs checking individually -- a duplicate "
              "implementation, a dynamic dispatch or a registry entry all look "
              "identical to dead code from here. See this file's docstring.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
