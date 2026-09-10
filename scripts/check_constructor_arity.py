"""Is this reachable *with the arguments it needs*?

`check_reachability.py` answers whether code is on an execution path. It cannot
answer the question that cost this audit three closed-and-inert mechanisms in a
single pass:

    class ScenarioTracker:
        def __init__(self, db, producer=None, redis=None):
            self._redis = redis

    tracker = ScenarioTracker(db, tracker_producer)      # two arguments

`self._redis` was None for the life of the service. Every path needing it
returned early and silently -- the Phase 4.12 calibration write, the
open-questions offer, the resolved-history backfill. Three mechanisms reported
closed in this audit, none of them saying anything, and the visible end of it was
"562 resolved scenarios, 0 calibration samples".

Nothing catches this. The call is valid Python. The unit tests drive the class
directly and pass, because they construct it themselves with everything it
wants. pyflakes sees a correct call. The reachability check sees a reachable
class. The defect lives in the gap between the signature and one call site.

What this reports: a constructor invoked without a parameter that defaults to
`None`, where the class stores it on `self` and reads it elsewhere. Only `None`
counts -- it means absent, and absence changes behaviour, while a default of 30
or 5.0 is a setting nobody was obliged to override. Measured on this tree,
reporting every defaulted parameter gave 61 findings and almost all were
`max_backoff` and `queue_size` on a websocket client; restricting to `None`
gives 2, both explainable. A guard with false positives is one people learn to
skip, which is the failure this check exists to prevent.

    $ python scripts/check_constructor_arity.py
"""
import ast
import pathlib
import sys
from typing import Dict, List, Optional, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCANNED = ("services", "shared")


def _iter_files():
    for base in SCANNED:
        for f in (ROOT / base).rglob("*.py"):
            if "__pycache__" in str(f):
                continue
            yield f


def _classes(tree: ast.AST) -> Dict[str, ast.ClassDef]:
    return {n.name: n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}


def _init_of(cls: ast.ClassDef) -> Optional[ast.FunctionDef]:
    for n in cls.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == "__init__":
            return n
    return None


def _params(init) -> List[Tuple[str, bool]]:
    """(name, defaults_to_none) for every parameter after self.

    Only a `None` default counts. `None` means absent, and absence changes what
    the class does -- it is the difference between a dependency that was not
    supplied and a setting that was not overridden. A parameter defaulting to
    30 or 5.0 or "" is a deliberate value, and reporting those buries the one
    case that matters under sixty that do not.

    Measured on this tree: reporting every defaulted parameter gave 61
    findings, almost all of them `max_backoff`, `ping_timeout` and `queue_size`
    on a websocket client. Restricting to None gives the real ones. A guard with
    false positives is one people learn to skip, which is the failure this whole
    check exists to avoid.
    """
    args = init.args.args[1:]
    ndef = len(init.args.defaults)
    first_default = len(args) - ndef
    out = []
    for i, a in enumerate(args):
        if i < first_default:
            out.append((a.arg, False))
            continue
        default = init.args.defaults[i - first_default]
        is_none = isinstance(default, ast.Constant) and default.value is None
        out.append((a.arg, is_none))
    return out


def _attribute_for(init, param: str) -> Optional[str]:
    """The `self.x` a parameter is stored on, if it is stored at all."""
    for node in ast.walk(init):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name) and node.value.id == param:
            for t in node.targets:
                if isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self":
                    return t.attr
    return None


def _reads_attribute(cls: ast.ClassDef, attr: str, init) -> bool:
    """Whether the class body reads that attribute outside __init__."""
    for node in ast.walk(cls):
        if node is init:
            continue
        if isinstance(node, ast.Attribute) and node.attr == attr:
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                # An assignment target is a write, not a read.
                parent_is_store = isinstance(getattr(node, "ctx", None), ast.Store)
                if not parent_is_store:
                    return True
    return False


def scan() -> List[str]:
    # Index every class that defines __init__, across the tree.
    defs: Dict[str, Tuple[pathlib.Path, ast.ClassDef]] = {}
    trees: Dict[pathlib.Path, ast.AST] = {}
    for f in _iter_files():
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        trees[f] = tree
        for name, cls in _classes(tree).items():
            if _init_of(cls) is not None:
                defs[name] = (f, cls)

    findings: List[str] = []
    for f, tree in trees.items():
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
                continue
            target = defs.get(node.func.id)
            if target is None:
                continue
            _src_file, cls = target
            init = _init_of(cls)
            params = _params(init)
            if init.args.vararg or init.args.kwarg:
                continue  # *args/**kwargs: arity is not knowable here

            supplied = set()
            for i, _a in enumerate(node.args):
                if i < len(params):
                    supplied.add(params[i][0])
            supplied.update(k.arg for k in node.keywords if k.arg)

            for name, defaults_to_none in params:
                if name in supplied or not defaults_to_none:
                    continue
                attr = _attribute_for(init, name)
                if attr is None:
                    continue  # never stored: nothing downstream can miss it
                if not _reads_attribute(cls, attr, init):
                    continue  # stored and never read: genuinely optional
                findings.append(
                    f"{f.relative_to(ROOT)}:{node.lineno}: "
                    f"{node.func.id}(...) omits `{name}`, which {cls.name} stores as "
                    f"`self.{attr}` and reads elsewhere in the class"
                )
    return sorted(set(findings))


def main() -> int:
    findings = scan()
    if not findings:
        print("constructor arity: every dependency a class reads is supplied where it is built.")
        return 0
    print(f"constructor arity: {len(findings)} call(s) omitting a dependency the class uses:\n")
    for f in findings:
        print("  " + f)
    print(
        "\nA parameter defaulting to None makes every path that needs it return early "
        "and silently. That is how three mechanisms closed in this audit ran inert "
        "while their unit tests passed."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
