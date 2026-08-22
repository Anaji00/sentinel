"""Static sweep for the bug class behind the cyber outage: references to enum
members and model fields that do not exist. These raise only when one specific
runtime path executes, so they survive tests and kill services in production.

Resolves local aliases (`sec = e.security_data; sec.severity`) -- the form the
real crash took, and the form a naive attribute check misses."""
import ast, pathlib, sys
from enum import Enum
import shared.models.events as ev
from pydantic import BaseModel

ENUMS  = {n: o for n, o in vars(ev).items() if isinstance(o, type) and issubclass(o, Enum)}
MODELS = {n: o for n, o in vars(ev).items() if isinstance(o, type) and issubclass(o, BaseModel)}

# payload attribute name -> model class, e.g. "security_data" -> SecurityData
BY_ATTR = {}
for m in MODELS.values():
    for f, info in m.model_fields.items():
        if not f.endswith("_data"):
            continue
        ann = str(info.annotation)
        for mn, mc in MODELS.items():
            if f"events.{mn}" in ann or ann.endswith(mn) or f"[{mn}]" in ann:
                BY_ATTR[f] = mc

def check(path, src):
    out = []
    try: tree = ast.parse(src)
    except SyntaxError: return out
    # local aliases: name = <anything>.<payload_attr>
    alias = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign) and isinstance(n.value, ast.Attribute):
            if n.value.attr in BY_ATTR:
                for t in n.targets:
                    if isinstance(t, ast.Name):
                        alias[t.id] = BY_ATTR[n.value.attr]
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and not n.attr.startswith("_"):
            base, model = None, None
            if isinstance(n.value, ast.Name):
                if n.value.id in ENUMS:                       # EnumName.MEMBER
                    E = ENUMS[n.value.id]
                    if not hasattr(E, n.attr):
                        out.append((n.lineno, f"{n.value.id}.{n.attr} is not a member"))
                    continue
                if n.value.id in alias:                        # aliased payload
                    base, model = n.value.id, alias[n.value.id]
            elif isinstance(n.value, ast.Attribute) and n.value.attr in BY_ATTR:
                base, model = n.value.attr, BY_ATTR[n.value.attr]   # e.payload.field
            if model and n.attr not in model.model_fields and not hasattr(model, n.attr):
                out.append((n.lineno, f"{base}.{n.attr} not on {model.__name__}"))
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in MODELS:
            M = MODELS[n.func.id]
            for kw in n.keywords:
                if kw.arg and kw.arg not in M.model_fields:
                    out.append((n.lineno, f"{M.__name__}(...) has no field '{kw.arg}'"))
    return out

if __name__ == "__main__":
    targets = [pathlib.Path(a) for a in sys.argv[1:]] or \
        list(pathlib.Path("services").rglob("*.py")) + list(pathlib.Path("shared").rglob("*.py"))
    total = 0
    for f in targets:
        for ln, msg in sorted(set(check(f, f.read_text(encoding="utf-8")))):
            print(f"{f}:{ln}: {msg}"); total += 1
    print(f"\n{total} issue(s); payload models resolved: {len(BY_ATTR)}")
    sys.exit(1 if total else 0)
