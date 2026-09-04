"""Static sweep for code that runs in the wrong context, or in none.

`find_unreached_code.py` already answers "is this function ever referenced".
This answers the question that survived it: *given that something calls it, can
it work where it is called from?* Three repairs during the September audit were
written, reviewed, verified against live data and did nothing:

  * a graph type backfill placed inside `start_supervisor()`, reachable only
    from that module's `__main__` -- and no compose service runs that file, so
    the function had a caller and the caller had no runtime

  * a Redis write via `safe_create_task` inside a *synchronous* function that
    its caller offloads with `run_in_executor`. There is no event loop on a
    worker thread, so task creation raised into an except that logged at debug,
    and the store stayed empty while the surrounding work reported success

  * an insertion that landed between `@staticmethod` and its `def`, rebinding
    the decorator to the new neighbour. The file stayed syntactically perfect
    and the original method silently became an instance method whose signature
    still declared one argument -- `takes 1 positional argument but 2 were
    given`, on every call, for as long as it took someone to read the log

A reference check passes all three: each function is called. A unit test passes
all three: called directly, each behaves. They fail only in the arrangement the
deployment uses, which is the one thing neither exercises.

    $ python scripts/check_reachability.py
"""
import ast
import pathlib
import sys

# Only services: a script *should* have a __main__ and no compose entry, that
# being what a script is.
ENTRYPOINT_ROOTS = ("services",)
ALL_ROOTS = ("services", "shared")


def _files(roots):
    for root in roots:
        for f in pathlib.Path(root).rglob("*.py"):
            if "__pycache__" not in str(f):
                yield f


def _parse(f):
    """compile() first, then parse.

    ast.parse builds a tree for `continue` outside a loop and reports the file
    clean; compile() raises. That difference crash-looped the correlation
    service during the audit this check comes from, and every static pass in it
    had been using the permissive one.
    """
    src = f.read_text(encoding="utf-8")
    compile(src, str(f), "exec")
    return ast.parse(src)


def orphan_entrypoints(trees, compose_text):
    """A __main__ block in a service file no compose entry runs."""
    out = []
    for f, tree in trees.items():
        if not any(r in str(f).replace("\\", "/") for r in ENTRYPOINT_ROOTS):
            continue
        has_main = any(
            isinstance(n, ast.If) and "__main__" in ast.dump(n.test)
            for n in tree.body
        )
        if not has_main:
            continue
        posix = str(f).replace("\\", "/")
        if posix in compose_text or f.name in compose_text:
            continue
        out.append((f, 1, f"__main__ entry point that no compose service runs -- "
                          f"anything reachable only from here does not execute"))
    return out


def thread_offloaded_async(trees):
    """create_task or await inside a sync function some caller offloads."""
    offloaded = set()
    for tree in trees.values():
        for n in ast.walk(tree):
            if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "run_in_executor":
                for arg in n.args[1:]:
                    if isinstance(arg, (ast.Name, ast.Attribute)):
                        offloaded.add(getattr(arg, "id", None) or arg.attr)
                    elif isinstance(arg, ast.Call) and arg.args:
                        a0 = arg.args[0]
                        nm = getattr(a0, "id", None) or getattr(a0, "attr", None)
                        if nm:
                            offloaded.add(nm)

    out = []
    for f, tree in trees.items():
        for n in ast.walk(tree):
            if not isinstance(n, ast.FunctionDef) or n.name not in offloaded:
                continue  # sync defs only; an async one cannot be offloaded
            for inner in ast.walk(n):
                if isinstance(inner, ast.Await):
                    out.append((f, inner.lineno,
                                f"await inside {n.name}(), which a caller offloads to a thread"))
                elif isinstance(inner, ast.Call):
                    nm = getattr(inner.func, "id", None) or getattr(inner.func, "attr", None)
                    if nm in ("create_task", "safe_create_task", "ensure_future"):
                        out.append((f, inner.lineno,
                                    f"{nm}() inside {n.name}(), which a caller offloads to a "
                                    f"thread -- no event loop runs there"))
    return out


def displaced_decorators(trees):
    """@staticmethod on a method taking self, or a method that lost one."""
    out = []
    for f, tree in trees.items():
        for cls in ast.walk(tree):
            if not isinstance(cls, ast.ClassDef):
                continue
            for b in cls.body:
                if not isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                names = {d.id for d in b.decorator_list if isinstance(d, ast.Name)}
                first = b.args.args[0].arg if b.args.args else None
                if "staticmethod" in names and first == "self":
                    out.append((f, b.lineno,
                                f"{b.name}() is @staticmethod and takes self"))
                elif not (names & {"staticmethod", "classmethod"}) \
                        and first not in (None, "self", "cls"):
                    out.append((f, b.lineno,
                                f"{b.name}() is an instance method whose first parameter is "
                                f"'{first}' -- a displaced @staticmethod?"))
    return out


if __name__ == "__main__":
    compose = pathlib.Path("docker-compose.yml")
    compose_text = compose.read_text(encoding="utf-8") if compose.exists() else ""

    trees, syntax = {}, []
    for f in _files(ALL_ROOTS):
        try:
            trees[f] = _parse(f)
        except SyntaxError as e:
            syntax.append((f, e.lineno or 1, f"SYNTAX: {e.msg}"))

    findings = syntax
    findings += orphan_entrypoints(trees, compose_text)
    findings += thread_offloaded_async(trees)
    findings += displaced_decorators(trees)

    for f, ln, msg in sorted(set(findings), key=lambda r: (str(r[0]), r[1])):
        print(f"{f}:{ln}: {msg}")
    print(f"\n{len(findings)} issue(s) across {len(trees)} file(s). "
          f"Unreferenced functions are find_unreached_code.py's question, not this one.")
    sys.exit(1 if findings else 0)
