"""The guards this audit built against its own recurring defect were not enforced.

Four static checks live in `scripts/`. Three of them were invoked by nothing --
they ran when someone remembered, which is never at the moment you are moving
fast. And `check_model_refs.py` did not merely go unrun: it raised
`ModuleNotFoundError` on its own first import, so it had been unrunnable for as
long as it had existed.

    check_reachability.py     written because four repairs in this audit were
                              correct, reviewed, and on no execution path
    find_unreached_code.py    written to answer "is this ever called"
    check_model_refs.py       written after a cyber outage caused by a
                              reference to a field that does not exist
    check_constructor_arity.py  written after three closed mechanisms ran inert

That is the shape this catalogue records more than any other -- built, and
nothing reaches it -- applied to the guards against that shape.

The immediate cause was mine: I crash-looped the reasoning service by deploying
between writing a line and running the suite, and the undefined-name sweep that
would have caught it does live in the suite. A guard you have to remember is one
you skip exactly when you need it.
"""
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script)],
        capture_output=True, text=True, timeout=300, cwd=str(ROOT),
    )


def test_the_model_reference_check_can_at_least_run():
    """It raised ModuleNotFoundError on its own import and nothing noticed."""
    r = _run("check_model_refs.py")
    assert "ModuleNotFoundError" not in r.stderr, r.stderr[-400:]
    assert "issue(s)" in r.stdout, r.stdout[-400:]


def test_no_reference_to_a_field_or_enum_member_that_does_not_exist():
    """The bug class behind a cyber outage: raises only on one runtime path."""
    r = _run("check_model_refs.py")
    assert "\n0 issue(s)" in r.stdout, r.stdout[-800:]


# Entry points that exist for operators and are deliberately not compose
# services. Each is annotated in its own file as not being the live path.
ALLOWED_ENTRYPOINTS = 3


def test_unreachable_entry_points_do_not_grow():
    """Four repairs in this audit were stranded behind one of these."""
    r = _run("check_reachability.py")
    found = int(r.stdout.split(" issue(s)")[0].strip().split()[-1]) if " issue(s)" in r.stdout else 0
    assert found <= ALLOWED_ENTRYPOINTS, (
        f"{found} unreachable entry point(s), up from {ALLOWED_ENTRYPOINTS}:\n{r.stdout[-900:]}"
    )


# Functions with no visible caller. Each needs individual judgement -- a
# registry entry and a dynamic dispatch look identical to dead code from here --
# so this is a ratchet on growth rather than a demand for zero.
MAX_UNREACHED = 40


def test_functions_with_no_caller_do_not_grow():
    r = _run("find_unreached_code.py")
    n = sum(
        1 for l in r.stdout.splitlines()
        if l.strip().startswith(("services", "shared")) and ":" in l
    )
    assert n <= MAX_UNREACHED, (
        f"{n} functions with no visible caller, up from {MAX_UNREACHED}. "
        "Each needs checking individually; what must not happen is the number growing."
    )


@pytest.mark.parametrize(
    "script",
    ["check_reachability.py", "check_model_refs.py", "check_constructor_arity.py"],
)
def test_every_guard_still_executes(script):
    """A guard that has stopped running is worse than one that was never written."""
    r = _run(script)
    assert r.returncode in (0, 1), f"{script} crashed:\n{r.stderr[-500:]}"
    assert r.stdout.strip(), f"{script} produced no output at all"
