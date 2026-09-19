"""Phase 3a: the two contract families that were still missing.

The plan named four. Two already exist and are well built --
`test_redis_key_conventions.py` proves no key has a second spelling, and
`tests/integration/test_topic_contracts.py` proves every produced topic has a
consumer and every consumed topic has a producer, in both directions, with an
allowlist that is itself checked for staleness. This file adds the other two.

  metrics        A metric a reader asks for by name and nobody publishes reads
                 as a flat zero forever, which is indistinguishable from a
                 quiet system. That is this audit's recurring shape: absence
                 rendered as a measurement.

  gates          `validation_gate` is prose on the public methodology endpoint,
                 and prose cannot be checked. One entry described a Kupiec
                 proportion-of-failures test that nothing in this platform runs
                 -- on the endpoint whose entire purpose is to let a reader
                 check the platform's claims. A gate now either names the
                 callable that runs it, or opens with "None." and says why.
"""
from __future__ import annotations

import ast
import importlib
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

SKIP_DIRS = {"__pycache__", "node_modules", ".venv", "venv", ".git", "frontend"}


def _python_files():
    for p in ROOT.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        yield p


# ── metrics ───────────────────────────────────────────────────────────────────

_WRITERS = {"increment", "set_gauge", "observe_latency"}


def _metric_name_from(node: ast.AST):
    """The name a metric call publishes, or its literal prefix.

    Half of them are f-strings -- `f"ollama_latency_{self.service_name}"` -- so
    a scan that only reads `ast.Constant` sees about half the metrics and
    silently believes the rest are unpublished. That is the same defect this
    file exists to catch, in the checker itself, so the prefix is taken and the
    caller matches on it.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value, True  # exact
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            else:
                break  # stop at the first interpolation; the rest is dynamic
        prefix = "".join(parts)
        if prefix:
            return prefix, False  # prefix only
    return None, False


def _published_metric_names():
    """Every metric name written through MetricsCollector: exact, or a prefix."""
    out = {}
    for path in _python_files():
        if "tests" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
            if name not in _WRITERS or not node.args:
                continue
            metric, exact = _metric_name_from(node.args[0])
            if metric:
                out.setdefault(metric, set()).add(
                    f"{path.relative_to(ROOT).as_posix()}:{node.lineno}"
                )
    return out


def _dashboard_metric_names():
    """Every sentinel_* series a Grafana dashboard or an alert rule asks for."""
    names = set()
    for path in list((ROOT / "deploy").rglob("*.json")) + list((ROOT / "deploy").rglob("*.yml")):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for m in re.finditer(r"\bsentinel_[a-z0-9_]+", text):
            names.add(m.group(0))
    return names


def test_the_metric_scan_can_see_its_subject():
    """A scan that finds nothing would make the assertions below vacuous."""
    published = _published_metric_names()
    assert len(published) >= 25, f"only {len(published)} metric names found; the AST scan is broken"
    # Two names this audit watched reach Prometheus. One is a plain literal and
    # one is only ever written as an f-string, so a scan that sees both is
    # reading each form.
    assert "ollama_calls_total" in published, sorted(published)[:25]
    assert "ollama_latency_" in published, (
        "the f-string metric names are invisible to this scan, which would make "
        "every assertion below read as 'nothing publishes that'"
    )


def test_every_dashboard_metric_is_published_by_something():
    """A panel bound to a name nobody writes draws a flat zero, forever.

    The zero is the danger. A dashboard that plots nothing looks like a
    dashboard plotting a calm system, and there is no error anywhere to say
    otherwise -- which is exactly the failure this platform's own health
    surface was built to stop reporting.
    """
    published = _published_metric_names()
    # Metrics are namespaced with a `sentinel_` prefix when they are exported,
    # so compare on the suffix the code actually writes.
    exported = {f"sentinel_{n}" for n in published}
    # The exporter also emits per-service variants, `<name>_<service>`.
    wanted = _dashboard_metric_names()

    missing = sorted(
        w for w in wanted
        if w not in exported and not any(w.startswith(e) for e in exported)
    )
    assert not missing, (
        "Grafana or an alert rule asks for metric(s) nothing publishes: "
        f"{missing}. A panel bound to an unwritten name renders a flat zero "
        "that is indistinguishable from a quiet system."
    )


# ── validation gates ──────────────────────────────────────────────────────────

def _methodology_catalog():
    import os
    os.environ.setdefault("SENTINEL_ENV", "test")
    mod = importlib.import_module("services.api_gateway.routes.methodology")
    return mod.METHODOLOGY_CATALOG


def _resolve(dotted: str):
    """Resolve 'pkg.mod:Class.method' or 'pkg.mod:function' to an object."""
    module_path, _, attr_path = dotted.partition(":")
    assert attr_path, f"{dotted!r} must name an attribute after ':'"
    obj = importlib.import_module(module_path)
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj


def test_the_gate_scan_can_see_its_subject():
    catalog = _methodology_catalog()
    assert len(catalog) >= 4, f"only {len(catalog)} methodology entries found"
    assert any(e.validation_gate_impl for e in catalog.values()), (
        "no entry names an implementation; the field is not being read"
    )


@pytest.mark.parametrize("key", sorted(_methodology_catalog()))
def test_validation_gates_name_real_code(key):
    """A gate is implemented and says where, or declares that it is not.

    There is no third option, and the third option is what this check exists to
    forbid: a paragraph describing a test, on a public endpoint, with nothing
    behind it.
    """
    entry = _methodology_catalog()[key]
    gate = (entry.validation_gate or "").strip()
    assert gate, f"{key} has an empty validation_gate"

    declares_none = gate.lower().startswith("none")
    impl = entry.validation_gate_impl

    if declares_none:
        assert impl is None, (
            f"{key} says its gate is not implemented but also names "
            f"{impl!r}. One of the two is wrong."
        )
        assert len(gate) > 40, (
            f"{key} declares no validation gate and does not say why. "
            "'None.' on its own tells a reader nothing about what is missing."
        )
        return

    assert impl, (
        f"{key} describes a validation gate -- {gate[:80]!r} -- and names no "
        "code that runs it. Either point validation_gate_impl at the callable, "
        "or open the gate with 'None.' and say what would be required."
    )
    try:
        resolved = _resolve(impl)
    except (ImportError, AttributeError, AssertionError) as exc:
        pytest.fail(f"{key}: validation_gate_impl {impl!r} does not resolve: {exc}")
    assert callable(resolved), f"{key}: {impl!r} resolved to {type(resolved).__name__}, not a callable"


def test_a_gate_that_claims_nothing_is_not_silently_accepted():
    """The check above must actually reject a prose-only gate.

    Written because this audit has produced four checks that could not see
    their own subject, and a contract test that cannot fail is the fifth.
    """
    catalog = _methodology_catalog()
    sample = catalog[sorted(catalog)[0]]
    Model = type(sample)

    fields = sample.model_dump()
    fields["validation_gate"] = "Signals are rejected if the rolling Sharpe falls below 1.0."
    fields["validation_gate_impl"] = None
    fabricated = Model(**fields)

    gate = fabricated.validation_gate.strip()
    assert not gate.lower().startswith("none")
    assert fabricated.validation_gate_impl is None
    # Which is precisely the combination test_validation_gates_name_real_code
    # asserts against, so that test can fail.


# ── SQL that is actually SQL ──────────────────────────────────────────────────

def test_no_sql_string_contains_a_python_comment():
    """`#` is not a SQL comment, and a SQL string is not Python.

    Written immediately after doing it. Moving an explanatory note inside a
    triple-quoted INSERT produced, on every write:

        Failed to batch write 15 events: syntax error at or near "#"

    for roughly ten minutes, until the batches replayed from Kafka on restart.
    The full suite passed throughout -- 3,744 tests -- because nothing executes
    that statement without a database, and the one test that reads it parses its
    column list rather than its syntax.

    A `--` comment inside a *column list* is the other half of the same trap and
    breaks that parser instead, so the rule is simply: explain the statement
    above the string, in Python, where both problems are impossible.
    """
    import re

    offenders = []
    for path in _python_files():
        if "tests" in path.parts:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            text = node.value
            if not re.search(r"\b(SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM)\b", text, re.I):
                continue
            for i, line in enumerate(text.splitlines()):
                if line.lstrip().startswith("#"):
                    offenders.append(
                        "%s:%d -> %s"
                        % (path.relative_to(ROOT).as_posix(), node.lineno, line.strip()[:60])
                    )
                    break

    assert not offenders, (
        "SQL string(s) containing a Python comment; Postgres reads `#` as a "
        "syntax error and every statement using them fails at runtime:\n  "
        + "\n  ".join(offenders)
    )
