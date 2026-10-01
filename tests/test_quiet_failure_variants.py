"""A site that fires for two reasons must not let the loud one bury the rare one.

Measured on the running deployment 2026-09-20, 35 minutes after a redeploy:

    agents.rule_synthesizer.unrouted_message = 134

134 of those 135 were `sentinel.correlations` messages, which carry a `rule_id`
and are therefore rule *firings* -- refusing them is correct and deliberate.
The 135th was a raw enriched event with no branch, which is a real routing gap.
It was visible in the log only because it happened to land on the call where
the shared escalation interval elapsed. At ten thousand refusals it would never
have been seen.

The module's own docstring says the count is what tells a new producer apart
from a feed being thrown away. It cannot, while both share a counter.

Separately, the drop detail was `sorted(message)[:8]` against a 20-key message:
`rule_id` sorts after `metrics_summary`, so the diagnostic for a routing
decision omitted the field the routing turns on. Reading those eight keys is
what made a correct refusal look like an unroutable shape.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import quiet_failures as qf  # noqa: E402


@pytest.fixture(autouse=True)
def _clean():
    qf.reset()
    yield
    qf.reset()


class _Log:
    def __init__(self):
        self.warnings = []

    def warning(self, fmt, *args):
        self.warnings.append(fmt % args)

    def debug(self, *a, **k):
        pass


# ── each shape gets its own ladder ───────────────────────────────────────────


def test_a_rare_shape_escalates_on_its_own_first_occurrence():
    """The whole point: a new producer says so even underneath a loud site."""
    log = _Log()
    for i in range(500):
        qf.dropped("site", "loud", log, variant="shape-a")
    before = len(log.warnings)
    qf.dropped("site", "rare", log, variant="shape-b")
    assert len(log.warnings) == before + 1, "a shape's first firing must escalate"
    assert "rare" in log.warnings[-1]


def test_without_a_variant_the_old_behaviour_is_unchanged():
    log = _Log()
    for _ in range(500):
        qf.dropped("site", "loud", log)
    before = len(log.warnings)
    qf.dropped("site", "rare", log)
    assert len(log.warnings) == before, "no variant means one shared ladder"


def test_the_site_total_still_counts_every_firing():
    """Splitting the ladder must not split the number the heartbeat reports."""
    log = _Log()
    for i in range(7):
        qf.dropped("site", "x", log, variant=f"shape-{i % 3}")
    assert qf.snapshot()["site"]["count"] == 7


def test_the_escalation_line_names_the_variant_and_both_counts():
    log = _Log()
    qf.dropped("site", "x", log, variant="shape-a")
    qf.dropped("site", "x", log, variant="shape-b")
    assert "[shape-b x1 of 2]" in log.warnings[-1]


def test_swallowed_takes_a_variant_too():
    log = _Log()
    for _ in range(500):
        qf.swallowed("site", ValueError("loud"), log, variant="a")
    before = len(log.warnings)
    qf.swallowed("site", KeyError("rare"), log, variant="b")
    assert len(log.warnings) == before + 1


# ── bounded ──────────────────────────────────────────────────────────────────


def test_the_variant_table_is_bounded():
    """An unbounded key space keyed by message shape would be a leak."""
    log = _Log()
    for i in range(qf.MAX_VARIANTS + 200):
        qf.dropped("site", "x", log, variant=f"shape-{i}")
    assert len(qf._VARIANT_COUNTS) <= qf.MAX_VARIANTS


def test_past_the_cap_a_new_variant_degrades_rather_than_raising():
    log = _Log()
    for i in range(qf.MAX_VARIANTS + 50):
        qf.dropped("site", "x", log, variant=f"shape-{i}")
    assert qf.snapshot()["site"]["count"] == qf.MAX_VARIANTS + 50


def test_a_variant_already_known_keeps_counting_past_the_cap():
    log = _Log()
    qf.dropped("site", "x", log, variant="first")
    for i in range(qf.MAX_VARIANTS + 50):
        qf.dropped("site", "x", log, variant=f"shape-{i}")
    qf.dropped("site", "x", log, variant="first")
    assert qf._VARIANT_COUNTS["site#first"] == 2


def test_reset_clears_the_variant_table():
    qf.dropped("site", "x", _Log(), variant="a")
    qf.reset()
    assert qf._VARIANT_COUNTS == {}


# ── the call site that found this ────────────────────────────────────────────


def test_a_rule_firing_is_not_counted_as_an_unroutable_shape():
    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    assert 'if message.get("rule_id"):' in src
    assert "agents.rule_synthesizer.rule_firing" in src, (
        "a correct refusal and a routing gap must not share a counter"
    )


def test_the_drop_detail_no_longer_truncates_past_the_routing_field():
    """`sorted(message)[:8]` hid rule_id, which is what routing turns on."""
    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in src.splitlines() if not line.lstrip().startswith("#")
    )
    assert "sorted(message)[:8]" not in code, "the truncating detail is still live code"
    assert 'detail=f"{len(keys)} keys: {keys}"' in code


def test_the_unroutable_drop_passes_the_shape_as_its_variant():
    src = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
    block = src[src.index("agents.rule_synthesizer.unrouted_message"):]
    block = block[: block.index("return")]
    assert 'variant=",".join(keys)' in block


# ── the reason slot held a logger at six of twenty call sites ───────────────


def test_no_call_passes_a_logger_where_the_reason_goes():
    """`dropped(site, reason, logger, ...)` -- six sites skipped `reason`.

    Visible in the deployment's own output once the counters reached a reader:

        Dropped input at correlation.statistical_discovery.stale_series,
        now 1 time(s): <Logger correlation.statistical_discovery (INFO)> (AAPL)

    The repr of a Logger stood where the explanation goes, and `logger`
    defaulted to None, so the line was emitted under "sentinel.quiet" instead
    of the site's own logger. The reason is the field that makes a count
    actionable, and at those six sites it was never written. One of the six was
    added in this same session, by copying the broken pattern beside it.
    """
    import ast

    bad = []
    for folder in ("services", "shared"):
        for path in (ROOT / folder).rglob("*.py"):
            if "__pycache__" in path.parts or "test" in path.name:
                continue
            src = path.read_text(encoding="utf-8", errors="replace")
            if "dropped(" not in src:
                continue
            try:
                tree = ast.parse(src)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or len(node.args) < 2:
                    continue
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
                if name != "dropped":
                    continue
                reason = node.args[1]
                if isinstance(reason, (ast.Constant, ast.JoinedStr, ast.BinOp)):
                    continue
                bad.append(f"{path.name}:{node.lineno} -> {ast.unparse(reason)}")
    assert not bad, f"the reason argument is not a string at: {bad}"


def test_a_reason_is_what_the_log_line_actually_prints():
    log = _Log()
    qf.dropped("site", "the reason it was discarded", log)
    assert "the reason it was discarded" in log.warnings[-1]
