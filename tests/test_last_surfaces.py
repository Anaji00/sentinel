"""The twelve surfaces left over, and the four that were not what they said.

Most of what this pass covered was sound and stayed untouched: feature flags
implement real deterministic-hash rollouts and a working kill switch; semantic
search has an honest similarity floor; the DLQ, attribution, watchlists and
integrations routes all do what their names say; the audit ledger's hash chain
covers every field it returns and is enforced by table constraints and an
append-only trigger.

Four were not:

  * `/reports/templates` advertised three report types with prose describing
    different content, and `/reports/generate` had no parameter to receive a
    choice -- while the generator had no notion of a template at all.
  * That generator's docstrings claimed correlations, scenarios and portfolio
    risk. The only occurrences of "correlation" and "scenario" in the entire
    file were those two docstrings.
  * `/methodology` published, for parametric VaR, an empirical covariance
    matrix, a `tradfi:historical_covariance` input and a Kupiec
    proportion-of-failures gate -- none of which exist -- and a CVaR formula
    the code did not implement.
  * Analyst rule feedback was written, aggregated, flagged `needs_review`, and
    read by nothing at all.
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# -- reports -----------------------------------------------------------------


class _ReportDB:
    """Answers the generator's three queries by which table they name."""

    def __init__(self, events=None, correlations=None, scenarios=None):
        self.events = events or []
        self.correlations = correlations or []
        self.scenarios = scenarios or []
        self.tables = []

    async def query(self, sql, *params):
        if "FROM correlations" in sql:
            self.tables.append("correlations")
            return self.correlations
        if "FROM scenarios" in sql:
            self.tables.append("scenarios")
            return self.scenarios
        self.tables.append("events")
        return self.events


CORRELATION_ROW = {
    "correlation_id": "c-1",
    "rule_name": "Maritime Chokepoint Evasion",
    "alert_tier": 3,
    "detected_at": None,
    "description": "Two tankers dark inside the Strait of Hormuz",
}

SCENARIO_ROW = {
    "scenario_id": "s-1",
    "headline": "Hormuz transit risk repricing",
    "significance": "Freight rates and crude both moved within the window.",
    "status": "HYPOTHESIS",
    "confidence_overall": 71,
    "primary_entity_name": "Strait of Hormuz",
    "created_at": None,
}


async def test_the_brief_contains_the_sections_its_docstrings_promised():
    """Neither table was queried, and neither section existed."""
    from services.reporting.report_generator import ReportGenerator

    db = _ReportDB(correlations=[CORRELATION_ROW], scenarios=[SCENARIO_ROW])
    brief = await ReportGenerator(db_client=db, redis_client=None).generate_brief(
        timeframe_hours=24, template_id="DAILY_EXECUTIVE_BRIEF"
    )

    assert "correlations" in db.tables and "scenarios" in db.tables
    assert brief["correlations_count"] == 1
    assert brief["scenarios_count"] == 1
    assert "Maritime Chokepoint Evasion" in brief["markdown"]
    assert "Hormuz transit risk repricing" in brief["markdown"]


async def test_the_template_actually_selects_the_sections():
    """Picking one could not change anything: there was no template concept."""
    from services.reporting.report_generator import ReportGenerator

    db = _ReportDB(correlations=[CORRELATION_ROW], scenarios=[SCENARIO_ROW])
    flash = await ReportGenerator(db_client=db, redis_client=None).generate_brief(
        template_id="INCIDENT_FLASH_REPORT"
    )

    assert flash["template_id"] == "INCIDENT_FLASH_REPORT"
    assert flash["timeframe_hours"] == 4, "the template's own window"
    assert "correlations" not in flash["sections"]
    assert flash["correlations_count"] == 0
    assert "Infrastructure & Feed Health" not in flash["markdown"]
    assert "Generated Scenarios" in flash["markdown"]


def test_the_catalogue_served_is_the_catalogue_honoured():
    """Two hand-written lists could describe different reports, and did."""
    route = (ROOT / "services" / "api_gateway" / "routes" / "reports.py").read_text(
        encoding="utf-8"
    )
    assert "REPORT_TEMPLATES.values()" in route
    assert "WEEKLY_PORTFOLIO_RISK" not in route, (
        "the template promising parametric VaR and CVaR tail risk is gone; this "
        "generator has never computed either"
    )


def test_an_unknown_template_is_refused_not_silently_defaulted():
    """A caller asking for a flash report and getting a daily brief cannot tell."""
    route = (ROOT / "services" / "api_gateway" / "routes" / "reports.py").read_text(
        encoding="utf-8"
    )
    assert "is not a report template" in route
    assert "status_code=404" in route


def test_the_generator_no_longer_claims_what_it_does_not_produce():
    """Portfolio risk is a property of a book, not of an observation window."""
    source = (ROOT / "services" / "reporting" / "report_generator.py").read_text(
        encoding="utf-8"
    )
    head = source[: source.index('"""', source.index('"""') + 3)]
    assert "Portfolio risk still does not appear here" in head
    # The old sentence survives only as the quotation that records it.
    assert head.count("correlation graphs, and portfolio risk") == 1
    assert 'It said "correlation graphs, and portfolio risk"' in head


# -- the CVaR the platform publishes -----------------------------------------


def test_expected_shortfall_uses_the_published_formula():
    """`z_99 * vol * 1.25` where /methodology publishes phi(z)/(1-alpha).

    2.908 against 2.665 -- the endpoint documenting the platform's risk
    mathematics described a computation the platform did not perform.
    """
    import math

    from services.api_gateway.routes.portfolio import _expected_shortfall_multiplier

    got = _expected_shortfall_multiplier(2.3263, 0.99)
    expected = (math.exp(-0.5 * 2.3263 ** 2) / math.sqrt(2 * math.pi)) / 0.01
    assert got == pytest.approx(expected)
    assert got == pytest.approx(2.6655, abs=0.001)
    assert got != pytest.approx(2.3263 * 1.25, abs=0.001)


def test_the_methodology_entry_names_only_inputs_that_exist():
    from services.api_gateway.routes.methodology import METHODOLOGY_CATALOG

    entry = METHODOLOGY_CATALOG["parametric_var_cvar"]
    assert "tradfi:historical_covariance" not in entry.data_inputs
    assert "redis:sentinel:macro:realised_vol" in entry.data_inputs
    # A gate nobody runs is a claim, on the endpoint that exists for checking
    # claims.
    assert entry.validation_gate.startswith("None."), (
        "the gate named a Kupiec proportion-of-failures test nothing runs; it "
        "must now say plainly that the figure is unvalidated"
    )
    assert "there is no covariance matrix in this platform" in entry.description


def test_the_zero_correlation_assumption_is_stated():
    """sqrt(sum(w^2)) is the portfolio volatility only if nothing co-moves."""
    from services.api_gateway.routes.methodology import METHODOLOGY_CATALOG

    joined = " ".join(METHODOLOGY_CATALOG["parametric_var_cvar"].assumptions).lower()
    assert "zero correlation" in joined


# -- analyst feedback reaching the thing that prunes rules -------------------


class _Raw:
    def __init__(self, values):
        self.values = values

    async def hgetall(self, key):
        return self.values.get(key, {})

    async def smembers(self, key):
        return self.values.get(key, set())


class _Redis:
    def __init__(self, **values):
        self.raw = _Raw(values)


async def test_a_rules_record_is_what_the_platform_already_knew():
    """Both inputs existed; neither reached the curator that prunes rules."""
    from shared.utils.rule_feedback import RULE_FEEDBACK_KEY, rule_performance

    redis = _Redis(**{
        f"{RULE_FEEDBACK_KEY}:rule_x": {"total": "10", "negative": "8", "wrong": "8"},
    })
    record = await rule_performance(redis, "rule_x", {"rule_x": 42})

    assert record["total"] == 10
    assert record["negative"] == 8
    assert record["negative_share"] == 0.8
    assert record["needs_review"] is True
    assert record["times_fired"] == 42


async def test_one_complaint_in_three_hundred_is_not_a_review_trigger():
    from shared.utils.rule_feedback import needs_review

    assert needs_review(total=4, negative=4) is False, "not enough to judge"
    assert needs_review(total=300, negative=3) is False, "one analyst's morning"
    assert needs_review(total=10, negative=6) is True


async def test_the_prune_prompt_carries_the_record():
    """The docstring said "hit rates"; the summaries carried a name and an expiry.

    The model was being asked which rules are obsolete from their names.
    """
    from unittest.mock import AsyncMock, patch

    from services.agents.rule_agent import RuleSynthesizerAgent
    from shared.utils.rule_feedback import RULE_FEEDBACK_KEY

    agent = RuleSynthesizerAgent.__new__(RuleSynthesizerAgent)
    agent.logger = __import__("logging").getLogger("test")
    agent.redis = _Redis(**{
        f"{RULE_FEEDBACK_KEY}:rule_x": {"total": "9", "negative": "9"},
    })

    captured = {}

    async def _capture(**kwargs):
        captured.update(kwargs)

        class _D:
            prune_rule_ids = []
            reasoning = ""

        return _D()

    agent._execute_with_telemetry = _capture
    with patch(
        "services.agents.rule_agent.firing_counts",
        AsyncMock(return_value={"rule_x": 137}),
    ):
        await agent._evaluate_and_prune_rules(
            {"rule_x": json.dumps({"rule_name": "Some Rule", "trigger_event_type": "headline"})},
            "context",
        )

    prompt = captured.get("user_prompt", "")
    assert '"times_fired":137' in prompt.replace(" ", "")
    assert '"flagged_by_analysts":true' in prompt.replace(" ", "")
    assert "Weigh that record above the rule" in prompt


def test_the_key_convention_has_one_definition():
    """A key spelled out in two places is how `sentinel:watched:equities` was
    read under a name nobody wrote."""
    route = (ROOT / "services" / "api_gateway" / "routes" / "feedback.py").read_text(
        encoding="utf-8"
    )
    assert 'RULE_FEEDBACK_KEY = "sentinel:feedback:rule"' not in route
    assert "from shared.utils.rule_feedback import" in route


# -- and the smaller repairs -------------------------------------------------


def test_a_case_that_was_not_stored_was_not_created():
    """It returned a 200, a case id, and audited a creation that never happened."""
    source = (ROOT / "services" / "api_gateway" / "routes" / "cases.py").read_text(
        encoding="utf-8"
    )
    create = source[source.index("async def create_case"):]
    create = create[: create.index("async def get_case")]
    assert "if not redis:" in create
    assert "status_code=503" in create
    # The guarded write is gone: the store is now required, not optional.
    code = [ln for ln in create.splitlines() if not ln.strip().startswith("#")]
    assert not any(ln.strip() == "if redis:" for ln in code)


def test_the_eviction_claim_matches_the_deployment():
    """A correction. The ledger said Redis "runs allkeys-lru and is evictable".

    It did when that was written. The policy was changed to volatile-lru during
    this audit, after allkeys-lru evicted 83,289 keys including every recorded
    prediction -- and under volatile-lru a key with no TTL is never evicted. I
    repeated the stale claim in two comments of my own before checking compose.
    """
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "--maxmemory-policy volatile-lru" in compose

    for path in (
        ROOT / "shared" / "utils" / "audit_ledger.py",
        ROOT / "shared" / "broker" / "paper.py",
        ROOT / "shared" / "broker" / "__init__.py",
    ):
        text = path.read_text(encoding="utf-8")
        offending = [
            line for line in text.splitlines()
            if "allkeys-lru" in line and "used to say" not in line and "after allkeys-lru" not in line
        ]
        assert not offending, f"{path.name} still states the stale policy: {offending}"
