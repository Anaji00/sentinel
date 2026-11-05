"""Gap 8: the platform's own thresholds are an adversary's specification.

`/api/v1/methodology` publishes the exact detection parameters, thresholds and
calibration methods this platform runs on. For a system whose product is
noticing behaviour, that is a specification of what to stay under -- and it was
unauthenticated until this audit closed that half.

The remaining half is what is published. A fixed threshold is a constant an
adversary can plan against; a threshold learned from a live null distribution
has moved by the time they act. Saying which is which is the honest minimum, and
the endpoint said neither.

**Three of nine published parameters did not match the code**, and one of them
is a risk control:

  Branching Ratio     "0.65, fitted via EM algorithm"
                      -> no EM in the tree; L-BFGS-B MLE, estimated per pair
  Lag Horizon         "Minimised AIC over lag grid [1, 10]"
                      -> selected by BIC, changed to BIC by this audit
  Hard Position Clamp "15.0%"
                      -> MAX_SINGLE_POSITION_PCT = 0.10

That is the same defect as the "Empirically calibrated ... over 5-year rolling
backtests" claim corrected on this same endpoint earlier -- an endpoint
asserting a rigour the code does not perform -- and it survived that pass.
These tests pin the published values to the constants so the two cannot drift
apart again silently.
"""
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "services" / "api_gateway" / "routes" / "methodology.py").read_text(encoding="utf-8")


def test_no_published_parameter_claims_a_method_the_tree_does_not_contain():
    """It claimed expectation-maximisation. There is none."""
    assert "EM algorithm" not in SRC
    tree_has_em = False
    for base in ("services", "shared"):
        for f in (ROOT / base).rglob("*.py"):
            if "__pycache__" in str(f) or "methodology" in str(f):
                continue
            if re.search(r"\bEM algorithm\b|expectation.maximi", f.read_text(encoding="utf-8"), re.I):
                tree_has_em = True
    assert not tree_has_em


def test_the_lag_criterion_published_is_the_one_used():
    from shared.utils import quant_calc
    import inspect

    src = inspect.getsource(quant_calc.granger_causality)
    uses_bic = "best_bic" in src or "bic" in src.lower()
    assert uses_bic, "granger no longer selects by BIC; the endpoint says it does"
    assert "Minimised BIC" in SRC or "Minimized BIC" in SRC
    assert "Minimized AIC over lag grid" not in SRC


def test_the_published_position_clamp_is_the_one_enforced():
    """A risk-governance hard rule published at 15% and enforced at 10%."""
    from services.agents.quant_trading_engine import MAX_SINGLE_POSITION_PCT

    m = re.search(r'name="Hard Position Clamp".*?default_value="([\d.]+)%"', SRC, re.S)
    assert m, "the clamp parameter is no longer published under that name"
    published = float(m.group(1)) / 100.0
    assert published == pytest.approx(MAX_SINGLE_POSITION_PCT), (
        f"published {published:.0%}, enforced {MAX_SINGLE_POSITION_PCT:.0%}"
    )


def test_a_reader_can_tell_a_constant_from_a_quantile():
    from services.api_gateway.routes.methodology import MethodologyParameter

    assert "is_learned" in MethodologyParameter.model_fields
    assert SRC.count("is_learned=True") >= 2, (
        "nothing is marked learned, so every published parameter reads as a "
        "fixed threshold an adversary can plan against"
    )


def test_the_endpoint_still_requires_credentials():
    """It was the only route in the gateway with no authentication at all."""
    assert SRC.count("require_role(Role.VIEWER)") >= 2


def test_the_false_calibration_claim_stays_corrected():
    """'Empirically calibrated to maximize risk-adjusted Sharpe over 5-year
    rolling backtests', against a backtester run on 300 five-minute bars."""
    body = re.sub(r"#[^\n]*", "", SRC)          # strip the comments recording it
    assert "5-year rolling backtests" not in body


# -- the page a reader actually sees ------------------------------------------
#
# Every correction above landed on the endpoint while /methodology rendered its
# own hardcoded copy, so readers went on seeing the 5-year backtest, the EM fit
# and the 15% cap. These tests were green throughout.

PAGE = (ROOT / "frontend" / "src" / "app" / "(dashboard)" / "methodology" / "page.tsx").read_text(encoding="utf-8")


def test_the_page_renders_the_catalog_rather_than_its_own_copy():
    assert "'/methodology'" in PAGE and "`/methodology/${selectedId}`" in PAGE
    assert not re.search(r"(calibration_method|validation_gate|formula_\w+)\s*:\s*\n?\s*['\"`]", PAGE), (
        "page.tsx holds methodology content again; it belongs in routes/methodology.py"
    )


def _entry(key):
    from services.api_gateway.routes.methodology import METHODOLOGY_CATALOG

    return METHODOLOGY_CATALOG[key]


def test_every_entry_can_be_read_without_latex():
    from services.api_gateway.routes.methodology import METHODOLOGY_CATALOG

    for key, m in METHODOLOGY_CATALOG.items():
        assert m.plain.strip(), key
        assert m.formula_plain.strip() and "\\" not in m.formula_plain, key


def test_the_published_delta_is_the_one_the_engine_passes():
    """It published a window of [0.20, 0.35]; the engine asks for one delta."""
    engine = (ROOT / "services" / "agents" / "quant_trading_engine.py").read_text(encoding="utf-8")
    call = engine[engine.index("generate_covered_call_recommendation("):]
    passed = re.search(r"target_delta=([\d.]+)", call[:500])
    assert passed, "the engine no longer passes target_delta to this call"
    delta = next(p for p in _entry("covered_call_optimization").parameters if p.name == "Target Delta")
    assert float(delta.default_value) == float(passed.group(1))


def test_the_covered_call_claims_describe_what_runs():
    """A Brent solver, live SOFR and a 6% yield gate, none of which this path has."""
    from shared.utils import quant_calc

    entry = _entry("covered_call_optimization")
    text = " ".join([*entry.assumptions, entry.validation_gate, *[p.default_value for p in entry.parameters]])
    assert "Brent" not in text
    assert "6%" not in text and "15%" not in text
    assert "Live NY Fed SOFR" not in text
    assert f"{quant_calc.DEFAULT_RISK_FREE_RATE:.1%}" in text or quant_calc.DEFAULT_RISK_FREE_RATE != 0.045
    engine = (ROOT / "services" / "agents" / "quant_trading_engine.py").read_text(encoding="utf-8")
    call = engine[engine.index("generate_covered_call_recommendation("):]
    assert re.search(r"\br=", call[: call.index(")")]) is None, (
        "the engine now passes a rate to this call, so the catalog's fixed-rate claim is out of date"
    )


def test_the_hawkes_decay_is_published_as_fitted():
    """It published a fixed 3600-second half-life; HawkesMLE fits beta."""
    params = {p.name: p for p in _entry("hawkes_point_process").parameters}
    assert params["Decay Rate"].is_learned
    assert "3600" not in params["Decay Rate"].default_value
