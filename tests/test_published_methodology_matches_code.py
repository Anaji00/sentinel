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
