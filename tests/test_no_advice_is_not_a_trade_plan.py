"""With no advisor brief, /financial/advice says so; it does not invent plays."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes.scenarios import no_financial_advice  # noqa: E402


def test_the_empty_answer_carries_no_trades_or_metrics() -> None:
    out = no_financial_advice("No advisor brief has been produced.")
    assert out["available"] is False and out["brief"] is None
    assert "NVDA" not in str(out) and "conviction" not in str(out)


def test_the_fixed_plan_is_gone_from_the_route() -> None:
    src = (ROOT / "services/api_gateway/routes/scenarios.py").read_text(encoding="utf-8")
    assert '"entry_level": 219.16' not in src and "generate_fallback_financial_advice" not in src
