"""Layer audit 2.4: apply_display_text raised 'unsupported format string passed to NoneType' on events whose anomaly score, volume or price was None; the failure
was counted under the wrong name (cross_domain_signals), now display_text."""
import importlib
import pathlib
from types import SimpleNamespace as NS

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _mod():
    return importlib.import_module("services.enrichment.main")


def _event(**kw):
    base = dict(headline="", summary="", type="x", source="s", region=None, anomaly_score=None, primary_entity=None, financial_data=None, crypto_data=None,
                prediction_market_data=None, vessel_data=None, flight_data=None, security_data=None)
    base.update(kw)
    return NS(**base)


def test_missing_numbers_do_not_raise() -> None:
    m = _mod()
    fd = NS(ticker="ABC", underlying_price=None, premium_usd=2e6, volume=None)
    e = _event(financial_data=fd)
    m.apply_display_text(e)
    assert "ABC" in e.headline and "Anomaly score: 0.00" in e.summary
    cd = NS(pair="BTC-USD", price=None, funding_rate=None)
    e2 = _event(crypto_data=cd)
    m.apply_display_text(e2)
    assert "BTC-USD" in e2.headline and "$0.00" in e2.summary
    e3 = _event()
    m.apply_display_text(e3)
    assert "Anomaly score: 0.00" in e3.summary


def test_failure_counter_is_named_for_what_failed() -> None:
    src = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert "pipeline_errors_total:display_text" in src
    assert "pipeline_errors_total:cross_domain_signals" not in src
