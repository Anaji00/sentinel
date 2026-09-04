"""
tests/test_financial_context_reaches_prompt.py

The enrichment layer works to attach sector, notional, strike and implied
volatility to every financial event. The reasoning layer discarded all of it.

The compressed event table prefers the headline:

    headline = evt.get("headline") or evt.get("summary")
    if headline:
        detail = str(headline)[:60]
    elif evt.get("financial_data"):
        ...

A financial event always has a headline, so the financial branch was
unreachable and what reached the model was sixty characters of prose:

    "OPTIONS FLOW CALL Sweep | JHX (JHX260904C01170000) | Premium:"

Which omits the two facts that make the event reasonable about. Sector is how
contagion travels -- "bad earnings at X hits its peers" cannot be asked of a
record that does not say what X is comparable to. And premium is what a position
cost while notional is what it controls; on a live sweep those differed by 34x,
so a model shown only the premium is reading the wrong order of magnitude.

This is the harmony failure in miniature: three layers each doing their job, and
the handoff between two of them dropping the work of the third.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.reasoning.context_builder import _financial_facts, _money  # noqa: E402

SWEEP = {
    "financial_data": {
        "ticker": "Z", "sector": "Real Estate", "notional_usd": 750000.0,
        "premium_usd": 22000.0, "strike": 37.5, "expiry": "2027-01-15",
        "implied_volatility": 0.42,
    }
}


def test_sector_reaches_the_model():
    """Contagion runs along sector. Without it the peer question cannot be put."""
    assert "Real Estate" in _financial_facts(SWEEP)


def test_notional_reaches_the_model():
    """Premium is the cost, notional is the exposure, and they differ by 34x
    on this very sweep."""
    assert "notional $750k" in _financial_facts(SWEEP)


def test_the_contract_terms_reach_the_model():
    """A directional bet is unreadable without a strike and an expiry."""
    facts = _financial_facts(SWEEP)
    assert "K37.5" in facts and "2027-01-15" in facts


def test_implied_volatility_reaches_the_model():
    assert "IV42%" in _financial_facts(SWEEP)


def test_a_headlined_event_still_gets_its_facts():
    """The defect: the headline branch returned early and the payload was
    never consulted."""
    source = (ROOT / "services" / "reasoning" / "context_builder.py").read_text(encoding="utf-8")
    block = source.split("detail = str(headline)[:60]")[1][:900]
    assert "_financial_facts(evt)" in block


def test_a_non_financial_event_adds_nothing():
    """Vessels and flights must not gain an empty bracket."""
    assert _financial_facts({"vessel_data": {"mmsi": "123"}}) == ""
    assert _financial_facts({}) == ""


def test_an_empty_payload_adds_nothing():
    assert _financial_facts({"financial_data": {"ticker": "X"}}) == ""


def test_malformed_values_do_not_raise():
    """These fields arrive null and arrive as text; the prompt builder failing
    would cost the whole scenario, not one row."""
    bad = {"financial_data": {
        "sector": None, "notional_usd": "abc", "strike": None,
        "expiry": None, "implied_volatility": [],
    }}
    assert _financial_facts(bad) == ""


def test_it_stays_terse_enough_for_the_budget():
    """The prompt is cut to a character budget; this rides on every financial
    row and has to earn its space."""
    assert len(_financial_facts(SWEEP)) < 80


def test_money_is_shown_at_a_readable_unit():
    """'$0.0M' on every row is why the crypto headlines were unreadable."""
    assert _money(750_000) == "$750k"
    assert _money(4_200_000) == "$4.2M"
    assert _money(2_500_000_000) == "$2.5B"
    assert _money(85) == "$85"


def test_money_handles_rubbish():
    assert _money(None) == "" and _money("abc") == ""
