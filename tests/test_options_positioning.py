"""A ticker's day of option volume by right, and the stock-tier prompt that reads it."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.options_positioning import positioning_key, summarize  # noqa: E402


def test_volume_is_summed_by_right() -> None:
    out = summarize([
        ("CALL", {"dailyBar": {"v": 1200}}), ("CALL", {"dailyBar": {"v": 800}}),
        ("PUT", {"dailyBar": {"v": 1500}}), ("PUT", {"dailyBar": {}}), ("CALL", {}),
    ])
    assert out["call_volume"] == 2000 and out["put_volume"] == 1500
    assert out["put_call_ratio"] == 0.75 and out["contracts_traded"] == 3


def test_a_chain_that_did_not_trade_says_nothing() -> None:
    assert summarize([("CALL", {"dailyBar": {"v": 0}})]) is None
    assert positioning_key("nvda") == "sentinel:options:positioning:NVDA"


def test_the_thesis_reads_evidence_and_positioning() -> None:
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert "await self._thesis_evidence_block(" in src
    assert "Use ONLY facts and numbers present above" in src
    assert 'f"exposes:{ticker.upper()}")' in src


def test_only_todays_bars_count() -> None:
    out = summarize([
        ("CALL", {"dailyBar": {"v": 100, "t": "2026-10-07T04:00:00Z"}}),
        ("PUT", {"dailyBar": {"v": 900, "t": "2026-10-06T04:00:00Z"}}),   # yesterday's session
        ("PUT", {"dailyBar": {"v": 50, "t": "2026-10-07T04:00:00Z"}}),
    ], day="2026-10-07")
    assert out["call_volume"] == 100 and out["put_volume"] == 50


def _leg(right, strike, exp, iv, delta):
    return ({"option_type": right, "strike": strike, "expiry": exp},
            {"impliedVolatility": iv, "greeks": {"delta": delta}})


def test_iv_skew_and_call_put_spread_are_read_from_the_chain() -> None:
    from datetime import date
    from shared.utils.options_positioning import iv_measures

    today = date(2026, 10, 7)
    month, week = "2026-11-06", "2026-10-09"           # 30 days out, and too near
    chain = [
        _leg("CALL", 100, month, 0.40, 0.52), _leg("PUT", 100, month, 0.42, -0.48),
        _leg("CALL", 95, month, 0.43, 0.65), _leg("PUT", 95, month, 0.46, -0.35),
        _leg("CALL", 90, month, 0.47, 0.78), _leg("PUT", 90, month, 0.52, -0.24),
        _leg("CALL", 100, week, 0.90, 0.50), _leg("PUT", 90, week, 2.00, -0.25),
        _leg("CALL", 80, month, None, 0.9),
    ]
    out = iv_measures(chain, today)
    assert out["skew_expiry_days"] == 30
    assert out["iv_skew"] == round(0.52 - 0.40, 4) and out["atm_iv"] == 0.40
    assert out["cp_pairs"] == 3
    assert out["cp_iv_spread"] == round(((0.40 - 0.42) + (0.43 - 0.46) + (0.47 - 0.52)) / 3, 4)


def test_a_chain_without_quotes_gives_no_iv_measure() -> None:
    from datetime import date
    from shared.utils.options_positioning import iv_measures

    assert iv_measures([_leg("CALL", 100, "2026-11-06", None, None)], date(2026, 10, 7)) is None
