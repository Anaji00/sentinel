"""G4 enforced: only the five cells that survived liquid, borrowable names reach the live idea layer (shared/utils/tradable_cells.py)."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import evidence_fusion as ef  # noqa: E402
from shared.utils import tradable_cells as tc  # noqa: E402


def _cell(family, horizon, verdict="proven", units=100):
    return {"family": family, "horizon": horizon, "verdict": verdict, "units": units}


def test_exactly_the_five_g4_survivors_are_tradable() -> None:
    assert len(tc.TRADABLE_CELLS) == 5
    for f, h in [("8k_delisting_notice", "30d"), ("8k_dilution", "30d"), ("insider_opportunistic_sell", "30d"),
                 ("options_put_sweep", "1d"), ("options_put_sweep", "1h")]:
        assert tc.is_tradable_cell(f, h)
    for f, h in [("8k_dilution", "1d"), ("earnings_fade_large", "5d"), ("stock_news_negative", "1d"),
                 ("8k_officer_change", "1h"), ("8k_delisting_notice", "1d")]:
        assert not tc.is_tradable_cell(f, h)


def test_a_proven_cell_outside_the_list_becomes_shadow_and_keeps_its_numbers() -> None:
    g = tc.gate_cell({**_cell("stock_news_negative", "1d"), "mean_bps": 55})
    assert g["verdict"] == "shadow" and g["shadow_of"] == "proven" and g["mean_bps"] == 55
    assert tc.gate_cell(_cell("8k_dilution", "30d"))["verdict"] == "proven"
    assert tc.gate_cell(_cell("8k_dilution", "5d", verdict="no_edge"))["verdict"] == "no_edge"     # only proven cells are touched


def test_the_evidence_layer_gives_a_shadow_family_no_weight_and_a_surviving_one_full_weight() -> None:
    rec = ef.family_record([_cell("stock_news_negative", "1d"), _cell("options_put_sweep", "1d")])
    assert ef.status_of(rec["stock_news_negative"]) == "shadow" and ef.WEIGHTS["shadow"] == 0.0
    assert ef.status_of(rec["options_put_sweep"]) == "proven" and ef.WEIGHTS["proven"] == 1.0


def test_a_family_with_a_surviving_and_a_failing_horizon_keeps_the_surviving_one() -> None:
    rec = ef.family_record([_cell("8k_dilution", "1d", units=2552), _cell("8k_dilution", "30d", units=129)])
    assert rec["8k_dilution"]["horizon"] == "30d" and ef.status_of(rec["8k_dilution"]) == "proven"


def test_instruments_must_be_liquid_and_shorts_easy_to_borrow_and_unknown_is_not_a_pass() -> None:
    ok = ["stock", True, True, "Acme"]
    assert tc.instrument_ok(300e6, -1, ok)[0] is True
    assert tc.instrument_ok(300e6, 1, None)[0] is True                       # a long needs no borrow data
    assert tc.instrument_ok(100e6, 1, ok)[0] is False                       # under the liquidity level
    assert tc.instrument_ok(None, 1, ok)[0] is False                        # no history
    assert tc.instrument_ok(300e6, -1, None)[0] is False                    # a short with no borrow data
    assert tc.instrument_ok(300e6, -1, ["stock", True, False, "Acme"])[0] is False
    assert tc.instrument_ok(float("nan"), 1, ok)[0] is False


def test_the_signal_engine_and_quant_engine_use_the_gate() -> None:
    se = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert "tradable_cells.is_tradable_cell(*k)" in se and "_instrument_ok(o.instrument, direction, now)" in se
    assert se.index("_instrument_ok(o.instrument, direction, now)") < se.index('"hit": shrunk_hit_rate')
    assert "SHADOW_KEY" in se and "agents.signal_engine.not_tradable" in se
    qe = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert "tradable_cells.gate_cells(cells)" in qe
