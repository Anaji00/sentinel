"""Trade plans are computed; the model only explains them."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.trade_plan import (  # noqa: E402
    conviction, deterministic_play, direction_votes, thesis_warranted,
)

BULL = {"ma_alignment": "BULLISH", "ema_12": 101.0, "ema_26": 100.0}
BEAR = {"ma_alignment": "BEARISH", "ema_12": 99.0, "ema_26": 100.0}
MIXED = {"ma_alignment": "BULLISH", "ema_12": 99.0, "ema_26": 100.0}


def test_direction_needs_two_net_votes() -> None:
    assert direction_votes(BULL, []).direction == 1
    assert direction_votes(BEAR, [-1]).direction == -1
    assert direction_votes(MIXED, []).direction == 0
    # A trigger can tip a split tape, but not override two agreeing views.
    assert direction_votes(MIXED, [1]).direction == 0
    assert direction_votes(BEAR, [1]).direction == 0


def test_the_same_inputs_give_the_same_play() -> None:
    a = deterministic_play("NVDA", BULL, [1], {"options_call_sweep"}, set(), 4.0)
    b = deterministic_play("NVDA", BULL, [1], {"options_call_sweep"}, set(), 4.0)
    assert a == b and a["action"] == "BUY"


def test_unproven_plays_never_reach_the_top_rr_tier() -> None:
    play = deterministic_play("NVDA", BULL, [1], {"options_call_sweep"}, set(), 4.0)
    assert play["evidence_status"] == "unproven" and play["conviction_score"] < 0.8
    assert "UNPROVEN" in play["quantitative_rationale"]
    proven = deterministic_play("NVDA", BULL, [1], {"options_call_sweep"}, {"options_call_sweep"}, 4.0)
    assert proven["evidence_status"] == "proven" and proven["conviction_score"] >= 0.8


def test_hold_is_no_play() -> None:
    assert deterministic_play("NVDA", MIXED, [], set(), set(), 4.0) is None
    assert conviction(direction_votes(MIXED, []), proven=True) == 0.0


def test_only_a_real_trigger_earns_a_thesis() -> None:
    assert thesis_warranted("NVDA", 0.62, {"NVDA"}, set())
    assert thesis_warranted("XYZ", 0.62, set(), {"insider_cluster"})
    assert thesis_warranted("XYZ", 0.85, set(), set())
    assert not thesis_warranted("XYZ", 0.62, set(), set())


def test_the_engine_asks_the_model_for_prose_not_trades() -> None:
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert "schema=FinancialAdviceBrief" not in src
    assert "schema=TradeThesis" in src
    assert "Specify action (BUY/SELL/HOLD)" not in src


def test_only_a_validated_model_votes_and_it_can_prove_a_play() -> None:
    from shared.utils.trade_plan import model_vote

    assert model_vote({"quintile": 5, "validated": False}) == 0
    assert model_vote({"quintile": 5, "validated": True}) == 1
    assert model_vote({"quintile": 3, "validated": True}) == 0
    mixed = {"ma_alignment": "BULLISH", "ema_12": 99.0, "ema_26": 100.0}
    play = deterministic_play("NVDA", mixed, [1], set(), set(), 4.0, model={"quintile": 5, "validated": True})
    assert play and play["action"] == "BUY" and play["evidence_status"] == "proven"
    assert deterministic_play("NVDA", mixed, [1], set(), set(), 4.0, model={"quintile": 5, "validated": False}) is None


def test_a_supported_model_votes_but_never_proves() -> None:
    from shared.utils.trade_plan import model_support

    strong = {"quintile": 5, "validated": False, "ic_t": 5.5, "net_sharpe": 1.4, "positive_fold_share": 1.0}
    weak = {**strong, "ic_t": 1.0}
    assert model_support(strong) == "supported" and model_support(weak) is None
    assert model_support({**strong, "validated": True}) == "validated"
    mixed = {"ma_alignment": "BEARISH_STACK", "ema_12": 11.0, "ema_26": 10.0}
    play = deterministic_play("NVDA", mixed, [], set(), set(), 4.0, model=strong)
    assert play["action"] == "BUY" and play["evidence_status"] == "model_supported"
    assert play["conviction_score"] < 0.8


def test_technicals_alone_make_no_play_when_evidence_is_required() -> None:
    assert deterministic_play("NVDA", BULL, [1], {"block_trade"}, set(), 4.0) is not None
    assert deterministic_play("NVDA", BULL, [1], {"block_trade"}, set(), 4.0, require_evidence=True) is None
    proven = deterministic_play("NVDA", BULL, [1], {"8k_dilution"}, {"8k_dilution"}, 4.0, require_evidence=True)
    assert proven["evidence_status"] == "proven"
