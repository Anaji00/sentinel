"""The chosen model changes only when another clearly beats it (MODEL_TRIALS.md, 6 Nov 2026)."""

import json
import pathlib
import sys
from types import SimpleNamespace

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.model_choice import SWITCH_MARGIN, choose, incumbents_from_report  # noqa: E402


def m(name, sharpe, validated=False, dates=100, ic_t=3.0):
    return SimpleNamespace(model=name, net_sharpe=sharpe, validated=validated, dates=dates, ic_t=ic_t)


def test_a_close_call_keeps_the_incumbent_and_names_the_challenger() -> None:
    reps = [m("gbm", 0.60), m("lgbm_rank", 0.65)]
    chosen, challenger = choose(reps, "gbm")
    assert chosen.model == "gbm" and challenger.model == "lgbm_rank"


def test_a_clear_win_switches_and_the_margin_itself_is_enough() -> None:
    assert choose([m("gbm", 0.60), m("lgbm_rank", 0.60 + SWITCH_MARGIN)], "gbm")[0].model == "lgbm_rank"
    assert choose([m("gbm", 0.60), m("lgbm_rank", 0.95)], "gbm") == (choose([m("gbm", 0.60), m("lgbm_rank", 0.95)], "gbm")[0], None)


def test_no_incumbent_or_a_zero_margin_is_the_plain_maximum() -> None:
    reps = [m("gbm", 0.60), m("lgbm_rank", 0.61)]
    assert choose(reps, None)[0].model == "lgbm_rank" and choose(reps, "")[0].model == "lgbm_rank"
    assert choose(reps, "gbm", margin=0)[0].model == "lgbm_rank"


def test_an_incumbent_that_no_longer_qualifies_is_replaced() -> None:
    # errored or too few dates
    assert choose([m("gbm", 0.9, dates=0), m("ridge", 0.1)], "gbm")[0].model == "ridge"
    # a rival has now passed the gate: only validated models are in the pool
    assert choose([m("gbm", 0.9), m("lgbm_rank", 0.5, validated=True)], "gbm")[0].model == "lgbm_rank"
    # the incumbent is gone from the candidates entirely
    assert choose([m("ridge", 0.2)], "gbm")[0].model == "ridge"
    assert choose([], "gbm") == (None, None)


def test_a_missing_sharpe_never_wins_a_tie_break_by_accident() -> None:
    assert choose([m("gbm", None), m("ridge", 0.1)], "gbm")[0].model == "ridge"


def test_the_stored_history_flips_half_as_often() -> None:
    """The 26-run replay from MODEL_TRIALS.md, as gaps: the new winner against the incumbent, for the 8 recorded flips."""
    gaps = [0.03, 0.56, 0.01, 0.13, 0.07, 0.02, 0.15, 0.20]
    kept = sum(1 for g in gaps if g >= SWITCH_MARGIN)
    assert kept == 4 and kept <= len(gaps) * 0.6


def test_the_incumbent_is_read_from_a_stored_report() -> None:
    report = {"horizons": {"monthly": {"chosen": "gbm"}, "weekly": {"chosen": "lgbm_rank"}, "daily": {"chosen": None}}}
    want = {"monthly": "gbm", "weekly": "lgbm_rank"}
    assert incumbents_from_report(report) == want and incumbents_from_report(json.dumps(report)) == want
    for bad in (None, "", "not json", {"horizons": None}, {"horizons": {"monthly": "gbm"}}, 5):
        assert incumbents_from_report(bad) == {}


def test_the_engine_and_the_run_use_it() -> None:
    fm = (ROOT / "shared/utils/factor_model.py").read_text(encoding="utf-8")
    fe = (ROOT / "services/agents/factor_engine.py").read_text(encoding="utf-8")
    assert "choose(reps, (incumbents or {}).get(h))" in fm and '"held_over"' in fm
    assert "await self._incumbents()" in fe and "incumbents_from_report" in fe
