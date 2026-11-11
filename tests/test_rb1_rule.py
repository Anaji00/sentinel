"""RB1, the plain momentum-minus-volatility rule, and the calendar offset it runs on (shared/utils/factor_model.py)."""

import math
import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import factor_model as fm  # noqa: E402


def test_the_rule_ranks_high_momentum_and_low_volatility_first() -> None:
    mom = np.array([0.5, 0.1, -0.3, 0.2])
    vol = np.array([0.2, 0.2, 0.2, 0.9])
    s = fm.rb1_rule(mom, vol)
    assert int(np.argmax(s)) == 0 and int(np.argmin(s)) in (2, 3)
    assert s[3] < s[1]                                            # same-ish momentum, far higher volatility: lower


def test_a_name_missing_an_input_scores_the_middle_and_flat_input_scores_zero() -> None:
    s = fm.rb1_rule(np.array([0.5, np.nan, -0.5, 0.1]), np.array([0.2, 0.2, 0.2, 0.2]))
    assert s[1] == 0.0 and np.isfinite(s).all()                  # missing momentum and flat volatility: z 0 minus z 0
    assert fm.rb1_rule(np.array([1.0, 1.0, 1.0]), np.array([1.0, 1.0, 1.0])).tolist() == [0.0, 0.0, 0.0]
    assert fm.rb1_rule(np.array([np.nan, np.nan]), np.array([1.0, 2.0])).shape == (2,)


def test_the_model_is_wired_through_predict_and_keeps_names_with_missing_features() -> None:
    X = np.zeros((4, len(fm.FEATURES)))
    X[:, fm.FEATURES.index("mom_12_1")] = [0.4, 0.0, -0.4, 0.1]
    X[:, fm.FEATURES.index("vol_63")] = [0.2, 0.2, 0.2, 0.2]
    pred = fm._predict("rb1_rule", None, X)
    assert int(np.argmax(pred)) == 0 and int(np.argmin(pred)) == 2
    assert fm._make("rb1_rule") is None and "rb1_rule" in fm.NAN_OK_MODELS and "rb1_rule" not in fm.NAN_NATIVE_MODELS


def test_the_calendar_offset_shifts_the_test_dates_and_defaults_to_the_live_calendar() -> None:
    src = (ROOT / "shared/utils/factor_model.py").read_text(encoding="utf-8")
    assert "offset: int = 0" in src and "range(MIN_HISTORY + offset, T - horizon, sp.test_every)" in src


def test_the_fee_adjustment_recovers_the_spread_volatility_from_the_report() -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    import run_rb1
    # net 80 bps a period, per-period Sharpe 0.2 => volatility 400 bps; after a 44 bps fee: (80-44)/400 * sqrt(12)
    assert abs(run_rb1.fee_adjusted_sharpe(80.0, 0.2, 21) - (36 / 400 * math.sqrt(12))) < 1e-9
    assert run_rb1.fee_adjusted_sharpe(80.0, 0.0) is None and run_rb1.fee_adjusted_sharpe(-5.0, 0.1) is None
