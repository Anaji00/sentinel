"""Trial F in shadow: the weekly model on beta-adjusted forward returns."""

import pathlib
import sys
from datetime import date, timedelta

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import factor_model as fm  # noqa: E402
from shared.utils import ic_monitor as icm  # noqa: E402


def test_the_target_nets_beta_times_the_market() -> None:
    dates = [date(2026, 1, 1) + timedelta(days=i) for i in range(4)]
    close = np.array([[100, 100, 100], [110, 105, 120], [121, 110.25, 144], [121, 110.25, 144]], dtype=np.float32)
    panel = fm.Panel(dates, ["SPY", "A", "B"], close, close, np.ones_like(close), {})
    beta = np.array([[1.0, 0.5, 2.0]] * 4, dtype=np.float32)
    t = fm.forward_beta_adjusted(panel, 1, beta)
    m = np.log(1.1)
    assert abs(t[0, 1] - (np.log(1.05) - 0.5 * m)) < 1e-6
    assert abs(t[0, 2] - (np.log(1.2) - 2.0 * m)) < 1e-6
    assert np.isnan(t[-1]).all()
    nan_beta = np.full_like(beta, np.nan)                          # unknown beta counts as one
    assert abs(fm.forward_beta_adjusted(panel, 1, nan_beta)[0, 1] - fm.forward_excess(panel, 1)[0, 1]) < 1e-6


def test_the_shadow_is_graded_like_the_weekly_model() -> None:
    assert icm.HORIZON_DAYS["weekly_trial_f"] == icm.HORIZON_DAYS["weekly"] == 5


def test_tree_models_see_a_missing_optional_feature_as_missing() -> None:
    names = fm.FEATURES
    feats = {f: np.random.default_rng(1).normal(size=(1, 50)).astype(np.float32) for f in names}
    opt = next(f for f in names if f in fm.OPTIONAL_FEATURES)
    req = next(f for f in names if f not in fm.OPTIONAL_FEATURES)
    feats[opt][0, :10] = np.nan                       # no source for ten names
    feats[req][0, 40:] = np.nan                       # a required feature missing for the last ten
    hist_ok = np.ones(50, dtype=bool)
    filled = fm._cross_section(feats, 0, hist_ok)
    native = fm._cross_section(feats, 0, hist_ok, nan_ok=True)
    k = names.index(opt)
    assert (filled[:10, k] == 0.0).all() and np.isnan(native[:10, k]).all()
    assert fm._complete(native)[:10].all()            # still ranked: the gap is only optional
    assert not fm._complete(native)[40:].any()        # a required gap drops the name, as before
    # The two trial candidates (22 Oct 2026) are built only from these trees.
    assert fm.NAN_NATIVE_MODELS == {"gbm", "lgbm_rank", "lgbm_rank2", "avg2"}
