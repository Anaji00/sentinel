"""Option-implied measures rebuilt from closing prices."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import option_iv as oiv  # noqa: E402


def test_implied_vol_inverts_black_scholes_and_refuses_arbitrage() -> None:
    for right in ("C", "P"):
        for k in (80.0, 100.0, 120.0):
            p = oiv.bs_price(right, 100.0, k, 30 / 365, oiv.RISK_FREE, 0.35)
            assert abs(oiv.implied_vol(p, right, 100.0, k, 30 / 365) - 0.35) < 1e-4
    assert oiv.implied_vol(19.0, "C", 120.0, 100.0, 30 / 365) is None        # below intrinsic
    assert oiv.implied_vol(0.0, "C", 100.0, 100.0, 30 / 365) is None
    assert 0.45 < oiv.bs_delta("C", 100.0, 100.0, 30 / 365, oiv.RISK_FREE, 0.3) < 0.6


def test_monthly_expiries_are_third_fridays_or_the_thursday_before_a_holiday() -> None:
    got = oiv.monthly_expiries([date(2024, 3, 15), date(2024, 3, 22), date(2024, 3, 8),
                                date(2026, 6, 18), date(2026, 6, 12), date(2024, 4, 19)])
    assert got == [date(2024, 3, 15), date(2024, 4, 19), date(2026, 6, 18)]


def test_day_measures_recover_a_known_skew_and_call_put_spread() -> None:
    day, spot, exp = date(2024, 5, 1), 100.0, date(2024, 5, 31)
    t = (exp - day).days / 365

    def iv_of(right: str, k: float) -> float:
        smile = 0.30 + 0.01 * max(0.0, 100.0 - k)          # puts below the money dearer
        return smile + (0.02 if right == "C" else 0.0)      # calls 2 points over puts at each strike

    rows = []
    for k in range(80, 121, 5):
        for right in ("C", "P"):
            rows.append((exp, float(k), right, oiv.bs_price(right, spot, k, t, oiv.RISK_FREE, iv_of(right, k)), 10.0))
    m = oiv.day_measures(day, spot, rows)
    assert m is not None
    assert abs(m["cp_iv_spread"] - 0.02) < 1e-3 and m["cp_pairs"] >= 3
    assert abs(m["atm_iv"] - 0.32) < 0.01
    assert m["iv_skew"] > 0.0                               # an OTM put dearer than the ATM call
    assert m["call_volume"] == m["put_volume"] == 90 and m["put_call_ratio"] == 1.0
    assert oiv.day_measures(day, 0.0, rows) is None


def _collector():
    import importlib.util
    path = ROOT / "services" / "collector-tradfi" / "option_history.py"
    spec = importlib.util.spec_from_file_location("option_history", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_the_backfill_asks_each_month_whose_expiry_a_session_can_reach() -> None:
    m = _collector()
    assert m.third_friday(2024, 3) == date(2024, 3, 15) and m.third_friday(2026, 6) == date(2026, 6, 19)
    months = m._months(date(2024, 2, 1), date(2024, 4, 30))
    assert months[0] == (2024, 2) and months[-1] == (2024, 6)    # an April session reaches June's expiry
    assert len(months) == len(set(months)) == 5


def test_the_models_option_features_match_trial_l_definitions() -> None:
    import math
    import numpy as np
    from shared.utils import factor_model as fm

    rng = np.random.default_rng(2)
    T, N = 80, 3
    close = (100 * np.exp(np.cumsum(rng.normal(0, 0.02, (T, N)), axis=0))).astype(np.float32)
    atm = np.full((T, N), 0.3, dtype=np.float32)
    atm[40:, 0] = 0.5                                    # implied jumps on name 0
    atm[60:63, 1] = np.nan                               # a three-session gap is carried over
    cp = np.full((T, N), np.nan, dtype=np.float32)
    cp[-5:, 2] = [0.01, 0.02, np.nan, 0.03, 0.04]
    f = fm.option_features(close, atm, None, cp, None)
    assert abs(f["opt_cp_spread"][-1, 2] - 0.025) < 1e-6           # mean of the known last five
    assert np.isnan(f["opt_skew"]).all() and np.isnan(f["opt_log_pcr"]).all()
    assert abs(f["opt_iv_change"][45, 0] - 0.2) < 1e-6 and abs(f["opt_iv_change"][70, 0]) < 1e-6
    assert np.isfinite(f["opt_vol_spread"][62, 1])
    lr = np.diff(np.log(close[:, 1].astype(np.float64)))
    rv = float(np.std(lr[50 - 21:50])) * math.sqrt(252)               # rows 30..50: the last 21 returns
    assert abs(f["opt_vol_spread"][50, 1] - (rv - 0.3)) < 1e-4
    assert set(f) <= set(fm.FEATURES) and set(f) <= fm.OPTIONAL_FEATURES


def test_a_sip_request_never_ends_inside_the_free_plans_delay() -> None:
    import importlib.util
    from datetime import datetime, timezone

    path = ROOT / "services" / "collector-tradfi" / "option_history.py"
    spec = importlib.util.spec_from_file_location("option_history_sip", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    now = datetime(2026, 10, 22, 22, 5, tzinfo=timezone.utc)
    assert mod.sip_end(date(2026, 10, 21), now) == "2026-10-21"          # a past day as asked
    assert mod.sip_end(date(2026, 10, 22), now) == "2026-10-22T21:49:00Z"  # today: now less 16 minutes
    # Midnight UTC: the 22nd is still running in New York (a date-only end means its end there).
    midnight = datetime(2026, 10, 23, 0, 0, tzinfo=timezone.utc)
    assert mod.sip_end(date(2026, 10, 22), midnight) == "2026-10-22T23:44:00Z"
    assert mod.sip_end(date(2026, 10, 22), datetime(2026, 10, 23, 4, 30, tzinfo=timezone.utc)) == "2026-10-22"
