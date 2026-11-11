"""The registered multi-horizon peer-residual rule P1 (shared/utils/peer_residual.py)."""

import pathlib
import sys

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import peer_residual as pr  # noqa: E402


def test_beta_recovers_a_known_slope_and_refuses_thin_or_flat_input() -> None:
    rng = np.random.default_rng(1)
    x = rng.normal(0, 0.01, 60)
    y = 1.4 * x + rng.normal(0, 0.002, 60)
    assert abs(pr.beta(y, x) - 1.4) < 0.15
    assert pr.beta(y[:20], x[:20]) is None
    assert pr.beta(y, np.zeros(60)) is None
    assert pr.beta([np.nan] * 60, x) is None


def test_the_residual_removes_what_beta_explains() -> None:
    z = pr.residual_z(np.array([0.03, -0.043]), np.array([0.02, 0.077]), 1.0, np.array([0.01, 0.02]))
    assert np.allclose(z, [1.0, -6.0])
    assert np.isnan(pr.residual_z(0.03, 0.01, 1.0, 0.0))


def test_breadth_counts_only_members_with_a_return() -> None:
    share, n = pr.breadth_opposite(-0.043, [0.13, 0.09, 0.095, None, float("nan"), -0.01])
    assert n == 4 and abs(share - 0.75) < 1e-9
    assert pr.breadth_opposite(0.0, [0.01]) == (None, 1)


def test_the_nvda_session_fires_and_each_gate_can_stop_it() -> None:
    base = dict(z=-4.7, breadth=0.83, r_stock=-0.043, atr=0.025, rvol=1.9)
    assert pr.qualifies(**base) is True
    assert pr.qualifies(**{**base, "z": -2.4}) is False                        # gap not large enough
    assert pr.qualifies(**{**base, "breadth": 0.6}) is False                   # peers not mostly opposite
    assert pr.qualifies(**{**base, "r_stock": -0.01, "atr": 0.01}) is False    # small against its own range
    assert pr.qualifies(**{**base, "r_stock": -0.015, "atr": 0.005}) is True   # small in percent but 3 ATR
    assert pr.qualifies(**{**base, "rvol": 1.0}) is False                      # volume does not confirm
    assert pr.qualifies(**{**base, "rvol": 1.0}, use_rvol=False) is True       # the 5-session horizon has no volume gate
    assert pr.qualifies(**{**base, "breadth": None}) is False


def test_the_array_form_agrees_with_the_scalar_form() -> None:
    rng = np.random.default_rng(5)
    z, b, r, a, v = (rng.normal(0, s, 200) for s in (3, 0.5, 0.03, 0.01, 1))
    b, a, v = np.clip(np.abs(b) + 0.5, 0, 1), np.abs(a), np.abs(v) + 0.8
    arr = pr.qualifies_array(z, b, r, a, v)
    one = np.array([pr.qualifies(z[i], b[i], r[i], a[i], v[i]) for i in range(200)])
    assert (arr == one).all() and arr.any()
