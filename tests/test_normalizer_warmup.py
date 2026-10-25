"""The per-name baseline: log scale, and no score before it has a distribution."""

import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment import anomaly_scorer as asc  # noqa: E402


def _lua_step(state: dict, x: float, alpha: float = 0.1, warmup: int = asc.NORMALIZE_WARMUP) -> float:
    """DYNAMIC_NORMALIZE_LUA, step for step, in Python."""
    n = state.get("n", 0)
    m = state.get("mean", x)
    v = state.get("var", 1.0)
    z = (x - m) / (math.sqrt(v) + 1e-5)
    if n < warmup:
        z = 0.0
    state["mean"] = alpha * x + (1 - alpha) * m
    state["var"] = alpha * (x - m) ** 2 + (1 - alpha) * v
    state["n"] = n + 1
    return z


def test_the_script_and_its_python_mirror_agree_on_the_rules() -> None:
    lua = asc.DYNAMIC_NORMALIZE_LUA
    assert "if n < warmup then" in lua and "n_key" in lua
    assert "local v = var and tonumber(var) or 1.0" in lua


def test_a_new_names_second_trade_is_not_an_extreme_outlier() -> None:
    state: dict = {}
    zs = [_lua_step(state, asc.log_scale(x)) for x in (500_000, 1_000_000, 600_000, 800_000, 700_000)]
    assert zs == [0.0] * 5                                    # warm-up: no claims yet
    # Raw dollars with a unit variance read the second trade at ~500,000 sigma.
    raw: dict = {}
    _lua_step(raw, 500_000, warmup=0)
    assert _lua_step(raw, 1_000_000, warmup=0) > 1e5
    # On the log scale a doubling is under one sigma, even without the warm-up.
    lg: dict = {}
    _lua_step(lg, asc.log_scale(500_000), warmup=0)
    assert abs(_lua_step(lg, asc.log_scale(1_000_000), warmup=0)) < 1.0


def test_a_real_outlier_still_stands_out_after_warm_up() -> None:
    state: dict = {}
    for x in [100_000, 120_000, 90_000, 110_000, 105_000, 95_000, 100_000, 115_000] * 3:
        _lua_step(state, asc.log_scale(x))
    assert _lua_step(state, asc.log_scale(5_000_000)) > 3.0


def test_log_scale_keeps_sign_and_handles_junk() -> None:
    assert asc.log_scale(0) == 0.0
    assert abs(asc.log_scale(-(math.e - 1)) + 1.0) < 1e-12               # sign kept: log1p(e - 1) = 1
    assert asc.log_scale("x") == 0.0 and asc.log_scale(float("nan")) == 0.0
