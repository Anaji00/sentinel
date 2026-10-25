"""Costs at the size a trade would be taken, per stock."""

import math
import pathlib
import random
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import trading_costs as tc  # noqa: E402


def _bars(spread: float, n: int = 120, daily_vol: float = 0.02, seed: int = 4):
    """A mid-price walk with intraday range, closes printing at the bid or ask."""
    rng = random.Random(seed)
    mid = 100.0
    highs, lows, closes = [], [], []
    for _ in range(n):
        mid *= math.exp(rng.gauss(0, daily_vol))
        rng_hi, rng_lo = mid * math.exp(abs(rng.gauss(0, daily_vol))), mid * math.exp(-abs(rng.gauss(0, daily_vol)))
        close = mid * (1 + (spread / 2 if rng.random() < 0.5 else -spread / 2))
        highs.append(max(rng_hi, close))
        lows.append(min(rng_lo, close))
        closes.append(close)
    return highs, lows, closes


def test_the_spread_estimate_ranks_wide_markets_above_tight_ones() -> None:
    tight = tc.effective_spread(*_bars(0.001, daily_vol=0.01))
    wide = tc.effective_spread(*_bars(0.03, daily_vol=0.01))
    assert tight is not None and wide is not None and wide > 2 * tight
    assert 0.015 < wide < 0.05
    assert tc.effective_spread([1.0] * 10, [1.0] * 10, [1.0] * 10) is None    # too few sessions


def test_the_estimate_counts_only_where_it_is_informative() -> None:
    assert tc.half_spread(2e9, 0.03) == 0.0001                                # liquid: the tier stands
    assert tc.half_spread(5e6, 0.03) == 0.015                                 # thin: its own wide spread
    assert tc.half_spread(5e6, 0.0001) == 0.00125                             # never under its tier (12.5 bps)


def test_costs_grow_with_size_and_shrink_with_liquidity() -> None:
    small = tc.round_trip(10_000, 5e6, 0.04, 0.004)
    big = tc.round_trip(500_000, 5e6, 0.04, 0.004)
    liquid = tc.round_trip(500_000, 5e9, 0.015, 0.0002)
    assert liquid < small < big
    assert abs(big - 2 * (0.002 + 0.04 * math.sqrt(0.1))) < 1e-12              # spread + impact both ways
    assert tc.round_trip(10_000, None, None) == 2 * tc.UNKNOWN_ONE_SIDE


def test_size_is_cut_where_impact_eats_the_edge() -> None:
    # The same 1% forecast: in a liquid large cap the cap binds; in a thin,
    # volatile name the cost-aware size is far smaller, and capacity says why.
    liquid = tc.sized(0.01, 0.30, 5, adv_usd=2e9, daily_vol=0.015, spread=0.0003)
    thin = tc.sized(0.01, 0.30, 5, adv_usd=4e6, daily_vol=0.05, spread=0.006)
    assert liquid["weight"] == 0.05 and thin["weight"] < 0.01
    assert thin["capacity_usd"] < liquid["capacity_usd"]
    assert thin["cost"] > liquid["cost"]
    assert tc.sized(0.002, 0.30, 5, adv_usd=4e6, daily_vol=0.05, spread=0.006)["weight"] == 0.0   # spread alone wins
    assert tc.capacity_usd(0.002, 4e6, 0.05, 0.006) == 0.0
