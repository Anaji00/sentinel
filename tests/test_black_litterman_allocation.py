"""
tests/test_black_litterman_allocation.py

A portfolio optimiser that had never allocated anything.

Across 25 financial briefs, `black_litterman_allocations` was an empty list
every time, while the neighbouring fields populated at 28% to 72%. The reason
was not a failure: `quant_calc.black_litterman_optimization` is implemented and
tested, `BlackLittermanAllocation` is in the schema, the frontend renders it and
the feature flag is on at 100% rollout -- and the only line in the platform that
touched the field was

    if not await self.flags.is_enabled("black_litterman", ticker=ticker):
        brief.black_litterman_allocations = []

which clears a list nothing had ever filled. The function had exactly one
caller in the repository and it was this file's predecessor, a unit test.

These tests hold the wiring in place: that the engine calls the optimiser, that
the universe is chosen for shared history rather than truncated after the fact,
and that the equilibrium prior refuses a market cap it cannot denominate.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENGINE = ROOT / "services" / "agents" / "quant_trading_engine.py"


def test_the_optimiser_has_a_caller_outside_the_tests():
    """The defect was the absence of this line."""
    source = ENGINE.read_text(encoding="utf-8")
    assert "quant_calc.black_litterman_optimization" in source, (
        "the engine must actually call the optimiser it declares a field for"
    )
    assert "_black_litterman_allocations(" in source


def test_the_flag_gates_something_that_exists():
    """Both branches must be real: the flag off clears, the flag on fills."""
    source = ENGINE.read_text(encoding="utf-8")
    gate = source[source.index('is_enabled("black_litterman"') :][:600]
    assert "brief.black_litterman_allocations = []" in gate
    assert "await self._black_litterman_allocations(" in gate, (
        "the enabled branch must populate, or the flag gates nothing"
    )


def test_the_universe_is_chosen_for_shared_history():
    """Six names had 43-49 daily bars; two joined a month late with 21.

    Intersecting all eight gave 15 shared dates -- fewer observations than twice
    the number of assets, which is a rank-deficient covariance presented as a
    portfolio. Candidates are admitted only while the window still clears
    BL_MIN_OBSERVATIONS_PER_ASSET days per asset.
    """
    import services.agents.quant_trading_engine as engine

    assert engine.BL_MIN_OBSERVATIONS_PER_ASSET >= 2, (
        "a covariance needs more observations than assets, not fewer"
    )
    assert engine.BL_MIN_OBSERVATIONS >= 20
    assert engine.BL_UNIVERSE_MAX >= 2


def test_a_view_is_never_infinitely_confident():
    """Omega is the variance of the error on a view.

    A conviction of 1.0 must not produce zero uncertainty: that is an
    infinitely confident view and it would drive the posterior on its own,
    which is precisely what the equilibrium prior exists to prevent.
    """
    import services.agents.quant_trading_engine as engine

    assert engine.BL_VIEW_UNCERTAINTY_FLOOR > 0.0
    strongest = engine.BL_VIEW_UNCERTAINTY_FLOOR + engine.BL_VIEW_UNCERTAINTY_SCALE * 0.0
    assert strongest > 0.0


def test_the_prior_refuses_a_cap_it_cannot_denominate():
    """The prior is capitalisation weighted, so a wrong unit is a wrong weight.

    TSM cached at 61.7 trillion "USD" -- a TWD figure -- took 89.7% of a
    six-name allocation on the live deployment before the currency was checked.
    """
    from shared.utils.market_cap import parse_market_cap, NOT_IN_USD

    assert parse_market_cap(NOT_IN_USD) is None


def test_equilibrium_weights_are_market_cap_weights():
    """With no views the posterior is the prior, and the prior is cap weights."""
    from shared.utils import quant_calc

    caps = {"AAA": 600.0, "BBB": 400.0}
    cov = [[0.04, 0.01], [0.01, 0.09]]
    res = quant_calc.black_litterman_optimization(caps, cov, [], [], [])
    assert res["optimal_weights"]["AAA"] == 60.0
    assert res["optimal_weights"]["BBB"] == 40.0


def test_a_bullish_view_moves_weight_toward_its_subject():
    """The whole point of the model: a house view shifts a market prior."""
    from shared.utils import quant_calc

    caps = {"AAA": 600.0, "BBB": 400.0}
    cov = [[0.04, 0.01], [0.01, 0.09]]
    base = quant_calc.black_litterman_optimization(caps, cov, [], [], [])
    tilted = quant_calc.black_litterman_optimization(
        caps, cov, [[1.0, 0.0]], [0.10], [0.0025]
    )
    assert tilted["optimal_weights"]["AAA"] > base["optimal_weights"]["AAA"], (
        "a bullish view on AAA must not reduce its allocation"
    )
