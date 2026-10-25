"""Options priced under the model's forecast against what they cost."""

import math
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import option_ev as oe  # noqa: E402


def _bs_call(s, k, t, r, sig):
    d1 = (math.log(s / k) + (r + sig * sig / 2) * t) / (sig * math.sqrt(t))
    return s * oe._ncdf(d1) - k * math.exp(-r * t) * oe._ncdf(d1 - sig * math.sqrt(t))


def _chain(s, t, iv, r=0.04, spread=0.0, strikes=range(80, 125, 5)):
    """Quotes priced by Black-Scholes at implied vol `iv`."""
    out = []
    for k in strikes:
        c = _bs_call(s, k, t, r, iv)
        p = c - s + k * math.exp(-r * t)
        out.append(oe.Quote(f"C{k}", "C", float(k), "2026-11-20", c - spread, c + spread))
        out.append(oe.Quote(f"P{k}", "P", float(k), "2026-11-20", max(p - spread, 0.01), p + spread))
    return out


def test_under_the_risk_neutral_drift_the_expectation_is_black_scholes() -> None:
    fwd_value = oe.expected_payoff("C", 100, 100, 1.0, 0.05, 0.2) * math.exp(-0.05)
    assert abs(fwd_value - 10.4506) < 1e-3
    put = oe.expected_payoff("P", 100, 100, 1.0, 0.05, 0.2) * math.exp(-0.05)
    assert abs(put - 5.5735) < 1e-3
    assert abs(oe.prob_above(100, 100, 1.0, 0.0, 0.2) - oe._ncdf(-0.1)) < 1e-12


def test_no_edge_no_idea_and_an_edge_picks_a_trade_that_pays() -> None:
    s, t = 100.0, 30 / 365
    fair = {"2026-11-20": _chain(s, t, 0.30, spread=0.05)}
    # Forecast drift at the risk-free rate and vol at implied: nothing is worth buying after the spread.
    assert oe.best(1, fair, {"2026-11-20": 30}, s, 0.04, 0.30) is None
    # A strong up-forecast (about +6% over the month): a bullish structure with positive EV.
    pick = oe.best(1, fair, {"2026-11-20": 30}, s, 0.70, 0.30)
    assert pick is not None and pick["structure"] in ("long_call", "bull_call_debit", "bull_put_credit")
    assert pick["ev"] > 0 and pick["prob_profit"] >= oe.MIN_PROB_PROFIT and 0 < pick["kelly_share"] <= 0.02


def test_rich_premium_favours_selling_it_and_cheap_premium_favours_buying() -> None:
    s, t = 100.0, 30 / 365
    rich = {"2026-11-20": _chain(s, t, 0.60, spread=0.02)}
    cheap = {"2026-11-20": _chain(s, t, 0.15, spread=0.02)}
    sell = oe.best(1, rich, {"2026-11-20": 30}, s, 0.30, 0.25)
    buy = oe.best(1, cheap, {"2026-11-20": 30}, s, 0.30, 0.35)
    # Rich: never a bare bought option. An in-the-money call spread and the
    # matching put credit spread are the same payoff (put-call parity): both sell
    # the overpriced volatility.
    assert sell is not None and sell["structure"] in ("bull_put_credit", "bull_call_debit")
    assert sell["prob_profit"] > 0.6
    assert buy is not None and buy["structure"] == "long_call"


def test_an_option_idea_settles_on_the_close_at_expiry() -> None:
    spread = {"net": -200.0, "max_loss": 200.0, "legs": [
        {"right": "C", "side": "buy", "strike": 100.0}, {"right": "C", "side": "sell", "strike": 105.0}]}
    assert oe.settle(spread, 110.0) == {"pnl": 300.0, "return_on_risk": 1.5}      # full width, less the debit
    assert oe.settle(spread, 95.0) == {"pnl": -200.0, "return_on_risk": -1.0}
    credit = {"net": 150.0, "max_loss": 350.0, "legs": [
        {"right": "P", "side": "sell", "strike": 100.0}, {"right": "P", "side": "buy", "strike": 95.0}]}
    assert oe.settle(credit, 101.0)["pnl"] == 150.0 and oe.settle(credit, 90.0)["pnl"] == -350.0


def test_the_edge_is_earned_only_up_to_the_forecast_horizon() -> None:
    # A month's edge (40% a year above an 8% drift) priced into a three-month option.
    assert oe.drift_to(0.25, 0.48, 1 / 12, 0.08) == 0.08 + 0.40 / 3
    assert oe.drift_to(1 / 24, 0.48, 1 / 12, 0.08) == 0.48            # inside the horizon: the full edge
    assert oe.drift_to(0.25, 0.48, None, 0.08) == 0.48
    s, t = 100.0, 90 / 365
    chain = {"2027-01-15": _chain(s, t, 0.30, spread=0.02)}
    full = oe.best(1, chain, {"2027-01-15": 90}, s, 0.48, 0.30)
    capped = oe.best(1, chain, {"2027-01-15": 90}, s, 0.48, 0.30, 21 / 252, 0.08)
    assert full is not None and (capped is None or capped["ev_per_risk"] < full["ev_per_risk"])


def test_the_earnings_move_counts_only_for_expiries_after_the_release() -> None:
    assert oe.vol_to(10 / 365, 10, 0.30, (20, 0.08)) == 0.30            # expires before the release
    after = oe.vol_to(30 / 365, 30, 0.30, (20, 0.08))
    longer = oe.vol_to(90 / 365, 90, 0.30, (20, 0.08))
    assert after > longer > 0.30                                        # one move, spread over more time
    assert abs(after - math.sqrt(0.09 + 0.0064 * 365 / 30)) < 1e-12


def test_deep_in_the_money_legs_are_not_candidates() -> None:
    deep = oe.Quote("C600", "C", 600.0, "2026-12-18", 280.0, 289.66, delta=0.99)
    deep_hi = oe.Quote("C610", "C", 610.0, "2026-12-18", 282.94, 290.0, delta=0.98)
    near = oe.Quote("C880", "C", 880.0, "2026-12-18", 60.0, 62.0, delta=0.52)
    got = oe.candidates(1, [deep, deep_hi, near], 880.0, 62 / 365, 0.3, 0.5)
    assert got and all(lg["strike"] == 880.0 for c in got for lg in c["legs"])


def test_a_leg_without_greeks_is_judged_at_the_forecast_volatility() -> None:
    deep = oe.Quote("C70", "C", 70.0, "2026-11-20", 23.0, 24.03)              # no delta quoted
    near = oe.Quote("C95", "C", 95.0, "2026-11-20", 3.0, 3.2)
    got = oe.candidates(1, [deep, near], 93.0, 34 / 365, 0.3, 0.45)
    assert got and all(lg["strike"] == 95.0 for c in got for lg in c["legs"])
