"""Trade ideas: structure from the vol regime, real legs by delta, honest economics."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import recommendations as rc  # noqa: E402

TODAY = date(2026, 10, 8)


def _c(right, strike, exp, delta, bid, ask, iv=0.4):
    sym = f"XYZ{exp.replace('-', '')[2:]}{right}{int(strike * 1000):08d}"
    return ({"symbol": sym, "option_type": "CALL" if right == "C" else "PUT", "strike": strike, "expiry": exp},
            {"latestQuote": {"bp": bid, "ap": ask}, "greeks": {"delta": delta}, "impliedVolatility": iv})


CHAIN = [
    _c("C", 100, "2026-11-20", 0.52, 4.9, 5.1), _c("C", 105, "2026-11-20", 0.44, 3.0, 3.2),
    _c("C", 112, "2026-11-20", 0.26, 1.4, 1.5), _c("C", 118, "2026-11-20", 0.15, 0.6, 0.7),
    _c("P", 95, "2026-11-20", -0.44, 3.1, 3.3), _c("P", 90, "2026-11-20", -0.29, 1.8, 1.9),
    _c("P", 85, "2026-11-20", -0.16, 0.9, 1.0), _c("P", 80, "2026-11-20", -0.08, 0.2, 0.9),  # too wide
    _c("C", 105, "2026-10-10", 0.45, 1.0, 1.1),                                               # too near
]


def test_the_regime_decides_the_structure() -> None:
    assert rc.iv_regime(0.30, 0.40) == "cheap" and rc.iv_regime(0.60, 0.40) == "rich"
    assert rc.iv_regime(0.45, 0.40) == "fair" and rc.iv_regime(None, 0.4) == "unknown"
    assert rc.choose_structure(1, "cheap") == "long_call"
    assert rc.choose_structure(-1, "rich") == "bear_call_credit"
    assert rc.choose_structure(1, "fair") == "bull_call_debit"
    assert rc.choose_structure(-1, "unknown") == "bear_put_debit"


def test_contracts_are_real_quoted_and_inside_the_window() -> None:
    by = rc.tradable_chain(CHAIN, TODAY, rc.EXPIRY_WINDOWS["monthly"])
    assert list(by) == ["2026-11-20"]
    assert all(lg.strike != 80 for lg in by["2026-11-20"])     # the wide quote is refused


def test_debit_spread_economics() -> None:
    by = rc.tradable_chain(CHAIN, TODAY, rc.EXPIRY_WINDOWS["monthly"])
    idea = rc.build_option_idea("bull_call_debit", by, TODAY, rc.EXPIRY_WINDOWS["monthly"])
    assert [lg["strike"] for lg in idea["legs"]] == [105, 112]
    debit = 3.1 - 1.45
    assert idea["max_loss"] == round(debit * 100, 2) and idea["max_gain"] == round((7 - debit) * 100, 2)
    assert idea["breakeven"] == round(105 + debit, 2) and idea["net"] < 0


def test_credit_spread_economics() -> None:
    by = rc.tradable_chain(CHAIN, TODAY, rc.EXPIRY_WINDOWS["monthly"])
    idea = rc.build_option_idea("bull_put_credit", by, TODAY, rc.EXPIRY_WINDOWS["monthly"])
    assert [(lg["side"], lg["strike"]) for lg in idea["legs"]] == [("sell", 90), ("buy", 85)]
    credit = 1.85 - 0.95
    assert idea["net"] == round(credit * 100, 2) and idea["max_loss"] == round((5 - credit) * 100, 2)
    assert idea["prob_profit_by_delta"] == round(1 - 0.29, 2)


def test_no_chain_no_option_idea() -> None:
    assert rc.build_option_idea("long_call", {}, TODAY, rc.EXPIRY_WINDOWS["weekly"]) is None


def test_ideas_take_both_ends_and_skip_the_index() -> None:
    scores = {t: {"percentile": p, "quintile": 1 + int(p * 4.99)} for t, p in
              [("AAA", 0.99), ("BBB", 0.9), ("SPY", 1.0), ("YYY", 0.05), ("ZZZ", 0.0)]}
    ideas = rc.stock_ideas(scores, n=2)
    assert [(i["ticker"], i["direction"]) for i in ideas] == [("AAA", 1), ("BBB", 1), ("ZZZ", -1), ("YYY", -1)]


def test_stock_plan_stops_on_the_right_side() -> None:
    assert rc.stock_plan(1, 100.0, 2.0, 21, 80.0)["stop"] == 96.0
    assert rc.stock_plan(-1, 100.0, 2.0, 5, None)["stop"] == 104.0
    assert rc.realized_vol([100.0] * 30) == 0.0
    assert rc.atr([11.0] * 20, [9.0] * 20, [10.0] * 20) == 2.0


def test_ideas_must_be_tradable_by_a_person() -> None:
    stock = ["stock", True, True, "Apple Inc. Common Stock"]
    nobor = ["stock", False, False, "Jaguar Health, Inc. Common Stock"]
    fund = ["fund", True, True, "Direxion Daily MU Bull 2X ETF"]
    assert rc.idea_screen(1, stock, 250.0, 3e6, 0.3) is None
    assert rc.idea_screen(1, fund, 40.0, 3e6, 0.3).startswith("not a common stock")
    assert rc.idea_screen(1, stock, 6.0, 3e6, 0.3).startswith("price under")
    assert rc.idea_screen(1, stock, 50.0, 1e4, 0.3) == "too thinly traded"
    assert rc.idea_screen(1, stock, 50.0, 3e6, 1.8).startswith("realised volatility")
    assert rc.idea_screen(-1, nobor, 50.0, 3e6, 0.5) == "not easy to borrow for a short"
    assert rc.idea_screen(1, nobor, 50.0, 3e6, 0.5) is None


def test_a_pinned_name_still_gets_a_real_stop() -> None:
    assert rc.stock_plan(-1, 54.76, 0.05, 5, None)["stop"] == round(54.76 * 1.02, 2)
