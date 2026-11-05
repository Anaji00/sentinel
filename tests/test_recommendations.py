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
    import math
    # Below a long's entry and above a short's, widening with the hold.
    assert rc.stock_plan(1, 100.0, 2.0, 21, 80.0)["stop"] == round(100.0 - 1.2 * 2.0 * math.sqrt(21), 2)
    assert rc.stock_plan(-1, 100.0, 2.0, 5, None)["stop"] == round(100.0 + 1.2 * 2.0 * math.sqrt(5), 2)
    assert rc.stock_plan(1, 100.0, 2.0, 1, None)["stop"] == 96.0            # a day's hold keeps two ATRs


def test_a_longer_hold_gets_a_wider_stop() -> None:
    gap = [100.0 - rc.stock_plan(1, 100.0, 2.0, h, None)["stop"] for h in (1, 5, 21)]
    assert gap[0] < gap[1] < gap[2]
    assert gap[2] > 10.0               # a month's hold: wider than a month's ordinary swing at this ATR
    assert rc.realized_vol([100.0] * 30) == 0.0
    assert rc.atr([11.0] * 20, [9.0] * 20, [10.0] * 20) == 2.0


def test_ideas_must_be_tradable_by_a_person() -> None:
    stock = ["stock", True, True, "Apple Inc. Common Stock"]
    nobor = ["stock", False, False, "Jaguar Health, Inc. Common Stock"]
    fund = ["fund", True, True, "Direxion Daily MU Bull 2X ETF"]
    liquid = 1.2e8 * rc.DAILY_BAR_VOLUME_SHARE      # $120M a day across every exchange
    assert rc.idea_screen(1, stock, 250.0, liquid, 0.3) is None
    assert rc.idea_screen(1, fund, 40.0, liquid, 0.3).startswith("not a common stock")
    assert rc.idea_screen(1, stock, 6.0, liquid, 0.3).startswith("price under")
    assert rc.idea_screen(1, stock, 50.0, 4e5 * rc.DAILY_BAR_VOLUME_SHARE, 0.3) == "too thinly traded"
    assert rc.idea_screen(1, stock, 50.0, liquid, 1.8).startswith("realised volatility")
    assert rc.idea_screen(-1, nobor, 50.0, liquid, 0.5) == "not easy to borrow for a short"
    assert rc.idea_screen(1, nobor, 50.0, liquid, 0.5) is None


def test_a_pinned_name_still_gets_a_real_stop() -> None:
    assert rc.stock_plan(-1, 54.76, 0.05, 5, None)["stop"] == round(54.76 * 1.02, 2)


def _sig(group, name, direction, status, display="x"):
    return {"group": group, "name": name, "direction": direction, "status": status, "display": display}


def test_insider_buying_and_buy_flow_outweigh_the_models_short() -> None:
    signals = [
        _sig("model", "weekly rank", -1, "supported"),                       # the model is not evidence here
        _sig("insider", "insider clusters, 90d", 1, "published", "3"),
        _sig("insider", "open-market net, 90d (plans excluded)", 1, "published", "$+2.10M"),
        _sig("events", "block buy accumulation", 1, "published"),
        _sig("options", "large print premium, calls - puts, 5d", 1, "no_edge"),   # measured no edge: counts 0
    ]
    bal = rc.evidence_balance(signals)
    assert bal["bull"] == 3.0 and bal["bear"] == 0.0
    why = rc.overruled(-1, "supported", bal)
    assert why and "insider clusters" in why
    assert rc.overruled(-1, "validated", rc.evidence_balance(signals[:3])) is None   # 2 < 3: the model holds
    assert rc.event_led(bal) == 1


def test_proven_bad_news_outweighs_a_model_long_and_mixed_evidence_carries_nothing() -> None:
    bad = [_sig("filings", "8-K 3.02 dilution", -1, "proven")]
    assert rc.overruled(1, "supported", rc.evidence_balance(bad))
    mixed = rc.evidence_balance([_sig("insider", "clusters", 1, "published"),
                                 _sig("filings", "8-K 3.02 dilution", -1, "proven"),
                                 _sig("events", "block buys", 1, "published")])
    assert rc.event_led(mixed) == 0


def test_the_models_own_factors_are_not_counted_again_as_evidence() -> None:
    signals = [_sig("fundamentals", "value vs its industry", 1, "published"),
               _sig("price", "12-1 month momentum", 1, "published"),
               _sig("options", "IV skew", 1, "published"),
               _sig("insider", "insider clusters, 90d", 1, "published")]
    bal = rc.evidence_balance(signals)
    assert bal["bull"] == 1.0 and len(bal["bull_reasons"]) == 1


def test_evidence_pouring_in_weighs_more_than_one_print() -> None:
    one = {"group": "events", "name": "block buy", "direction": 1, "status": "published", "display": "1", "value": 1}
    many = {**one, "value": 12}
    assert rc.evidence_balance([one])["bull"] == 1.0
    assert rc.evidence_balance([many])["bull"] == 2.0
    # Outweighing takes a full vote more than the model: a flood alone ties a
    # supported model (2 vs 2) and does not; with an insider cluster it does.
    assert rc.overruled(-1, "supported", rc.evidence_balance([many])) is None
    cluster = {"group": "insider", "name": "insider clusters, 90d", "direction": 1, "status": "published",
               "display": "2", "value": 2}
    assert rc.overruled(-1, "supported", rc.evidence_balance([many, cluster]))


def test_block_trades_carry_their_tick_side_into_the_families() -> None:
    from datetime import datetime, timezone

    from shared.utils.signal_families import observations

    base = {"type": "equity_block", "event_id": "b", "created_at": datetime(2026, 10, 8, tzinfo=timezone.utc),
            "primary_entity_id": "MSFT", "financial_data": {"ticker": "MSFT"}}
    fams = {o.family: o.direction for o in observations({**base, "tags": ["aggressor_buy"]})}
    assert fams.get("block_buy") == 1
    fams = {o.family: o.direction for o in observations({**base, "tags": ["aggressor_sell"]})}
    assert fams.get("block_sell") == -1


def test_earnings_are_estimated_a_quarter_on_and_flagged_inside_the_hold() -> None:
    from datetime import date

    nxt = rc.next_earnings(date(2026, 7, 29), date(2026, 10, 9))
    assert nxt == date(2026, 10, 28)
    assert rc.earnings_inside(nxt, date(2026, 10, 9), 21) is True      # a month's hold spans it
    assert rc.earnings_inside(nxt, date(2026, 10, 9), 5) is False      # a week's does not
    assert rc.next_earnings(None, date(2026, 10, 9)) is None


def test_risk_is_stated_in_dollars_and_volatile_names_get_less() -> None:
    calm = rc.risk_terms(1, 100.0, 95.0, 5e8, 0.20)
    wild = rc.risk_terms(1, 100.0, 85.0, 5e8, 0.80)
    assert calm["loss_at_stop_usd"] == 500.0 and wild["loss_at_stop_usd"] == 1500.0
    assert wild["vol_sized_usd"] < calm["vol_sized_usd"] <= 10_000


def test_the_reasons_are_the_drivers_pushing_the_ideas_way() -> None:
    from shared.utils.signal_plain import idea_reasons

    drivers = [["ivol_63", 0.04, -0.45], ["mom_12_1", -0.03, -0.3], ["quality_ind", 0.02, 0.41],
               ["ey", 0.01, 0.2]]
    assert idea_reasons(drivers, 1) == ["moves little apart from the market",
                                        "is more profitable and cash-backed than its industry",
                                        "is cheap for its earnings"]
    assert idea_reasons(drivers, -1) == ["has fallen over the past year"]
    assert idea_reasons(None, 1) == []


def test_a_stock_both_horizons_want_is_one_line_first() -> None:
    import importlib
    eng = importlib.import_module("services.agents.recommendation_engine")
    horizons = {
        "weekly": {"ideas": [{"ticker": "NEM", "direction": 1, "reasons": ["a"]},
                             {"ticker": "GIS", "direction": -1}]},
        "monthly": {"ideas": [{"ticker": "NEM", "direction": 1}, {"ticker": "TER", "direction": 1}]},
    }
    c = eng.consensus(horizons)
    assert c[0]["ticker"] == "NEM" and c[0]["horizons"] == ["weekly", "monthly"] and c[0]["reasons"] == ["a"]
    assert [e["ticker"] for e in c[1:]] == ["TER", "GIS"]          # buys before sells after agreement


def test_ideas_ranked_within_industry_take_each_industrys_best_first() -> None:
    chips = {f"C{i}": {"percentile": 0.70 + 0.05 * i} for i in range(6)}      # every chip above every utility
    utils = {f"U{i}": {"percentile": 0.05 + 0.05 * i} for i in range(5)}
    scores = {**chips, **utils}
    industries = {**{t: "Semiconductors" for t in chips}, **{t: "Electric Services" for t in utils}}
    wide = rc.stock_ideas(scores, n=2)
    assert {i["ticker"] for i in wide if i["direction"] > 0} == {"C5", "C4"}
    neutral = rc.stock_ideas(scores, n=2, industries=industries)
    longs = [i for i in neutral if i["direction"] > 0]
    shorts = [i for i in neutral if i["direction"] < 0]
    assert [i["ticker"] for i in longs] == ["C5", "U4"]                       # each industry's best
    assert [i["ticker"] for i in shorts] == ["U0", "C0"]                      # and each one's worst
    assert longs[1]["percentile"] == 0.25 and longs[1]["industry_percentile"] == 0.9   # forecast on the universe rank
    # A large industry's second name does not jump a small industry's best.
    big = {f"B{i}": {"percentile": 0.40 + 0.01 * i} for i in range(40)}
    small = {f"S{i}": {"percentile": 0.10 + 0.01 * i} for i in range(5)}
    secs = {**{t: "Big" for t in big}, **{t: "Small" for t in small}}
    got = [i["ticker"] for i in rc.stock_ideas({**big, **small}, n=3, industries=secs) if i["direction"] > 0]
    assert got == ["B39", "S4", "B38"]


def test_ideas_that_move_together_count_once() -> None:
    import random
    rng = random.Random(1)
    theme = [rng.gauss(0, 0.02) for _ in range(63)]
    a = [t + rng.gauss(0, 0.004) for t in theme]
    b = [t + rng.gauss(0, 0.004) for t in theme]                   # the same theme
    c = [rng.gauss(0, 0.02) for _ in range(63)]                    # its own story
    assert rc.correlation(a, b) > 0.9 and abs(rc.correlation(a, c)) < 0.4
    assert rc.too_close(b, [a]) and not rc.too_close(c, [a])
    assert not rc.too_close(a, []) and rc.correlation(a[:5], b[:5]) is None
    closes = [100.0 * (1.01 ** k) for k in range(70)]
    assert len(rc.log_returns(closes)) == rc.CORRELATION_WINDOW


def test_effective_bets_counts_independent_positions() -> None:
    import random
    rng = random.Random(7)
    theme = [rng.gauss(0, 0.02) for _ in range(63)]
    same = [[t + rng.gauss(0, 0.001) for t in theme] for _ in range(6)]
    apart = [[rng.gauss(0, 0.02) for _ in range(63)] for _ in range(6)]
    assert rc.effective_bets(same) < 1.2
    assert rc.effective_bets(apart) > 4.5
    assert rc.effective_bets([theme]) == 1.0 and rc.effective_bets([]) is None
