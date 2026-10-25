"""Every signal carries its evidence status, and only measured or documented ones count."""

import math
import pathlib
import sys
from datetime import date, timedelta

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import signal_matrix as sm  # noqa: E402


def _series(n=300, drift=0.001):
    c = [100 * math.exp(drift * i + 0.01 * math.sin(i)) for i in range(n)]
    return c, [x * 1.01 for x in c], [x * 0.99 for x in c], [1e6] * n


def test_price_signals_compute_and_label_themselves() -> None:
    m = sm.Matrix("XYZ")
    c, h, lo, v = _series()
    out = sm.price_signals(m, c, h, lo, v, c, "2026-10-07")
    names = {s.name: s for s in m.signals}
    assert names["12-1 month momentum"].status == "published" and names["12-1 month momentum"].direction == 1
    assert names["RSI 14"].status == "context" and names["RSI 14"].direction == 0
    assert abs(names["beta to SPY, 6 months"].value - 1.0) < 1e-6
    assert out["rv21"] is not None and out["price"] == c[-1]


def test_only_measured_or_documented_signals_count_and_no_edge_never_does() -> None:
    m = sm.Matrix("XYZ")
    m.add(sm.Signal("events", "8k dilution", 1, "1", -1, "proven"))
    m.add(sm.Signal("model", "weekly rank", 0.9, "p90", 1, "supported"))
    m.add(sm.Signal("options", "sweeps", 1e6, "$1M", 1, "no_edge"))
    m.add(sm.Signal("price", "RSI", 80, "80", 1, "context"))
    t = m.tally()
    assert t["by_status"]["no_edge"]["bull"] == 1
    assert t["net"] == -3 + 2 and t["weighted_votes"] == 5
    assert abs(t["lean"] - (-1 / 5)) < 1e-9


def test_event_families_take_the_study_verdict() -> None:
    m = sm.Matrix("XYZ")
    sm.event_signals(m, {"factors": [
        {"family": "8k_dilution", "source": "filings", "direction": -1, "events": 1, "last_seen": "x",
         "status": "proven", "study": {"verdict": "proven", "units": 873}},
        {"family": "block_trade", "source": "tape", "direction": 1, "events": 3, "last_seen": "x", "status": "no_edge"},
        {"family": "contract_award", "source": "contracts", "direction": 0, "events": 1, "last_seen": "x",
         "status": "unstudied"},
    ]})
    st = {s.name: (s.status, s.direction) for s in m.signals}
    assert st["8k dilution"] == ("proven", -1)
    assert st["block trade"] == ("no_edge", 1)
    assert st["contract award"] == ("context", 0)


def test_execution_cost_rises_with_illiquidity() -> None:
    m1, m2 = sm.Matrix("BIG"), sm.Matrix("SMALL")
    share = sm.DAILY_BAR_VOLUME_SHARE          # the daily bars' feed: consolidated, or IEX before
    big = sm.execution_signals(m1, 100.0, 1e9 * share, 0.25, ["stock", True, True, "x"])
    small = sm.execution_signals(m2, 100.0, 5e6 * share, 0.25, ["stock", False, False, "x"])
    assert abs(big["adv_usd"] - 1e9) < 1                 # read back as a consolidated amount
    assert small["cost_bps_1m"] > big["cost_bps_1m"]
    assert any(s.display == "not shortable" for s in m2.signals)


def test_the_earnings_reaction_is_market_adjusted_from_the_close_before() -> None:
    from datetime import datetime, timezone
    from services.agents.dossier_engine import earnings_reaction

    days = [date(2026, 9, 1) + timedelta(days=k) for k in range(10)]
    closes = [100.0] * 5 + [110.0] * 5
    spy = {d: (100.0 if i < 5 else 101.0) for i, d in enumerate(days)}
    accepted = datetime(2026, 9, 3, 20, 5, tzinfo=timezone.utc)     # after the close on day index 2
    r, when = earnings_reaction(days, closes, spy, accepted)
    assert when == str(days[4]) and abs(r - 0.0) < 1e-9             # base day 2 -> day 4: both 100
    r2, _ = earnings_reaction(days, closes, spy, datetime(2026, 9, 4, 20, 5, tzinfo=timezone.utc))
    assert abs(r2 - (0.10 - 0.01)) < 1e-9


def test_every_signal_gets_a_plain_sentence_and_the_verdict_never_overclaims() -> None:
    from shared.utils.signal_plain import explain_matrix

    m = sm.Matrix("XYZ")
    c, h, lo, v = _series()
    sm.price_signals(m, c, h, lo, v, c, "2026-10-07")
    sm.execution_signals(m, 100.0, 1e6, 0.3, ["stock", True, True, "x"])
    m.add(sm.Signal("model", "weekly rank", 0.95, "p95 (quintile 5)", 1, "supported"))
    doc = explain_matrix(m.as_dict())
    assert all(s["plain"] and s["tier"] for s in doc["signals"])
    assert not any("{" in s["plain"] for s in doc["signals"])
    v = doc["verdict"]
    assert v["lean"] == "leans bullish" and v["confidence"] == "moderate"
    assert "Nothing here is proven" in v["why"]
    empty = explain_matrix(sm.Matrix("Q").as_dict())["verdict"]
    assert empty["headline"] == "No clear lean" and empty["confidence"] == "none"


def test_fundamentals_read_from_the_models_ranks_with_their_research_direction() -> None:
    from shared.utils.signal_plain import explain

    m = sm.Matrix("XYZ")
    sm.fundamental_signals(m, {"bm": 0.45, "asset_growth": 0.4, "roa": 0.0, "mom_12_1": 0.3}, "2026-10-07")
    by = {s.name: s for s in m.signals}
    assert by["value (book-to-market)"].direction == 1 and by["asset growth"].direction == -1
    assert by["profitability (ROA)"].direction == 0 and len(m.signals) == 3
    assert "SEC filings" in explain(by["value (book-to-market)"].as_dict())


def test_the_industry_composites_reach_the_board_in_plain_english() -> None:
    from shared.utils.signal_plain import explain

    m = sm.Matrix("JPM")
    sm.fundamental_signals(m, {"value_ind": 0.42, "quality_ind": -0.4}, "2026-10-08")
    by = {s.name: s for s in m.signals}
    assert by["value vs its industry"].direction == 1 and by["quality vs its industry"].direction == -1
    assert "its own industry" in explain(by["value vs its industry"].as_dict())


def test_options_skew_is_ranked_against_todays_stocks() -> None:
    pct = sm.cross_section_pct({"A": 0.01, "B": 0.05, "C": 0.20, "D": None, "E": 0.10, "F": 0.02})
    assert pct["C"] == 1.0 and pct["A"] == 0.0 and "D" not in pct
    m = sm.Matrix("C")
    sm.options_signals(m, {"iv_skew": 0.20, "cp_iv_spread": 0.0}, [], {}, None, "2026-10-08",
                       {"iv_skew": 0.95, "cp_iv_spread": 0.5})
    by = {s.name: s for s in m.signals}
    assert by["IV skew (OTM put - ATM call)"].direction == -1
    assert "steeper than 95% of stocks today" in by["IV skew (OTM put - ATM call)"].display
    assert by["call - put IV spread"].direction == 0


def test_the_board_names_each_signal_in_everyday_words() -> None:
    from shared.utils.signal_plain import everyday

    rank = everyday({"group": "model", "name": "weekly rank", "display": "p12 (quintile 1)", "value": 0.12})
    assert rank == {"label": "Our model's weekly ranking", "shown": "bottom 12%"}
    assert everyday({"group": "model", "name": "monthly rank", "display": "p98 (quintile 5)", "value": 0.98})["shown"] == "top 2%"
    assert everyday({"group": "price", "name": "12-1 month momentum", "display": "+40%"})["label"] == "1-year trend"
    assert everyday({"group": "price", "name": "return 5d", "display": "+2%"})["label"] == "Price change, 5d"
    cost = everyday({"group": "execution", "name": "cost to trade $1M (est.)", "display": "18 bps (12 impact)", "value": 18.0})
    assert cost["shown"] == "0.18%"
    # A name nobody mapped still reads as a label, not as nothing.
    assert everyday({"group": "events", "name": "auditor change", "display": "1 event"})["label"] == "Auditor change"


def test_what_stands_out_is_the_most_extreme_described_traits() -> None:
    from shared.utils.signal_plain import DRIVER_TEXT, stand_out

    feats = {"mom_12_1": 0.45, "vol_63": -0.40, "beta_126": 0.1, "not_described": 0.49, "rev_5": -0.31}
    out = stand_out(feats, k=2)
    assert [t["feature"] for t in out] == ["mom_12_1", "vol_63"]
    assert out[0]["text"] == DRIVER_TEXT["mom_12_1"][0] and out[1]["text"] == DRIVER_TEXT["vol_63"][1]
    # Near the middle is not "standing out"; nothing given, nothing returned.
    assert all(t["feature"] != "beta_126" for t in stand_out(feats))
    assert stand_out(None) == [] and stand_out({}) == []
