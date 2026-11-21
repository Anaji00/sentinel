from datetime import date

from shared.utils.track_slice import backtest_of, outcome_spread, percentile, slice_summary

REPORT = {"horizons": {"monthly": {"chosen": "lgbm_rank", "models": [
    {"model": "other", "dates": 5, "mean_ic": 0.9, "net_sharpe": 9.0},
    {"model": "lgbm_rank", "dates": 102, "mean_ic": 0.0364, "net_sharpe": 0.78, "validated": False}]}}}


def test_percentile_interpolates():
    assert percentile([0.0, 10.0], 0.5) == 5.0 and percentile([3.0], 0.9) == 3.0


def test_spread_is_withheld_below_five_outcomes_but_n_is_kept():
    assert outcome_spread([0.01, 0.02, None]) == {"p10_bps": None, "p50_bps": None, "p90_bps": None, "n": 2}
    s = outcome_spread([-0.02, -0.01, 0.0, 0.01, 0.02, 0.03])
    assert s["n"] == 6 and s["p10_bps"] < s["p50_bps"] < s["p90_bps"]


def test_slice_summary_counts_right_calls_and_dates_the_live_book():
    rows = [{"net_excess": 0.01, "idea_day": date(2026, 10, 26)}, {"net_excess": -0.02, "idea_day": date(2026, 10, 24)},
            {"net_excess": 0.03, "idea_day": date(2026, 11, 2)}, {"net_excess": None, "idea_day": date(2026, 10, 1)}]
    s = slice_summary(rows, [], REPORT, "monthly")
    assert s["n_calls"] == 3 and s["share_right"] == 0.667 and s["live"] == {"n": 3, "since": "2026-10-24"}
    assert s["mean_net_bps_after_costs"] == 66.7 and s["median_net_bps"] == 100.0
    assert s["backtest"]["ic"] == 0.0364 and s["backtest"]["months"] == 102


def test_empty_slice_is_null_not_zero():
    s = slice_summary([], [], None, "weekly")
    assert s["n_calls"] == 0 and s["share_right"] is None and s["mean_net_bps_after_costs"] is None
    assert s["live"]["since"] is None and s["backtest"]["ic"] is None and s["outcome_spread"]["p50_bps"] is None


def test_backtest_months_only_for_monthly():
    rep = {"horizons": {"weekly": {"chosen": "m", "models": [{"model": "m", "dates": 40, "mean_ic": 0.01, "net_sharpe": 0.2}]}}}
    b = backtest_of(rep, "weekly")
    assert b["months"] is None and b["periods"] == 40
    assert backtest_of(rep, "daily")["ic"] is None


def test_open_marked_trades_count_so_stopped_losers_are_not_the_whole_story():
    closed = [{"net_excess": -0.08, "idea_day": date(2026, 10, 8), "exit_reason": "stop"}] * 3
    running = [{"net_excess": 0.05, "idea_day": date(2026, 10, 18)}] * 5
    s = slice_summary(closed, running, None, "monthly")
    assert s["n_calls"] == 8 and s["n_closed"] == 3 and s["n_open_marked"] == 5 and s["stopped"] == 3
    assert s["share_right"] == 0.625 and s["live"]["since"] == "2026-10-08"


def test_the_slice_reports_how_many_days_the_calls_span():
    rows = [{"net_excess": 0.01, "idea_day": date(2026, 10, 8)}, {"net_excess": 0.02, "idea_day": date(2026, 10, 8)},
            {"net_excess": -0.01, "idea_day": date(2026, 10, 14)}]
    s = slice_summary(rows, [{"net_excess": 0.0, "idea_day": date(2026, 10, 9)}], None, "monthly")
    assert s["n_calls"] == 4 and s["n_dates"] == 3 and s["first_date"] == "2026-10-08" and s["last_date"] == "2026-10-14"
    e = slice_summary([], [], None, "monthly")
    assert e["n_dates"] == 0 and e["first_date"] is None and e["last_date"] is None


def test_an_open_trade_without_a_bar_is_counted_as_unmarked_not_dropped():
    # Review F14 (18 Nov): the open-marks query is a LEFT JOIN now, so a trade with no daily bar arrives with no mark and is reported.
    from shared.utils.track_slice import slice_summary
    out = slice_summary([], [{"idea_day": "2026-11-02", "net_excess": 0.01}, {"idea_day": "2026-11-03", "net_excess": None}], None, "monthly")
    assert out["n_open_marked"] == 1 and out["n_open_unmarked"] == 1 and out["n_calls"] == 1
