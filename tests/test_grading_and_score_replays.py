"""Pure-function tests for the 13 Nov grading and score-replay work: excess_return, score_resolution, macro_fanout, excess_grading."""
import asyncio
import math
from datetime import date, datetime, timedelta

from shared.utils import excess_return as er
from shared.utils import macro_fanout as mf
from shared.utils import score_resolution as sr


# ---- excess_return -------------------------------------------------------------------------------------------------------------

def _days(n, start=date(2026, 9, 1)):
    return [start + timedelta(days=i) for i in range(n)]


def test_direction_sign_and_sector_etf():
    assert er.direction_sign("Long") == 1 and er.direction_sign("bearish") == -1
    assert er.direction_sign("flat") is None and er.direction_sign(None) is None
    assert er.sector_etf_for_sic(3674) == "XLK"      # semiconductors
    assert er.sector_etf_for_sic(None) is None and er.sector_etf_for_sic(6770) is None


def test_window_return_uses_last_close_on_or_before_and_rejects_stale_bars():
    days = _days(10)
    closes = [100.0 + i for i in range(10)]
    assert er.window_return(days, closes, days[2], days[5]) == closes[5] / closes[2] - 1
    # a Sunday falls back to the Saturday bar in this synthetic calendar; an end before the first bar is None
    assert er.window_return(days, closes, date(2026, 8, 1), days[5]) is None
    assert er.window_return(days, closes, days[5], days[5]) is None
    # more than MAX_STALE_DAYS past the last bar is a gap, not a close
    assert er.close_on_or_before(days, closes, days[-1] + timedelta(days=er.MAX_STALE_DAYS + 1)) is None


def test_call_window_reads_strings_and_naive_timestamps():
    s, e = er.call_window("2026-09-15T22:30:00+00:00", 24)
    assert (s, e) == (date(2026, 9, 15), date(2026, 9, 16))
    assert er.call_window(datetime(2026, 9, 15, 23, 0), 2)[1] == date(2026, 9, 16)


def test_excess_returns_none_propagates():
    x = er.excess_returns(0.05, 0.02, None)
    assert math.isclose(x["excess_spy"], 0.03) and x["excess_sector"] is None
    assert er.excess_returns(None, 0.02, 0.01)["excess_spy"] is None


def test_clustered_t_treats_each_day_as_one_unit():
    # 10 calls on one day move together: one cluster, no t; five days of identical sign and spread give a t
    assert er.clustered_t({"d": [1.0] * 10})["t"] is None
    out = er.clustered_t({i: [1.0 + 0.1 * i] * 4 for i in range(6)})
    assert out["days"] == 6 and out["n"] == 24 and out["t"] > 5
    assert er.clustered_t({})["n"] == 0


def test_hit_report_splits_directions_and_ignores_unsigned():
    d0 = date(2026, 9, 1)
    rows = [("up", d0 + timedelta(days=i), 0.01 if i % 2 == 0 else -0.01, True) for i in range(10)]
    rows += [("down", d0 + timedelta(days=i), 0.02, False) for i in range(10)]
    rows += [("flat", d0, 0.5, True), ("up", d0, None, True)]
    rep = {r["direction"]: r for r in er.hit_report(rows)}
    assert rep["up"]["n"] == 10 and math.isclose(rep["up"]["hit_rate"], 0.5)
    assert rep["down"]["hit_rate"] == 1.0 and rep["down"]["raw_hit_rate"] == 0.0
    assert rep["all"]["n"] == 20


# ---- score_resolution ----------------------------------------------------------------------------------------------------------

def test_flag_defaults_off(monkeypatch):
    monkeypatch.delenv(sr.FLAG, raising=False)
    assert not sr.v2_enabled()
    monkeypatch.setenv(sr.FLAG, "1")
    assert sr.v2_enabled()


def test_short_volume_score_never_exceeds_old_and_follows_formula():
    assert sr.short_volume_score(0.95, None) == 0.95
    assert sr.short_volume_score(0.95, 2.0) == 0.55
    assert sr.short_volume_score(0.60, 8.0) == 0.60          # capped at the old score
    z = 5.0
    assert sr.short_volume_score(0.95, z) == round(0.55 + 0.40 * (1 - math.exp(-(z - 2) / 3)), 3)
    assert sr.short_volume_score(0.9, 0.5) == 0.55           # below the gate's own z of 2 is treated as 2


def test_short_volume_z_matches_population_sd_and_floors_flat_history():
    hist = [0.4, 0.5, 0.6, 0.5]
    mu = 0.5
    sd = math.sqrt(sum((h - mu) ** 2 for h in hist) / 4)
    assert math.isclose(sr.short_volume_z(0.8, hist), (0.8 - mu) / sd)
    assert math.isclose(sr.short_volume_z(0.8, [0.5] * 5), (0.8 - 0.5) / 0.03)
    assert sr.short_volume_z(0.8, [0.5]) is None


def test_news_z_and_score_range():
    assert sr.news_score(None) == 0.5 and sr.news_score(-3) == 0.5
    assert sr.news_score(4) == 0.75 and sr.news_score(40) == 0.75
    assert sr.news_score(2) == 0.625
    assert sr.news_z(0, 0) == 0.0                              # empty baseline, no articles: not unusual
    assert sr.news_z(4, 0) == 8.0                              # four articles against none: floored sd of 0.5
    mu = 160 / sr.NEWS_WINDOWS_IN_BASELINE
    assert math.isclose(sr.news_z(5, 160), (5 - mu) / math.sqrt(mu))


def test_spearman_ranks_ties_and_constants():
    assert sr.ranks([3, 1, 1, 2]) == [4.0, 1.5, 1.5, 3.0]
    assert math.isclose(sr.spearman([1, 2, 3, 4], [10, 20, 30, 40]), 1.0)
    assert math.isclose(sr.spearman([1, 2, 3, 4], [4, 3, 2, 1]), -1.0)
    assert sr.spearman([1, 1, 1], [1, 2, 3]) is None and sr.spearman([1, 2], [1, 2]) is None


def test_top_share_mean_splits_ties_at_the_cutoff():
    scores = [0.95] * 4 + [0.5] * 6
    values = [4.0, 4.0, 0.0, 0.0] + [1.0] * 6
    # top 30% = 3 slots, all inside the tie at 0.95: expected value is the tie's mean, 2.0
    assert math.isclose(sr.top_share_mean(scores, values, 0.30), 2.0)
    assert sr.top_share_mean([], [], 0.01) is None
    assert sr.top_share_mean([0.9, 0.8], [None, None], 0.5) is None
    assert sr.top_share_mean([0.9, 0.8, 0.7], [3.0, 2.0, 1.0], 0.01) == 3.0


def test_cut_changes_and_counts():
    old, new = [0.95, 0.7, 0.5], [0.6, 0.7, 0.62]
    ch = sr.cut_changes(old, new, cuts=(0.6, 0.65, 0.9))
    assert ch == {"newly_above_cut": 1, "newly_below_cut": 2}
    assert sr.counts_above_cut(old, new, cuts=(0.6,))[0.6] == (2, 3)


def test_distribution_reports_mode_and_shares():
    d = sr.distribution([0.82] * 8 + [0.5, 0.6], levels=(0.8,))
    assert d["n"] == 10 and d["distinct"] == 3 and d["mode"] == 0.82 and d["mode_share"] == 0.8 and d["share_ge_0.8"] == 0.8
    assert sr.distribution([]) == {"n": 0}


def test_five_session_abs_move():
    days = _days(12)
    closes = [100.0] * 6 + [110.0] * 6
    assert math.isclose(sr.five_session_abs_move(days, closes, days[1]), 0.10)
    assert sr.five_session_abs_move(days, closes, days[9]) is None          # not enough bars after
    assert sr.five_session_abs_move(days, closes, date(2026, 1, 1)) is None


def test_short_volume_verdicts_pass_and_fail():
    old = [0.95] * 30 + [0.7 + 0.001 * i for i in range(70)]
    moves = [0.05] * 100
    # an unchanged score fails the cap bar and passes the rank and cut bars
    same = {b: ok for b, _, ok in sr.short_volume_verdicts(old, old, moves)}
    assert not same["share on the 0.95 cap < 5%"] and same["rank correlation with old >= 0.9"] and same["no event newly above a cut"]
    up = {b: ok for b, _, ok in sr.short_volume_verdicts([0.5, 0.6, 0.7], [0.65, 0.6, 0.7], [None] * 3)}
    assert not up["no event newly above a cut"]


def test_news_verdicts():
    old = [0.5] * 1000
    flat = {b: ok for b, _, ok in sr.news_verdicts(old, [0.5] * 1000)}
    assert not any(flat[k] for k in flat if k.startswith(">= 10")) and flat["no count above any cut changes"]
    spread = [0.5 + 0.0005 * i for i in range(1000)]
    res = {b: ok for b, _, ok in sr.news_verdicts(old, spread)}
    assert res[">= 10 distinct values over 1000 events"] and not res["no count above any cut changes"]
    assert not sr.news_verdicts(old[:10], spread[:10])[0][2]                 # fewer than 1,000 events cannot pass


def test_vessel_and_extended_hours_proposals_stay_under_old_score():
    assert sr.spoof_speed_knots("AIS identity anomaly: X implies 756.4 kn over 27.4 nm") == 756.4
    assert sr.spoof_speed_knots("AIS position anomaly: X reporting inland") is None
    assert sr.vessel_spoof_score(None) == 0.82 and sr.vessel_spoof_score(50.0) == 0.62
    assert sr.vessel_spoof_score(5000.0) == 0.82 and sr.vessel_spoof_score(10 ** 7) == 0.82
    assert 0.62 < sr.vessel_spoof_score(217.8) < 0.82
    assert sr.extended_hours_score(0.97, None) == 0.97
    assert sr.extended_hours_score(0.97, 250_000) == 0.5
    assert sr.extended_hours_score(0.6, 25_000_000) == 0.6                   # capped at the old score
    assert sr.extended_hours_score(0.39996, 1e12) == 0.39996                 # a rounded size never lifts a score over its old value


# ---- macro_fanout --------------------------------------------------------------------------------------------------------------

def test_generic_rule_matching():
    for name in ("US2Y Yield Drop and D Equity Upswing", "Macro Energy Price Headline to Equity Block Convergence",
                 "Cross-Asset Yield and Equity Negative Correlation Detection", "US10Y Yield Rise Predicts Nike Selloff"):
        assert mf.is_generic_fanout_rule(name), name
    for name in ("Macro Energy Cost Pressure Convergence", "Macro Yield Headline to Equity Anomaly", "US10Y spike", None, ""):
        assert not mf.is_generic_fanout_rule(name), name


def test_fanout_verdicts():
    ok = {b: p for b, _, p in mf.fanout_verdicts(100, 60, 20, 0, 1.2, 1.3)}
    assert all(ok.values())
    bad = {b: p for b, _, p in mf.fanout_verdicts(100, 40, 20, 2, 1.3, 1.2)}
    assert not any(bad.values())
    empty = mf.fanout_verdicts(0, 0, 0, 0, None, None)
    assert not any(p for _, _, p in empty) and mf.mean_or_none([]) is None


# ---- excess_grading (fake database) --------------------------------------------------------------------------------------------

class FakeDb:
    def __init__(self, pending, bars):
        self.pending, self.bars, self.executed = pending, bars, []

    async def query(self, sql, *args):
        return self.pending if "agent_predictions" in sql else self.bars

    async def execute(self, sql, *args):
        self.executed.append((sql, args))


def test_grade_pending_grades_counts_missing_bars_and_survives_a_bad_row(monkeypatch):
    monkeypatch.delenv("EXCESS_ALIGNED_WINDOWS", raising=False)      # the old window is under test; .env now turns the aligned one on and a dotenv load can leak it
    from services.agents import excess_grading as eg
    start = date(2026, 9, 1)
    bars = []
    for i in range(12):
        d = start + timedelta(days=i)
        bars += [{"ticker": "AAA", "day": d, "close": 100.0 + 2 * i}, {"ticker": "SPY", "day": d, "close": 400.0 + i}]
    ts = "2026-09-03T12:00:00+00:00"
    pending = [
        {"id": 1, "ticker": "AAA", "direction": "up", "occurred_at": ts, "horizon_hours": 48, "sic": None},
        {"id": 2, "ticker": "ZZZ", "direction": "down", "occurred_at": ts, "horizon_hours": 48, "sic": None},   # no bars
        {"id": 3, "ticker": "AAA", "direction": "up", "occurred_at": None, "horizon_hours": 48, "sic": None},   # bad row
    ]
    db = FakeDb(pending, bars)
    out = asyncio.run(eg.grade_pending(db))
    assert out == {"selected": 3, "graded": 1, "no_bars": 1, "late": 0, "failed": 1}      # "late" added by review F13; nothing here is late, so the rest is unchanged
    graded = [a for sql, a in db.executed if "excess_graded_at = NOW()" in sql]
    assert len(graded) == 1 and graded[0][0] == 1
    # closes on Sep 3 and Sep 5 are indexes 2 and 4 (the aligned window of layer audit 5.2 is off by default: tests/test_aligned_grading_window.py)
    stock = (100.0 + 8) / (100.0 + 4) - 1
    spy = (400.0 + 4) / (400.0 + 2) - 1
    assert math.isclose(graded[0][1], stock) and math.isclose(graded[0][2], spy) and math.isclose(graded[0][5], stock - spy)
    attempts = sorted(a[0] for sql, a in db.executed if "excess_attempts = excess_attempts + 1" in sql)
    assert attempts == [2, 3]


def test_late_bars_are_not_counted_as_missing_but_missing_bars_still_park():
    # Review F13 (18 Nov): a top-up outage must not park rows for good; a ticker with no bars while SPY is current still does.
    from services.agents import excess_grading as eg
    start = date(2026, 9, 1)
    ts = "2026-09-03T12:00:00+00:00"
    row = {"id": 1, "ticker": "ZZZ", "direction": "up", "occurred_at": ts, "horizon_hours": 48, "sic": None}
    late_bars = [{"ticker": "SPY", "day": start + timedelta(days=i), "close": 400.0 + i} for i in range(3)]         # SPY stops Sep 3, before the Sep 5 exit
    db = FakeDb([row], late_bars)
    assert asyncio.run(eg.grade_pending(db))["late"] == 1 and not [1 for sql, _ in db.executed if "excess_attempts" in sql]
    current = [{"ticker": "SPY", "day": start + timedelta(days=i), "close": 400.0 + i} for i in range(12)]
    db = FakeDb([row], current)
    out = asyncio.run(eg.grade_pending(db))
    assert out["late"] == 0 and out["no_bars"] == 1 and [1 for sql, _ in db.executed if "excess_attempts" in sql]


def test_short_volume_score_b_and_outcome_verdicts():
    assert sr.short_volume_score_b(0.95, None) == 0.95
    assert sr.short_volume_score_b(0.95, 2.0) == 0.70
    assert sr.short_volume_score_b(0.72, 30.0) == 0.72                      # capped at the old score
    old = [0.7 + 0.001 * i for i in range(400)]
    moves = [0.01 * (i % 7) for i in range(400)]
    res = sr.outcome_verdicts(old, old, moves)
    assert res[0][2] is True                                                # equal Spearman passes (new >= old)
    assert sr.outcome_verdicts(old[:50], old[:50], moves[:50])[0][2] is False   # fewer than 300 events with an outcome
    assert sr.share_below([0.5, 0.65, 0.8], 0.7) == 2 / 3
