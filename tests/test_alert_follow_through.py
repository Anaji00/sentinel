"""Alert families graded on direction as well as size (producer_grades.follow_table),
and the verdict carried into case files."""
from datetime import date, datetime, timedelta, timezone

from shared.utils import case_file as cf
from shared.utils import producer_grades as pg


def test_follow_table_reads_continuation_and_reversal():
    rows = []
    for i in range(40):
        p = 0.02 if i % 2 else -0.02
        rows.append(("Cont", p, p * 0.5 + (0.001 if i % 3 else -0.001)))
        rows.append(("Rev", p, -p * 0.5 + (0.001 if i % 3 else -0.001)))
        rows.append(("Noise", p, 0.01 if i % 4 < 2 else -0.01))
    by = {r["group"]: r for r in pg.follow_table(rows)}
    assert by["Cont"]["verdict"] == "continues" and by["Cont"]["continued"] == 1.0 and by["Cont"]["follow_bp"] > 0
    assert by["Rev"]["verdict"] == "reverses" and by["Rev"]["continued"] == 0.0 and by["Rev"]["follow_bp"] < 0
    assert by["Noise"]["verdict"] == "no direction"


def test_follow_table_skips_missing_and_flat_priors():
    rows = [("A", None, 0.01), ("A", 0.0, 0.01), ("A", 0.01, None), ("A", 0.01, float("nan"))] + [("A", 0.01, 0.01)] * 3
    (r,) = pg.follow_table(rows)
    assert r["n"] == 3 and r["verdict"] == "too few"


def test_prior_move_is_the_session_before_entry_net_of_spy():
    days = [date(2026, 10, 1) + timedelta(days=i) for i in range(5)]
    closes = [100.0, 100.0, 110.0, 110.0, 110.0]
    spy = {d: 100.0 for d in days}
    # Logged before day 3's open (UTC midnight is the prior NY evening): entry is day index 3,
    # the prior session is day 1 -> day 2, +10%.
    at = datetime(2026, 10, 4, 4, 0, tzinfo=timezone.utc)
    got = pg.prior_move(days, closes, spy, at)
    assert got is not None and abs(got - 0.0953) < 1e-3
    assert pg.prior_move(days, closes, {}, at) is None


def test_tendencies_need_enough_alerts_and_a_verdict():
    grades = {"family_follow_1d": [
        {"group": "Peer", "n": 174, "follow_bp": -82.0, "verdict": "reverses"},
        {"group": "Thin", "n": 12, "follow_bp": -300.0, "verdict": "reverses"},
        {"group": "Flat", "n": 500, "follow_bp": 3.0, "verdict": "no direction"}],
        "family_follow_5d": [{"group": "Peer", "n": 90, "follow_bp": 40.0, "verdict": "continues"}]}
    t = pg.follow_tendencies(grades)
    assert set(t) == {"Peer"}
    assert "reversed the prior session's move over the next session (-82 bp, n 174)" in t["Peer"]
    assert "continued" in t["Peer"] and "next 5 sessions" in t["Peer"]
    assert pg.follow_tendencies(None) == {}


def test_case_file_alert_fact_carries_tendency_and_old_shape_still_builds():
    new = cf.build("ABC", alerts=[("2026-10-29 14:00", "Peer", True, "reversed the prior session's move")])
    old = cf.build("ABC", alerts=[("2026-10-29 14:00", "Peer", True)])
    (a,) = [f for f in new["facts"] if f["kind"] == "alert"]
    (b,) = [f for f in old["facts"] if f["kind"] == "alert"]
    assert "after past alerts the stock reversed the prior session's move" in a["text"]
    assert b["text"] == "Alert: Peer (graded to precede bigger moves)"
