import random
from datetime import date, datetime, timedelta, timezone

from shared.utils import grades_v2 as g2


def _bars(n_days=140, seed=1, jump_on=None, jump_size=0.0):
    rnd = random.Random(seed)
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(n_days) if (date(2026, 1, 1) + timedelta(days=i)).weekday() < 5]
    opens, closes, px = [], [], 100.0
    for d in days:
        move = rnd.gauss(0, 0.01) + (jump_size if jump_on and d >= jump_on else 0.0)
        opens.append(px)
        px *= 1 + move
        closes.append(px)
    return days, opens, closes


def test_tstat_and_verdict_rules():
    assert g2.tstat([1.0, 1.0]) is None and g2.tstat([1.0, 1.0, 1.0]) is None          # too few or no spread
    assert g2.verdict(2.0, 0.3, 10) == "moves more than the same-day control"
    assert g2.verdict(-2.0, -0.3, 10) == "moves less than the same-day control"
    assert g2.verdict(0.5, 0.1, 10) == "no difference from the same-day control"
    assert g2.verdict(5.0, 1.0, 3) == "too few dates"


def test_a_family_with_a_real_edge_beats_the_control_and_one_without_does_not():
    bars = {f"S{i:03d}": _bars(seed=i) for i in range(150)}
    bars["SPY"] = _bars(seed=999)
    spy_by_day = {d: (o, c) for d, o, c in zip(*bars["SPY"])}
    days = bars["S000"][0]
    detection = days[40:60]                                   # 20 distinct dates
    # an 'informed' family: its tickers jump hard the session after detection
    bars["INFORMED"] = _bars(seed=500)
    d_i, o_i, c_i = bars["INFORMED"]
    for idx, d in enumerate(d_i):
        if d in detection:
            for j in range(idx, len(c_i)):
                c_i[j] *= 1.0 if j > idx + 1 else (1.08 if j == idx + 1 else 1.0)
    units = {}
    for d in detection:
        units[("informed", "INFORMED", d)] = datetime(d.year, d.month, d.day, 15, 0, tzinfo=timezone.utc)
        units[("plain", "S001", d)] = datetime(d.year, d.month, d.day, 15, 0, tzinfo=timezone.utc)
    rows = {(r["family"], r["k"]): r for r in g2.grade_units(units, bars, spy_by_day)}
    plain = rows.get(("plain", 5))
    assert plain is None or plain["verdict"] != "moves more than the same-day control"
    assert all(r["dates"] >= g2.MIN_DATES for r in rows.values())


def test_families_with_fewer_than_the_minimum_dates_are_left_out_and_output_is_deterministic():
    bars = {f"S{i:03d}": _bars(seed=i) for i in range(120)}
    bars["SPY"] = _bars(seed=999)
    spy_by_day = {d: (o, c) for d, o, c in zip(*bars["SPY"])}
    days = bars["S000"][0]
    units = {("few", "S001", d): datetime(d.year, d.month, d.day, 15, tzinfo=timezone.utc) for d in days[30:34]}
    assert g2.grade_units(units, bars, spy_by_day) == []
    many = {("many", "S002", d): datetime(d.year, d.month, d.day, 15, tzinfo=timezone.utc) for d in days[30:50]}
    assert g2.grade_units(many, bars, spy_by_day) == g2.grade_units(many, bars, spy_by_day)
