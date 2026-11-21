from shared.utils.breadth import build_series


def day(d, n=1000, adv=600, dec=300, hi=20, lo=5, a50=700, n50=990, a200=650, n200=900, n_prev=None, n_52=900):
    return {"day": d, "n": n, "n_prev": n_prev if n_prev is not None else n, "adv": adv, "dec": dec, "hi52": hi, "lo52": lo, "n_52": n_52,
            "above50": a50, "n_50": n50, "above200": a200, "n_200": n200}


def test_shares_and_unchanged_are_computed_per_session():
    s = build_series([day("2026-11-10"), day("2026-11-11", adv=100, dec=850)], 10)["series"]
    assert [x["day"] for x in s] == ["2026-11-10", "2026-11-11"]
    assert s[0]["share_up"] == 0.6 and s[0]["unchanged"] == 100 and s[0]["share_above_50d"] == 0.7071 and s[0]["share_above_200d"] == 0.7222
    assert s[1]["advancers"] == 100 and s[1]["decliners"] == 850 and s[1]["new_52w_highs"] == 20


def test_a_thinly_covered_session_is_dropped_not_shown_as_a_crash():
    rows = [day("2026-11-05"), day("2026-11-06"), day("2026-11-09", n=300, adv=20, dec=270), day("2026-11-10")]
    out = build_series(rows, 10)
    assert [x["day"] for x in out["series"]] == ["2026-11-05", "2026-11-06", "2026-11-10"] and out["dropped_thin"] == ["2026-11-09"]


def test_missing_denominators_are_null_and_days_are_limited_to_the_newest():
    r = day("2026-11-10", n_52=0, n50=0, n200=0, n_prev=0)
    s = build_series([day("2026-11-07"), day("2026-11-09"), r], 2)["series"]
    assert [x["day"] for x in s] == ["2026-11-09", "2026-11-10"]
    last = s[-1]
    assert last["new_52w_highs"] is None and last["share_above_50d"] is None and last["share_above_200d"] is None and last["share_up"] is None
    assert build_series([], 5) == {"series": [], "dropped_thin": []}
