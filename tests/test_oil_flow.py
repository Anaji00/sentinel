"""Oil-flow indices: radar readings, gate transits, laden calls and lead-lag."""
from datetime import date, datetime, timedelta, timezone

from shared.utils import oil_flow as of

T0 = datetime(2026, 10, 30, tzinfo=timezone.utc)
GATE = of.GATES[0]


def test_a_calibrated_radar_reading_is_parsed_with_its_no_ais_contacts():
    r = of.parse_sar(
        "Singapore Strait: radar target density 0.00527867 (normal, z=-1.92); 6 radar contact(s) with no AIS",
        "Sentinel-1 SAR over Singapore Strait on 2026-10-31: 56909 target pixels of 10780933 water pixels. "
        "VV sigma0 > 1 linear (0.0 dB). This is a target density, not a vessel count.")
    assert r is not None and r.site == "singapore_strait" and r.day == date(2026, 10, 31)
    assert abs(r.density - 56909 / 10780933) < 1e-12 and r.no_ais == 6 and r.calibration == "sigma0>1"


def test_the_retired_backscatter_threshold_is_not_a_reading():
    # Read 1.0 everywhere for two days in September: a different instrument.
    assert of.parse_sar("Bab-el-Mandeb: radar target density 1.0 (no baseline yet)",
                        "Sentinel-1 SAR over Bab-el-Mandeb on 2026-09-15: 10 target pixels of 10 water pixels. "
                        "VV backscatter > 0.0 dB. This is a target density, not a vessel count.") is None


def test_transits_need_both_sides_of_the_line_inside_the_band_and_close_in_time():
    east = [(T0, 1.25, 103.95), (T0 + timedelta(hours=1), 1.26, 104.05)]
    assert of.transits(east, GATE) == [(T0 + timedelta(hours=1), "east")]
    west = [(T0, 1.25, 104.05), (T0 + timedelta(hours=1), 1.25, 103.95)]
    assert of.transits(west, GATE)[0][1] == "west"
    outside_band = [(T0, 1.60, 103.95), (T0 + timedelta(hours=1), 1.60, 104.05)]
    stale = [(T0, 1.25, 103.95), (T0 + timedelta(hours=13), 1.25, 104.05)]
    assert of.transits(outside_band, GATE) == [] and of.transits(stale, GATE) == []


def test_laden_is_judged_against_the_ships_own_range_and_only_once_it_has_one():
    assert of.draught_state(20.0, 10.0, 21.0) == "laden"
    assert of.draught_state(10.5, 10.0, 21.0) == "ballast"
    assert of.draught_state(15.5, 10.0, 21.0) is None          # mid-range: not called
    assert of.draught_state(11.0, 10.5, 11.5) is None          # never seen both ways
    assert of.draught_state(11.0, None, None) is None


def test_zscore_needs_a_baseline():
    assert of.zscore(5.0, [1.0, 2.0]) is None
    hist = [1.0, 2.0] * 6
    assert abs(of.zscore(1.5, hist)) < 1e-9 and of.zscore(10.0, hist) > 2


def test_lead_lag_finds_a_series_that_leads_by_two_trading_days():
    days = [date(2026, 9, 1) + timedelta(days=i) for i in range(40)]
    x = {d: float((i * 7) % 11) for i, d in enumerate(days)}
    y = {d: x[days[i - 2]] if i >= 2 else 0.0 for i, d in enumerate(days)}
    best = max((c for c in of.lead_lag(x, y) if c["r"] is not None), key=lambda c: abs(c["r"] or 0.0))
    assert best["lag"] == 2 and best["r"] > 0.99 and best["n"] == 38


def test_too_few_aligned_days_give_no_correlation():
    x = {date(2026, 10, i): float(i) for i in range(1, 6)}
    assert all(c["r"] is None for c in of.lead_lag(x, x))


def test_changes_step_over_gaps_and_weekly_sums_land_on_eia_fridays():
    s = {date(2026, 10, 1): 100.0, date(2026, 10, 2): 110.0, date(2026, 10, 5): 99.0}
    assert of.changes(s) == {date(2026, 10, 2): 10.0, date(2026, 10, 5): -11.0}
    fri = date(2026, 10, 9)
    assert of.weekly_sum({date(2026, 10, 3): 1.0, date(2026, 10, 9): 2.0, date(2026, 10, 2): 5.0}, [fri]) == {fri: 3.0}


def test_a_receiver_outage_is_a_coverage_gap_not_an_empty_strait():
    vol = {date(2026, 10, i): 10000.0 for i in range(1, 15)}
    vol[date(2026, 10, 7)] = 0.0
    vol[date(2026, 10, 8)] = 1500.0
    assert of.coverage_gaps(vol) == {date(2026, 10, 7), date(2026, 10, 8)}


def test_the_best_of_eleven_lags_must_clear_chance():
    assert 0.5 < of.chance_r(27) < 0.6           # r = 0.4 on 27 days is what chance gives
    assert of.chance_r(400) < 0.15
