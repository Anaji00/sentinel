"""Source health against each source's own rhythm, and the book's exposure lens."""
from datetime import date, datetime, timedelta, timezone

from services.agents.exposure_lens_job import market_beta
from shared.utils import exposure_lens as el
from shared.utils import source_health as sh

NOW = datetime(2026, 10, 31, 7, 0, tzinfo=timezone.utc)


def _row(source, etype, hours_ago, n_base, n_24h, max_gap_h=None, min_day=None, n_days=13):
    return {"source": source, "type": etype, "last_seen": NOW - timedelta(hours=hours_ago), "n_base": n_base,
            "n_24h": n_24h, "max_gap_h": max_gap_h, "min_day": min_day, "n_days": n_days}


def test_a_market_feed_quiet_over_a_weekend_is_not_silent():
    # Ten trading days with events, the weekends empty.
    weekend = _row("alpaca_options", "options_flow", 40, 6000, 0, max_gap_h=64, min_day=300, n_days=9)
    (r,) = sh.assess([weekend], NOW)
    assert r["status"] == "ok"                                  # 40 h is inside 1.25 x its 64 h weekend gap


def test_silence_thin_and_retired():
    # wsj_markets, not cnbc_finance: that one is expected to be quiet (its URLs are credited to cnbc_world).
    rows = [_row("wsj_markets", "headline", 250, 300, 0, max_gap_h=20, min_day=10),
            _row("polymarket", "prediction_market_trade", 1, 4000, 18, max_gap_h=3, min_day=151),
            _row("OpenSky", "flight_position", 600, 0, 0)]
    by = {r["source"]: r for r in sh.assess(rows, NOW)}
    assert by["wsj_markets"]["status"] == "silent"
    assert by["polymarket"]["status"] == "thin"
    assert by["OpenSky"]["status"] == "retired" and "squawk" in by["OpenSky"]["why"]


def test_exposure_lens_sums_weight_times_beta_and_flags_over_the_limit():
    # A leg of 1 keeps this arithmetic readable; the job passes the account's real leg.
    w = el.book_weights(["AAA", "BBB"], ["CCC"], leg=1.0)
    assert w == {"AAA": 0.5, "BBB": 0.5, "CCC": -1.0}
    assert el.book_weights(["AAA", "BBB"], ["CCC"], leg=0.5) == {"AAA": 0.25, "BBB": 0.25, "CCC": -0.5}
    betas = [{"ticker": "AAA", "macro": "CL=F", "beta": 0.4}, {"ticker": "CCC", "macro": "CL=F", "beta": -0.1},
             {"ticker": "BBB", "macro": "US10Y", "beta": 0.0002}]
    net = el.net_exposures(w, betas)
    assert abs(net["CL=F"] - 0.3) < 1e-9 and abs(net["US10Y"] - 0.0001) < 1e-12
    rows = {r["instrument"]: r for r in el.lens(net, 0.68, {"CL=F": 2, "US10Y": 1})}
    assert rows["CL=F"]["flag"] and abs(rows["CL=F"]["book_move"] - 0.03) < 1e-9       # +10% oil -> +3%
    assert not rows["US10Y"]["flag"]
    assert rows["SPY (market)"]["flag"] and rows["SPY (market)"]["book_move"] == -0.034  # beta 0.68, SPY -5%


def test_market_beta_of_a_book_long_a_levered_copy_of_spy():
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(150)]
    spy, lev = [], []
    p, q = 100.0, 100.0
    for i, d in enumerate(days):
        r = 0.01 if i % 2 else -0.008
        p *= 1 + r
        q *= 1 + 2 * r
        spy.append((d, p))
        lev.append((d, q))
    assert abs(market_beta({"SPY": spy, "LEV": lev}, {"LEV": 0.5}) - 1.0) < 0.05


def test_a_platform_outage_is_found_and_a_quiet_hour_is_not():
    first = NOW - timedelta(hours=48)
    hourly = {first + timedelta(hours=i): 200.0 for i in range(49)}
    for i in range(10, 20):                    # ten hours down, a trickle from a feed that bypasses the pipeline
        hourly[first + timedelta(hours=i)] = 3.0
    hourly.pop(first + timedelta(hours=15))
    hourly[first + timedelta(hours=30)] = 1.0  # one bad hour is not an outage
    assert sh.outage_windows(hourly, first, first + timedelta(hours=48)) == [
        (first + timedelta(hours=10), first + timedelta(hours=20))]


def test_an_outage_running_to_the_last_hour_is_closed():
    first = NOW - timedelta(hours=24)
    hourly = {first + timedelta(hours=i): 100.0 for i in range(20)}
    assert sh.outage_windows(hourly, first, first + timedelta(hours=24)) == [
        (first + timedelta(hours=20), first + timedelta(hours=25))]
