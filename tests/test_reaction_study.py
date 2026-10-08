"""The reaction study: no look-ahead, honest counting, and noise never proven.

The property that matters most is the last pair of tests: thirty families of
pure noise on random-walk prices produce no proven signal, and a family with a
real, injected effect is found.
"""

import math
import pathlib
import random
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import reaction_study as rs  # noqa: E402
from shared.utils.signal_families import Episode, episodes, observations  # noqa: E402

T0 = datetime(2026, 7, 1, tzinfo=timezone.utc)
STEP = 300  # five-minute bars


def walk(days: int, seed: int, vol: float = 0.0012, jumps=None, drift: float = 0.0):
    """A random-walk close series; `jumps` adds permanent log steps at given times."""
    rng = random.Random(seed)
    times, closes = [], []
    logp = math.log(100.0)
    start = T0.timestamp()
    jumps = sorted(jumps or [])
    j = 0
    for i in range(days * 24 * 12):
        t = start + i * STEP
        logp += rng.gauss(drift, vol)
        while j < len(jumps) and jumps[j][0] <= t:
            logp += jumps[j][1]
            j += 1
        times.append(t)
        closes.append(math.exp(logp))
    return times, closes


def _ep(family, inst, when, direction=1, key=None):
    return Episode(family, inst, direction, when, key or f"{family}|x|{inst}", ["e"])


def test_entry_is_the_first_bar_after_the_platform_knew():
    s = walk(3, 1)
    known = T0 + timedelta(minutes=7)  # between bars
    entry, exit_, _ = rs.forward_window(s, known, timedelta(hours=1))
    assert entry == (T0 + timedelta(minutes=10)).timestamp()
    assert exit_ >= entry + 3600


def test_no_bars_means_unmeasurable_not_zero():
    s = walk(3, 1)
    assert rs.forward_window(s, T0 + timedelta(days=30), timedelta(hours=1)) is None


def test_equities_are_measured_net_of_the_benchmark():
    stock = walk(30, 2)
    bench = stock  # identical series: the abnormal move must be zero
    stats = rs.horizon_stats(stock, timedelta(hours=1))
    m = rs.measure(_ep("sector_news_positive", "AAA", T0 + timedelta(days=5)), timedelta(hours=1), stock, bench, stats)
    assert m is not None and abs(m.abnormal) < 1e-12


def test_episodes_of_one_story_count_once():
    rows = [
        {"event_id": str(i), "type": "headline", "created_at": T0 + timedelta(minutes=10 * i),
         "headline": "Iran attacks tanker in Strait of Hormuz", "tags": [], "sentiment": 0.0}
        for i in range(30)
    ]
    obs = [o for r in rows for o in observations(r)]
    eps = [e for e in episodes(obs) if e.instrument == "CL=F"]
    assert len(eps) == 1 and len(eps[0].event_ids) == 30


def test_overlapping_episodes_merge_into_one_unit():
    s = walk(40, 3)
    stats = rs.horizon_stats(s, timedelta(days=1))
    eps = [_ep("f", "CL=F", T0 + timedelta(days=10, hours=h)) for h in (0, 2, 4)]
    ms = [rs.measure(e, timedelta(days=1), s, None, stats) for e in eps]
    assert len(rs.units([m for m in ms if m], timedelta(days=1))) == 1


def test_benjamini_hochberg_matches_a_known_case():
    q = rs.benjamini_hochberg([0.01, 0.04, 0.03, 0.20])
    assert [round(x, 4) for x in q] == [0.04, 0.0533, 0.0533, 0.2]


def test_a_prior_contradicted_by_the_data_is_never_proven():
    c = rs.CellResult(family="insider_cluster", horizon="1d", episodes=40, units=40, instruments=["AAA"],
                      p=0.0001, sign=-1, verdict="pending_fdr", validation_mean=-0.5, validation_t=-3,
                      mean_bps=-80, cost_bps=10, hit_rate=0.7)
    assert rs.finalize([c])[0].verdict == "contradicts_prior"


def _study(family_eps, series, horizon="1d"):
    h = rs.HORIZONS[horizon]
    marks = {}
    for eps in family_eps.values():
        for e in eps:
            marks.setdefault(e.instrument, []).append(e.known_at.timestamp())
    stats = {k: rs.horizon_stats(v, h, marks.get(k, ())) for k, v in series.items()}
    cells = []
    for fam, eps in family_eps.items():
        ms = [rs.measure(e, h, series.get(e.instrument), None, stats.get(e.instrument)) for e in eps]
        cells.append(rs.evaluate_cell(fam, horizon, [m for m in ms if m], len(eps)))
    return rs.finalize(cells)


def test_pure_noise_is_never_proven():
    """Thirty families of random episodes on random-walk futures: nothing proven."""
    rng = random.Random(42)
    series = {f"N{i}=F": walk(90, 100 + i) for i in range(6)}
    fams = {}
    for f in range(30):
        inst = f"N{f % 6}=F"
        fams[f"noise_{f}"] = [
            _ep(f"noise_{f}", inst, T0 + timedelta(days=rng.uniform(1, 80)), 1, f"noise_{f}|{n}|{inst}")
            for n in range(60)
        ]
    cells = _study(fams, series)
    assert [c.family for c in cells if c.verdict == "proven"] == []


def test_a_real_effect_is_found():
    """A family whose episodes are followed by a 1.5% move is proven at one day."""
    rng = random.Random(7)
    times = sorted(T0 + timedelta(days=rng.uniform(1, 80)) for _ in range(60))
    jumps = [((t + timedelta(minutes=30)).timestamp(), 0.015) for t in times]
    series = {"EFF=F": walk(90, 9, jumps=jumps)}
    eps = [_ep("real", "EFF=F", t, 1, f"real|{i}|EFF=F") for i, t in enumerate(times)]
    cell = _study({"real": eps}, series)[0]
    assert cell.verdict == "proven", (cell.verdict, cell.reason)
    assert cell.sign == 1 and cell.mean_bps > 100


def test_combining_is_capped():
    assert rs.combine([0.7, 0.7, 0.7, 0.7]) == 0.8
    assert abs(rs.combine([0.6]) - 0.6) < 1e-9
    assert rs.shrunk_hit_rate(0.9, 2) < 0.6


def test_a_trend_is_not_mistaken_for_an_edge():
    """Random 'up' episodes on a strongly rising market prove nothing."""
    rng = random.Random(5)
    series = {f"T{i}=F": walk(90, 300 + i, drift=0.00006) for i in range(4)}  # ~+1.7%/day
    fams = {
        f"trend_{f}": [
            _ep(f"trend_{f}", f"T{f % 4}=F", T0 + timedelta(days=rng.uniform(1, 80)), 1, f"trend_{f}|{n}")
            for n in range(60)
        ]
        for f in range(12)
    }
    assert [c.family for c in _study(fams, series) if c.verdict == "proven"] == []


def test_five_day_volatility_scales_from_daily_when_history_is_short():
    s = walk(75, 21)
    assert rs.horizon_stats(s, timedelta(days=5)) is None  # 15 windows, too few
    daily = rs.horizon_stats(s, timedelta(days=1))
    scaled = rs.scaled_horizon_stats(s, timedelta(days=5))
    assert scaled is not None and abs(scaled[1] - daily[1] * math.sqrt(5)) < 1e-12


def test_an_instrument_signalled_every_day_still_has_a_baseline():
    s = walk(60, 22)
    marks = [(T0 + timedelta(days=d, hours=3)).timestamp() for d in range(60)]
    assert rs.horizon_stats(s, timedelta(days=1), marks) is not None
