"""Errors the API meter and the logs exposed on 4 Nov 2026, each pinned."""
import asyncio
import importlib.util
import pathlib
import threading
from datetime import datetime, timezone

import numpy as np

from shared.utils.reddit_feed import parse_reddit_atom
from shared.utils.streaming_detectors import RRCFDetector

ROOT = pathlib.Path(__file__).resolve().parents[1]

ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <author><name>/u/trader</name></author>
    <content type="html">&lt;div&gt;&lt;p&gt;Oil &amp;amp; tankers&lt;/p&gt;&lt;/div&gt; submitted by &lt;a&gt; /u/trader &lt;/a&gt; [link]</content>
    <id>t3_abc123</id>
    <link href="https://www.reddit.com/r/energy/comments/abc123/oil/" />
    <published>2026-11-04T14:00:00+00:00</published>
    <title>Oil &amp;amp; Hormuz</title>
  </entry>
  <entry><id>t3_nodate</id><title>No date</title></entry>
</feed>"""


def test_reddit_atom_yields_the_fields_the_collector_publishes():
    (post,) = parse_reddit_atom(ATOM)                     # the undated entry is skipped
    assert post["id"] == "abc123" and post["title"] == "Oil & Hormuz"
    assert post["selftext"] == "Oil & tankers" and post["author"] == "trader"
    assert post["permalink"] == "/r/energy/comments/abc123/oil/"
    assert post["created_utc"] == datetime(2026, 11, 4, 14, tzinfo=timezone.utc).timestamp()


def test_scoring_one_detector_from_two_threads_does_not_raise():
    """Two executor threads on one domain raised "deque mutated during iteration"."""
    det = RRCFDetector(num_trees=4, window_size=32, shingle_size=1)
    errors = []

    def work(seed):
        rng = np.random.default_rng(seed)
        try:
            det.insert_batch([rng.normal(size=3) for _ in range(150)])
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=work, args=(s,)) for s in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors


def _backfill():
    path = ROOT / "services" / "collector-tradfi" / "historical_backfill.py"
    spec = importlib.util.spec_from_file_location("historical_backfill_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_one_timed_out_chunk_loses_that_chunk_and_not_the_batch():
    bf = _backfill()

    class Db:
        calls = 0

        async def execute_many(self, sql, rows):
            self.calls += 1
            if self.calls == 2:
                raise asyncio.TimeoutError()

    db = Db()
    rows = [("A", i) for i in range(bf.WRITE_CHUNK_ROWS * 2 + 5)]
    assert asyncio.run(bf._write_rows(db, rows)) == bf.WRITE_CHUNK_ROWS + 5
    assert db.calls == 3
    assert asyncio.run(bf._write_rows(db, [])) == 0


def test_the_baseline_is_exposure_times_move_and_abstains_when_it_cannot_say():
    from shared.utils.macro_exposure import exposure_direction as d

    assert d(0.4, 0.02) == "up" and d(-0.4, 0.02) == "down" and d(-0.4, -0.02) == "up"
    assert d(0.4, 0.001) is None and d(None, 0.02) is None and d(0.4, None) is None and d(0.0, 0.02) is None


class _Counter:
    def __init__(self, fail=False):
        self.n, self.fail = {}, fail

    async def incr(self, k):
        if self.fail:
            raise ConnectionError("down")
        self.n[k] = self.n.get(k, 0) + 1
        return self.n[k]

    async def expire(self, k, s):
        return True


def test_the_shadow_sample_admits_per_hour_and_nothing_it_cannot_count():
    from services.agents.stock_correlation_agent import sample_admitted

    raw = _Counter()
    t0, t1 = datetime(2026, 11, 4, 14, 5, tzinfo=timezone.utc), datetime(2026, 11, 4, 15, 5, tzinfo=timezone.utc)
    assert asyncio.run(sample_admitted(raw, 1, t0)) is True
    assert asyncio.run(sample_admitted(raw, 1, t0)) is False
    assert asyncio.run(sample_admitted(raw, 1, t1)) is True
    assert asyncio.run(sample_admitted(_Counter(fail=True), 1, t0)) is False


class _Raw:
    def __init__(self, fail=False):
        self.keys, self.fail, self.ex = {}, fail, None

    async def set(self, k, v, ex=None, nx=False):
        if self.fail:
            raise ConnectionError("down")
        self.ex = ex
        if nx and k in self.keys:
            return None
        self.keys[k] = v
        return True

    async def incr(self, k):
        self.keys[k] += 1
        return self.keys[k]


class _Redis:
    def __init__(self, fail=False):
        self.raw = _Raw(fail)


def test_ship_rules_alert_once_per_region_per_window():
    from services.correlation.main import _capped_for_episode, _episode_is_open
    from shared.models.events import AlertTier

    rule, r = "rule_maritime_chokepoint_evasion", _Redis()
    assert asyncio.run(_episode_is_open(r, rule, "Strait of Hormuz")) is False
    assert r.raw.ex == 6 * 3600
    assert asyncio.run(_episode_is_open(r, rule, "Strait of Hormuz")) is True
    assert asyncio.run(_episode_is_open(r, rule, "Red Sea")) is False           # other waters are a new episode
    assert asyncio.run(_episode_is_open(r, "rule_other", "Red Sea")) is False   # other rules are untouched
    assert asyncio.run(_episode_is_open(_Redis(fail=True), rule, "Red Sea")) is False   # unchecked: let it through
    assert _capped_for_episode(rule, AlertTier.CRITICAL) is AlertTier.ELEVATED
    assert _capped_for_episode(rule, AlertTier.ALERT) is AlertTier.ALERT
    assert _capped_for_episode("rule_other", AlertTier.CRITICAL) is AlertTier.CRITICAL


def test_the_baseline_reads_a_field_the_agents_exposure_rows_actually_have():
    """It read "beta", which these rows do not carry, and recorded nothing until 4 Nov."""
    from services.agents.stock_correlation_agent import macro_exposures
    from shared.utils.macro_exposure import exposure_direction

    (row,) = macro_exposures([{"macro": "CL=F", "rel": "POSITIVE_EXPOSURE_TO", "weight": -0.3}])
    assert exposure_direction(row.get("weight"), 0.02) == "down"
