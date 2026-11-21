from shared.utils.redis_health import plan_baseline, summarize


def test_share_and_new_evictions_raise_alarms():
    s = summarize({"used_memory": 340e6, "maxmemory": 400e6, "maxmemory_policy": "volatile-lru"}, {"evicted_keys": 70}, 64)
    assert s["used_share"] == 0.85 and s["evicted_since_baseline"] == 6 and len(s["alarms"]) == 2


def test_quiet_redis_has_no_alarm_and_first_look_has_no_delta():
    s = summarize({"used_memory": 200e6, "maxmemory": 640e6}, {"evicted_keys": 64}, 64)
    assert s["alarms"] == [] and s["evicted_since_baseline"] == 0 and s["max_mb"] == 640.0
    first = summarize({"used_memory": 200e6, "maxmemory": 640e6}, {"evicted_keys": 64}, None)
    assert first["evicted_since_baseline"] is None and first["alarms"] == []


def test_no_limit_means_unknown_share_not_zero():
    s = summarize({"used_memory": 5e6, "maxmemory": 0}, {}, None)
    assert s["used_share"] is None and s["max_mb"] is None


def test_the_baseline_window_clears_the_alarm_after_a_day_and_survives_garbage():
    now = 1_800_000_000
    assert plan_baseline(None, "r1", now, 64) == (64, f"r1|{now}|64")                     # first look: the count now is the baseline
    assert plan_baseline(f"r1|{now - 3600}|64", "r1", now, 70) == (64, None)             # same run, window open: the stored count, nothing rewritten
    assert plan_baseline(f"r1|{now - 90000}|64", "r1", now, 70) == (70, f"r1|{now}|70")  # the window is over: a fresh one, the alarm clears
    assert plan_baseline(f"r1|{now - 3600}|64", "r2", now, 5) == (0, f"r2|{now}|0")      # a new Redis run: its counter started at zero
    for garbage in ("", "nonsense", "r1|x|y", "r1|1|", bytes([255, 254]), "a|b|c|d"):
        base, new = plan_baseline(garbage, "r1", now, 9)
        assert base == 9 and new == f"r1|{now}|9"
    assert plan_baseline(b"r1|1799990000|7", "r1", now, 9) == (7, None)                    # bytes from the client are read like text
