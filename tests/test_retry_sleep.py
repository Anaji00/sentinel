from shared.utils.retry_sleep import next_delay


def test_success_waits_the_normal_interval_and_failures_retry_soon_a_bounded_number_of_times():
    normal = 12 * 3600
    assert next_delay(0, normal) == normal
    assert [next_delay(n, normal) for n in (1, 2, 3, 4)] == [900.0] * 4
    assert next_delay(5, normal) == normal                      # retries used up: back to the normal cadence
    assert next_delay(1, 600) == 600.0                          # never longer than the normal interval


def test_the_long_interval_collectors_retry_a_failed_pass_soon_instead_of_waiting_the_full_interval():
    """short_interest, company_facts, fundamentals and vendor_shares slept 12 h, 6 h, 24 h and 7 days after a failure (13 Nov 2026)."""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-filings"
    for name in ("short_interest", "company_facts", "fundamentals", "vendor_shares"):
        src = (root / f"{name}.py").read_text(encoding="utf-8")
        assert "next_delay(failures," in src and "failures += 1" in src and "failures = 0" in src, name


def test_a_short_volume_event_stores_its_z_score_without_changing_the_score():
    """So the cap-saturated score (21% of events at 0.95) can be replayed against stored events before it is changed (13 Nov 2026)."""
    import importlib.util
    import pathlib
    path = pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-filings" / "catalysts.py"
    spec = importlib.util.spec_from_file_location("catalysts_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    history = [0.40, 0.42, 0.38, 0.41, 0.39]
    ev = mod.short_spike_event("ABC", "20261112", 0.80, history, 1_000_000.0)
    assert ev.anomaly_score == 0.95                                          # unchanged: min(0.95, 0.55 + 1.5 * jump)
    z = [t for t in ev.tags if t.startswith("short_vol_z:")]
    assert len(z) == 1 and float(z[0].split(":")[1]) > 5 and any(t.startswith("short_vol_jump:") for t in ev.tags)
    flat = mod.short_spike_event("ABC", "20261112", 0.50, [0.4, 0.4, 0.4], 1000.0)
    # a flat history is floored at the gate's minimum sd (0.03), not left at zero: z = (0.50 - 0.40) / 0.03
    assert "short_vol_z:3.33" in flat.tags


def test_the_remaining_long_loops_retry_a_failed_pass_soon():
    """EIA (6 h), FRED first release (24 h), the reaction study (6 h) and the SAR feeds (6 h) waited the full interval after a failure; a database restart cost
    the reaction study a night on 13 Nov. Reviewed 13 Nov 2026."""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[1] / "services"
    for rel, needles in (("collector-macro/main.py", ("next_delay(failures, EIA_INTERVAL_SEC)", "next_delay(failures, FIRST_RELEASE_INTERVAL_SEC)")),
                         ("agents/signal_engine.py", ("next_delay(failures, STUDY_INTERVAL_SEC)",)),
                         ("collector-sar/feeds.py", ("next_delay(failures, FEED_INTERVAL_SEC)",))):
        src = (root / rel).read_text(encoding="utf-8")
        for n in needles:
            assert n in src, (rel, n)


def test_the_yahoo_fast_quote_has_its_own_guard_so_the_history_fallback_still_runs():
    """yfinance raised KeyError 'currentTradingPeriod' inside fast_info (11 swallowed failures, 13 Nov 2026) and that skipped the history fallback."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "services" / "collector-macro" / "main.py").read_text(encoding="utf-8")
    i = src.index("def _sync_yf")
    body = src[i:i + 2200]
    assert "collector_macro._sync_yf.fast_info" in body and body.index("fast_info") < body.index('t.history(period="2d")')
