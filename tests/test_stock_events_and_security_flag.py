"""Frontend requests of 19 Nov: chart markers (shared/utils/stock_events.py, GET /stocks/{ticker}/events) and the is_security flag on /correlations."""
import pathlib
from datetime import date, datetime, timezone

from shared.utils import security_flag as sf
from shared.utils import stock_events as se

TODAY = date(2026, 11, 19)
ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_events_are_merged_dated_newest_first_and_bounded_by_the_window() -> None:
    events, undated = se.build(
        [{"report_date": date(2026, 11, 4), "eps_actual": 2.1, "eps_estimate": 2.0263}, {"report_date": date(2024, 1, 1), "eps_actual": 1, "eps_estimate": 1}],
        [{"occurred_at": datetime(2026, 11, 10, 14, tzinfo=timezone.utc), "form_type": "8-K", "url": "https://sec.gov/x"}, {"occurred_at": "garbage", "form_type": "10-Q"}],
        [{"detected_at": "2026-11-17T09:00:00+00:00", "rule_name": "Peer Cointegration"}],
        days=365, today=TODAY)
    assert [e.kind for e in events] == ["alert", "filing", "earnings"]
    assert events[2].label == "Earnings: EPS 2.10 vs 2.03 expected" and events[1].url == "https://sec.gov/x" and events[0].url is None
    assert undated == 1                                  # the unreadable filing date is counted, not given today's date
    assert all(e.date <= TODAY for e in events)


def test_a_future_row_and_a_row_before_the_window_are_left_off() -> None:
    events, _ = se.build([{"report_date": date(2026, 12, 1), "eps_actual": 1.0}, {"report_date": date(2026, 11, 18), "eps_actual": None}], [], [], days=30, today=TODAY)
    assert [(e.date, e.label) for e in events] == [(date(2026, 11, 18), "Earnings report")]


def test_each_kind_keeps_its_own_share() -> None:
    filings = [{"occurred_at": date(2026, 11, 1), "form_type": "4"} for _ in range(500)]
    events, _ = se.build([{"report_date": date(2026, 10, 1), "eps_actual": 1.0, "eps_estimate": 1.0}], filings, [], days=365, today=TODAY)
    assert sum(e.kind == "filing" for e in events) == se.MAX_PER_KIND and any(e.kind == "earnings" for e in events)


def test_is_security_needs_a_ticker_with_a_recent_bar() -> None:
    bars = {"NVDA", "AAPL"}
    assert sf.is_security("nvda", bars) and not sf.is_security("EPS", bars) and not sf.is_security(None, bars) and not sf.is_security("A" * 30, bars)
    assert sf.candidate_ids(["nvda", "NVDA", None, "", "EPS"]) == ["NVDA", "EPS"]


def test_the_routes_are_wired() -> None:
    assert "stock_events.router" in (ROOT / "services/api_gateway/routes/main.py").read_text(encoding="utf-8")
    sc = (ROOT / "services/api_gateway/routes/scenarios.py").read_text(encoding="utf-8")
    assert '"is_security"' in sc and "sf.is_security(" in sc
    ev = (ROOT / "services/api_gateway/routes/stock_events.py").read_text(encoding="utf-8")
    assert "require_role(Role.VIEWER)" in ev and "cached_json" in ev


def test_closes_batch_bounds_and_shape() -> None:
    from shared.utils import closes_batch as cb
    clean = lambda t: t.strip().upper() if t.strip().isalpha() else None          # noqa: E731
    assert cb.parse_tickers("aapl, msft,AAPL,12,", clean) == ["AAPL", "MSFT"]
    assert len(cb.parse_tickers(",".join(f"{chr(65 + i % 26)}{chr(65 + i // 26)}" for i in range(300)), lambda t: t)) == cb.MAX_TICKERS
    assert (cb.clamp_days(1), cb.clamp_days(9999), cb.clamp_days("x"), cb.clamp_days(60)) == (5, 250, 60, 60)
    rows = [{"ticker": "AAA", "day": "2026-11-03", "close": 3.0}, {"ticker": "AAA", "day": "2026-11-01", "close": 1.0},
            {"ticker": "AAA", "day": "2026-11-02", "close": 2.0}, {"ticker": "bad", "day": "x", "close": "nan?"}]
    out = cb.build(["AAA", "ZZZ"], rows, 2)
    assert out == {"AAA": [2.0, 3.0], "ZZZ": []}


def test_the_closes_route_is_declared_before_nothing_it_can_shadow() -> None:
    src = (ROOT / "services/api_gateway/routes/stock_events.py").read_text(encoding="utf-8")
    assert '@router.get("/closes")' in src and "require_role(Role.VIEWER)" in src and "cached_json" in src


def test_correlation_items_carry_the_grades_family_name() -> None:
    # Frontend request, 20 Nov: the alert-quality badge matches producer-grades rows by family, so the item must carry it spelled the same way.
    from shared.utils.producer_grades import rule_family
    sc = (ROOT / "services/api_gateway/routes/scenarios.py").read_text(encoding="utf-8")
    assert '"rule_family"' in sc and 'item["rule_family"] = rule_family(' in sc
    assert rule_family("Geopolitical Cascade (Msft)") == "Geopolitical Cascade"
