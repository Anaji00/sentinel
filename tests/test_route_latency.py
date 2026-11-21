from shared.utils.route_latency import LatencyBook, MAX_ROUTES, route_template


def test_percentiles_slow_and_error_counts_per_route():
    b = LatencyBook()
    for ms in range(1, 101):
        b.record("/api/v1/a/{x}", float(ms), 200)
    b.record("/api/v1/a/{x}", 2500.0, 500)
    row = b.table()[0]
    assert row["route"] == "/api/v1/a/{x}" and row["requests"] == 101 and row["slow"] == 1 and row["errors"] == 1
    assert row["p50_ms"] == 51.0 and 90 <= row["p95_ms"] <= 100 and row["max_ms"] == 2500.0


def test_slowest_routes_come_first_and_the_table_is_bounded():
    b = LatencyBook()
    b.record("fast", 5.0, 200)
    b.record("slow", 900.0, 200)
    assert [r["route"] for r in b.table()] == ["slow", "fast"]
    for i in range(MAX_ROUTES + 50):
        b.record(f"/r/{i}", 1.0, 200)
    assert len(b._samples) <= MAX_ROUTES + 1                 # the overflow shares one "(other)" row


def test_the_key_is_the_template_never_the_raw_path():
    class R:
        path = "/api/v1/stocks/{ticker}/snapshot"
    assert route_template(R(), "/api/v1/stocks/NVDA/snapshot") == "/api/v1/stocks/{ticker}/snapshot"
    assert route_template(None, "/api/v1/stocks/NVDA/snapshot") == "(unmatched)"
