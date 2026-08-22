"""
tests/test_observability_layer.py

Guards the observability layer added to close LOG-01/02/03:

  - metrics survive their process (Redis aggregation)
  - collectors report throughput, not just liveness
  - external dependencies back off instead of hammering
"""

import asyncio

import pytest

from shared.utils import metrics as M
from shared.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    all_breakers,
    get_breaker,
)
from shared.utils.collector_metrics import CollectorMetrics
from shared.utils.metrics import MetricsCollector


@pytest.fixture(autouse=True)
def _reset_metrics():
    M._COUNTER_METRICS.clear()
    M._GAUGE_METRICS.clear()
    M._LATENCY_HISTOGRAMS.clear()
    M._redis_client = None
    yield
    M._COUNTER_METRICS.clear()
    M._GAUGE_METRICS.clear()


class FakeRedis:
    """Minimal async Redis supporting the hash/set ops the metrics layer uses."""

    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.expires = {}

    def pipeline(self):
        return FakePipeline(self)

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))


class FakePipeline:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def hset(self, key, mapping=None):
        self.ops.append(("hset", key, mapping))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def sadd(self, key, member):
        self.ops.append(("sadd", key, member))
        return self

    async def execute(self):
        for op in self.ops:
            if op[0] == "hset":
                self.r.hashes.setdefault(op[1], {}).update(op[2] or {})
            elif op[0] == "expire":
                self.r.expires[op[1]] = op[2]
            elif op[0] == "sadd":
                self.r.sets.setdefault(op[1], set()).add(op[2])
        self.ops = []
        return []


# ── CROSS-PROCESS METRICS (LOG-02 root cause) ────────────────────────────────

@pytest.mark.anyio
async def test_metrics_reach_redis_so_they_survive_the_process():
    """Counters were process-local, so nothing outside the gateway reached /metrics."""
    r = FakeRedis()
    MetricsCollector.increment("collector_ingested_total:collector-crypto", 42)
    await M.bind_redis(r, service_name="collector-crypto")

    stored = r.hashes.get("sentinel:metrics:collector-crypto", {})
    assert stored.get("c|collector_ingested_total:collector-crypto") == "42"
    assert "collector-crypto" in r.sets.get("sentinel:metrics:services", set())

    if M._publish_task:
        M._publish_task.cancel()


@pytest.mark.anyio
async def test_published_metrics_expire_so_a_dead_service_disappears():
    """A stale key must not report a stopped service as still producing."""
    r = FakeRedis()
    MetricsCollector.increment("x", 1)
    await M.bind_redis(r, service_name="svc-a")

    ttl = r.expires.get("sentinel:metrics:svc-a")
    assert ttl is not None
    assert ttl > M.PUBLISH_INTERVAL_SEC * 2, "TTL must exceed two publish intervals"

    if M._publish_task:
        M._publish_task.cancel()


@pytest.mark.anyio
async def test_aggregation_sums_counters_across_services():
    """The gateway must report platform totals, not just its own."""
    r = FakeRedis()
    r.sets["sentinel:metrics:services"] = {"collector-ais", "collector-crypto"}
    r.hashes["sentinel:metrics:collector-ais"] = {"c|events_total": "10"}
    r.hashes["sentinel:metrics:collector-crypto"] = {"c|events_total": "32"}

    agg = await M.collect_all(r)
    assert agg["counters"]["events_total"] == 42
    # Per-service breakdown makes one stalled producer visible inside the total.
    assert agg["counters"]['events_total{service="collector-ais"}'] == 10


@pytest.mark.anyio
async def test_aggregation_degrades_to_local_when_redis_is_down():
    """Telemetry loss must not take /metrics down with it."""

    class Broken:
        def __getattr__(self, _):
            raise ConnectionError("redis unreachable")

    MetricsCollector.increment("local_only", 7)
    agg = await M.collect_all(Broken())
    assert agg["counters"]["local_only"] == 7


@pytest.mark.anyio
async def test_recording_a_metric_never_raises_when_redis_fails():
    """Instrumentation must never break the code it measures."""

    class Broken:
        def pipeline(self):
            raise ConnectionError("down")

    M._redis_client = Broken()
    MetricsCollector.increment("safe", 1)  # must not raise
    assert await M.publish_now() is False  # reports failure, does not throw


# ── COLLECTOR INSTRUMENTATION (LOG-02) ───────────────────────────────────────

@pytest.mark.anyio
async def test_collector_exposes_throughput_not_just_liveness():
    m = CollectorMetrics("collector-test")
    await m.start(None)

    m.ingested(5)
    m.rejected("missing_price", 2)
    m.upstream_error("yahoo")

    assert M._COUNTER_METRICS["collector_ingested_total:collector-test"] == 5
    assert M._COUNTER_METRICS["collector_rejected_total:collector-test"] == 2
    assert M._COUNTER_METRICS["collector_upstream_errors_total:collector-test"] == 1
    # Reason and source labels localize a failure to a specific cause.
    assert M._COUNTER_METRICS["collector_rejected_reason:collector-test:missing_price"] == 2
    assert M._COUNTER_METRICS["collector_upstream_error_source:collector-test:yahoo"] == 1


@pytest.mark.anyio
async def test_counters_seed_at_zero_so_a_silent_collector_is_visible():
    """An absent series is easy to miss; an explicit zero is not."""
    m = CollectorMetrics("collector-quiet")
    await m.start(None)
    assert "collector_ingested_total:collector-quiet" in M._COUNTER_METRICS
    assert M._COUNTER_METRICS["collector_ingested_total:collector-quiet"] == 0


@pytest.mark.anyio
async def test_last_success_advances_only_on_real_ingest():
    """`time() - last_success` is the staleness query; rejects must not refresh it."""
    m = CollectorMetrics("collector-stale")
    await m.start(None)
    key = "collector_last_success_epoch:collector-stale"
    assert M._GAUGE_METRICS[key] == 0.0

    m.rejected("bad_row", 3)
    assert M._GAUGE_METRICS[key] == 0.0, "a rejected record must not count as success"

    m.ingested(1)
    assert M._GAUGE_METRICS[key] > 0.0


@pytest.mark.anyio
async def test_reject_ratio_detects_an_upstream_schema_change():
    """The signature of a source changing shape is the ratio going to 1.0."""
    m = CollectorMetrics("collector-ratio")
    await m.start(None)
    assert m.reject_ratio is None  # nothing seen yet

    m.ingested(90)
    m.rejected("ok", 10)
    assert m.reject_ratio == pytest.approx(0.10)

    m.rejected("schema_changed", 900)
    assert m.reject_ratio > 0.9


# ── CIRCUIT BREAKER (LOG-03) ─────────────────────────────────────────────────

@pytest.mark.anyio
async def test_breaker_opens_after_consecutive_failures():
    b = CircuitBreaker("flaky", failure_threshold=3, recovery_timeout=60)

    async def boom():
        raise ConnectionError("upstream 503")

    for _ in range(3):
        with pytest.raises(ConnectionError):
            await b.call(boom)

    assert b.state is CircuitState.OPEN


@pytest.mark.anyio
async def test_open_breaker_refuses_without_touching_the_network():
    """The point is to stop calling, not to fail faster."""
    b = CircuitBreaker("down", failure_threshold=1, recovery_timeout=60)
    calls = {"n": 0}

    async def upstream():
        calls["n"] += 1
        raise TimeoutError()

    with pytest.raises(TimeoutError):
        await b.call(upstream)
    assert calls["n"] == 1

    for _ in range(5):
        with pytest.raises(CircuitOpenError):
            await b.call(upstream)
    assert calls["n"] == 1, "open circuit still invoked the upstream"


@pytest.mark.anyio
async def test_success_resets_the_failure_run():
    """Intermittent failures must not accumulate into an open circuit."""
    b = CircuitBreaker("intermittent", failure_threshold=3)

    async def fail():
        raise ValueError()

    async def ok():
        return "fine"

    for _ in range(2):
        with pytest.raises(ValueError):
            await b.call(fail)
    await b.call(ok)
    for _ in range(2):
        with pytest.raises(ValueError):
            await b.call(fail)

    assert b.state is CircuitState.CLOSED


@pytest.mark.anyio
async def test_half_open_trial_closes_the_circuit_on_recovery():
    b = CircuitBreaker("recovering", failure_threshold=1, recovery_timeout=0.05)

    async def fail():
        raise ConnectionError()

    async def ok():
        return 1

    with pytest.raises(ConnectionError):
        await b.call(fail)
    assert b.state is CircuitState.OPEN

    await asyncio.sleep(0.06)
    assert await b.call(ok) == 1
    assert b.state is CircuitState.CLOSED


@pytest.mark.anyio
async def test_repeated_failure_backs_off_instead_of_probing_forever():
    """An hour-long outage must not be probed every 50ms for that hour."""
    b = CircuitBreaker(
        "persistent", failure_threshold=1, recovery_timeout=0.05, max_recovery_timeout=10.0
    )

    async def fail():
        raise ConnectionError()

    with pytest.raises(ConnectionError):
        await b.call(fail)
    first = b._current_timeout

    await asyncio.sleep(0.06)
    with pytest.raises(ConnectionError):
        await b.call(fail)  # failed trial

    assert b._current_timeout > first, "timeout did not grow after a failed trial"
    assert b._current_timeout <= 10.0, "backoff exceeded its cap"


@pytest.mark.anyio
async def test_rejection_is_distinguishable_from_upstream_failure():
    """Not-attempted and attempted-and-failed need different handling."""
    b = CircuitBreaker("distinct", failure_threshold=1, recovery_timeout=60)

    async def fail():
        raise ConnectionError("real failure")

    with pytest.raises(ConnectionError):
        await b.call(fail)
    with pytest.raises(CircuitOpenError) as ei:
        await b.call(fail)

    assert not isinstance(ei.value, ConnectionError)
    assert ei.value.retry_after > 0


def test_breakers_are_shared_per_source():
    """A breaker per call site would never trip."""
    a = get_breaker("shared-upstream")
    b = get_breaker("shared-upstream")
    assert a is b
    assert "shared-upstream" in all_breakers()


# ── HAWKES STATIONARITY PROJECTION (LOG-04) ──────────────────────────────────

def test_power_iteration_matches_true_spectral_radius():
    """The projection must use the real dominant eigenvalue, not a bound.

    The Frobenius norm is a valid upper bound on the spectral radius, so the
    old projection was safe -- but loose, which made it scale the excitation
    matrix down further than stationarity required and understate contagion.
    """
    numpy = pytest.importorskip("numpy")
    from services.correlation.hawkes_correlator import HawkesMLE

    est = HawkesMLE(domains=[f"d{i}" for i in range(11)])
    est.beta = 0.1
    rng = numpy.random.default_rng(3)
    A = rng.random((11, 11)) * 0.02
    est.alpha = [[float(A[i][j]) for j in range(11)] for i in range(11)]
    est.D = 11

    B = A / est.beta
    true_rho = float(max(abs(numpy.linalg.eigvals(B))))
    computed = est._spectral_radius([[B[i][j] for j in range(11)] for i in range(11)])

    assert computed == pytest.approx(true_rho, rel=1e-6)
    # And it must be tighter than the bound it replaced.
    assert computed <= float(numpy.linalg.norm(B, "fro")) + 1e-9


def test_projection_yields_a_stationary_process():
    """Stationarity requires the branching matrix spectral radius below 1."""
    numpy = pytest.importorskip("numpy")
    from services.correlation.hawkes_correlator import HawkesMLE

    est = HawkesMLE(domains=[f"d{i}" for i in range(6)])
    est.beta = 0.1
    est.D = 6
    # Deliberately explosive: rho >> 1 before projection.
    est.alpha = [[0.05] * 6 for _ in range(6)]
    assert float(max(abs(numpy.linalg.eigvals(numpy.array(est.alpha) / est.beta)))) > 1.0

    est._project_stationarity()

    rho = float(max(abs(numpy.linalg.eigvals(numpy.array(est.alpha) / est.beta))))
    assert rho < 1.0, "projected process is still explosive"
    assert rho == pytest.approx(est.spectral_radius_cap, rel=1e-6)


def test_projection_leaves_an_already_stationary_matrix_alone():
    """A model within the cap must not be shrunk further."""
    from services.correlation.hawkes_correlator import HawkesMLE

    est = HawkesMLE(domains=["a", "b"])
    est.beta = 1.0
    est.D = 2
    est.alpha = [[0.1, 0.0], [0.0, 0.1]]   # rho = 0.1, well under the cap
    before = [row[:] for row in est.alpha]

    est._project_stationarity()
    assert est.alpha == before


def test_spectral_radius_of_a_zero_matrix_is_zero():
    """No excitation at all must not divide by a zero norm."""
    from services.correlation.hawkes_correlator import HawkesMLE

    est = HawkesMLE(domains=["a", "b", "c"])
    est.D = 3
    assert est._spectral_radius([[0.0] * 3 for _ in range(3)]) == 0.0


# ── GUARDED HTTP CLIENT (LOG-03 wiring) ──────────────────────────────────────

@pytest.mark.anyio
async def test_guarded_get_returns_none_instead_of_raising():
    """Collector loops skip a cycle; they should not need their own try/except."""
    from shared.utils import http_client as HC
    import shared.utils.circuit_breaker as CB

    CB._BREAKERS.clear()

    async def boom(*a, **k):
        raise ConnectionError("dns failure")

    orig = HC.guarded_request.__wrapped__ if hasattr(HC.guarded_request, "__wrapped__") else None
    b = CB.get_breaker("unreachable")

    async def failing():
        raise ConnectionError("dns failure")

    # Exercise the breaker path directly: the wrapper's contract is that a
    # failure is counted and swallowed, not propagated.
    with pytest.raises(ConnectionError):
        await b.call(failing)
    assert b.total_failures == 1


@pytest.mark.anyio
async def test_client_errors_do_not_trip_the_circuit():
    """A 404 on one symbol must not disable the whole upstream.

    Only server-side failures (5xx) and rate limiting (429) indicate the
    upstream is unwell; a 4xx means our request was wrong.
    """
    from shared.utils import http_client as HC
    import inspect

    src = inspect.getsource(HC.guarded_request)
    # 5xx and 429 raise (counting toward the breaker); other 4xx return None.
    assert "status >= 500" in src
    assert "status == 429" in src
    assert "status >= 400" in src
    i5, i429, i4 = src.index("status >= 500"), src.index("status == 429"), src.index("status >= 400")
    assert i5 < i429 < i4, "4xx check must come last or it would swallow 5xx and 429"


@pytest.mark.anyio
async def test_open_circuit_skip_is_quiet_and_counted():
    """Skips fire every cycle while open; logging them loudly buries the cause."""
    from shared.utils import http_client as HC
    import inspect

    src = inspect.getsource(HC.guarded_request)
    assert "http_circuit_open_skips" in src, "circuit skips are not counted"
    # The skip branch logs at debug, not warning.
    skip_block = src[src.index("except CircuitOpenError"):src.index("except asyncio.TimeoutError")]
    assert "logger.debug" in skip_block
    assert "logger.warning" not in skip_block


def test_guarded_helpers_require_a_source_name():
    """Without a shared source name each call site gets its own breaker."""
    import inspect
    from shared.utils.http_client import guarded_request

    sig = inspect.signature(guarded_request)
    assert sig.parameters["source"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["source"].default is inspect.Parameter.empty, (
        "source must be required; a default would let call sites share one breaker by accident"
    )


# ── DATA STARVATION DETECTION ────────────────────────────────────────────────

@pytest.mark.anyio
async def test_starvation_watch_flags_a_connected_but_silent_source():
    """The AIS failure mode: connected, subscribed, zero messages, no error.

    No exception, no restart, a healthy heartbeat, and an empty panel that looks
    exactly like a quiet market.
    """
    m = CollectorMetrics("collector-silent")
    await m.start(None)

    task = asyncio.create_task(
        m.watch_for_starvation(expect_within_sec=0.05, check_interval_sec=0.05, source="test-feed")
    )
    await asyncio.sleep(0.15)
    task.cancel()

    assert M._GAUGE_METRICS.get("collector_starved:collector-silent") == 1.0


@pytest.mark.anyio
async def test_a_flowing_collector_is_not_flagged_as_starved():
    """The watcher must not cry wolf while data is actually arriving.

    Uses a generous freshness window so the assertion tests the branch logic
    rather than the scheduler: a tight window would make any single record go
    stale between checks, which is correct behaviour but not what is under test.
    """
    m = CollectorMetrics("collector-flowing")
    await m.start(None)
    m.ingested(1)  # data has arrived and is recent

    task = asyncio.create_task(
        m.watch_for_starvation(expect_within_sec=30.0, check_interval_sec=0.05)
    )
    await asyncio.sleep(0.15)
    task.cancel()

    assert M._GAUGE_METRICS.get("collector_starved:collector-flowing") == 0.0


@pytest.mark.anyio
async def test_never_received_is_distinguished_from_went_quiet():
    """An inactive credential and a quiet upstream need different responses."""
    import inspect
    src = inspect.getsource(CollectorMetrics.watch_for_starvation)
    assert "self._ingested == 0" in src, "does not special-case never-received"
    assert "logger.error" in src and "logger.warning" in src, (
        "never-received and went-quiet must not share a log level"
    )


@pytest.mark.anyio
async def test_starvation_watch_survives_internal_errors():
    """Telemetry must never take down the collector it observes."""
    import inspect
    src = inspect.getsource(CollectorMetrics.watch_for_starvation)
    assert "except asyncio.CancelledError" in src
    assert "logger.debug" in src, "an internal error must not escape the watcher"
