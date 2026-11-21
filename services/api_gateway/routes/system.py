import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from typing import Any, Dict

from services.api_gateway.dependencies import get_db_optional, get_redis_client, get_redis_optional
from shared.utils.rbac import Role, require_role
from shared.utils.config import config

logger = logging.getLogger("api-gateway.system")

router = APIRouter(prefix="/api/v1/health", tags=["System"])

@router.get("/")
async def health_check(request: Request, redis = Depends(get_redis_client)):
    """Verify backend infrastructure status across all critical services."""
    redis_connected = False
    if redis:
        try:
            redis_connected = await redis.ping()
        except Exception:
            redis_connected = False

    timescale_connected = False
    try:
        db = request.app.state.db
        if db:
            await db.query("SELECT 1")
            timescale_connected = True
    except Exception:
        timescale_connected = False

    neo4j_connected = False
    try:
        neo4j = request.app.state.neo4j
        if neo4j:
            await neo4j.execute("RETURN 1")
            neo4j_connected = True
    except Exception:
        neo4j_connected = False

    all_healthy = redis_connected and timescale_connected and neo4j_connected

    return {
        "status": "online" if all_healthy else "degraded",
        "redis_connected": redis_connected,
        "timescale_connected": timescale_connected,
        "neo4j_connected": neo4j_connected,
        "active_configuration": {
            "maritime_dark_thresholds": config.get("maritime", {}).get("dark_threshold_hours"),
            "tracked_financial_instruments": len(config.get("financial", {}).get("geo_instruments", [])),
        }
    }


def _render_prometheus(counters: dict, gauges: dict, latencies: dict) -> str:
    """Renders aggregated metrics in Prometheus text exposition format.

    Metric names may already carry a label suffix (e.g. `foo{service="x"}`)
    from cross-service aggregation, so the label block is split off before the
    name is sanitized and reattached afterwards.
    """
    lines = []

    def emit(name: str, value, kind: str) -> None:
        base, _, labels = name.partition("{")
        clean = base.replace(":", "_").replace(".", "_").replace("-", "_")
        suffix = "{" + labels if labels else ""
        if not suffix:  # TYPE is declared once per metric family, not per series
            lines.append(f"# TYPE sentinel_{clean} {kind}")
        lines.append(f"sentinel_{clean}{suffix} {value}")

    for k, v in sorted(counters.items()):
        emit(k, v, "counter")
    for k, v in sorted(gauges.items()):
        emit(k, v, "gauge")
    for k, stats in sorted(latencies.items()):
        clean = k.replace(":", "_").replace(".", "_").replace("-", "_")
        lines.append(f"# TYPE sentinel_{clean}_seconds summary")
        lines.append(f'sentinel_{clean}_seconds{{quantile="0.5"}} {stats["p50_ms"] / 1000.0}')
        lines.append(f'sentinel_{clean}_seconds{{quantile="0.95"}} {stats["p95_ms"] / 1000.0}')
        lines.append(f'sentinel_{clean}_seconds_count {stats["count"]}')

    return "\n".join(lines) + "\n"


@router.get("/metrics")
async def get_system_metrics(redis=Depends(get_redis_optional)):
    """Prometheus metrics aggregated across every Sentinel process.

    Counters are recorded process-locally and published to Redis; this endpoint
    sums them. Without that aggregation only the gateway's own counters would
    ever be scraped, since each service is a separate container.
    """
    from fastapi.responses import PlainTextResponse
    from shared.utils.metrics import MetricsCollector, collect_all

    agg = await collect_all(redis)
    latencies = MetricsCollector.get_summary()["latencies"]
    return PlainTextResponse(
        _render_prometheus(agg["counters"], agg["gauges"], latencies),
        media_type="text/plain",
    )


@router.get("/metrics/json")
async def get_system_metrics_json(redis=Depends(get_redis_optional)):
    """JSON summary for dashboard components, aggregated across services."""
    from shared.utils.metrics import MetricsCollector, collect_all
    from shared.utils.circuit_breaker import all_breakers

    agg = await collect_all(redis)
    return {
        "counters": agg["counters"],
        "gauges": agg["gauges"],
        "latencies": MetricsCollector.get_summary()["latencies"],
        "circuit_breakers": all_breakers(),
    }

@router.get("/sources")
async def get_source_health(redis=Depends(get_redis_optional)):
    """Each data source judged against its own rhythm: ok, thin, silent, or
    retired with the reason (services/agents/source_health_job.py)."""
    import json as _json
    from services.agents.source_health_job import REPORT_KEY

    raw = getattr(redis, "raw", redis) if redis is not None else None
    blob = await raw.get(REPORT_KEY) if raw is not None else None
    return _json.loads(blob) if blob else {"generated_at": None, "counts": {}, "sources": []}

@router.get("/liveness")
async def get_mechanism_liveness(redis=Depends(get_redis_optional)):
    """Which declared mechanisms have run, how often, and when they last did.

    The question this answers is the one that produced the largest single class
    of defect in this platform's audit: a mechanism built correctly, wired
    correctly, deployed, and never once executed. The admission bar that had
    never refused a candidate; the focus set that never reached the financial
    domain; the volume/open-interest chain wired end to end and receiving
    nothing; a vector index nobody had pruned. None was visible from the code,
    because the code was right every time.

    `never` is the list worth reading. A declared mechanism with a lifetime
    count of zero is a claim the platform makes about itself and has never once
    honoured.
    """
    from shared.utils.liveness import report

    rows = await report(redis)
    never = [r for r in rows if r["never"]]
    return {
        "declared": len(rows),
        "never_fired": len(never),
        # Named separately rather than left for a caller to filter, because the
        # filtered list is the entire point and a summary count is easy to skim
        # past.
        "never": [{"mechanism": r["mechanism"], "describes": r["describes"]} for r in never],
        "mechanisms": rows,
    }


@router.get("/model-quota")
async def model_quota(
    redis=Depends(get_redis_client),
    user: Dict[str, Any] = Depends(require_role(Role.VIEWER)),
) -> Dict[str, Any]:
    """Where each free-tier hosted model stands against its per-minute and per-day limits, most used first.
    Read only. `status` is ok, near limit (80% of any limit), at limit, cooling down (the provider refused a call) or no key set."""
    from shared.utils import model_quota as mq

    raw = getattr(redis, "raw", redis)
    models = await mq.snapshot(raw)
    return {"count": len(models), "models": models,
            "note": None if models else "No hosted-model limits are configured."}


@router.get("/redis")
async def redis_health(redis=Depends(get_redis_optional)):
    """Redis memory against its limit and keys evicted in roughly the last 24 hours (shared/utils/redis_health.py). Any eviction in that window, or 85%
    full, is listed in `alarms`, and the alarm clears by itself once a day passes without one. The baseline (eviction count at the window's start) is stored
    with the Redis run id and its start time; the counter restarts with the process (a new run's baseline is 0), a read never moves it, and an unreadable
    baseline is treated as unknown rather than as an error."""
    import time

    from shared.utils.redis_health import plan_baseline, summarize

    raw = getattr(redis, "raw", redis)
    if raw is None:
        raise HTTPException(status_code=503, detail="Redis is not reachable.")
    mem, stats, server = await raw.info("memory"), await raw.info("stats"), await raw.info("server")
    run_id = str(server.get("run_id") or "")
    evicted = int(stats.get("evicted_keys") or 0)
    key = "sentinel:redis:evicted_baseline"
    baseline, new_value = plan_baseline(await raw.get(key), run_id, int(time.time()), evicted)
    if new_value is not None:
        await raw.set(key, new_value)
    return summarize(mem, stats, baseline)


@router.get("/latency")
async def route_latency(min_requests: int = 1, limit: int = 50):
    """Per-route latency in this gateway process since it started: requests, mean, p50, p95, max, slow (over 1 s) and 5xx counts over each
    route's last 500 requests, slowest p95 first. Keyed by route template, so no tickers or user data appear."""
    from shared.utils.route_latency import BOOK, SLOW_MS, WINDOW

    return {"slow_ms": SLOW_MS, "window": WINDOW, "routes": BOOK.table(min_count=max(1, min_requests), limit=max(1, min(limit, 200)))}


@router.get("/correlation-funnel")
async def correlation_funnel(days: int = 7, db=Depends(get_db_optional), redis=Depends(get_redis_optional)):
    """What each correlation rule family produced in the last `days` (1 to 30): counts by alert tier, share of all correlations, rules and entities behind it
    (shared/utils/correlation_funnel.py). The input to the registered question of which families only ever reach WATCH and could stop feeding agents. Report only."""
    from shared.utils import correlation_funnel as cf
    from shared.utils.response_cache import cached_json

    if db is None:
        raise HTTPException(status_code=503, detail="The funnel is temporarily unavailable.")
    n = max(1, min(int(days), 30))
    raw = getattr(redis, "raw", redis)

    async def compute():
        rows = await db.query("SELECT rule_name, alert_tier, count(*) AS n, count(DISTINCT primary_entity_id) AS entities FROM correlations "
                              "WHERE detected_at > NOW() - make_interval(days => $1) GROUP BY 1, 2", n)
        return {"days": n, **cf.build([dict(r) for r in rows or []])}

    return await cached_json(raw, f"api:health:correlation_funnel:{n}", compute, ttl=600, max_stale=3600)
