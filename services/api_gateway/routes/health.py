"""
services/api_gateway/routes/health.py

Data Health Dashboard, Liveness/Readiness Probes, and Secrets Audit.
"""

import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from shared.utils.heartbeat import get_all_heartbeats_status
from shared.utils.source_freshness import source_freshness, stale_sources, degraded_sources
from shared.utils.secrets import audit_secrets_environment
from shared.utils.quiet_failures import swallowed
from shared.utils.rbac import Role, require_role
from shared.utils.mailer import is_configured as mailer_is_configured
from shared.utils.market_session import (
    holiday_table_is_current,
    HOLIDAY_TABLE_THROUGH,
)
from shared.db import get_neo4j
from services.api_gateway.dependencies import get_db_optional, get_redis_optional, get_redis_client

# How long a vessel may go dark before the maritime detectors consider it
# notable. Reported so the dashboard shows the running configuration rather
# than a literal it carried when the field was absent.
MARITIME_DARK_GAP_HOURS = 4

logger = logging.getLogger("api-gateway.health")

router = APIRouter(prefix="/api/v1/health", tags=["Data Health & Telemetry"])


@router.get("/liveness")
async def get_liveness():
    """Basic service liveness check."""
    return {"status": "UP"}


@router.get("/readiness")
async def get_readiness(
    db = Depends(get_db_optional),
    redis = Depends(get_redis_optional),
):
    """Deep readiness check verifying TimescaleDB and Redis connectivity."""
    db_ok = False
    redis_ok = False

    if db:
        try:
            res = await db.query("SELECT 1 as ping;")
            db_ok = bool(res and res[0].get("ping") == 1)
        except Exception as e:
            logger.warning(f"Readiness DB probe failed: {e}")

    if redis:
        try:
            raw_redis = getattr(redis, "raw", redis)
            ping_res = await raw_redis.ping()
            redis_ok = bool(ping_res)
        except Exception as e:
            logger.warning(f"Readiness Redis probe failed: {e}")

    ready = db_ok and redis_ok
    status_code = 200 if ready else 503
    return {
        "status": "READY" if ready else "DEGRADED",
        "database": "CONNECTED" if db_ok else "DISCONNECTED",
        "redis": "CONNECTED" if redis_ok else "DISCONNECTED",
    }


@router.get("/data")
async def get_data_health_dashboard(
    redis = Depends(get_redis_optional),
    db = Depends(get_db_optional),
):
    """
    Data Health Dashboard: Scans all 15 Sentinel collectors and background pipelines.
    Returns liveness status, heartbeat age, and cluster operational ratio.
    """
    status = await get_all_heartbeats_status(redis)

    # Whether the feeds are still producing, not only whether the collectors
    # are still running.
    #
    # A collector can be perfectly alive while the feed behind it has stopped:
    # the poll loop runs, the HTTP call returns, and it returns nothing. The
    # heartbeat above cannot see that, so ten silent sources looked identical
    # to ten quiet markets and the difference had to be established by hand.
    try:
        freshness = await source_freshness(redis)
        stale = await stale_sources(redis)
        if isinstance(status, dict):
            status["sources"] = freshness
            status["stale_sources"] = [r["source"] for r in stale]
            status["stale_source_count"] = len(stale)
            # Alive but no longer producing at the rate it used to. Silence is
            # caught above; decay is not, because the cadence a source is
            # judged against decays with it.
            degraded = [r for r in freshness if r.get("rate_collapsed")]
            status["degraded_sources"] = [
                {"source": r["source"], "times_slower": r.get("rate_ratio_vs_reference")}
                for r in degraded
            ]
            status["degraded_source_count"] = len(degraded)
    except Exception as e:
        logger.debug("Source freshness unavailable: %s", e)

    # The fields the dashboard actually reads.
    #
    # SystemHealthHUD binds five names off this payload -- status,
    # redis_connected, timescale_connected, neo4j_connected and
    # active_configuration -- and the endpoint published none of them. It
    # publishes `system_status`; the component reads `status`. So against a
    # stack whose three datastores are all healthy the panel rendered
    # "CONNECTING..." forever with three red DISCONNECTED tiles, and its
    # "ACTIVE RUNTIME CONFIGURATION" block fell through to two hardcoded
    # literals. useSWR<T> is an assertion over parsed JSON rather than a check
    # against it, so nothing could catch that.
    #
    # These are worth publishing on their own merits: a health endpoint that
    # cannot say whether the graph is reachable is not finished.
    if isinstance(status, dict):
        redis_ok = False
        timescale_ok = False
        neo4j_ok = False
        try:
            if redis is not None:
                raw_redis = getattr(redis, "raw", redis)
                redis_ok = bool(await raw_redis.ping())
        except Exception as _exc:
            swallowed("api_gateway.routes.health.redis_probe", _exc, logger)
        try:
            if db is not None:
                rows = await db.query("SELECT 1 AS ping;")
                timescale_ok = bool(rows and rows[0].get("ping") == 1)
        except Exception as _exc:
            swallowed("api_gateway.routes.health.timescale_probe", _exc, logger)
        try:
            graph = await get_neo4j()
            if graph is not None:
                rows = await graph.query("RETURN 1 AS ping")
                neo4j_ok = bool(rows)
        except Exception as _exc:
            swallowed("api_gateway.routes.health.neo4j_probe", _exc, logger)

        status["redis_connected"] = redis_ok
        status["timescale_connected"] = timescale_ok
        status["neo4j_connected"] = neo4j_ok
        # Alias, so the field the dashboard reads and the field this endpoint
        # has always published are the same value rather than two contracts.
        status["status"] = status.get("system_status")

        # Configuration the deployment is actually running, in place of the two
        # literals the panel used when this key was absent.
        status["active_configuration"] = {
            "maritime_dark_thresholds": MARITIME_DARK_GAP_HOURS,
            "tracked_financial_instruments": await _watched_equity_count(redis),
            # A table-driven market calendar has one failure mode: running past
            # the last year it knows. The guard for it existed and had no
            # caller, so from 2028 every holiday would have been scored as an
            # ordinary session with nothing saying so.
            "market_calendar_current": holiday_table_is_current(),
            "market_calendar_through": HOLIDAY_TABLE_THROUGH,
            # Mail is what verification and password reset ride on. Unset, the
            # signup path already declines to promise a link -- but nothing
            # reported the transport was absent, and the credential audit does
            # not check any SMTP_* variable.
            "mail_transport_configured": mailer_is_configured(),
        }

    return status


async def _watched_equity_count(redis) -> int:
    """How many instruments are under active surveillance right now."""
    try:
        if redis is None:
            return 0
        raw_redis = getattr(redis, "raw", redis)
        return int(await raw_redis.zcard("sentinel:watched:equities") or 0)
    except Exception as _exc:
        swallowed("api_gateway.routes.health.watched_count", _exc, logger)
        return 0


@router.get("/sources")
async def get_source_freshness(redis = Depends(get_redis_optional)):
    """Per-source production freshness, worst first.

    Each entry carries how long the source has been silent, the cadence it is
    being judged against, and which rule decided -- its own measured interval,
    or the absolute ceiling when it has not been seen often enough for an
    interval to mean anything. The distinction matters: an hourly poller silent
    for fifty minutes is normal and a tick feed silent for fifty minutes is not.
    """
    return await source_freshness(redis)


@router.get(
    "/secrets",
    # An operator-only view. It returns the deployment's internal topology --
    # database host, port, name and user, the Kafka bootstrap servers, the
    # Neo4j URI and user -- in clear, plus a masked preview of every secret
    # that discloses eight characters and the exact length. It carried no role
    # guard at all, so any signed-in VIEWER could read it.
    dependencies=[Depends(require_role(Role.ADMIN))],
)
async def get_secrets_audit():
    """
    Environment Secrets Audit: Non-leaking configuration audit
    with masked tokens and credential readiness ratios.
    """
    return audit_secrets_environment()
