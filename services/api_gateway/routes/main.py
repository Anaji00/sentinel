"""
services/api-gateway/routes/main.py

This is the MAIN ENTRY POINT for the API Gateway.
It acts as the "Front Door" to the entire Sentinel platform, securely routing
incoming web requests to the appropriate databases and backend services.
"""

import sys
import os
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# ENVIRONMENT SETUP: Dynamically find the root folder and add it to the system path.
# This ensures Python can find our custom `shared` modules (like `shared.db`).
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-gateway-main")

from shared.db import get_neo4j, get_timescale, get_redis
from services.api_gateway.dependencies import verify_api_key
from services.api_gateway.routes import (
    auth,
    oidc,
    billing,
    system,
    search,
    integrations,
    scenarios,
    events,
    graph,
    radar,
    agents,
    flags,
    backtest,
    explain,
    health,
    watchlists,
    audit,
    cases,
    portfolio,
    reports,
    filings,
    methodology,
    sovereignty,
    attribution,
    feedback,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    LIFESPAN MANAGER (Startup & Shutdown)
    Everything before the `yield` runs exactly once when the server starts.
    Everything after the `yield` runs exactly once when the server stops.
    """
    # We create our database connection pools once, and store them in `app.state`.
    # `app.state` acts as a global storage box that any route can reach into later.
    app.state.db = await get_timescale()
    app.state.neo4j = await get_neo4j()
    app.state.redis = await get_redis()
    # Migrates the single environment-defined operator into the accounts table so
    # authentication can stop comparing a plaintext ADMIN_PASSWORD.
    await auth.ensure_admin_account(app.state.db)
    yield
    # CLEANUP: Gracefully close all database connections on shutdown.
    logger.info("Shutting down — closing database connections...")
    try:
        if hasattr(app.state, "db") and app.state.db and app.state.db._pool:
            await app.state.db._pool.close()
            logger.info("TimescaleDB pool closed.")
    except Exception as e:
        logger.error(f"Error closing TimescaleDB pool: {e}")
    try:
        if hasattr(app.state, "neo4j") and app.state.neo4j:
            await app.state.neo4j.close()
            logger.info("Neo4j driver closed.")
    except Exception as e:
        logger.error(f"Error closing Neo4j driver: {e}")
    try:
        if hasattr(app.state, "redis") and app.state.redis:
            await app.state.redis.raw.aclose()
            logger.info("Redis connection closed.")
    except Exception as e:
        logger.error(f"Error closing Redis connection: {e}")
    logger.info("Shutdown complete.")
    
app = FastAPI(
    title="SENTINEL Intelligence API",
    description="Secure REST interface for querying Sentinel multi-domain intelligence.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_api_key)]
)

from shared.utils.env_guard import SAFE_DEV_ENVS

# Default localhost origins used only in dev/test environments.
_DEV_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3001",
    "http://localhost:8000",
]


def build_cors_kwargs(sentinel_env: str, raw_cors_origins: str) -> dict:
    """Build the CORSMiddleware kwargs for the given environment.

    In dev/test environments (SAFE_DEV_ENVS), the open localhost regex is enabled
    for developer agility. In production it is disabled to prevent an unauthorized
    local process from pivoting against a user's authenticated browser session; the
    allowlist must come explicitly from CORS_ALLOWED_ORIGINS.

    Pure function of its inputs so the production branch can be tested directly.
    """
    env = (sentinel_env or "").lower().strip()
    cors_origins = [o.strip() for o in raw_cors_origins.split(",") if o.strip()]

    kwargs = {
        "allow_credentials": True,
        "allow_methods": ["*"],
        "allow_headers": ["*"],
    }

    if env in SAFE_DEV_ENVS:
        kwargs["allow_origins"] = cors_origins or list(_DEV_CORS_ORIGINS)
        kwargs["allow_origin_regex"] = r"https?://(localhost|127\.0\.0\.1)(:\d+)?"
    else:
        if not cors_origins:
            logger.warning(
                "CORS: SENTINEL_ENV=%r is not a dev environment and "
                "CORS_ALLOWED_ORIGINS is empty. All cross-origin browser requests "
                "will be rejected. Set CORS_ALLOWED_ORIGINS explicitly.",
                env,
            )
        kwargs["allow_origins"] = cors_origins
        kwargs["allow_origin_regex"] = None

    return kwargs


app.add_middleware(
    CORSMiddleware,
    **build_cors_kwargs(os.getenv("SENTINEL_ENV", "dev"), os.getenv("CORS_ALLOWED_ORIGINS", "")),
)

# MODULAR ROUTING:
app.include_router(auth.router)
# Registered unconditionally; every route inside answers 404 unless OIDC is
# configured. Mounting conditionally would mean the sign-in page could not ask
# whether SSO exists without the answer itself being a 404.
app.include_router(oidc.router)
app.include_router(billing.router)
app.include_router(system.router)
app.include_router(scenarios.router)
app.include_router(events.router)
app.include_router(graph.router)
app.include_router(radar.router)
app.include_router(agents.router)
app.include_router(flags.router)
app.include_router(backtest.router)
app.include_router(explain.router)
# Signal attribution and entity merge review: which weights earn their number,
# and which spellings name one subject.
app.include_router(attribution.router)
# The one feedback loop the platform lacked -- everything it learned, it learned
# from the market, which cannot see an alert that is valid and useless.
app.include_router(feedback.router)
app.include_router(health.router)
app.include_router(watchlists.router)
app.include_router(audit.router)
app.include_router(cases.router)
app.include_router(portfolio.router)
app.include_router(reports.router)
app.include_router(filings.router)
app.include_router(methodology.router)
app.include_router(sovereignty.router)
app.include_router(search.router)
app.include_router(integrations.router)

from fastapi.responses import PlainTextResponse
from shared.utils.metrics import MetricsCollector
from services.api_gateway.dependencies import get_redis_client, get_redis_optional

@app.get("/metrics", include_in_schema=False)
@app.get("/api/v1/metrics", include_in_schema=False)
async def root_metrics(redis = Depends(get_redis_optional)):
    """Prometheus scrape target (deploy/prometheus.yml points here).

    Delegates to the aggregating implementation so the scrape reflects every
    Sentinel process, not just this one. Rendering only the gateway's own
    process-local counters here would silently discard the collector, enrichment
    and correlation metrics that the aggregation layer exists to surface.
    """
    return await system.get_system_metrics(redis)

@app.get("/metrics/json", include_in_schema=False)
async def root_metrics_json(redis = Depends(get_redis_optional)):
    """JSON summary, aggregated across services."""
    return await system.get_system_metrics_json(redis)

from fastapi import FastAPI, Depends, Request

@app.get("/health", include_in_schema=False)
async def root_health(request: Request = None, redis = Depends(get_redis_client)):
    """Convenience alias for /api/v1/health."""
    from services.api_gateway.routes import system
    return await system.health_check(request, redis)

if __name__ == "__main__":
    # LOCAL DEVELOPMENT SERVER:
    # Uvicorn is the lightning-fast web server that runs FastAPI.
    # `reload=True` means every time you save a Python file, the server 
    # automatically restarts so you can test your changes instantly.
    import uvicorn
    uvicorn.run("services.api_gateway.routes.main:app", host="0.0.0.0", port=8000, reload=True)
