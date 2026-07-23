"""
services/api-gateway/routes/main.py

This is the MAIN ENTRY POINT for the API Gateway.
It acts as the "Front Door" to the entire Sentinel platform, securely routing
incoming web requests to the appropriate databases and backend services.
"""

import sys
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
from services.api_gateway.routes import system, scenarios, events, graph, radar, agents

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
    yield
    # CLEANUP: Code placed here would close the database connections gracefully.
    logger.info("Shutting down...")
    
app = FastAPI(
    title="SENTINEL Intelligence API",
    description="Secure REST interface for querying Sentinel multi-domain intelligence.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_api_key)]
)

import os

# CORS (Cross-Origin Resource Sharing):
# Driven by CORS_ALLOWED_ORIGINS env var for spec compliance with allow_credentials=True
raw_cors_origins = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001,http://localhost:8000")
cors_origins = [origin.strip() for origin in raw_cors_origins.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# MODULAR ROUTING:
app.include_router(system.router)
app.include_router(scenarios.router)
app.include_router(events.router)
app.include_router(graph.router)
app.include_router(radar.router)
app.include_router(agents.router)

if __name__ == "__main__":
    # LOCAL DEVELOPMENT SERVER:
    # Uvicorn is the lightning-fast web server that runs FastAPI.
    # `reload=True` means every time you save a Python file, the server 
    # automatically restarts so you can test your changes instantly.
    import uvicorn
    uvicorn.run("services.api_gateway.routes.main:app", host="0.0.0.0", port=8000, reload=True)
