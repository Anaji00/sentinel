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
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-gateway-main")

from shared.db import get_neo4j, get_timescale, get_redis
from services.api_gateway.dependencies import verify_api_key
from services.api_gateway.routes import system, scenarios, events, graph

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    LIFESPAN MANAGER (Startup & Shutdown)
    Everything before the `yield` runs exactly once when the server starts.
    Everything after the `yield` runs exactly once when the server stops.
    """
    # We create our database connection pools once, and store them in `app.state`.
    # `app.state` acts as a global storage box that any route can reach into later.
    app.state.db = get_timescale()
    app.state.neo4j = get_neo4j()
    app.state.redis = get_redis()
    yield
    # CLEANUP: Code placed here would close the database connections gracefully.
    logger.info("Shutting down...")
    
app = FastAPI(
    title="SENTINEL Intelligence API",
    description="Secure REST interface for querying Sentinel multi-domain intelligence.",
    version="1.0.0",
    lifespan=lifespan,
    # GLOBAL SECURITY: By placing the API Key verification here, we lock down the 
    # ENTIRE application. Every single route will require a valid X-API-KEY header, 
    # so developers don't have to remember to add it to new routes manually.
    dependencies=[Depends(verify_api_key)]
)

# CORS (Cross-Origin Resource Sharing):
# Web browsers block frontend apps (like a React dashboard) from making requests 
# to an API on a different port/domain by default. This middleware acts as a 
# bouncer, telling the browser "It's okay, allow requests from anywhere (*)".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# MODULAR ROUTING:
# Instead of writing 5,000 lines of code in this one file, we import specific 
# "mini-apps" (routers) and plug them into the main app.
app.include_router(system.router)
app.include_router(scenarios.router)
app.include_router(events.router)
app.include_router(graph.router)

if __name__ == "__main__":
    # LOCAL DEVELOPMENT SERVER:
    # Uvicorn is the lightning-fast web server that runs FastAPI.
    # `reload=True` means every time you save a Python file, the server 
    # automatically restarts so you can test your changes instantly.
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
