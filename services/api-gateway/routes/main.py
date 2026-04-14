import sys
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api-gateway-main")

from shared.db import get_neo4j, get_timescale, get_redis
from dependencies import verify_api_key
from routes import system, scenarios, graph, events

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes databases and attaches them to the global app state."""
    app.state.db = get_timescale()
    app.state.neo4j = get_neo4j()
    app.state.redis = get_redis()
    yield
    logger.info("Shutting down...")
    
app = FastAPI(
    title="SENTINEL Intelligence API",
    description="Secure REST interface for querying Sentinel multi-domain intelligence.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_api_key)]

)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(system.router)
app.include_router(scenarios.router)
app.include_router(events.router)
app.include_router(graph.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

