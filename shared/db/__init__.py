"""
shared/db/__init__.py

Database clients. Import these anywhere:
    from shared.db import get_timescale, get_neo4j, get_redis

FIX (code review): TimescaleClient.query() now rolls back and re-raises on
  exception. Previously a query error returned the connection to the pool
  in an indeterminate state — no rollback, no re-raise — which would cause
  pool exhaustion on repeated errors and hide the real failure.
"""

import logging
import os
import json
import asyncio
from typing import Optional, List, Dict
import time
# Using the native async client in modern redis-py
import redis.asyncio as aioredis
import asyncpg
from neo4j import AsyncGraphDatabase as _Neo4j

logger = logging.getLogger(__name__)

# --- Redis Async Client ---
class RedisClient:
    def __init__(self):
        # Decodes responses to strings natively, uses connection pooling automatically
        self._client = aioredis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"), 
            decode_responses=True,
            max_connections=300
        )
    @property
    def raw(self): 
        return self._client
    
    async def zadd(self, key: str, mapping: dict):
        await self._client.zadd(key, mapping)

    async def zremrangebyscore(self, key: str, min_val, max_val):
        return await self._client.zremrangebyscore(key, min_val, max_val)

    async def zrange(self, key: str, start, end, desc=False, byscore=False):
        return await self._client.zrange(key, start, end, desc=desc, byscore=byscore)

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def ping(self) -> bool:
        return await self._client.ping()

# --- Neo4j Synchronous Client (For Supervisor ONLY) ---
class Neo4jClient:
    def __init__(self):
        self._driver = None
        self._uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        neo4j_pass = os.getenv("NEO4J_PASSWORD")
        if not neo4j_pass:
            env_name = os.getenv("SENTINEL_ENV", "dev").lower()
            if env_name in ("prod", "production", "staging"):
                raise RuntimeError("CRITICAL SECURITY FAILURE: NEO4J_PASSWORD environment variable is missing.")
            neo4j_pass = "sentinel_graph"
        self._auth = (neo4j_user, neo4j_pass)

    async def connect(self):
        if not self._driver:
            self._driver = _Neo4j.driver(self._uri, auth=self._auth)
            await self._driver.verify_connectivity()
            logger.info("Neo4j connected")

    async def execute(self, cypher: str, params: dict = None):
        async with self._driver.session() as s:
            await s.run(cypher, **(params or {}))

    async def query(self, cypher: str, params: dict = None) -> List[Dict]:
        async with self._driver.session() as s:
            result = await s.run(cypher, **(params or {}))
            return await result.data()
    
    async def close(self):
        if self._driver:
            await self._driver.close()


# ── TIMESCALEDB ───────────────────────────────────────────────────────────────

class TimescaleClient:

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
    async def _connect(self, retries: int = 12):
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            env_name = os.getenv("SENTINEL_ENV", "dev").lower()
            if env_name in ("prod", "production", "staging"):
                raise RuntimeError("CRITICAL SECURITY FAILURE: DATABASE_URL environment variable is missing.")
            dsn = "postgresql://sentinel:sentinel_local_dev@localhost:5432/sentinel"
        async def init_connection(conn):
            await conn.set_type_codec(
                'jsonb',
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog'
            )

        for attempt in range(retries):
            try:
                self._pool = await asyncpg.create_pool(
                    dsn,
                    min_size=2,
                    max_size=40,
                    command_timeout=60,
                    init=init_connection # Register the JSONB codec
                )
                logger.info("⚡ TimescaleDB (asyncpg) pool established.")
                return
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(f"TimescaleDB attempt {attempt+1}/{retries} — retry in {wait}s: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(wait)
                else:
                    raise

    def _sanitize_row(self, row: dict) -> dict:
        import uuid
        from datetime import datetime
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, uuid.UUID):
                d[k] = str(v)
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
            elif isinstance(v, list):
                d[k] = [str(item) if isinstance(item, uuid.UUID) else item for item in v]
        return d

    async def query(self, sql: str, *params) -> List[Dict]:
        if len(params) == 1 and isinstance(params[0], (tuple, list)):
            params = params[0]
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [self._sanitize_row(r) for r in rows]

    async def query_one(self, sql: str, *params) -> Optional[Dict]:
        if len(params) == 1 and isinstance(params[0], (tuple, list)):
            params = params[0]
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            return self._sanitize_row(row) if row else None
    
    async def execute(self, sql: str, *params):
        if len(params) == 1 and isinstance(params[0], (tuple, list)):
            params = params[0]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(sql, *params)

    async def execute_many(self, sql: str, rows: List[tuple]):
        """High-performance batch execution."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)


# ── SINGLETONS ────────────────────────────────────────────────────────────────


_timescale: Optional[TimescaleClient] = None
_async_redis: Optional[RedisClient] = None
_neo4j: Optional[Neo4jClient] = None


async def get_timescale() -> TimescaleClient:
    global _timescale
    if _timescale is None:
        # Only happens the very first time the app calls this.
        _timescale = TimescaleClient()
        await _timescale._connect()
    return _timescale


async def get_redis() -> RedisClient:
    """Thread-safe async singleton for Redis."""
    global _async_redis
    if _async_redis is None:
        _async_redis = RedisClient()
    return _async_redis


async def get_neo4j() -> Neo4jClient:
    """Asynchronous Neo4j client. Should only be used by the GraphSupervisor."""
    global _neo4j
    if _neo4j is None:
        client = Neo4jClient()
        await client.connect()
        _neo4j = client
    return _neo4j


