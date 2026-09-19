"""
shared/db/__init__.py

Database clients. Import these anywhere:
    from shared.db import get_timescale, get_neo4j, get_redis

Note: TimescaleClient.query() uses asyncpg's connection.fetch() which
  auto-releases the connection back to the pool on completion or error.
  Write operations (execute/execute_many) are wrapped in explicit
  transactions for rollback safety.
"""

import logging
import re
import os
import json
import asyncio as _asyncio
import asyncio
from typing import Optional, List, Dict
import time
# Using the native async client in modern redis-py
import redis.asyncio as aioredis
import asyncpg
from neo4j import AsyncGraphDatabase as _Neo4j
from shared.utils.env_guard import resolve_env_var
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger(__name__)

# --- Redis Async Client ---
class RedisClient:
    def __init__(self):
        redis_url = resolve_env_var("REDIS_URL", "redis://localhost:6379/0")
        if "redis://redis:" in redis_url or "@redis:" in redis_url:
            try:
                import socket
                socket.gethostbyname("redis")
            except socket.gaierror:
                redis_url = redis_url.replace("redis://redis:", "redis://localhost:").replace("@redis:", "@localhost:")

        # Decodes responses to strings natively, uses connection pooling automatically
        self._client = aioredis.from_url(
            redis_url, 
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

    async def get(self, key: str) -> Optional[str]:
        return await self._client.get(key)

    async def set(self, key: str, value: str, ex: Optional[int] = None):
        await self._client.set(key, value, ex=ex)

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def ping(self) -> bool:
        return await self._client.ping()

# --- Neo4j Synchronous Client (For Supervisor ONLY) ---
class Neo4jClient:
    def __init__(self):
        self._driver = None
        # The configured address is kept, verbatim and permanently.
        #
        # This used to resolve "neo4j" with socket.gethostbyname() at
        # construction and, on gaierror, rewrite bolt://neo4j: to
        # bolt://localhost: -- for the life of the process. The fallback exists
        # so the supervisor can be run outside compose, which is reasonable; the
        # trigger was not. A DNS lookup fails while the container is still
        # coming up, or for the seconds a compose network is being rebuilt, and
        # one such moment permanently pinned a correctly configured client to
        # localhost, where nothing listens. Observed: enrichment crash-looped
        # indefinitely on `Couldn't connect to localhost:7687` with
        # NEO4J_URI=bolt://neo4j:7687 correct in its own environment, because
        # Neo4j had been down for the few seconds the client was constructed.
        #
        # A transient failure must not become a permanent one. The fallback now
        # happens at connect time, per attempt, and only after the configured
        # address has actually refused a connection -- so a later attempt tries
        # the real address again.
        self._configured_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self._uri = self._configured_uri

        neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        neo4j_pass = resolve_env_var("NEO4J_PASSWORD", "sentinel_graph")
        self._auth = (neo4j_user, neo4j_pass)

    def _candidate_uris(self) -> List[str]:
        """The configured address first, then a local fallback if one applies.

        Only a container-style hostname gets a fallback: rewriting an explicit
        remote address to localhost would silently point a production client at
        the wrong database, which is worse than failing.
        """
        candidates = [self._configured_uri]
        for host in ("neo4j", "sentinel-neo4j"):
            local = self._configured_uri.replace(f"//{host}:", "//localhost:")
            if local != self._configured_uri and local not in candidates:
                candidates.append(local)
        return candidates

    async def connect(self):
        if self._driver:
            return

        last_error = None
        for uri in self._candidate_uris():
            driver = None
            try:
                driver = _Neo4j.driver(uri, auth=self._auth)
                await driver.verify_connectivity()
                self._driver = driver
                self._uri = uri
                if uri != self._configured_uri:
                    logger.warning(
                        "Neo4j connected on fallback %s after %s refused a connection. "
                        "This is the out-of-compose path; if the service is meant to "
                        "reach %s, that is the fault to fix.",
                        uri, self._configured_uri, self._configured_uri,
                    )
                else:
                    logger.info("Neo4j connected (%s)", uri)
                return
            except Exception as e:
                last_error = e
                logger.warning("Neo4j unreachable at %s: %s", uri, e)
                if driver is not None:
                    try:
                        await driver.close()
                    except Exception as _exc:
                        swallowed("db.connect", _exc, logger)

        # Raised, not swallowed. The caller decides whether the graph is
        # optional; this class must not pretend it connected.
        raise ConnectionError(
            f"No reachable Neo4j among {self._candidate_uris()}: {last_error}"
        )

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

def _is_only_comments(statement: str) -> bool:
    """True when a fragment carries no executable SQL.

    Splitting a commented script leaves trailing fragments that are nothing but
    a comment; sending one to Postgres is a syntax error.
    """
    body = re.sub(r"/\*.*?\*/", "", statement, flags=re.S)
    body = re.sub(r"--[^\n]*", "", body)
    return not body.strip()


class TimescaleClient:

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
    async def _connect(self, retries: int = 12):
        dsn = resolve_env_var(
            "DATABASE_URL",
            "postgresql://sentinel:sentinel_local_dev@localhost:5432/sentinel",
        )
        if "@timescaledb:" in dsn and not os.path.exists("/.dockerenv"):
            try:
                import socket
                socket.gethostbyname("timescaledb")
            except socket.gaierror:
                dsn = dsn.replace("@timescaledb:", "@localhost:")
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
                    # Sized against the server's budget, not against ambition.
                    #
                    # This was min_size=2, max_size=40 -- in every service. The
                    # server runs max_connections=60, so a single service could
                    # claim two thirds of the whole budget and roughly a dozen
                    # of them share it. Observed: "FATAL: sorry, too many
                    # clients already", which locks out not just the next
                    # service but psql, the migrator and anything trying to
                    # diagnose the problem.
                    #
                    # 60 connections across ~12 database-using services leaves
                    # 5 each with a little headroom for the migrator and an
                    # operator's session. A service that genuinely needs more
                    # can be given it explicitly; the default must not be able
                    # to starve its neighbours.
                    min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
                    max_size=int(os.getenv("DB_POOL_MAX_SIZE", "5")),
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
        if len(params) == 1 and isinstance(params[0], tuple):
            params = params[0]
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [self._sanitize_row(r) for r in rows]

    async def query_one(self, sql: str, *params) -> Optional[Dict]:
        if len(params) == 1 and isinstance(params[0], tuple):
            params = params[0]
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, *params)
            return self._sanitize_row(row) if row else None
    
    async def execute(self, sql: str, *params):
        if len(params) == 1 and isinstance(params[0], tuple):
            params = params[0]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(sql, *params)

    @staticmethod
    def _split_statements(sql: str) -> List[str]:
        """A script into its statements, respecting quotes and dollar-quoting.

        Naive splitting on ";" breaks on a semicolon inside a string literal or
        a $$-quoted function body, and this runs DDL against a live database --
        so it tracks what it is inside of rather than assuming.
        """
        statements: List[str] = []
        buf: List[str] = []
        i = 0
        n = len(sql)
        in_single = in_double = in_line_comment = in_block_comment = False
        dollar_tag = None

        while i < n:
            ch = sql[i]
            two = sql[i:i + 2]

            if in_line_comment:
                buf.append(ch)
                if ch == "\n":
                    in_line_comment = False
                i += 1
                continue
            if in_block_comment:
                buf.append(ch)
                if two == "*/":
                    buf.append(sql[i + 1])
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            if dollar_tag:
                buf.append(ch)
                if sql.startswith(dollar_tag, i):
                    buf.extend(sql[i + 1:i + len(dollar_tag)])
                    i += len(dollar_tag)
                    dollar_tag = None
                    continue
                i += 1
                continue
            if in_single:
                buf.append(ch)
                if ch == "'":
                    in_single = False
                i += 1
                continue
            if in_double:
                buf.append(ch)
                if ch == '"':
                    in_double = False
                i += 1
                continue

            if two == "--":
                in_line_comment = True
                buf.append(ch)
                i += 1
                continue
            if two == "/*":
                in_block_comment = True
                buf.append(ch)
                i += 1
                continue
            if ch == "'":
                in_single = True
                buf.append(ch)
                i += 1
                continue
            if ch == '"':
                in_double = True
                buf.append(ch)
                i += 1
                continue
            if ch == "$":
                end = sql.find("$", i + 1)
                if end != -1 and sql[i + 1:end].replace("_", "").isalnum() or (end == i + 1):
                    dollar_tag = sql[i:end + 1]
                    buf.append(sql[i:end + 1])
                    i = end + 1
                    continue
            if ch == ";":
                statements.append("".join(buf))
                buf = []
                i += 1
                continue

            buf.append(ch)
            i += 1

        statements.append("".join(buf))
        return [s for s in (stmt.strip() for stmt in statements) if s and not _is_only_comments(s)]

    async def execute_without_transaction(self, sql: str, *params, timeout: float = None):
        """Executes each statement on its own, outside any transaction block.

        Required by TimescaleDB's policy and refresh functions, which refuse to
        run inside one. This sent the whole script in a single call, and asyncpg
        sends a multi-statement script over the simple query protocol -- which
        Postgres wraps in one implicit transaction, so the method did the exact
        thing its name says it does not. Migration 0026 is what found it:
        `refresh_continuous_aggregate() cannot run inside a transaction block`.

        Statements are sent one at a time. A single statement is its own
        implicit transaction, which is not a transaction *block*, and that is
        the distinction these functions test for.
        """
        if len(params) == 1 and isinstance(params[0], tuple):
            params = params[0]
        statements = self._split_statements(sql) if not params else [sql]
        async with self._pool.acquire() as conn:
            for statement in statements:
                # `timeout` overrides the pool's command_timeout for this
                # statement alone. The pool is built with 60 seconds, which
                # suits the request path and not DDL: a GIN index across this
                # hypertable's 24 chunks takes about 140 seconds, and without
                # an override the client cancels its own migration and the
                # migrator exits non-zero having changed nothing.
                if timeout is None:
                    await conn.execute(statement, *params)
                else:
                    await conn.execute(statement, *params, timeout=timeout)

    async def execute_many(self, sql: str, rows: List[tuple]):
        """High-performance batch execution."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)


# ── SINGLETONS ────────────────────────────────────────────────────────────────


_timescale: Optional[TimescaleClient] = None
_async_redis: Optional[RedisClient] = None
_neo4j: Optional[Neo4jClient] = None
_timescale_lock = _asyncio.Lock()
_redis_lock = _asyncio.Lock()
_neo4j_lock = _asyncio.Lock()


async def get_timescale() -> TimescaleClient:
    global _timescale
    if _timescale is not None:
        return _timescale
    async with _timescale_lock:
        if _timescale is None:
            _timescale = TimescaleClient()
            await _timescale._connect()
    return _timescale


async def get_redis() -> RedisClient:
    """Async singleton for Redis with lock guard against concurrent initialization."""
    global _async_redis
    if _async_redis is not None:
        return _async_redis
    async with _redis_lock:
        if _async_redis is None:
            _async_redis = RedisClient()
    return _async_redis


async def get_neo4j() -> Neo4jClient:
    """Asynchronous Neo4j client. Should only be used by the GraphSupervisor."""
    global _neo4j
    if _neo4j is not None:
        return _neo4j
    async with _neo4j_lock:
        if _neo4j is None:
            client = Neo4jClient()
            await client.connect()
            _neo4j = client
    return _neo4j


