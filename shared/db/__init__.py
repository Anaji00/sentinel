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
import time
from typing import Optional, Dict, List, Any

import psycopg2  # The PostgreSQL database driver (the "phone line" to the DB)
from psycopg2.extras import RealDictCursor, execute_values  # Helpers for dictionary rows and fast inserts
from psycopg2 import pool as pgpool  # Manages the "Connection Pool" (explained below)

import redis as _redis  # The official Python client for Redis
from neo4j import GraphDatabase as _Neo4j  # The official Python driver for Neo4j

logger = logging.getLogger(__name__)


# ── TIMESCALEDB ───────────────────────────────────────────────────────────────

class TimescaleClient:
    # CONCEPT: The "Archive" & "Ledger".
    # This connects to PostgreSQL optimized with TimescaleDB.
    # ROLE: Stores the hard facts (Events, Vessel Positions).
    # WHY TIMESCALE? Standard Postgres gets slow after 100M rows of time-data.
    # Timescale chops data into "chunks" (by day/week) so it stays fast even with billions of rows.

    def __init__(self):
        self._pool = None
        # On initialization, we immediately try to establish the connection pool.
        self._connect()

    def _connect(self, retries: int = 12):
        # LOOP: Try to connect 'retries' times (default 12).
        # If the database is restarting, we don't want to crash immediately.
        for attempt in range(retries):
            try:
                # CONNECTION POOLING:
                # Opening a connection to a DB is "expensive" (takes time, like dialing a phone).
                # If we dialed, spoke for 1 second, and hung up for every single query, the app would be slow.
                # A POOL is like a taxi rank with 2-20 taxis waiting with engines running.
                # We "borrow" a connection, use it, and give it back to the pool immediately.
                # ThreadedConnectionPool is thread-safe, allowing multiple web requests to run in parallel.
                self._pool = pgpool.ThreadedConnectionPool(
                    minconn=2, maxconn=20,
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", 5432)),
                    dbname=os.getenv("POSTGRES_DB", "sentinel"),
                    user=os.getenv("POSTGRES_USER", "sentinel"),
                    password=os.getenv("POSTGRES_PASSWORD", "sentinel_local_dev"),
                )
                logger.info("TimescaleDB connected")
                return
            except Exception as e:
                # EXPONENTIAL BACKOFF: Wait 1s, then 2s, 4s... up to 30s. Don't hammer a dead DB.
                # If the DB is down, spamming it with requests every millisecond makes it harder to recover.
                wait = min(2 ** attempt, 30)
                logger.warning(f"TimescaleDB attempt {attempt+1}/{retries} — retry in {wait}s: {e}")
                if attempt < retries - 1:
                    time.sleep(wait)
                else:
                    raise

    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        # READ OPERATION (SELECT)
        # 1. Borrow a connection from the pool. This blocks if all 20 connections are busy.
        conn = self._pool.getconn()
        try:
            # RealDictCursor:
            # Standard SQL drivers return tuples: ('Vessel A', 10.5).
            # This forces Python Dicts: {'name': 'Vessel A', 'speed': 10.5}.
            # It's much easier to read and prevents bugs where you mix up column order.
            # The 'with' block ensures the cursor is closed even if the code crashes inside.
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params) # Send the SQL to the server
                return [dict(r) for r in cur.fetchall()] # Convert all results to a list of dicts
        except Exception:
            # TRANSACTION SAFETY (Rollback):
            # If a query fails (e.g., syntax error), the connection gets "confused" (Aborted state).
            # If we simply put it back in the pool, the NEXT person to use it will instantly fail.
            # conn.rollback() is the "Undo" button. It resets the connection to a clean state.
            conn.rollback()
            raise # We re-raise the error so the calling function knows something went wrong.
        finally:
            # CRITICAL: Always give the connection back to the pool.
            # If we forget this, the pool eventually runs out of connections (Leak) and the app freezes.
            self._pool.putconn(conn)

    def query_one(self, sql: str, params: tuple = ()) -> Optional[Dict]:
        # Helper function: Just return the first result (or None if empty).
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: tuple = ()):
        # WRITE OPERATION (INSERT, UPDATE, DELETE)
        # Similar to query(), but we must COMMIT (Save) the transaction.
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            # COMMIT: The "Save Game" button.
            # Until we run this, the data is only temporary. If the power goes out, it's lost.
            conn.commit()
        except Exception:
            # If any part fails, Rollback (Undo) everything since the last commit.
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def execute_many(self, sql: str, rows: List[tuple]):
        # BATCH INSERT:
        # Imagine you have 1,000 letters to mail.
        # BAD:  Drive to the post office 1,000 times (1 letter per trip).
        # GOOD: Put all 1,000 letters in one bag and drive once.
        # execute_values packs thousands of rows into ONE SQL statement.
        # This is 100x faster for bulk data (like vessel positions).
        conn = self._pool.getconn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows)
            conn.commit() # Save the whole batch at once.
        except Exception:
            conn.rollback() # If even one row fails, undo the whole batch (Atomic).
            raise
        finally:
            self._pool.putconn(conn)


# ── NEO4J ─────────────────────────────────────────────────────────────────────

class Neo4jClient:
    # CONCEPT: The "Detective's Wall".
    # ROLE: Stores relationships. Nodes (dots) and Edges (lines).
    # Example: (Vessel)-[OWNS]->(Company)-[SANCTIONED]->(Country).
    # WHY NEO4J? SQL is terrible at "friend of a friend of a friend" queries (too many JOINs).

    def __init__(self):
        # The driver manages its own connection pool internally, so we don't need a separate Pool object.
        self._driver = None
        self._connect()

    def _connect(self, retries: int = 12):
        uri      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
        user     = os.getenv("NEO4J_USER",     "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "sentinel_graph")
        # Retry logic same as TimescaleClient.
        for attempt in range(retries):
            try:
                self._driver = _Neo4j.driver(uri, auth=(user, password))
                self._driver.verify_connectivity()
                logger.info("Neo4j connected")
                return
            except Exception as e:
                wait = min(2 ** attempt, 30)
                logger.warning(f"Neo4j attempt {attempt+1}/{retries} — retry in {wait}s: {e}")
                if attempt < retries - 1:
                    time.sleep(wait)
                else:
                    raise

    def query(self, cypher: str, params: Dict = None) -> List[Dict]:
        # CYPHER: The "SQL" language for Graphs.
        # Example: MATCH (v:Vessel)-[:NEAR]->(p:Port) RETURN v.name
        # Session: A lightweight container for the transaction.
        with self._driver.session() as s:
            # run() executes the Cypher. data() converts the result to a Python Dictionary.
            return [r.data() for r in s.run(cypher, **(params or {}))]

    def query_one(self, cypher: str, params: Dict = None) -> Optional[Dict]:
        rows = self.query(cypher, params)
        return rows[0] if rows else None

    def execute(self, cypher: str, params: Dict = None):
        # Used for creating/updating nodes (WRITE operations).
        with self._driver.session() as s:
            s.run(cypher, **(params or {}))

    def close(self):
        if self._driver:
            self._driver.close()


# ── REDIS ─────────────────────────────────────────────────────────────────────

class RedisClient:
    # CONCEPT: The "Scratchpad" (RAM).
    # ROLE: Caching, Deduplication, and fast checks.
    # WHY REDIS? It stores data in RAM (Memory), not on Hard Disk.
    # Access time is in microseconds (0.000001s). SQL is milliseconds (0.001s).
    # We use it to check "Did we just alert on this vessel?" instantly.

    def __init__(self):
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        # decode_responses=True means we get Strings back, not raw Bytes.
        self._client = _redis.from_url(url, decode_responses=True)
        self._client.ping()
        logger.info("Redis connected")

    def get(self, key: str) -> Optional[str]:
        # Retrieve a value. Returns None if the key doesn't exist.
        return self._client.get(key)

    def set(self, key: str, value: str, ttl: int = None):
        if ttl:
            # TTL (Time To Live):
            # "Remember this for X seconds, then automatically self-destruct."
            # Useful for: "Don't alert on this vessel again for 1 hour."
            self._client.setex(key, ttl, value)
        else:
            self._client.set(key, value)

    def delete(self, key: str):
        self._client.delete(key)

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(key))

    def sadd(self, key: str, *values):
        # SET ADD: Adds items to a list where duplicates are automatically ignored.
        self._client.sadd(key, *values)

    def sismember(self, key: str, value: str) -> bool:
        # SET IS MEMBER: Efficiently checks "Is X in the list?"
        return bool(self._client.sismember(key, value))

    def publish(self, channel: str, message: str):
        # PUB/SUB: Broadcasts a message to anyone listening on 'channel'.
        self._client.publish(channel, message)

    def incr(self, key: str) -> int:
        return self._client.incr(key)

    @property
    def raw(self):
        return self._client


# ── SINGLETONS ────────────────────────────────────────────────────────────────

# CONCEPT: The "Singleton Pattern".
# Problem: Creating a new DB connection takes time. If every function created its
# own TimescaleClient(), we'd have thousands of connections and crash the server.
# Solution: We create ONE global instance. Everyone shares it.
# The `get_timescale()` function checks: "Do we have one? If yes, use it. If no, make one."

_timescale: Optional[TimescaleClient] = None
_neo4j:     Optional[Neo4jClient]     = None
_redis_cli: Optional[RedisClient]     = None


def get_timescale() -> TimescaleClient:
    global _timescale
    if _timescale is None:
        # Only happens the very first time the app calls this.
        _timescale = TimescaleClient()
    return _timescale


def get_neo4j() -> Neo4jClient:
    global _neo4j
    if _neo4j is None:
        _neo4j = Neo4jClient()
    return _neo4j


def get_redis() -> RedisClient:
    global _redis_cli
    if _redis_cli is None:
        _redis_cli = RedisClient()
    return _redis_cli
