"""
shared/utils/api_meter.py

Every outbound HTTP request, counted per service, external host and status.

Only LLM calls were metered (sentinel:llm_usage), so "how close are we to a
free plan's limit" had no measured answer for Alpaca, Finnhub, SEC or Yahoo.
Thirty services call out through aiohttp and urllib from hundreds of sites, so
the count is taken once at the client, installed from shared/__init__.py.

The host, and a route masked by route_of. A URL's query and any path segment that
is not a plain lower-case word are never kept, since those can carry API keys,
tokens and tickers. Metering never fails or slows a call: a counter goes up in
memory and a daemon thread flushes to Redis once a minute, with its own connection.
"""
from __future__ import annotations

import logging
import os
import re
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlsplit

logger = logging.getLogger("shared.api_meter")

DAY_KEY = "sentinel:api_meter:day:{day}"          # hash "service|host|status" -> calls
HOUR_KEY = "sentinel:api_meter:hour:{hour}"       # the same, by UTC hour
ROUTE_KEY = "sentinel:api_meter:route:{hour}"     # hash "service|host|route" -> calls, by UTC hour
DAY_TTL_SEC = 120 * 86400
HOUR_TTL_SEC = 21 * 86400
FLUSH_SEC = 60.0

_INTERNAL = {"localhost", "host.docker.internal"}
_ROUTE_WORD = re.compile(r"^[a-z][a-z_-]{0,24}$")
_ROUTE_VERSION = re.compile(r"^v\d+(beta\d+)?$")
_counts: Counter = Counter()
_routes: Counter = Counter()
_lock = threading.Lock()
_installed = False
_flusher_started = False


def host_of(url: Any) -> str:
    """The lower-case host of a URL or yarl.URL, or "" when it has none. Pure."""
    try:
        return (urlsplit(str(url)).hostname or "").lower()
    except ValueError:
        return ""


def route_of(url: Any) -> str:
    """The path with every segment that is not a plain lower-case word or an API
    version masked as ":x", at most four segments. The query is never read. Pure."""
    try:
        parts = [p for p in urlsplit(str(url)).path.split("/") if p][:4]
    except ValueError:
        return ""
    return "/" + "/".join(p if _ROUTE_WORD.match(p) or _ROUTE_VERSION.match(p) else ":x" for p in parts)


def is_external(host: str) -> bool:
    """A public host: compose service names have no dot, and private addresses
    are the platform talking to itself. Pure."""
    if not host or "." not in host or host in _INTERNAL:
        return False
    return not re.match(r"^(127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)", host)


def status_class(status: Optional[int]) -> str:
    """"2xx", "429" (kept apart: it is the limit itself), "4xx", "5xx", or "error". Pure."""
    if status is None:
        return "error"
    return "429" if status == 429 else f"{status // 100}xx"


def service_name(argv0: str) -> str:
    """Which service this process is, from its script path. Pure.

    services/collector-news/main.py -> collector-news
    /app/services/agents/factor_engine.py -> agents/factor_engine"""
    parts = argv0.replace("\\", "/").split("/")
    if "services" not in parts:
        return (parts[-1] or "unknown").removesuffix(".py")
    rest = parts[parts.index("services") + 1:]
    if rest and rest[-1] == "main.py":
        rest = rest[:-1]
    return "/".join(rest).removesuffix(".py") or "unknown"


def _record(url: Any, status: Optional[int]) -> None:
    host = host_of(url)
    if not is_external(host):
        return
    service = service_name(sys.argv[0] if sys.argv else "")
    field = f"{service}|{host}|{status_class(status)}"
    route = f"{service}|{host}|{route_of(url)}"
    with _lock:
        _counts[field] += 1
        _routes[route] += 1
    _start_flusher()


def _flush(client: Any) -> None:
    with _lock:
        batch = dict(_counts)
        routes = dict(_routes)
        _counts.clear()
        _routes.clear()
    if not batch:
        return
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H")
    day, hour, route_key = DAY_KEY.format(day=now.date().isoformat()), HOUR_KEY.format(hour=stamp), ROUTE_KEY.format(hour=stamp)
    try:
        pipe = client.pipeline()
        for field, n in batch.items():
            pipe.hincrby(day, field, n)
            pipe.hincrby(hour, field, n)
        for field, n in routes.items():
            pipe.hincrby(route_key, field, n)
        pipe.expire(day, DAY_TTL_SEC)
        pipe.expire(hour, HOUR_TTL_SEC)
        pipe.expire(route_key, HOUR_TTL_SEC)
        pipe.execute()
    except Exception:
        with _lock:                 # put back for the next minute; a count is only lost with the process
            _counts.update(batch)
            _routes.update(routes)
        raise


def _flush_forever() -> None:
    import redis as sync_redis

    from shared.utils.env_guard import resolve_env_var
    from shared.utils.quiet_failures import swallowed
    client = None
    while True:
        time.sleep(FLUSH_SEC)
        try:
            if client is None:
                client = sync_redis.Redis.from_url(resolve_env_var("REDIS_URL", "redis://localhost:6379/0"), socket_timeout=5)
            _flush(client)
        except Exception as e:
            client = None
            swallowed("utils.api_meter.flush", e, logger)


def _start_flusher() -> None:
    global _flusher_started
    with _lock:
        if _flusher_started:
            return
        _flusher_started = True
    threading.Thread(target=_flush_forever, name="api-meter", daemon=True).start()


def install() -> None:
    """Wrap the clients. Off under pytest and with API_METER=0; never raises."""
    global _installed
    if _installed or "pytest" in sys.modules or os.getenv("API_METER", "1") == "0":
        return
    _installed = True
    try:
        import aiohttp
        original = aiohttp.ClientSession._request

        async def _request(self: Any, method: str, str_or_url: Any, **kwargs: Any) -> Any:
            try:
                resp = await original(self, method, str_or_url, **kwargs)
            except BaseException:
                _record(str_or_url, None)
                raise
            _record(str_or_url, resp.status)
            return resp

        aiohttp.ClientSession._request = _request  # type: ignore[method-assign]
    except ImportError:
        pass
    import urllib.error
    import urllib.request
    opened = urllib.request.urlopen

    def urlopen(url: Any, *args: Any, **kwargs: Any) -> Any:
        target = url.full_url if isinstance(url, urllib.request.Request) else url
        try:
            resp = opened(url, *args, **kwargs)
        except urllib.error.HTTPError as e:
            _record(target, e.code)
            raise
        except BaseException:
            _record(target, None)
            raise
        _record(target, getattr(resp, "status", None))
        return resp

    urllib.request.urlopen = urlopen  # type: ignore[assignment]


__all__ = ["DAY_KEY", "HOUR_KEY", "ROUTE_KEY", "host_of", "install", "is_external", "route_of", "service_name",
           "status_class"]
