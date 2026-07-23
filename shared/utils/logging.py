"""
shared/utils/logging.py

SENTINEL RICH CONSOLE LOGGING TELEMETRY FORMATTER
=================================================
Provides colorized ANSI console logging across all microservice containers.
Formats log headers, highlights high-conviction events, and structures
stdout streams for operational visibility during live trading execution.
"""

import logging
import os
import sys
import time
from typing import Dict, Optional, Any

# ANSI Color Codes
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

# Foreground Colors
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
WHITE = "\033[97m"


class SentinelConsoleFormatter(logging.Formatter):
    """Rich colorized Formatter for Sentinel stdout streams."""

    COLOR_MAP = {
        logging.DEBUG: DIM + CYAN,
        logging.INFO: GREEN,
        logging.WARNING: BOLD + YELLOW,
        logging.ERROR: BOLD + RED,
        logging.CRITICAL: BOLD + MAGENTA + "\033[41m",
    }

    def format(self, record: logging.LogRecord) -> str:
        # Base timestamp & level formatting
        color = self.COLOR_MAP.get(record.levelno, WHITE)
        level_name = record.levelname
        
        # Colorize specific high-value keywords in the message
        msg = str(record.msg)
        if record.args:
            try:
                msg = msg % record.args
            except Exception:
                pass

        # Highlight key action tokens
        if "DISCARD" in msg or "REJECTED" in msg or "BLOCK" in msg:
            msg = f"⛔ {BOLD}{RED}{msg}{RESET}"
        elif "BUY" in msg or "LONG" in msg or "CONVICTION" in msg:
            msg = f"🟢 {BOLD}{GREEN}{msg}{RESET}"
        elif "SELL" in msg or "SHORT" in msg:
            msg = f"🔴 {BOLD}{RED}{msg}{RESET}"
        elif "CIRCUIT BREAKER" in msg or "STRESS" in msg or "WARNING" in msg:
            msg = f"🚨 {BOLD}{YELLOW}{msg}{RESET}"
        elif "VERIFIED" in msg or "PASSED" in msg or "SUCCESS" in msg:
            msg = f"✅ {BOLD}{CYAN}{msg}{RESET}"
        elif "HEARTBEAT" in msg or "INFERENCE" in msg:
            msg = f"⏱️  {CYAN}{msg}{RESET}"
        else:
            msg = f"{color}{msg}{RESET}"

        time_str = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        name_str = f"{BOLD}{BLUE}[{record.name}]{RESET}"

        return f"{DIM}{time_str}{RESET} {name_str} {msg}"


# Noisy third-party loggers to suppress
NOISY_LOGGERS = (
    "aiokafka",
    "aiokafka.consumer",
    "aiokafka.producer",
    "aiokafka.conn",
    "aiokafka.protocol",
    "kafka",
    "kafka.conn",
    "asyncio",
    "httpx",
    "httpcore",
    "urllib3",
    "neo4j",
    "asyncpg",
)


def suppress_noisy_loggers(min_level: int = logging.WARNING):
    """Suppresses noisy third-party dependency loggers at WARNING level or higher."""
    for logger_name in NOISY_LOGGERS:
        lg = logging.getLogger(logger_name)
        lg.setLevel(min_level)
        lg.propagate = False
        if not lg.handlers:
            null_handler = logging.NullHandler()
            lg.addHandler(null_handler)


class ThrottledLogger:
    """
    Wraps a Logger instance to rate-limit log statements by key.
    Ensures repetitive log events only emit at most once per `interval_sec`.
    """

    def __init__(self, logger: logging.Logger, default_interval_sec: float = 10.0):
        self.logger = logger
        self.default_interval_sec = default_interval_sec
        self._last_logged: Dict[str, float] = {}

    def info(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        self.log(logging.INFO, key, msg, *args, interval_sec=interval_sec, **kwargs)

    def warning(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        self.log(logging.WARNING, key, msg, *args, interval_sec=interval_sec, **kwargs)

    def error(self, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        self.log(logging.ERROR, key, msg, *args, interval_sec=interval_sec, **kwargs)

    def log(self, level: int, key: str, msg: str, *args, interval_sec: Optional[float] = None, **kwargs):
        now = time.monotonic()
        interval = interval_sec if interval_sec is not None else self.default_interval_sec
        last = self._last_logged.get(key, 0.0)
        if now - last >= interval:
            self._last_logged[key] = now
            self.logger.log(level, msg, *args, **kwargs)


class BatchLogger:
    """
    Accumulates counters and events across processing loops, periodically
    flushing aggregated throughput statistics rather than per-event messages.
    """

    def __init__(
        self,
        logger: logging.Logger,
        name: str,
        flush_interval_sec: float = 10.0,
        max_items: int = 1000,
    ):
        self.logger = logger
        self.name = name
        self.flush_interval_sec = flush_interval_sec
        self.max_items = max_items
        self._last_flush = time.monotonic()
        self._counts: Dict[str, int] = {}
        self._total_errors = 0

    def add(self, category: str = "default", count: int = 1, is_error: bool = False):
        self._counts[category] = self._counts.get(category, 0) + count
        if is_error:
            self._total_errors += count

        now = time.monotonic()
        if (now - self._last_flush >= self.flush_interval_sec) or (sum(self._counts.values()) >= self.max_items):
            self.flush()

    def flush(self):
        if not self._counts:
            return
        now = time.monotonic()
        elapsed = max(now - self._last_flush, 0.001)
        total = sum(self._counts.values())
        rate = total / elapsed
        details = ", ".join(f"{cat}={cnt}" for cat, cnt in self._counts.items())
        self.logger.info(
            f"📊 BATCH SUMMARY [{self.name}] {total} events in {elapsed:.1f}s ({rate:.1f}/s) | {details} | errors={self._total_errors}"
        )
        self._counts.clear()
        self._last_flush = now


def setup_sentinel_logging(service_name: str, level=logging.INFO):
    """
    Configures root logger with SentinelConsoleFormatter across stdout.
    Automatically suppresses third-party library loggers.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Remove pre-existing handlers
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(SentinelConsoleFormatter())
    root.addHandler(handler)

    suppress_noisy_loggers(logging.WARNING)

    logger = logging.getLogger(service_name)
    logger.info(f"⚡ Sentinel Console Telemetry Initialized for service: {service_name}")
    return logger


def log_service_heartbeat(logger: logging.Logger, service_name: str, processed_count: int, error_count: int, rate: float, uptime: float):
    """Utility to emit standardized service heartbeat telemetry."""
    logger.info(f"⏱ HEARTBEAT | service={service_name} | processed={processed_count} | errors={error_count} | rate={rate:.1f}/s | uptime={uptime:.0f}s")

