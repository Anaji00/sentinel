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


def setup_sentinel_logging(service_name: str, level=logging.INFO):
    """
    Configures root logger with SentinelConsoleFormatter across stdout.
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

    logger = logging.getLogger(service_name)
    logger.info(f"⚡ Sentinel Console Telemetry Initialized for service: {service_name}")
    return logger


def log_service_heartbeat(logger: logging.Logger, service_name: str, processed_count: int, error_count: int, rate: float, uptime: float):
    """Utility to emit standardized service heartbeat telemetry."""
    logger.info(f"⏱ HEARTBEAT | service={service_name} | processed={processed_count} | errors={error_count} | rate={rate:.1f}/s | uptime={uptime:.0f}s")
