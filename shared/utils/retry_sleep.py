"""How long a periodic job waits after a run: its normal interval after success, a short retry after a failure, and the normal interval again once
`cap` consecutive failures have used the retries up. A failed top-up waited the full twelve hours before (short interest, 13 Nov 2026: FINRA served
30 Oct, the table stopped at 15 Oct). Bounded, so a broken source is not hammered. Pure."""
from __future__ import annotations

RETRY_SEC = 900.0
CAP = 4


def next_delay(consecutive_failures: int, normal_sec: float, retry_sec: float = RETRY_SEC, cap: int = CAP) -> float:
    """0 failures: the normal interval. 1..cap: the retry delay. Past the cap: the normal interval."""
    if consecutive_failures <= 0 or consecutive_failures > cap:
        return float(normal_sec)
    return float(min(retry_sec, normal_sec))


def is_connection_error(exc: BaseException) -> bool:
    """True for a failure that a short retry can cure (the connection dropped or was refused), false for a timeout or any other error: a timeout means the host is
    saturated and a retry of the same heavy read makes it worse (CLAUDE.md "don't retry overload"; review L4)."""
    if isinstance(exc, TimeoutError):
        return False
    return isinstance(exc, (ConnectionError, OSError)) or type(exc).__name__ in {"PostgresConnectionError", "InterfaceError", "ConnectionDoesNotExistError", "CannotConnectNowError"}


__all__ = ["CAP", "RETRY_SEC", "is_connection_error", "next_delay"]
