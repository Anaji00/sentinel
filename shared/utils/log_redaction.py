"""
shared/utils/log_redaction.py

Credential redaction at the logging boundary.

`shared/utils/secrets.py` has carried `mask_secret()` since the platform was
built, and nothing ever called it. Auditing "is masking applied at every
credential log site?" is the wrong question anyway: the answer decays the moment
someone adds a log line, and the dangerous sites are rarely the obvious ones.

The realistic leak is not `logger.info(api_key)` -- nobody writes that. It is an
exception whose text embeds a connection string:

    asyncpg.CannotConnectNowError:
      could not connect to postgresql://sentinel:SuperSecret@timescaledb:5432/sentinel

which reaches disk through an ordinary `logger.error(f"...: {e}")`.

A filter installed on the root handler redacts regardless of which call site
produced the record, including call sites that do not exist yet. Two strategies,
because each catches what the other misses:

  LITERAL   values of known-sensitive environment variables are matched exactly.
            Catches a secret in any format, including one this module has never
            seen a pattern for.

  PATTERN   structural matches -- credentials embedded in URLs, bearer tokens,
            long hex strings. Catches secrets that never passed through an
            environment variable this process read.
"""

import logging
import os
import re
from typing import Any, Iterable, List, Optional, Pattern, Set

REDACTED = "***REDACTED***"

# Which environment variables hold credentials is already decided by
# `shared.utils.secrets.is_sensitive_key`. Reusing it keeps one definition:
# a parallel fragment list here drifted immediately -- it missed
# API_GATEWAY_KEY, because that name does not contain the substring "API_KEY".
from shared.utils.secrets import is_sensitive_key

# Additional names that hold a credential without matching the shared patterns.
EXTRA_SENSITIVE_NAMES = ("DSN", "DATABASE_URL", "REDIS_URL", "NEO4J_URI")

# A value shorter than this is too likely to collide with ordinary text --
# redacting every occurrence of a 4-character password would mangle unrelated
# messages and make logs harder to read than the leak it prevents.
MIN_REDACTABLE_LENGTH = 8

# Structural patterns, applied after literal substitution.
_PATTERNS: List[Pattern[str]] = [
    # scheme://user:password@host  ->  keep the user, drop the password. The
    # username is operationally useful and is not itself a secret.
    re.compile(r"(?P<pre>[a-zA-Z][a-zA-Z0-9+.-]*://[^:/\s@]+:)(?P<secret>[^@/\s]+)(?P<post>@)"),
    # Authorization: Bearer <token>  /  Basic <token>
    re.compile(r"(?P<pre>(?:Bearer|Basic|Token)\s+)(?P<secret>[A-Za-z0-9\-._~+/=]{12,})(?P<post>)", re.I),
    # api_key=..., password: ..., token="..."  in query strings and dict reprs
    re.compile(
        r"(?P<pre>(?:api[_-]?key|apikey|password|passwd|secret|token|auth)"
        r"[\"']?\s*[:=]\s*[\"']?)(?P<secret>[^\s,;&\"'}\)]{6,})(?P<post>)",
        re.I,
    ),
]


def _sensitive_env_values() -> Set[str]:
    """Current values of environment variables that look like credentials.

    Connection URLs are included by name: the variable holds a whole DSN, and
    the password inside it is extracted separately so the host and username --
    which are useful in a log and are not secrets -- survive redaction.
    """
    values: Set[str] = set()
    for name, value in os.environ.items():
        if not value or len(value) < MIN_REDACTABLE_LENGTH:
            continue
        upper = name.upper()
        if is_sensitive_key(name) or any(n in upper for n in EXTRA_SENSITIVE_NAMES):
            values.add(value)
            # Redact the embedded password on its own too, so the same secret is
            # caught when it appears outside the URL that carried it.
            m = re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^:/\s@]*:([^@/\s]+)@", value)
            if m and len(m.group(1)) >= MIN_REDACTABLE_LENGTH:
                values.add(m.group(1))
    return values


def redact(text: str, literals: Optional[Iterable[str]] = None) -> str:
    """Removes credentials from *text*.

    Literal substitution runs first: an exact secret value is redacted wherever
    it appears, even in a shape no pattern anticipates. Structural patterns then
    catch credentials this process never held in its own environment.
    """
    if not text:
        return text

    for secret in (literals if literals is not None else _sensitive_env_values()):
        if secret and len(secret) >= MIN_REDACTABLE_LENGTH and secret in text:
            text = text.replace(secret, REDACTED)

    for pattern in _PATTERNS:
        text = pattern.sub(lambda m: f"{m.group('pre')}{REDACTED}{m.group('post')}", text)

    return text


class RedactingFilter(logging.Filter):
    """Strips credentials from every record passing through a handler.

    Installed on the handler rather than a named logger so it applies to
    third-party libraries too -- asyncpg and aioredis produce exactly the
    connection-string errors this exists to catch, and they log under their own
    logger names.

    Environment values are captured once at construction. Re-reading os.environ
    per record would be correct but costs a dict scan on every log line in a
    system that logs continuously; secrets do not change after startup.
    """

    def __init__(self, name: str = "", extra_secrets: Optional[Iterable[str]] = None):
        super().__init__(name)
        self._literals: Set[str] = _sensitive_env_values()
        if extra_secrets:
            self._literals.update(s for s in extra_secrets if s and len(s) >= MIN_REDACTABLE_LENGTH)

    def add_secret(self, value: str) -> None:
        """Registers an additional literal to redact (e.g. a runtime-issued token)."""
        if value and len(value) >= MIN_REDACTABLE_LENGTH:
            self._literals.add(value)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            # record.msg may be any object; only text is redactable.
            if isinstance(record.msg, str):
                record.msg = redact(record.msg, self._literals)

            # Args are substituted into the message at format time, so a secret
            # passed as an argument would bypass a msg-only redaction.
            if record.args:
                if isinstance(record.args, dict):
                    record.args = {
                        k: (redact(v, self._literals) if isinstance(v, str) else v)
                        for k, v in record.args.items()
                    }
                elif isinstance(record.args, tuple):
                    record.args = tuple(
                        redact(a, self._literals) if isinstance(a, str) else a
                        for a in record.args
                    )

            # exc_info renders lazily via formatException, so the traceback text
            # is not on the record yet. Pre-rendering it here is what closes the
            # connection-string leak, which is the whole point of this filter.
            if record.exc_info and record.exc_info[1] is not None:
                exc = record.exc_info[1]
                original = str(exc)
                cleaned = redact(original, self._literals)
                if cleaned != original:
                    # Replace the exception with a redacted stand-in of the same
                    # class, so downstream formatting and type checks still work.
                    try:
                        record.exc_info = (record.exc_info[0], type(exc)(cleaned), record.exc_info[2])
                    except Exception:
                        # Some exception types reject single-string construction.
                        record.exc_info = (record.exc_info[0], RuntimeError(cleaned), record.exc_info[2])
        except Exception:
            # A failure here must never suppress the log record itself: losing
            # the line is worse than an unredacted one, and re-raising inside a
            # filter can take down the emitting call site.
            pass
        return True


def install_redaction(logger: Optional[logging.Logger] = None,
                      extra_secrets: Optional[Iterable[str]] = None) -> RedactingFilter:
    """Attaches redaction to every handler on *logger* (root by default)."""
    target = logger or logging.getLogger()
    f = RedactingFilter(extra_secrets=extra_secrets)
    for handler in target.handlers:
        if not any(isinstance(existing, RedactingFilter) for existing in handler.filters):
            handler.addFilter(f)
    return f
