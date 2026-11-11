"""Shared plumbing for the stress rounds: a ledger of findings and a seeded generator.

A round does not assert one thing. It generates many cases from a seed, runs them
through real code, and records what happened as findings. A finding is a DEFECT when
the code broke a property that must always hold, a NOTE when the behaviour is
surprising but defensible, and a PASS when a whole family of generated cases held.
The ledger is what turns hundreds of cases into a report a person can read.
"""
from __future__ import annotations

import math
import random
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

PASS, NOTE, DEFECT = "PASS", "NOTE", "DEFECT"


@dataclass
class Finding:
    layer: str
    family: str
    verdict: str
    checked: int
    summary: str
    examples: List[str] = field(default_factory=list)


class Ledger:
    """Collects findings for one round, with per-family case counts."""

    def __init__(self, layer: str, seed: int) -> None:
        self.layer, self.seed = layer, seed
        self.findings: List[Finding] = []

    def family(self, name: str, cases: Iterable[Any], check: Callable[[Any], Optional[str]],
               describe: Callable[[Any], str] = repr, note: bool = False, keep: int = 3) -> Finding:
        """Runs `check` on each case. It returns None when the property held, or a
        reason. An exception inside the code under test is itself a failed property."""
        bad: List[str] = []
        n = 0
        for case in cases:
            n += 1
            try:
                reason = check(case)
            except Exception as exc:  # the code under test must not raise on any input
                reason = f"raised {type(exc).__name__}: {str(exc)[:90]}"
            if reason:
                bad.append(f"{describe(case)[:110]} -> {reason}")
        verdict = PASS if not bad else (NOTE if note else DEFECT)
        summary = (f"held on all {n} generated cases" if not bad
                   else f"failed on {len(bad)} of {n} generated cases")
        f = Finding(self.layer, name, verdict, n, summary, bad[:keep])
        self.findings.append(f)
        return f

    def record(self, name: str, verdict: str, summary: str, checked: int = 1, examples: Optional[List[str]] = None) -> Finding:
        f = Finding(self.layer, name, verdict, checked, summary, list(examples or []))
        self.findings.append(f)
        return f

    def tally(self) -> Dict[str, int]:
        out = {PASS: 0, NOTE: 0, DEFECT: 0}
        for f in self.findings:
            out[f.verdict] += 1
        return out

    def cases(self) -> int:
        return sum(f.checked for f in self.findings)


def rng(seed: int, salt: str = "") -> random.Random:
    return random.Random(f"{seed}:{salt}")


def nasty_floats(r: random.Random, n: int) -> List[float]:
    """Numbers real feeds have produced: zero, negatives, NaN, infinities, tiny, huge."""
    base = [0.0, -0.0, -1.0, 1e-9, 1e12, float("nan"), float("inf"), -float("inf"), 0.01, 100.0, 123456789.123]
    return base + [r.choice([1, -1]) * 10 ** r.uniform(-6, 9) for _ in range(n)]


def safe_repr(x: Any) -> str:
    try:
        return repr(x)
    except Exception:
        return f"<{type(x).__name__}>"


def finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


def trace_tail(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(type(exc), exc)).strip()[:120]


def upgrade_redis_double() -> None:
    """Gives the repository's in-memory Redis the commands the damping paths use.

    The scenario harness implements only the sorted-set commands EventStore issues.
    Rate budgets, recurrence counts and episode locks call get, set, incr and expire,
    and the engine catches the resulting error and carries on, so against the plain
    double those protections silently do nothing. Patched here, for the stress process only.
    """
    from tests.integration import market_scenarios as ms

    if getattr(ms._Raw, "_stress_patched", False):
        return

    def kv(self):
        return self._redis.__dict__.setdefault("_kv", {})

    async def get(self, key):
        return kv(self).get(key)

    async def set(self, key, value, ex=None, nx=False, **kw):
        store = kv(self)
        if nx and key in store:
            return None
        store[key] = value
        return True

    async def incr(self, key):
        store = kv(self)
        store[key] = int(store.get(key, 0)) + 1
        return store[key]

    async def expire(self, key, seconds):
        return True

    ms._Raw.get, ms._Raw.set, ms._Raw.incr, ms._Raw.expire = get, set, incr, expire

    def p_incr(self, key):
        self._queued.append(("incr", key, None, None))
        return self

    def p_expire(self, key, seconds):
        self._queued.append(("expire", key, seconds, None))
        return self

    async def execute(self):
        out = []
        for cmd, key, a, b in self._queued:
            if cmd == "zremrangebyscore":
                out.append(self._redis._zremrangebyscore(key, a, b))
            elif cmd == "incr":
                store = self._redis.__dict__.setdefault("_kv", {})
                store[key] = int(store.get(key, 0)) + 1
                out.append(store[key])
            else:
                out.append(True)
        self._queued.clear()
        return out

    ms._Pipeline.incr, ms._Pipeline.expire, ms._Pipeline.execute = p_incr, p_expire, execute
    ms._Raw._stress_patched = True
