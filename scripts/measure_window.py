"""
scripts/measure_window.py

Measures a stretch of running time, and proves it was running time.

Every throughput and latency figure in this deployment's audit was taken by
hand, and three of them were wrong for the same class of reason:

  * A window opened for forty minutes closed twelve hours later. The laptop had
    suspended from 09:52 to 21:23 -- no inference at all -- and a first-and-last
    reading divided real work by twelve hours of wall clock.

  * A comparison of two builds spanned a period when the model server was
    wedged. Nothing could run under either configuration, and the difference was
    attributed to the code.

  * A latency mean was computed over HTTP 500s, because the parser took the
    duration column and ignored the status column. Failed requests are fast.

So this samples periodically rather than at the ends, records a monotonic
timestamp with every sample, and reports active time separately from wall clock.
A gap longer than the sampling interval is a suspend, and it is subtracted
rather than averaged over.

    python scripts/measure_window.py --minutes 30
    python scripts/measure_window.py --minutes 60 --interval 120

Latency comes from Ollama's access log, filtered to 200s: a 500 that returns in
35 seconds is not a fast inference, it is a failure, and averaging the two
answers a question nobody asked.
"""

import argparse
import asyncio
import collections
import os
import re
import subprocess
import sys
import time
from typing import Optional
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

INFERENCE_SERVICES = ("agents-fast", "agents-heavy", "reasoning")

# A gap longer than this between consecutive samples means the host stopped,
# not that the system was quiet.
SUSPEND_FACTOR = 2.0

_DURATION = re.compile(r"\|\s+(\d+m[\d.]+s|[\d.]+m?s|[\d.]+(?:µs|us))\s+\|")
_STATUS = re.compile(r"\|\s+(\d{3})\s+\|")


def _seconds(token: str) -> Optional[float]:
    token = token.strip()
    for pattern, convert in (
        (r"^(\d+)m([\d.]+)s$", lambda m: int(m[1]) * 60 + float(m[2])),
        (r"^([\d.]+)ms$", lambda m: float(m[1]) / 1000),
        (r"^([\d.]+)(?:µs|us)$", lambda m: float(m[1]) / 1e6),
        (r"^([\d.]+)s$", lambda m: float(m[1])),
    ):
        match = re.match(pattern, token)
        if match:
            return convert(match)
    return None


class MeasurementUnavailable(RuntimeError):
    """The instrument could not read, as distinct from reading nothing.

    This module exists because three measurements were wrong for exactly that
    conflation, and the first version of it swallowed every docker failure and
    returned "". A missing CLI, a renamed container or a timeout then printed
    "no successful generates in the window" -- identical to a quiet system.
    """


def _docker(*args: str) -> str:
    try:
        result = subprocess.run(
            ["docker", *args], capture_output=True, text=True, timeout=60,
        )
    except FileNotFoundError as exc:
        raise MeasurementUnavailable("docker CLI not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise MeasurementUnavailable(f"docker {' '.join(args[:2])} timed out") from exc
    if result.returncode != 0:
        raise MeasurementUnavailable(
            f"docker {' '.join(args[:2])} failed: {result.stderr.strip()[:160]}"
        )
    return result.stdout


def _redis_password() -> str:
    password = os.getenv("REDIS_PASSWORD", "")
    if not password:
        env = ROOT / ".env"
        if env.exists():
            for line in env.read_text(encoding="utf-8").splitlines():
                if line.startswith("REDIS_PASSWORD="):
                    password = line.split("=", 1)[1].strip().strip('"')
                    break
    return password


def _redis_raw(script: str) -> str:
    """One shell inside the redis container, however many commands it runs."""
    return _docker(
        "exec", "sentinel-redis", "sh", "-c", script.format(pw=_redis_password())
    ).strip()


def _calls_by_service() -> dict:
    """Per-service inference counts, from the counters the services publish.

    Not from Ollama's access log keyed on container IP: Docker reassigns those
    on restart, so an attribution taken across a deploy silently mixes services
    together. That mistake needed two corrections before it was noticed.

    One `docker exec` for all of them rather than one each. The sampler runs
    against a host whose whole problem is a saturated serial resource, and
    spawning three container-exec startups per sample to read three integers is
    the sampler perturbing what it is measuring.
    """
    keys = " ".join(f"sentinel:metrics:{s}" for s in INFERENCE_SERVICES)
    raw = _redis_raw(
        f"for k in {keys}; do redis-cli -a '{{pw}}' --no-auth-warning "
        f"hget \"$k\" 'c|ollama_calls_total'; done"
    )
    values = [line.strip() for line in raw.splitlines() if line.strip() != ""]
    counts = {}
    for service, value in zip(INFERENCE_SERVICES, values + [""] * len(INFERENCE_SERVICES)):
        try:
            counts[service] = int(value)
        except (TypeError, ValueError):
            counts[service] = 0
    return counts


def _latencies(minutes: int) -> list:
    """Successful generate durations from Ollama's own log."""
    out = _docker("logs", f"--since", f"{minutes + 2}m", "sentinel-ollama")
    values = []
    for line in out.splitlines():
        if "/api/generate" not in line:
            continue
        status = _STATUS.search(line)
        if not status or status.group(1) != "200":
            continue
        duration = _DURATION.search(line)
        if not duration:
            continue
        seconds = _seconds(duration.group(1))
        if seconds:
            values.append(seconds)
    return sorted(values)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--interval", type=int, default=120, help="seconds between samples")
    args = parser.parse_args()

    samples = []
    deadline = time.time() + args.minutes * 60
    while True:  # noqa: RET503 -- broken below once the deadline passes
        samples.append((time.time(), _calls_by_service()))
        if time.time() >= deadline:
            break
        time.sleep(args.interval)

    first_ts, first = samples[0]
    last_ts, last = samples[-1]
    wall = (last_ts - first_ts) / 60

    suspend_threshold = args.interval * SUSPEND_FACTOR
    gaps = [
        (a, b, (b - a) / 60)
        for (a, _), (b, _) in zip(samples, samples[1:])
        if (b - a) > suspend_threshold
    ]
    active = sum(
        (b - a) for (a, _), (b, _) in zip(samples, samples[1:])
        if (b - a) <= suspend_threshold
    ) / 60

    print(f"\n  samples {len(samples)}   wall {wall:.0f} min   active {active:.0f} min")
    for start, end, minutes in gaps:
        print(f"  SUSPENDED {time.strftime('%H:%M', time.gmtime(start))}"
              f" -> {time.strftime('%H:%M', time.gmtime(end))}  ({minutes:.0f} min)")
    if not gaps:
        print("  no suspend gaps -- wall clock is running time")

    total = 0
    print()
    for service in INFERENCE_SERVICES:
        delta = last.get(service, 0) - first.get(service, 0)
        total += delta
        print(f"  {service:14} +{delta}")
    if active > 0:
        print(f"  {'total':14} +{total}  =  {total / (active / 60):.1f}/hr over active time")

    values = _latencies(int(wall))
    if values:
        n = len(values)
        print(f"\n  latency (200s only)  n={n}  mean {sum(values)/n:.1f}s"
              f"  p50 {values[n//2]:.1f}s  p90 {values[int(n*0.9)]:.1f}s")
    else:
        print("\n  latency: no successful generates in the window")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MeasurementUnavailable as exc:
        # Loudly, and with a distinct exit code: a window that could not be
        # measured must not be mistaken for a window in which nothing happened.
        print(f"\n  MEASUREMENT FAILED: {exc}", file=sys.stderr)
        raise SystemExit(2)
