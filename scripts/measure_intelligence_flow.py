"""What fraction of what the platform collects reaches a model.

Measured end to end on 2026-09-21, twice, over 16.4 and 28.8 minutes:

    events written        1,484/min      1,377/min
    enriched.events         313            294       ~21%
    sentinel.correlations   3.2            3.3       ~1.0% of enriched
    inference started       0.3            0.3
    inference completed     0.3            0.3       100% completion

One inference per roughly 4,500-5,000 events. That ratio is not wrong on its
face -- most events should not reach a model -- but nothing in the platform
stated it, so a healthy filter and a broken stage looked identical at every
step. Two of this audit's findings were stages sitting at zero for days
underneath a number nobody was dividing.

Run it against the live stack:

    python scripts/measure_intelligence_flow.py --seconds 900

Topic names come from `shared.kafka.Topics`, never typed. Three hand-typed
names during this audit read as "zero messages" on topics carrying millions,
which is indistinguishable from a feed that has genuinely stopped.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.kafka import Topics  # noqa: E402

# Not message topics: one is a list, the other travels through Redis.
_NOT_TOPICS = {"ALL_RAW", "SYSTEM_HEARTBEAT"}

# The stages worth naming in the summary, in the order signal moves through
# them. Anything else measured is still printed, just not as a conversion.
_FUNNEL = [
    ("collected", "PG:events"),
    ("enriched", Topics.ENRICHED_EVENTS),
    ("correlated", Topics.CORRELATIONS),
    ("inference started", "PG:tel_think"),
    ("inference done", "PG:tel_done"),
]


def _sh(args: list) -> str:
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=180)
        return out.stdout
    except (subprocess.SubprocessError, OSError):
        return ""


def _topics() -> Dict[str, str]:
    return {
        name: getattr(Topics, name)
        for name in dir(Topics)
        if name.isupper() and name not in _NOT_TOPICS
        and isinstance(getattr(Topics, name), str)
    }


def _kafka_offsets(container: str) -> Dict[str, int]:
    """End offset per topic, summed across partitions.

    One call for every topic, not one call per topic. The per-topic loop this
    replaces took 221 seconds and returned nothing at all -- a tool written to
    make a conversion ratio visible, which silently reported no Kafka stage.
    `GetOffsetShell` with no `--topic` lists every partition in 4.6 seconds.
    """
    out = _sh(["docker", "exec", container,
               "kafka-run-class", "kafka.tools.GetOffsetShell",
               "--broker-list", "localhost:9092"])
    wanted = set(_topics().values())
    counts: Dict[str, int] = {}
    for line in out.splitlines():
        # topic:partition:offset -- a topic name may not contain a colon.
        parts = line.strip().rsplit(":", 2)
        if len(parts) != 3:
            continue
        topic, _partition, offset = parts
        if topic not in wanted or not offset.lstrip("-").isdigit():
            continue
        counts[topic] = counts.get(topic, 0) + int(offset)
    return counts


_PG_QUERY = """
SELECT 'events', count(*) FROM events
UNION ALL SELECT 'tel_think', count(*) FROM agent_telemetry WHERE status='THINKING'
UNION ALL SELECT 'tel_done', count(*) FROM agent_telemetry WHERE status='COMPLETE'
UNION ALL SELECT 'tel_event', count(*) FROM agent_telemetry WHERE status LIKE 'EVENT:%'
UNION ALL SELECT 'predictions', count(*) FROM agent_predictions
UNION ALL SELECT 'pred_resolved', count(*) FROM agent_predictions WHERE resolved_at IS NOT NULL;
"""


def _pg_counts(container: str, user: str, db: str) -> Dict[str, int]:
    out = _sh(["docker", "exec", container, "psql", "-U", user, "-d", db,
               "-t", "-A", "-F", " ", "-c", _PG_QUERY])
    counts = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].lstrip("-").isdigit():
            counts["PG:" + parts[0]] = int(parts[1])
    return counts


def snapshot(args) -> Dict[str, int]:
    snap = {"_ts": int(time.time())}
    snap.update(_kafka_offsets(args.kafka))
    snap.update(_pg_counts(args.timescale, args.pg_user, args.pg_db))
    return snap


def report(a: Dict[str, int], b: Dict[str, int]) -> None:
    minutes = (b["_ts"] - a["_ts"]) / 60.0
    if minutes <= 0:
        print("the two snapshots are not apart in time")
        return

    rates = {}
    for key in sorted(set(a) & set(b)):
        if key == "_ts":
            continue
        rates[key] = (b[key] - a[key]) / minutes

    print(f"\nwindow: {minutes:.1f} minutes\n")
    print(f"{'STAGE':26} {'PER MIN':>10}   {'OF PREVIOUS':>12}")
    previous = None
    for label, key in _FUNNEL:
        rate = rates.get(key)
        if rate is None:
            print(f"{label:26} {'not measured':>10}")
            continue
        share = ""
        if previous:
            share = f"{100.0 * rate / previous:11.2f}%"
        elif previous == 0:
            share = "  (prev zero)"
        print(f"{label:26} {rate:10.1f}   {share:>12}")
        previous = rate

    first = rates.get(_FUNNEL[0][1], 0.0)
    last = rates.get(_FUNNEL[-1][1], 0.0)
    if last > 0:
        print(f"\none inference per {first / last:,.0f} collected events")
    else:
        # Zero is a finding, not a divide-by-zero. It is how this audit found
        # four agents doing no work at all.
        print("\nNO INFERENCE COMPLETED IN THIS WINDOW")

    silent = [k for k, v in rates.items() if v == 0 and not k.startswith("PG:")]
    if silent:
        print("\nno movement in the window:")
        for key in sorted(silent):
            print(f"  {key}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--seconds", type=int, default=900,
                   help="how long to measure for (default 900)")
    p.add_argument("--kafka", default="sentinel-kafka")
    p.add_argument("--timescale", default="sentinel-timescaledb")
    p.add_argument("--pg-user", default="sentinel")
    p.add_argument("--pg-db", default="sentinel")
    p.add_argument("--json", action="store_true", help="emit both snapshots")
    args = p.parse_args()

    print(f"measuring for {args.seconds}s ...", flush=True)
    first = snapshot(args)
    if len(first) <= 1:
        print("nothing readable -- is the stack up?")
        return 1
    time.sleep(args.seconds)
    second = snapshot(args)

    if args.json:
        print(json.dumps({"t0": first, "t1": second}, indent=2))
    report(first, second)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
