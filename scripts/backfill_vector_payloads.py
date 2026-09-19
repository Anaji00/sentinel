"""Give the vector index back the subject names it was written without.

`store_event` writes `entity_name`, `entity_id`, `headline`, `summary` and a
canonical `domain` onto every point it creates, and did not always. Measured on
the live collection before this ran:

    total points                    500,013
    missing entity_name             456,387   (91.3%)
    written in the last two days     33,184   0 missing

So the writer is correct and the corpus is not, and the corpus is an *input*:
the semantic rule counts distinct subjects among the points it retrieves, and a
point that cannot name its subject counts as none. 2,894 of 12,193 correlations
in seven days described themselves as spanning "0 distinct subject(s)", and
`effective_score = distinct_subjects * centrality` was zero for every one.

The reason given for never repairing it was that the points "cannot be
rewritten because store_event only upserts on ingest". That is not true --
Qdrant's set_payload rewrites a payload in place without touching the vector,
which is what this does. Nothing is re-embedded and no vector is read.

The names come from Postgres, keyed by the `event_id` each payload already
carries, so this invents nothing: a point whose event is gone from the events
table is left exactly as it is.

    python scripts/backfill_vector_payloads.py --dry-run
    python scripts/backfill_vector_payloads.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.vector_index import EVENT_COLLECTION  # noqa: E402

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333").rstrip("/")
DSN = os.environ.get("TIMESCALE_DSN") or os.environ.get("DATABASE_URL")

# Big enough that the round trips are not the cost, small enough that one
# failed batch is cheap to redo.
SCROLL_BATCH = 512
WRITE_BATCH = 256


def _post(path: str, body: Dict[str, Any], timeout: int = 120) -> Dict[str, Any]:
    req = urllib.request.Request(
        QDRANT_URL + path,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def _canonical_domain(raw: Optional[str]) -> Optional[str]:
    """The stored domain, normalised the way every reader already normalises it.

    soft_correlator repairs `.split("_")[0]` pseudo-domains on the way out
    because the points could supposedly not be rewritten. They can, so the
    repair is applied here once instead of on every read forever.
    """
    from services.correlation.soft_correlator import _LEGACY_DOMAIN_ALIASES, _CANONICAL_DOMAINS

    if not raw:
        return None
    d = str(raw).strip().lower()
    if d in _CANONICAL_DOMAINS:
        return None  # already correct; nothing to write
    return _LEGACY_DOMAIN_ALIASES.get(d)


def _repair_domains(dry_run: bool) -> int:
    """Second pass: pseudo-domains on points that already have a name.

    The main pass scrolls `is_empty: entity_name`, so a point written with a
    subject but a `.split("_")[0]` domain was never visited by it. Measured
    after the first full run: 39,944 points still carrying `market`, 1,363
    `equity`, 167 `headline`. The domain is what the cross-domain check
    compares, so a point reading `market` where its neighbour reads `tradfi`
    counts as a second domain and the cluster claims a breadth it does not have.
    """
    from services.correlation.soft_correlator import _LEGACY_DOMAIN_ALIASES

    fixed = 0
    for alias, canonical in sorted(_LEGACY_DOMAIN_ALIASES.items()):
        # The table contains identity entries -- crypto -> crypto, macro ->
        # macro, prediction -> prediction -- which are there so the read-side
        # normaliser can accept an already-canonical value. Rewriting those is
        # 348,000 writes that change nothing.
        if alias == canonical:
            continue
        flt = {"must": [{"key": "domain", "match": {"value": alias}}]}
        n = _post(f"/collections/{EVENT_COLLECTION}/points/count",
                  {"exact": True, "filter": flt})["result"]["count"]
        if not n:
            continue
        print(f"  domain {alias!r} -> {canonical!r}: {n:,} point(s)", flush=True)
        if dry_run:
            fixed += n
            continue
        # set_payload takes a filter directly, so this is one call per alias
        # rather than one per point.
        _post(
            f"/collections/{EVENT_COLLECTION}/points/payload?wait=true",
            {"payload": {"domain": canonical}, "filter": flt},
            timeout=600,
        )
        fixed += n
    return fixed


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument("--limit", type=int, default=0, help="stop after N points (0 = all)")
    ap.add_argument("--domains-only", action="store_true",
                    help="skip the name backfill; only normalise pseudo-domains")
    args = ap.parse_args()

    if args.domains_only:
        print("pseudo-domain pass only", flush=True)
        n = _repair_domains(args.dry_run)
        print(f"\n{'would fix' if args.dry_run else 'fixed'} {n:,} pseudo-domain(s)", flush=True)
        return 0

    import asyncpg

    conn = await asyncpg.connect(DSN)

    total = _post(f"/collections/{EVENT_COLLECTION}/points/count", {"exact": True})["result"]["count"]
    missing = _post(
        f"/collections/{EVENT_COLLECTION}/points/count",
        {"exact": True, "filter": {"must": [{"is_empty": {"key": "entity_name"}}]}},
    )["result"]["count"]
    print(f"collection {EVENT_COLLECTION}: {total:,} points, {missing:,} without entity_name "
          f"({100.0 * missing / max(total, 1):.1f}%)", flush=True)
    if args.dry_run:
        print("dry run: no writes", flush=True)

    scanned = repaired = unmatched = domain_fixed = 0
    offset: Any = None
    started = time.perf_counter()

    while True:
        body: Dict[str, Any] = {
            "limit": SCROLL_BATCH,
            "with_payload": True,
            "with_vector": False,
            "filter": {"must": [{"is_empty": {"key": "entity_name"}}]},
        }
        if offset is not None:
            body["offset"] = offset
        res = _post(f"/collections/{EVENT_COLLECTION}/points/scroll", body)["result"]
        points = res.get("points") or []
        offset = res.get("next_page_offset")
        if not points:
            break

        ids = []
        by_event: Dict[str, Any] = {}
        for p in points:
            payload = p.get("payload") or {}
            eid = payload.get("event_id")
            if not eid:
                continue
            ids.append(eid)
            by_event.setdefault(str(eid), []).append((p["id"], payload))

        scanned += len(points)
        if not ids:
            if args.limit and scanned >= args.limit:
                break
            continue

        rows = await conn.fetch(
            """
            SELECT event_id::text AS event_id,
                   primary_entity_name AS entity_name,
                   primary_entity_id   AS entity_id,
                   headline, summary, source
            FROM events
            WHERE event_id = ANY($1::uuid[])
            """,
            ids,
        )
        found = {r["event_id"]: r for r in rows}

        writes: List[Dict[str, Any]] = []
        for eid, entries in by_event.items():
            row = found.get(eid)
            if row is None:
                unmatched += len(entries)
                continue
            for point_id, payload in entries:
                patch: Dict[str, Any] = {}
                if row["entity_name"]:
                    patch["entity_name"] = row["entity_name"]
                if row["entity_id"]:
                    patch["entity_id"] = row["entity_id"]
                if row["headline"] and not payload.get("headline"):
                    patch["headline"] = row["headline"]
                if row["summary"] and not payload.get("summary"):
                    patch["summary"] = row["summary"]
                if row["source"] and not payload.get("source"):
                    patch["source"] = row["source"]
                fixed_domain = _canonical_domain(payload.get("domain"))
                if fixed_domain:
                    patch["domain"] = fixed_domain
                    domain_fixed += 1
                if patch:
                    writes.append({"id": point_id, "payload": patch})

        if writes and not args.dry_run:
            # One request per batch, not one per point.
            #
            # Every point needs a *different* payload, so they cannot be folded
            # into a single set_payload call -- but Qdrant's batch endpoint takes
            # a list of independent operations, which is the same thing in one
            # round trip. Measured at one call per point: 55 points/s, which is
            # 2.3 hours for the 456,387 that need repairing. The round trip was
            # the entire cost.
            for i in range(0, len(writes), WRITE_BATCH):
                chunk = writes[i:i + WRITE_BATCH]
                _post(
                    f"/collections/{EVENT_COLLECTION}/points/batch?wait=false",
                    {
                        "operations": [
                            {"set_payload": {"payload": w["payload"], "points": [w["id"]]}}
                            for w in chunk
                        ]
                    },
                )
        repaired += len(writes)

        if scanned % (SCROLL_BATCH * 10) == 0:
            rate = scanned / max(time.perf_counter() - started, 1e-6)
            print(f"  scanned {scanned:,}  repaired {repaired:,}  "
                  f"no matching event {unmatched:,}  ({rate:,.0f}/s)", flush=True)

        if args.limit and scanned >= args.limit:
            break
        if offset is None:
            break

    await conn.close()
    elapsed = time.perf_counter() - started
    print(f"\nscanned {scanned:,} point(s) in {elapsed:,.1f}s", flush=True)
    print(f"  payloads repaired      : {repaired:,}", flush=True)
    print(f"  pseudo-domains fixed   : {domain_fixed:,}", flush=True)
    print(f"  no event row to read   : {unmatched:,}", flush=True)
    if args.dry_run:
        print("  (dry run -- nothing was written)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
