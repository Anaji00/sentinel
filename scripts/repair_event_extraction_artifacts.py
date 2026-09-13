"""
scripts/repair_event_extraction_artifacts.py

Repairs three populations of stored events that carry extraction output the
producing code no longer emits.

Each cause is fixed. Rows written before the fix are not, and they are the rows
every historical query, every backtest and every "how often does this happen"
baseline reads.

  1. 11,800 events flagged `sanctioned_ofac` on a keyword too short to identify
     anyone.

     The OFAC sync used `len(name) > 3` as its only filter, so four-character
     surnames and word fragments entered an unanchored substring matcher:
     "maria", "lily" and "star" all matched vessel names. `is_usable_keyword`
     now holds the line at the sync, and `_matches_on_boundary` holds it at the
     match -- but the stored rows still assert that a container ship is a
     sanctioned party, and `sanctioned_ofac` drives the CRITICAL alert tier.

     Repairable precisely because the flag travels with its cause: every
     flagged event also carries `sanctioned_kw:<keyword>`, so each row can be
     re-judged against the current rule rather than guessed at.

  2. 886 events whose `named_entities` array holds tokens that cannot name an
     entity -- bare numbers, punctuation, prompt scaffolding.

  3. 229 events whose `primary_entity_name` is a mangled list, `AS[1299, 3257,
     ...]`, from a multi-origin BGP announcement whose origin arrived as a list
     and was formatted straight into the identifier. That string matches no
     other event and resolves to no graph node, so each of those events is its
     own singleton entity.

Idempotent: every phase selects on the corruption itself. Batched and bounded,
so it can be interrupted and resumed against a live database.

    python scripts/repair_event_extraction_artifacts.py --dry-run
    python scripts/repair_event_extraction_artifacts.py
"""

import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair_event_extraction_artifacts")

from shared.db import get_timescale
from shared.utils.entity_resolution import is_plausible_entity_name
from shared.utils.sanctions import is_usable_keyword

# `AS[1299, 3257]` and nothing else: anchored on the bracket so a real AS name
# containing a digit is untouched.
MANGLED_AS_RE = re.compile(r"^AS\[")

FLAG_OFAC = "sanctioned_ofac"
KW_PREFIX = "sanctioned_kw:"
FUZZY_PREFIX = "sanctioned_fuzzy:"


# ── 1. Sanctions flags raised by unusable keywords ──────────────────────────

# Keyset pagination, not OFFSET.
#
# Clearing a flag removes the row from this selection, so the result set shrinks
# underneath a paging cursor and an OFFSET walk silently skips exactly as many
# rows as it repaired. Paging on the sort key instead is stable under writes.
SELECT_FLAGGED = """
SELECT event_id, occurred_at, primary_entity_flags, tags
FROM events
WHERE $1 = ANY(primary_entity_flags)
  AND ($3::timestamptz IS NULL OR occurred_at < $3)
ORDER BY occurred_at DESC
LIMIT $2
"""

COUNT_FLAGGED = """
SELECT count(*) AS n FROM events WHERE $1 = ANY(primary_entity_flags)
"""

CLEAR_FLAGS = """
UPDATE events
SET primary_entity_flags = $2, tags = $3
WHERE event_id = $1
"""


def _matched_keywords(flags) -> list:
    """The keywords a row's flags say caused its sanctions hit."""
    out = []
    for flag in flags or []:
        text = str(flag)
        for prefix in (KW_PREFIX, FUZZY_PREFIX):
            if text.startswith(prefix):
                out.append(text[len(prefix):])
    return out


def _repaired_sanctions(flags, tags):
    """New (flags, tags) for a row, or None if the row is already correct.

    A row is cleared only when every keyword that raised it fails the current
    rule. One surviving usable keyword means the flag stands.
    """
    matched = _matched_keywords(flags)
    if not matched:
        # Flagged with no recorded cause. Not repairable and not safely
        # clearable: leave it and report it.
        return None
    if any(is_usable_keyword(kw) for kw in matched):
        return None
    new_flags = [
        f for f in (flags or [])
        if f != FLAG_OFAC
        and not str(f).startswith(KW_PREFIX)
        and not str(f).startswith(FUZZY_PREFIX)
    ]
    new_tags = [t for t in (tags or []) if t not in (FLAG_OFAC, "sanctioned_ofac_mention")]
    return new_flags, new_tags


async def repair_sanctions(db, batch: int, dry_run: bool) -> dict:
    total = (await db.query(COUNT_FLAGGED, FLAG_OFAC))[0]["n"]
    logger.info("Events flagged %s: %s", FLAG_OFAC, total)
    cleared = unexplained = 0
    cursor = None
    while True:
        rows = await db.query(SELECT_FLAGGED, FLAG_OFAC, batch, cursor)
        if not rows:
            break
        for row in rows:
            repaired = _repaired_sanctions(row["primary_entity_flags"], row["tags"])
            if repaired is None:
                if not _matched_keywords(row["primary_entity_flags"]):
                    unexplained += 1
                continue
            cleared += 1
            if not dry_run:
                await db.execute(CLEAR_FLAGS, row["event_id"], repaired[0], repaired[1])
        cursor = rows[-1]["occurred_at"]
        if len(rows) < batch:
            break
    logger.info(
        "Sanctions flags: %s to clear, %s flagged with no recorded keyword (left alone)",
        cleared, unexplained,
    )
    return {"cleared": cleared, "unexplained": unexplained, "total": total}


# ── 2. named_entities tokens that cannot name an entity ─────────────────────

COUNT_WITH_ENTITIES = """
SELECT count(*) AS n FROM events
WHERE named_entities IS NOT NULL AND cardinality(named_entities) > 0
"""

SELECT_WITH_ENTITIES = """
SELECT event_id, occurred_at, named_entities
FROM events
WHERE named_entities IS NOT NULL AND cardinality(named_entities) > 0
  AND ($2::timestamptz IS NULL OR occurred_at < $2)
ORDER BY occurred_at DESC
LIMIT $1
"""

UPDATE_ENTITIES = "UPDATE events SET named_entities = $2 WHERE event_id = $1"


async def repair_named_entities(db, batch: int, dry_run: bool) -> dict:
    total = (await db.query(COUNT_WITH_ENTITIES))[0]["n"]
    logger.info("Events carrying named_entities: %s", total)
    repaired = removed_tokens = 0
    cursor = None
    while True:
        rows = await db.query(SELECT_WITH_ENTITIES, batch, cursor)
        if not rows:
            break
        for row in rows:
            original = list(row["named_entities"] or [])
            kept = [e for e in original if is_plausible_entity_name(e)]
            if len(kept) == len(original):
                continue
            repaired += 1
            removed_tokens += len(original) - len(kept)
            if not dry_run:
                await db.execute(UPDATE_ENTITIES, row["event_id"], kept)
        cursor = rows[-1]["occurred_at"]
        if len(rows) < batch:
            break
    logger.info(
        "named_entities: %s events carry %s unusable tokens", repaired, removed_tokens
    )
    return {"events": repaired, "tokens": removed_tokens, "total": total}


# ── 3. BGP entity names mangled from a list ─────────────────────────────────

COUNT_MANGLED_AS = """
SELECT count(*) AS n FROM events WHERE primary_entity_name LIKE 'AS[%'
"""

SELECT_MANGLED_AS = """
SELECT event_id, primary_entity_id, primary_entity_name
FROM events
WHERE primary_entity_name LIKE 'AS[%'
ORDER BY occurred_at DESC
LIMIT $1
"""

UPDATE_AS = """
UPDATE events
SET primary_entity_id = $2, primary_entity_name = $2,
    tags = (SELECT array_agg(DISTINCT t) FROM unnest(COALESCE(tags, '{}') || $3::text[]) AS t)
WHERE event_id = $1
"""


def _split_mangled_as(name: str):
    """`AS[1299, 3257]` -> ("AS1299", ["AS3257"]). None if it is not that shape."""
    if not name or not MANGLED_AS_RE.match(name):
        return None
    inner = name[3:].rstrip("]")
    numbers = [n.strip().strip("'\"").lstrip("Aa").lstrip("Ss") for n in inner.split(",")]
    numbers = [n for n in numbers if n.isdigit()]
    if not numbers:
        return None
    return f"AS{numbers[0]}", [f"AS{n}" for n in numbers[1:]]


async def repair_bgp_entity_names(db, batch: int, dry_run: bool) -> dict:
    total = (await db.query(COUNT_MANGLED_AS))[0]["n"]
    logger.info("Events with a mangled AS entity name: %s", total)
    fixed = unparseable = 0
    while True:
        rows = await db.query(SELECT_MANGLED_AS, batch)
        if not rows:
            break
        progressed = False
        for row in rows:
            split = _split_mangled_as(row["primary_entity_name"])
            if split is None:
                unparseable += 1
                continue
            primary, co_origins = split
            tags = ["multi_origin_as"] + [f"co_origin:{a}" for a in co_origins[:5]]
            fixed += 1
            progressed = True
            if not dry_run:
                await db.execute(UPDATE_AS, row["event_id"], primary, tags)
        if dry_run or not progressed:
            break
    logger.info("BGP entity names: %s repairable, %s unparseable", fixed, unparseable)
    return {"fixed": fixed, "unparseable": unparseable, "total": total}


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report the scope and change nothing")
    parser.add_argument("--batch", type=int, default=500, help="rows per pass (default 500)")
    parser.add_argument(
        "--only", choices=["sanctions", "entities", "bgp"], action="append",
        help="run only these phases (repeatable; default: all)",
    )
    args = parser.parse_args()

    phases = set(args.only or ["sanctions", "entities", "bgp"])
    db = await get_timescale()
    try:
        if "sanctions" in phases:
            await repair_sanctions(db, args.batch, args.dry_run)
        if "entities" in phases:
            await repair_named_entities(db, args.batch, args.dry_run)
        if "bgp" in phases:
            await repair_bgp_entity_names(db, args.batch, args.dry_run)
    finally:
        pool = getattr(db, "_pool", None)
        if pool:
            await pool.close()

    if args.dry_run:
        logger.info("Dry run: no changes written.")
    return 0


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    raise SystemExit(asyncio.run(main()))
