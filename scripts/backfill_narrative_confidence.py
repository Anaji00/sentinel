"""Layer audit 4.1 (finding 10): make the "Overall confidence N%" sentence in each stored scenario narrative equal to `confidence_overall`.

The tracker keeps the sentence equal from now on (`_REFRESH_NARRATIVE_SQL`); this repairs the rows written before that. Text only: no score, status or history changes, no model call.
Dry run unless --apply. The old text of every row it rewrites is first copied to `scenarios_narrative_backup_21nov` (created if absent, never overwritten), and a second run changes nothing.
    docker exec -i -w /app sentinel-api_gateway python - [--apply] < scripts/backfill_narrative_confidence.py
"""
import asyncio
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

_STALE = r"""
    FROM scenarios WHERE narrative_summary ~ 'Overall confidence \d' AND confidence_overall IS NOT NULL
      AND (regexp_match(narrative_summary, 'Overall confidence (\d{1,3})\s*%'))[1]::int IS DISTINCT FROM confidence_overall
"""
_BACKUP = "CREATE TABLE IF NOT EXISTS scenarios_narrative_backup_21nov (scenario_id TEXT PRIMARY KEY, narrative_summary TEXT, saved_at TIMESTAMPTZ DEFAULT now())"
_SAVE = "INSERT INTO scenarios_narrative_backup_21nov (scenario_id, narrative_summary) SELECT scenario_id, narrative_summary " + _STALE + " ON CONFLICT DO NOTHING"
_FIX = r"""
    UPDATE scenarios SET narrative_summary = regexp_replace(narrative_summary, 'Overall confidence \d{1,3}\s*%', 'Overall confidence ' || confidence_overall || '%')
    WHERE scenario_id IN (SELECT scenario_id """ + _STALE + ")"


async def main(apply: bool) -> None:
    db = await get_timescale()
    n = (await db.query_one("SELECT count(*) AS n " + _STALE))["n"]
    total = (await db.query_one("SELECT count(*) AS n FROM scenarios WHERE narrative_summary ~ 'Overall confidence \d'"))["n"]
    print(f"stale narratives {n} of {total} with the sentence; apply={apply}")
    if apply and n:
        await db.execute(_BACKUP)
        await db.execute(_SAVE)
        await db.execute(_FIX)
        print("after:", (await db.query_one("SELECT count(*) AS n " + _STALE))["n"], "stale")


asyncio.run(main("--apply" in sys.argv))
