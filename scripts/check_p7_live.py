"""P7 live check (read-only): how the stored company-typed headline primaries split, and how many the P7 rules would retype or map.

Reads `events` for the last 30 days (headlines and social signals), applies the pure rules in shared/utils/entity_primary.py to each stored primary entity and title.
Nothing is written. Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/check_p7_live.py
"""
import asyncio
import collections
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402
from shared.utils.entity_primary import NON_SUBJECT_TOKENS, headline_company_tickers, is_place_or_agency  # noqa: E402

SQL = """
SELECT primary_entity_id AS pid, primary_entity_type AS ptype, headline
FROM events
WHERE type IN ('headline', 'social_signal') AND occurred_at > NOW() - INTERVAL '30 days' AND primary_entity_id IS NOT NULL
LIMIT 60000
"""


def bucket(pid: str) -> str:
    up = pid.strip().upper()
    if is_place_or_agency(pid):
        return "place_or_agency"
    if up in NON_SUBJECT_TOKENS:
        return "ordinary_word_ticker"
    if " " in pid.strip():
        return "multi_word_name_or_phrase"
    if up.isalpha() and up == pid.strip() and len(up) <= 5:
        return "ticker_shaped"
    return "other"


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(SQL)
    n = len(rows)
    by_type = collections.Counter(r["ptype"] for r in rows)
    comp = [r for r in rows if r["ptype"] == "company"]
    buckets = collections.Counter(bucket(r["pid"]) for r in comp)
    mapped = [r for r in comp if headline_company_tickers(r["headline"] or "")]
    changed = [r for r in mapped if headline_company_tickers(r["headline"])[0] != str(r["pid"]).strip().upper()]
    retyped = sum(v for k, v in buckets.items() if k in ("place_or_agency", "ordinary_word_ticker", "multi_word_name_or_phrase"))
    print(f"events {n}; by stored type {dict(by_type)}")
    print(f"company-typed {len(comp)}; buckets {dict(buckets)}")
    print(f"P7 would retype away from company (before mapping): {retyped} = {retyped / max(len(comp), 1):.1%}")
    print(f"headlines where the curated table names a listed issuer: {len(mapped)}; of those the stored primary differs: {len(changed)}")
    print("sample of differing:", [(r["pid"], headline_company_tickers(r["headline"])[0], (r["headline"] or "")[:60]) for r in changed[:8]])
    print("top retyped ids:", collections.Counter(r["pid"] for r in comp if bucket(r["pid"]) != "ticker_shaped" and bucket(r["pid"]) != "other").most_common(12))


asyncio.run(main())
