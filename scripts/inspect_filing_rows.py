"""Which date and summary fields filing events actually carry, by form type. Read-only. Run in a service container."""

import asyncio
import collections
import json

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT occurred_at, collected_at, filing_data, headline, summary FROM events WHERE type = 'filing' AND filing_data IS NOT NULL "
        "ORDER BY occurred_at DESC LIMIT 600")
    keys = collections.Counter()
    by_form = collections.defaultdict(lambda: collections.Counter())
    for r in rows:
        fd = r["filing_data"] if isinstance(r["filing_data"], dict) else json.loads(r["filing_data"])
        form = str(fd.get("form_type") or "?")
        for k, v in fd.items():
            keys[k] += 1
            if v not in (None, "", [], {}):
                by_form[form][k] += 1
        by_form[form]["_n"] += 1
        by_form[form]["_event_summary"] += 1 if r["summary"] else 0
    print("rows", len(rows), "| keys:", dict(keys.most_common(30)))
    for form, c in sorted(by_form.items(), key=lambda kv: -kv[1]["_n"])[:8]:
        n = c["_n"]
        print(f"  {form:10s} n={n:4d}", {k: v for k, v in c.items() if k in ("filing_date", "accepted_at", "report_date", "period_of_report", "acceptance_datetime", "description", "_event_summary", "items", "filed_at")})
    for r in rows[:3]:
        fd = r["filing_data"] if isinstance(r["filing_data"], dict) else json.loads(r["filing_data"])
        print("sample:", r["occurred_at"], "|", r["collected_at"], "|", {k: (str(v)[:60]) for k, v in fd.items()}, "| ev.summary:", str(r["summary"])[:80])


asyncio.run(main())
