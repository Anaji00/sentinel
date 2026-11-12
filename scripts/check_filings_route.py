"""Call the filings route's function against the live database and print the first rows. Run in the api_gateway container."""

import asyncio

from services.api_gateway.routes import filings as mod
from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    out = await mod.get_latest_filings(None, None, 50, user={}, db=db)
    blank = sum(1 for r in out if not r.filing_date)
    src = {}
    for r in out:
        src[r.filing_date_source] = src.get(r.filing_date_source, 0) + 1
    print("rows", len(out), "| blank dates", blank, "| by source", src, "| null summaries", sum(1 for r in out if r.summary is None))
    for r in out[:4]:
        print(" ", r.ticker, r.form_type, r.filing_date, r.filing_date_source, "|", (r.summary or "None")[:100])


asyncio.run(main())
