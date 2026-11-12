"""How many Hawkes inputs are stamped with the poll time rather than the event's own time, and how bursty the stamps are. Read-only.
Registered rule (Upgrade pre-registrations, phase 3): change the fit only if poll-stamped inputs exceed 10%. Run in a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT type, count(*) AS n, count(*) FILTER (WHERE abs(extract(epoch FROM (collected_at - occurred_at))) < 2) AS poll_stamped, "
        "count(*) FILTER (WHERE collected_at - occurred_at > INTERVAL '1 hour') AS old_event "
        "FROM events WHERE occurred_at > NOW() - INTERVAL '7 days' GROUP BY 1 HAVING count(*) > 100 ORDER BY 2 DESC")
    tot = sum(r["n"] for r in rows or [])
    ps = sum(r["poll_stamped"] for r in rows or [])
    print(f"events 7 days {tot} | stamped within 2 s of collection {ps} ({ps / max(1, tot):.1%})")
    print("type | n | poll-stamped share | collected over an hour after the event")
    for r in rows or []:
        print(f"  {r['type']:26s} {r['n']:7d} {r['poll_stamped'] / r['n']:.2f} {r['old_event'] / r['n']:.2f}")
    b = await db.query(
        "SELECT type, count(*) AS bursts, max(c) AS biggest FROM (SELECT type, date_trunc('second', occurred_at) AS s, count(*) AS c FROM events "
        "WHERE occurred_at > NOW() - INTERVAL '7 days' GROUP BY 1, 2 HAVING count(*) > 20) x GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    print("seconds holding more than 20 events with one timestamp (bulk flushes):", [(r["type"], r["bursts"], r["biggest"]) for r in b or []])
    tt = await db.query_one("SELECT count(*) AS n FROM events WHERE occurred_at > NOW() - INTERVAL '7 days'")
    inb = await db.query_one("SELECT coalesce(sum(c), 0) AS n FROM (SELECT count(*) AS c FROM events WHERE occurred_at > NOW() - INTERVAL '7 days' GROUP BY date_trunc('second', occurred_at) HAVING count(*) > 20) x")
    print(f"share of all events sitting in such bursts: {float(inb['n']) / max(1, tt['n']):.1%}")


asyncio.run(main())
