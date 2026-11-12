"""Richness of what the agents and models write, plus sentiment agreement. Read-only. Run in a service container."""

import asyncio

from shared.db import get_timescale


async def main() -> None:
    db = await get_timescale()
    t = await db.query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND (table_name LIKE '%scenario%' OR table_name LIKE '%predict%' OR table_name LIKE '%analyst%' OR table_name LIKE '%case%' OR table_name LIKE '%correlation%' OR table_name LIKE '%wargam%' OR table_name LIKE '%thesis%' OR table_name LIKE '%dossier%') ORDER BY 1")
    print("tables:", [r["table_name"] for r in t or []])

    print("\nFINBERT vs LLM (same headline, 7 days): finbert dominant class x llm score")
    rows = await db.query(
        "SELECT CASE WHEN n.positive >= n.negative AND n.positive >= n.neutral THEN 'pos' WHEN n.negative >= n.neutral THEN 'neg' ELSE 'neu' END AS fb, l.score, count(*) AS c "
        "FROM stock_news n JOIN stock_news_llm l ON l.news_id = n.id WHERE l.created_at > NOW() - INTERVAL '7 days' AND l.model NOT LIKE '%#stale' AND n.positive IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2")
    for r in rows or []:
        print(f"  finbert {r['fb']}  llm {r['score']:>2}  {r['c']}")
    rows = await db.query("SELECT round(max(GREATEST(positive, negative, neutral))::numeric, 2) AS top, count(*) AS n FROM stock_news WHERE created_at > NOW() - INTERVAL '7 days' AND positive IS NOT NULL GROUP BY floor(GREATEST(positive, negative, neutral) * 5) ORDER BY 1")
    print("  finbert top-class confidence buckets (0.2 wide):", [(float(r["top"]), r["n"]) for r in rows or []])

    print("\nSCENARIOS (reasoning output), 14 days")
    rows = await db.query(
        "SELECT count(*) AS n, count(DISTINCT headline) AS distinct_headlines, avg(length(headline)) AS hl, avg(length(significance)) AS sig, "
        "avg(jsonb_array_length(hypotheses)) AS nh, min(jsonb_array_length(hypotheses)) AS minh, max(jsonb_array_length(hypotheses)) AS maxh, "
        "avg(confidence_overall) AS conf, stddev(confidence_overall) AS confsd, count(DISTINCT confidence_overall) AS confdistinct "
        "FROM scenarios WHERE created_at > NOW() - INTERVAL '14 days'")
    print(" ", [dict(r) for r in rows or []])
    rows = await db.query(
        "SELECT h->>'label' AS label, (h->>'probability')::int AS p, length(h->>'mechanism') AS mech_len, jsonb_array_length(coalesce(h->'watch_signals','[]'::jsonb)) AS watch, "
        "jsonb_array_length(coalesce(h->'deny_signals','[]'::jsonb)) AS deny, jsonb_array_length(coalesce(h->'beneficiaries','[]'::jsonb)) AS ben "
        "FROM scenarios s, jsonb_array_elements(s.hypotheses) h WHERE s.created_at > NOW() - INTERVAL '14 days' LIMIT 4000")
    if rows:
        import collections
        ps = collections.Counter(r["p"] for r in rows)
        print("  hypothesis probability values (top):", ps.most_common(8))
        print("  hypotheses:", len(rows), "| mean mechanism chars", round(sum(r["mech_len"] or 0 for r in rows) / len(rows)), "| with no watch signals", sum(1 for r in rows if not r["watch"]), "| no deny signals", sum(1 for r in rows if not r["deny"]), "| no beneficiaries", sum(1 for r in rows if not r["ben"]))
        lab = collections.Counter(r["label"] for r in rows)
        print("  most repeated labels:", lab.most_common(5))
    rows = await db.query("SELECT headline, left(significance, 160) AS sig FROM scenarios WHERE created_at > NOW() - INTERVAL '14 days' ORDER BY created_at DESC LIMIT 3")
    for r in rows or []:
        print("  sample:", r["headline"], "|", r["sig"])


asyncio.run(main())
