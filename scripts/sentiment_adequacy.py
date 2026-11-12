"""How well does the news enricher's lexicon sentiment agree with FinBERT and the language-model score on the same headlines?
Read-only example run. Run inside a service container that has the enrichment code (agents-heavy)."""

import asyncio
import collections

from shared.db import get_timescale
from services.enrichment.enrichers.news import _sentiment


def sign(x, eps):
    return 0 if abs(x) < eps else (1 if x > 0 else -1)


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT n.headline, n.positive - n.negative AS fb, l.score AS llm FROM stock_news n JOIN stock_news_llm l ON l.news_id = n.id "
        "WHERE l.created_at > NOW() - INTERVAL '7 days' AND l.model NOT LIKE '%#stale' AND n.positive IS NOT NULL ORDER BY random() LIMIT 4000")
    n = len(rows)
    lex = [(_sentiment(r["headline"][:200]), float(r["fb"]), int(r["llm"])) for r in rows]
    zero = sum(1 for s, _, _ in lex if s == 0.0)
    print(f"headlines {n} | lexicon exactly 0: {zero / n:.1%} | distinct lexicon values: {len({round(s, 2) for s, _, _ in lex})}")
    for name, idx in (("FinBERT sign", 1), ("LLM score", 2)):
        tab = collections.Counter()
        for s, fb, llm in lex:
            ref = sign(fb, 0.25) if idx == 1 else llm
            tab[(sign(s, 0.15), ref)] += 1
        agree = sum(c for (a, b), c in tab.items() if a == b)
        nonzero_ref = sum(c for (a, b), c in tab.items() if b != 0)
        agree_nz = sum(c for (a, b), c in tab.items() if b != 0 and a == b)
        opposite = sum(c for (a, b), c in tab.items() if a * b < 0)
        print(f"  vs {name}: exact agreement {agree / n:.1%} | when the reference is not neutral: lexicon agrees {agree_nz / nonzero_ref:.1%}, "
              f"is neutral {sum(c for (a, b), c in tab.items() if b != 0 and a == 0) / nonzero_ref:.1%}, opposite {opposite / nonzero_ref:.1%}")
    ex = [(r["headline"][:90], round(s, 2), round(float(r["fb"]), 2), int(r["llm"])) for r, (s, _, _) in zip(rows[:400], lex[:400]) if abs(s) < 0.05 and abs(r["fb"]) > 0.8][:5]
    print("examples the lexicon calls neutral while FinBERT is confident:")
    for e in ex:
        print("  ", e)


asyncio.run(main())
