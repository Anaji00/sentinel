"""FinBERT against the lexicon on world headlines (shadow). Read-only. Run in sentinel-correlation (FinBERT is loaded there)."""

import asyncio

from services.correlation import stock_news_scorer as sns
from shared.db import get_timescale


def cls(s, eps):
    return 0 if abs(s) < eps else (1 if s > 0 else -1)


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(
        "SELECT headline, sentiment, source FROM events WHERE type = 'headline' AND source <> 'alpaca_news' AND sentiment IS NOT NULL "
        "AND created_at > NOW() - INTERVAL '3 days' ORDER BY random() LIMIT 400")
    scorer_cls = next(v for k, v in vars(sns).items() if isinstance(v, type) and hasattr(v, "score") and hasattr(v, "_load"))
    scorer = scorer_cls()
    scorer._load()
    probs = scorer.score([r["headline"][:200] for r in rows])
    n = len(rows)
    tab = {}
    for r, (pos, neg, neu) in zip(rows, probs):
        fb = cls(pos - neg, 0.25)
        lx = cls(float(r["sentiment"]), 0.15)
        tab[(lx, fb)] = tab.get((lx, fb), 0) + 1
    fb_nonzero = sum(c for (lx, fb), c in tab.items() if fb != 0)
    lx_nonzero = sum(c for (lx, fb), c in tab.items() if lx != 0)
    print(f"world headlines {n} | lexicon non-neutral {lx_nonzero / n:.1%} | FinBERT non-neutral {fb_nonzero / n:.1%}")
    print("when FinBERT sees a tone: lexicon agrees", f"{sum(c for (lx, fb), c in tab.items() if fb != 0 and lx == fb) / max(1, fb_nonzero):.1%}",
          "| neutral", f"{sum(c for (lx, fb), c in tab.items() if fb != 0 and lx == 0) / max(1, fb_nonzero):.1%}",
          "| opposite", f"{sum(c for (lx, fb), c in tab.items() if fb * lx < 0) / max(1, fb_nonzero):.1%}")
    print("when the lexicon sees a tone: FinBERT agrees", f"{sum(c for (lx, fb), c in tab.items() if lx != 0 and lx == fb) / max(1, lx_nonzero):.1%}",
          "| neutral", f"{sum(c for (lx, fb), c in tab.items() if lx != 0 and fb == 0) / max(1, lx_nonzero):.1%}",
          "| opposite", f"{sum(c for (lx, fb), c in tab.items() if fb * lx < 0) / max(1, lx_nonzero):.1%}")
    print("sources in the sample:", {s: sum(1 for r in rows if r["source"] == s) for s in sorted({r["source"] for r in rows})[:8]})
    shown = 0
    for r, (pos, neg, neu) in zip(rows, probs):
        if cls(float(r["sentiment"]), 0.15) == 0 and abs(pos - neg) > 0.8 and shown < 5:
            print("  lexicon neutral, FinBERT", round(pos - neg, 2), "|", r["headline"][:100])
            shown += 1


asyncio.run(main())
