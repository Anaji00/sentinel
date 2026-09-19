"""Repair the rows written before three writers were corrected.

Three findings in one audit pass had the same shape: a writer fixed, the history
it wrote left in place, and the history still read as an input. Forward-only is
half a fix whenever the corpus is also a source, and all three of these are.

  1. Emoji in `correlations.summary_headline`.
     The frontend's emoji-to-icon migration could not reach these because they
     are database content, written by the rules and rendered verbatim. Measured:
     12,193 of 12,193 correlations in seven days carried a non-ASCII character
     in that field. The five writers are fixed; these are the rows they wrote.

  2. Pseudo-domains in `correlations.primary_domain`.
     `.split("_")[0]` produced `vessel`, `bgp`, `equity`, `flight`, `market`
     instead of a domain. 2,261 rows, none written that way since 4 September.
     The vector store repairs this on read; Postgres has no such pass, so a
     domain filter silently omits them.

  3. English stopwords in `events.named_entities`.
     The ticker extractor accepted any short alphabetic word, so THE (14,770),
     TO (12,271), OF (10,508) and AND (10,466) are among the platform's most
     frequent "named entities". The 11,821-symbol allowlist landed on
     1 September and the daily share fell from 36% to 3% -- and the residual is
     entirely 'US', which spaCy tags as a GPE for "United States", so the writer
     is now correct. 24,283 events inside the live 90-day correlation window
     still carry one, and that array feeds `_identity_tokens`, which
     `_shares_a_subject` requires an overlap on: two unrelated events that both
     stored THE share a subject by the correlation engine's definition. It also
     feeds `entity_boost = len(named_entities) * per_entity` in the scorer.

Idempotent: every statement is a no-op on a row already repaired, so a re-run
costs a scan and changes nothing.

    python scripts/repair_corpus_history.py --dry-run
    python scripts/repair_corpus_history.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DSN = os.environ.get("TIMESCALE_DSN") or os.environ.get("DATABASE_URL")

# The thirty tokens that account for every stopword entity in the corpus.
# Written out rather than derived: a rule like "shorter than four characters"
# would also delete real tickers, and this is a deletion.
STOPWORD_ENTITIES = [
    "OF", "TO", "THE", "A", "AN", "AND", "IN", "ON", "FOR", "AT", "BY",
    "WITH", "AS", "IS", "IT", "OR", "BE", "ARE", "FROM", "THAT", "THIS",
    "HAS", "HAVE", "SAID", "SAYS", "NEW", "MORE", "OVER", "ITS",
]
# 'US' is deliberately absent: spaCy tags it as a GPE for "United States", and
# it is the entire residual the corrected writer still emits. Removing it would
# delete a real subject to tidy a metric.

# The pseudo-domains observed in the live table, and what each one is.
# A table rather than a rule, because deriving a domain from a prefix is the
# defect being repaired.
DOMAIN_ALIASES = {
    "vessel": "maritime",
    "flight": "aviation",
    "aircraft": "aviation",
    "adsb": "aviation",
    "equity": "tradfi",
    "market": "tradfi",
    "options": "tradfi",
    "filing": "tradfi",
    "earnings": "tradfi",
    "bgp": "cyber",
    "ransomware": "cyber",
    "headline": "news",
}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="count, change nothing")
    args = ap.parse_args()

    import asyncpg

    conn = await asyncpg.connect(DSN)
    mode = "would repair" if args.dry_run else "repaired"

    # ── 1. emoji in correlation headlines ────────────────────────────────────
    # Strip every non-ASCII character and tidy the whitespace it leaves. The
    # headlines are composed from ASCII rule names and entity names, so nothing
    # legitimate is lost -- and a headline that genuinely needed a non-ASCII
    # character would be one this repair should be told about explicitly.
    n = await conn.fetchval(
        "SELECT count(*) FROM correlations WHERE summary_headline ~ '[^\\x00-\\x7F]'"
    )
    if not args.dry_run and n:
        await conn.execute(
            """
            UPDATE correlations
            SET summary_headline = btrim(regexp_replace(
                    regexp_replace(summary_headline, '[^\\x00-\\x7F]', '', 'g'),
                    '\\s+', ' ', 'g'))
            WHERE summary_headline ~ '[^\\x00-\\x7F]'
            """
        )
    print(f"1. emoji in summary_headline      : {mode} {n:,} row(s)", flush=True)

    # ── 2. pseudo-domains on correlations ────────────────────────────────────
    total_domains = 0
    for alias, canonical in DOMAIN_ALIASES.items():
        n = await conn.fetchval(
            "SELECT count(*) FROM correlations WHERE primary_domain = $1", alias
        )
        if not n:
            continue
        total_domains += n
        if not args.dry_run:
            await conn.execute(
                "UPDATE correlations SET primary_domain = $2 WHERE primary_domain = $1",
                alias, canonical,
            )
        print(f"   {alias:>12} -> {canonical:<12} {n:,}", flush=True)
    print(f"2. pseudo-domains                 : {mode} {total_domains:,} row(s)", flush=True)

    # ── 3. stopword entities: reported, not rewritten ────────────────────────
    #
    # This was written as an UPDATE and TimescaleDB refused it:
    #
    #   ConfigurationLimitExceededError: tuple decompression limit exceeded
    #   current limit: 100000, tuples decompressed: 1931972
    #
    # `events` is a compressed hypertable. Rewriting one array column on
    # historical rows decompresses whole chunks -- 1.9M tuples for 23,080
    # events -- and then leaves them decompressed, which costs far more storage
    # than the defect costs accuracy. Raising the limit would make it run; it
    # would not make it a good idea.
    #
    # So the repair moved to the read side, where it belongs and where it is
    # free: `_NON_SUBJECT_TOKENS` in services/correlation/main.py, filtered in
    # `_fold`, which is the one function both `_identity_tokens` and
    # `_subject_tokens` pass through. That is where the harm was -- two
    # unrelated events that both stored THE satisfied `_shares_a_subject`.
    #
    # Counted here so the number stays visible and so this script says why it
    # is not acting, rather than quietly omitting a step it once had.
    n = await conn.fetchval(
        "SELECT count(*) FROM events WHERE named_entities && $1::text[]",
        STOPWORD_ENTITIES,
    )
    print(f"3. stopword named_entities        : {n:,} event(s) still carry one", flush=True)
    print("   not rewritten -- `events` is compressed and the UPDATE decompresses", flush=True)
    print("   1.9M tuples. Filtered on read instead, in correlation/_fold.", flush=True)

    await conn.close()
    if args.dry_run:
        print("\n(dry run -- nothing was written)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
