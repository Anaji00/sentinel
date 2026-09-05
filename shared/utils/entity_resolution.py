"""
shared/utils/entity_resolution.py

One real-world subject, one node.

graph_node_id settles how an *identifier* is spelled -- it is what stopped the
same wallet existing under 0X... and 0x.... It says nothing about identity: to
it, "Gazprom", "PJSC Gazprom" and "Gazprom PJSC" are three different subjects,
and the graph holds three nodes for one company.

That fragmentation is not cosmetic. Three things in this platform key on entity
identity and all three are weakened by it:

  - The consensus engine fuses bulletins *by entity*. Two agents looking at the
    same company under two spellings cannot corroborate or contradict each
    other, which is one reason sixteen of seventeen live signals carried
    contributing_agents: 1.
  - Centrality is a node's degree. An entity split across three nodes has its
    degree split too, and the correlation tier that reads centrality reads a
    third of the truth.
  - The pattern library and the correlation store both retrieve by entity, so a
    precedent filed under one spelling is invisible to a query using another.

The resolution here is deliberately conservative, in this order:

  1. An explicit alias, if one has been recorded. Nothing beats being told.
  2. A ticker, if the string is one and it validates. Tickers are already
     canonical and the platform has a validator for them.
  3. Structural normalisation: strip legal-form suffixes, punctuation and
     casing. "PJSC Gazprom" and "Gazprom, PJSC" both fold to GAZPROM.

What it deliberately does not do is fuzzy-match names. "Delta Air Lines" and
"Delta Apparel" are four characters apart and merging them would be worse than
the fragmentation this fixes: a wrong merge is unrecoverable downstream, where a
missed merge is merely a smaller graph. Fuzzy candidates are surfaced through
suggest_merges() for a human to confirm, and never applied automatically.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("shared.entity_resolution")

# Recorded aliases: an arbitrary spelling to the canonical key it resolves to.
ALIAS_KEY = "sentinel:entities:alias"

# The display name last seen for a canonical key, so resolving does not cost the
# readable form. The graph stores the canonical id; people read the name.
DISPLAY_KEY = "sentinel:entities:display"

# Merge candidates awaiting human confirmation, and the ones a human refused.
# A refusal is as much a fact as a confirmation and has to survive, or the same
# candidate is proposed forever.
MERGE_CANDIDATE_KEY = "sentinel:entities:merge_candidates"
MERGE_REJECTED_KEY = "sentinel:entities:merge_rejected"

# Legal-form suffixes and prefixes, which carry no identity. Ordered longest
# first so "PUBLIC JOINT STOCK COMPANY" strips before "COMPANY".
_LEGAL_FORMS = [
    # Spelled-out forms first: filings from German, French, Dutch, Japanese and
    # Spanish registries write these in full where their tickers abbreviate.
    "PUBLIC JOINT STOCK COMPANY",
    "AKTIENGESELLSCHAFT",
    "KABUSHIKI KAISHA",
    "NAAMLOZE VENNOOTSCHAP",
    "SOCIETE ANONYME",
    "SOCIEDAD ANONIMA",
    "AKTIEBOLAG",
    "LIMITED",
    "JOINT STOCK COMPANY",
    "PUBLIC LIMITED COMPANY",
    "LIMITED LIABILITY COMPANY",
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "HOLDINGS",
    "HOLDING",
    "GROUP",
    "PJSC", "OJSC", "ZAO", "OAO", "JSC",
    "PLC", "LLC", "LTD", "INC", "CORP", "CO",
    "GMBH", "AG", "NV", "BV", "SA", "SE", "SPA", "SRL", "AB", "AS", "OY",
    "PTE", "PTY", "SDN BHD", "BHD", "KK", "KGAA",
    "TRUST", "FUND", "PARTNERS", "LP", "LLP",
]

# Words that are not identity but appear inside names often enough to matter.
_NOISE_TOKENS = {"THE", "AND", "OF"}

_PUNCT = re.compile(r"[^A-Z0-9 ]+")
_WS = re.compile(r"\s+")
_TICKERISH = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


# Words that cannot stand alone as a company's identity.
#
# A fold landing on one of these has not identified a company, it has described
# an industry -- and two different companies described the same way would then
# share a canonical key and be merged everywhere at once.
# Shortest leading legal form that may be stripped. Below this the token is
# more often part of the name than a form attached to it.
MIN_LEADING_FORM_LEN = 3

_GENERIC_FOLDS = frozenset({
    "RECYCLING", "ARCHITECTS", "AUTOMOTIVE", "HOLDINGS", "HOLDING", "PARTNERS",
    "CAPITAL", "VENTURES", "INTERNATIONAL", "GLOBAL", "NATIONAL", "AMERICAN",
    "GENERAL", "STANDARD", "UNITED", "FIRST", "PACIFIC", "ATLANTIC", "CENTRAL",
    "MANAGEMENT", "INVESTMENTS", "INVESTMENT", "PROPERTIES", "RESOURCES",
    "INDUSTRIES", "TECHNOLOGIES", "TECHNOLOGY", "SYSTEMS", "SOLUTIONS",
    "SERVICES", "ENTERPRISES", "ASSOCIATES", "CONSULTING", "ENERGY", "MEDIA",
    "FINANCIAL", "BANCORP", "PHARMA", "BIOSCIENCES", "MOTORS", "AIRLINES",
})


def _is_usable_fold(text: str) -> bool:
    """Whether a folded name still identifies a particular company."""
    if not text:
        return False
    tokens = text.split()
    if not tokens:
        return False
    if len(tokens) == 1:
        only = tokens[0]
        # A single token that is a legal form, a generic industry word, or too
        # short to be a name identifies nothing.
        if only in _LEGAL_FORMS or only in _GENERIC_FOLDS or len(only) < 2:
            return False
    return True


def normalize_name(raw: Any) -> str:
    """The structural core of a name, with legal form and punctuation removed.

    Deterministic and side-effect free: the same string always folds the same
    way, which is what lets two services agree without consulting a store.

        "PJSC Gazprom"      -> "GAZPROM"
        "Gazprom, PJSC"     -> "GAZPROM"
        "Apple Inc."        -> "APPLE"
        "The Kroger Co."    -> "KROGER"
    """
    if raw is None:
        return ""
    text = str(raw).strip().upper()
    if not text:
        return ""

    text = _PUNCT.sub(" ", text)
    text = _WS.sub(" ", text).strip()

    # Strip legal forms wherever they sit -- Russian and Chinese filings lead
    # with them, Anglophone ones trail -- but never down to a name that no
    # longer identifies anybody.
    #
    # Stripping leading forms unconditionally destroys companies whose names
    # begin with those letters, and the residue is generic, which turns a
    # cosmetic error into a merge. Measured on the previous version:
    # "SA Recycling" and "AB Recycling" both folded to RECYCLING; "AG Growth
    # International" and "SA Growth International" both to GROWTH
    # INTERNATIONAL; "CO Architects" and "AB Architects" both to ARCHITECTS.
    # AG Growth International is a real listed company whose own name is "AG".
    # And "Holding AG" folded to AG -- the loop stripped HOLDING from the front
    # and left the legal form standing as the identifier.
    #
    # Two guards. A strip is rejected if it would leave nothing, a single
    # generic word, or a token that is itself a legal form; and leading strips
    # additionally require that more than one token survives, since a leading
    # form is far more often part of the name than a trailing one is.
    changed = True
    while changed:
        changed = False
        for form in _LEGAL_FORMS:
            if text.endswith(" " + form) and len(text) > len(form) + 1:
                candidate = text[: -len(form)].strip()
                if _is_usable_fold(candidate):
                    text, changed = candidate, True
            elif text.startswith(form + " ") and len(form) >= MIN_LEADING_FORM_LEN:
                # Length is the discriminator, because ambiguity is.
                #
                # "PJSC", "GMBH" and "AKTIENGESELLSCHAFT" leading a name are
                # legal forms and essentially never part of it, so stripping
                # them is safe and is the case this branch exists for. Two-letter
                # forms -- AG, SA, AB, CO -- are far more often the company's
                # own name: AG Growth International, SA Recycling, CO
                # Architects. Requiring more than one token instead was too
                # blunt and broke "PJSC Gazprom", which must fold to GAZPROM.
                candidate = text[len(form):].strip()
                if _is_usable_fold(candidate):
                    text, changed = candidate, True

    tokens = [t for t in text.split() if t not in _NOISE_TOKENS]
    folded = " ".join(tokens).strip()
    # Never return a fold that identifies nobody. Falling back to the
    # punctuation-normalised name keeps two distinct companies distinct, which
    # is the only property this function has to guarantee.
    return folded if _is_usable_fold(folded) else text


def looks_like_ticker(raw: Any) -> bool:
    """Whether a string is shaped like an equity symbol."""
    if raw is None:
        return False
    return bool(_TICKERISH.match(str(raw).strip().upper()))


def canonical_key(raw: Any) -> str:
    """The key a subject folds to before any store is consulted.

    This is the fallback the whole module rests on: it is available with no
    Redis, no network and no history, so a resolution never fails open into a
    fresh fragment when the alias store is unreachable.
    """
    if raw is None:
        return ""
    text = str(raw).strip()
    if looks_like_ticker(text):
        return text.upper()
    normalised = normalize_name(text)
    return normalised or text.upper()


async def resolve_entity(
    redis_client: Any,
    raw: Any,
    *,
    record_display: bool = True,
) -> str:
    """The canonical identifier for a subject, consulting recorded aliases.

    Falls back to canonical_key when the store is unavailable, so this is safe
    on a hot path: a Redis outage costs alias knowledge, never correctness of
    the deterministic fold.
    """
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""

    fold = canonical_key(text)
    if not redis_client:
        return fold

    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        # Both the literal string and its fold are looked up: an alias may have
        # been recorded against either.
        found = await raw_redis.hmget(ALIAS_KEY, [text.upper(), fold])
        for value in found or []:
            if value:
                resolved = value.decode() if isinstance(value, bytes) else str(value)
                if resolved:
                    if record_display:
                        await _remember_display(raw_redis, resolved, text)
                    return resolved
        if record_display:
            await _remember_display(raw_redis, fold, text)
    except Exception as e:
        logger.debug("Alias lookup failed for %r: %s", text, e)

    return fold


async def _remember_display(raw_redis: Any, canonical: str, seen_as: str) -> None:
    """Keeps the most recent readable spelling for a canonical key."""
    try:
        await raw_redis.hset(DISPLAY_KEY, canonical, seen_as)
    except Exception:
        pass


async def display_name(redis_client: Any, canonical: str) -> str:
    """The readable name for a canonical key, or the key itself."""
    if not canonical or not redis_client:
        return canonical or ""
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        value = await raw_redis.hget(DISPLAY_KEY, canonical)
        if value:
            return value.decode() if isinstance(value, bytes) else str(value)
    except Exception:
        pass
    return canonical


async def record_alias(redis_client: Any, alias: Any, canonical: Any) -> bool:
    """Records that `alias` names the same subject as `canonical`.

    Explicit and permanent: this is the top of the resolution order precisely
    so that being told beats being inferred.
    """
    if not redis_client or alias is None or canonical is None:
        return False
    alias_s = str(alias).strip().upper()
    canon_s = canonical_key(canonical)
    if not alias_s or not canon_s or alias_s == canon_s:
        return False
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        await raw_redis.hset(ALIAS_KEY, alias_s, canon_s)
        # And the fold of the alias, so a differently-punctuated form of the
        # same alias resolves without a second entry.
        fold = canonical_key(alias_s)
        if fold and fold != alias_s:
            await raw_redis.hset(ALIAS_KEY, fold, canon_s)
        return True
    except Exception as e:
        logger.debug("Could not record alias %r -> %r: %s", alias, canonical, e)
        return False


# ── Merge candidates: proposed, never applied ────────────────────────────────

# How close two names must be before they are worth a human's attention. High,
# because the cost of a proposal is an analyst's time and the cost of a wrong
# merge is permanent.
MERGE_SUGGESTION_RATIO = 0.90

# Below this length a shared prefix means very little -- "BP" and "BPX" are 67%
# similar and unrelated.
MIN_NAME_LEN_FOR_SUGGESTION = 6


def _similarity(a: str, b: str) -> float:
    """Ratio in [0,1] between two normalised names, without a dependency.

    difflib is standard library and adequate here: this only ever ranks
    candidates for a person to look at, and is never the basis of a merge.
    """
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()


async def suggest_merges(
    redis_client: Any,
    names: List[str],
    limit: int = 25,
) -> List[Dict[str, Any]]:
    """Pairs that look like the same subject, for a human to confirm.

    Never applied automatically. "Delta Air Lines" and "Delta Apparel" are four
    characters apart, and a wrong merge propagates into the graph, the consensus
    fusion and every stored correlation, where a missed merge costs only a
    smaller graph. Pairs a human has already refused are not proposed again.
    """
    folded: Dict[str, str] = {}
    for n in names:
        f = canonical_key(n)
        if f and len(f) >= MIN_NAME_LEN_FOR_SUGGESTION:
            folded.setdefault(f, str(n))

    rejected = set()
    if redis_client:
        try:
            raw_redis = getattr(redis_client, "raw", redis_client)
            members = await raw_redis.smembers(MERGE_REJECTED_KEY)
            rejected = {
                (m.decode() if isinstance(m, bytes) else str(m)) for m in (members or [])
            }
        except Exception:
            rejected = set()

    keys = sorted(folded)
    out: List[Dict[str, Any]] = []
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            pair_id = f"{a}|{b}"
            if pair_id in rejected:
                continue
            ratio = _similarity(a, b)
            if ratio >= MERGE_SUGGESTION_RATIO:
                out.append({
                    "pair_id": pair_id,
                    "a": a,
                    "b": b,
                    "a_seen_as": folded[a],
                    "b_seen_as": folded[b],
                    "similarity": round(ratio, 4),
                })

    out.sort(key=lambda r: -r["similarity"])
    return out[:limit]


async def reject_merge(redis_client: Any, pair_id: str) -> bool:
    """Records that a human looked at a candidate pair and said no."""
    if not redis_client or not pair_id:
        return False
    try:
        raw_redis = getattr(redis_client, "raw", redis_client)
        await raw_redis.sadd(MERGE_REJECTED_KEY, str(pair_id))
        return True
    except Exception as e:
        logger.debug("Could not record merge rejection %r: %s", pair_id, e)
        return False
