"""
shared/utils/sanctions.py
 
Single source of truth for sanctions keyword matching and MMSI→country mapping.
 
FIX (code review): Previously check_sanctions() and MMSI_COUNTRY were defined
  in enrichers/maritime.py AND entity_resolver.py — two copies that would
  inevitably drift. Both files now import from here.
 
Phase 2: replace keyword matching with full OFAC SDN list sync
  (daily download from https://sanctionslist.ofac.treas.gov/Home/SdnList)
  and fuzzy name matching.
"""

import logging
import math
import re
from typing import List

logger = logging.getLogger("shared.sanctions")

try:
    import ahocorasick
    HAS_AHOCORASICK = True
except ImportError:
    HAS_AHOCORASICK = False

# ── Matching discipline ───────────────────────────────────────────────────────
# A vessel name is matched against sanctions keywords by containment. Two rules
# below exist because the naive form of that test produced 812 falsely flagged
# vessels from 243 keywords, 336 of them from four-character tokens:
#
#   "ific" matched PACIFIC.  "chem" matched every chemical tanker.
#   "star", "atlas", "titan", "maria", "lily" are the commonest words in
#   merchant-vessel naming and are also surnames on the SDN list.
#
# MIN_SYNCED_KEYWORD_LEN  — an auto-synced alias shorter than this cannot
#   identify anything on its own. The curated list below is exempt: "irgc",
#   "dprk" and "nioc" are short *because* they are unambiguous.
# Word-boundary verification — the automaton finds substrings, so every hit is
#   re-checked against token boundaries before it counts. This is what stops
#   "ific" inside "PACIFIC" while still matching "PACIFIC" as a whole word.
MIN_SYNCED_KEYWORD_LEN = 8

# SDN entries typed as individuals are people. Matching a person's surname
# against a ship's name is what produced most of the false positives, so the
# sync keeps only vessel and entity (company) rows.
OFAC_MATCHABLE_TYPES = {"vessel", "entity", "-0-", ""}

SANCTIONED_KEYWORDS = [
    # Iran
    "irgc", "iran", "quds", "nioc", "sepah", "national iranian oil", "naftiran", "iranian oil", "iranian petroleum", "iranian shipping", "iranian maritime", "irisl",
    # North Korea
    "ocean maritime management", "ocean maritime", "korea national insurance", "dprk",
    # Russia
    "novorossiysk", "sovcomflot", "gazprom", "rosneft", "lukoil", "transneft", "wagner", "rostec", "sberbank", "vtb", "uralvagonzavod",
    # Syria
    "syrian arab", "general organization for refining",
    # Venezuela
    "pdvsa", "petroleos de venezuela",
]

# MMSI MID (Maritime Identification Digit) prefix → ISO 3166-1 alpha-2
# Full table: https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx
MMSI_COUNTRY: dict = {
    "201":"AL","203":"AT","209":"CY","210":"CY","211":"DE","212":"CY",
    "213":"SM","215":"MT","218":"DE","219":"DK","220":"DK","224":"ES",
    "225":"ES","226":"FR","227":"FR","228":"FR","229":"MT","230":"FI",
    "231":"FO","232":"GB","233":"GB","234":"GB","235":"GB","236":"GI",
    "237":"GR","238":"HR","239":"GR","240":"GR","241":"GR","242":"MA",
    "244":"NL","245":"NL","246":"NL","247":"IT","248":"MT","249":"MT",
    "250":"IE","251":"IS","252":"LI","253":"LU","254":"MC","255":"PT",
    "256":"MT","257":"NO","258":"NO","259":"NO","261":"PL","263":"PT",
    "264":"RO","265":"SE","266":"SE","270":"CZ","271":"TR","272":"UA",
    "273":"RU","275":"LV","276":"EE","277":"LT","278":"SI","279":"RS",
    "301":"AG","303":"US","304":"AG","305":"AG","306":"CW","307":"AW",
    "308":"BS","309":"BS","310":"BM","311":"BS","312":"BZ","314":"BB",
    "316":"CA","319":"KY","321":"CR","323":"CU","325":"DM","327":"DO",
    "329":"GP","330":"GD","331":"GL","332":"GT","334":"HN","336":"HT",
    "338":"US","339":"JM","341":"KN","343":"LC","345":"MX","347":"MQ",
    "348":"MS","350":"NI","351":"PA","352":"PA","353":"PA","354":"PA",
    "355":"PA","356":"PA","357":"PA","358":"PR","359":"SV","361":"PM",
    "362":"TT","364":"TC","366":"US","367":"US","368":"US","369":"US",
    "370":"PA","371":"PA","372":"PA","373":"PA","374":"PA","375":"VC",
    "376":"VC","377":"VC","378":"VG","379":"VI","401":"AF","403":"SA",
    "405":"BD","408":"BH","410":"BT","412":"CN","413":"CN","414":"CN",
    "416":"TW","417":"LK","419":"IN","422":"IR","423":"AZ","425":"IQ",
    "428":"IL","431":"JP","432":"JP","434":"TM","436":"KZ","437":"UZ",
    "438":"JO","440":"KR","441":"KR","443":"PS","445":"KP","447":"KP",
    "450":"KW","451":"LB","453":"MO","455":"MV","457":"MN","459":"NP",
    "461":"OM","463":"PK","466":"QA","468":"SY","470":"AE","472":"TJ",
    "473":"YE","477":"HK","478":"BA","503":"AU","506":"MM","508":"BN",
    "510":"FM","511":"PW","512":"NZ","514":"KH","515":"KH","516":"CX",
    "518":"CK","520":"FJ","523":"CC","525":"ID","529":"KI","531":"LA",
    "533":"MY","536":"MP","538":"MH","540":"NC","542":"NZ","544":"NR",
    "546":"PF","548":"PH","553":"PG","555":"PN","557":"SB","559":"WS",
    "561":"SG","563":"SG","564":"SG","565":"SG","566":"SG","567":"TH",
    "570":"TO","572":"TV","574":"VN","576":"VU","578":"WF","601":"ZA",
    "603":"AO","605":"DZ","607":"TF","608":"SH","609":"BI","610":"BJ",
    "611":"BW","612":"CF","613":"CM","615":"CG","616":"CI","617":"KM",
    "618":"CV","619":"KP","620":"DJ","621":"EG","622":"ER","624":"ET",
    "625":"GA","626":"GH","627":"GM","629":"GW","630":"GQ","631":"GN",
    "632":"BF","633":"KE","634":"SS","635":"LR","636":"LR","637":"LR",
    "638":"LS","642":"LY","644":"MU","645":"MG","647":"ML","649":"MR",
    "650":"MW","654":"MZ","655":"NA","656":"NE","657":"NG","659":"RE",
    "660":"RW","661":"SD","662":"SN","663":"SL","664":"SO","665":"ST",
    "666":"SZ","667":"TD","668":"TG","669":"TN","670":"TZ","671":"UG",
    "672":"ZM","674":"ZW","675":"ZW",
}

_automaton = None

# The keywords actually in force, which is not the same list as the constant.
#
# The fuzzy branch and the no-automaton fallback both iterated
# `SANCTIONED_KEYWORDS` -- the 31 hardcoded terms, 17 of them long enough to be
# fuzzy-eligible. Meanwhile `fetch_and_sync_ofac_sdn_list` downloads the
# Treasury SDN list and passes thousands of names to `_init_automaton`, which
# feeds the *exact* path only. So exact matching covered the synced universe
# and variant matching covered seventeen names, which is not a threshold
# problem or a scoring problem -- those names were never offered to it.
#
# Set by _init_automaton from the same list the automaton gets, so the two
# paths cannot describe different sanctions universes.
_active_keywords: List[str] = list(SANCTIONED_KEYWORDS)

# The curated list is trusted at any length; anything the sync adds must earn
# its place. Held as a set so the length rule can exempt exactly these.
_CURATED = {k.lower() for k in SANCTIONED_KEYWORDS}


def is_usable_keyword(kw: str) -> bool:
    """Can this keyword identify a sanctioned party on its own?

    A curated term is trusted however short -- "irgc" and "dprk" are short
    because they are unambiguous. A synced alias has to be long enough that
    matching it is evidence: a four-letter token is a fragment or a surname,
    and both produce false positives at scale.
    """
    k = (kw or "").strip().lower()
    if not k:
        return False
    if k in _CURATED:
        return True
    if len(k) >= MIN_SYNCED_KEYWORD_LEN:
        return True
    # Two or more words is distinctive even when short ("bank melli").
    return len(k.split()) >= 2 and len(k) >= 6


# ── Fuzzy candidate index ─────────────────────────────────────────────────────
# Which keywords could possibly score the threshold against a given name.
#
# Offering the fuzzy loop the whole synced list is correct and unaffordable:
# measured in the running enricher against the real SDN download, 13,187
# fuzzy-eligible keywords cost 1,367 ms for a single check_sanctions call --
# 615% of one core at the 4.5 events/s this pipeline runs at, on a service
# limited to four. The loop has to see the whole list and must not touch most
# of it.
#
# Two exhaustive cases, so the filter is a bound rather than a guess.
# token_set_ratio takes the best of three difflib comparisons built from the
# two token sets:
#
#   no shared token -> the shared part is empty, the first two comparisons are
#       0, and the score is the plain ratio of the two joined token strings.
#       difflib's ratio is 2*M/T with M <= the shorter string, so
#       2*min(la,lb)/(la+lb) is an upper bound that depends only on lengths.
#   a shared token  -> one side's tokens may be a subset of the other's, which
#       scores 100 at any length ("sovcomflot" inside "sovcomflot shipping
#       ltd"), so no length bound holds and the pair must be scored.
#
# So: everything sharing a token, plus everything inside the length window.
# Nothing else can reach the threshold, and the index makes both lookups O(1).
_fuzzy_terms: List[str] = []          # keyword, by index
_fuzzy_joined: List[str] = []         # its tokens, sorted and rejoined
_fuzzy_by_token: dict = {}            # token -> [index, ...]
_fuzzy_by_len: dict = {}              # len(joined) -> [index, ...]

# Names repeat constantly on this platform -- the same vessel reports a position
# every few seconds -- so the fuzzy verdict for a name is computed once and
# reused until the keyword list changes. Bounded, and cleared by a rebuild,
# because a cache that outlives the list it was computed from would keep
# answering from a retired sanctions universe.
_FUZZY_CACHE_MAX = 20_000
_fuzzy_cache: dict = {}


def _build_fuzzy_index(keywords: List[str]) -> None:
    global _fuzzy_terms, _fuzzy_joined, _fuzzy_by_token, _fuzzy_by_len
    terms, joined, by_token, by_len = [], [], {}, {}
    for kw in keywords:
        k = (kw or "").strip().lower()
        if len(k) < MIN_SYNCED_KEYWORD_LEN:
            continue
        toks = sorted(set(k.split()))
        if not toks:
            continue
        j = " ".join(toks)
        idx = len(terms)
        terms.append(kw)
        joined.append(j)
        for t in toks:
            by_token.setdefault(t, []).append(idx)
        by_len.setdefault(len(j), []).append(idx)
    _fuzzy_terms, _fuzzy_joined, _fuzzy_by_token, _fuzzy_by_len = terms, joined, by_token, by_len
    _fuzzy_cache.clear()


def _fuzzy_candidates(name_tokens: set, joined_len: int, threshold: float) -> set:
    """Indices of keywords that could reach `threshold` for this name."""
    out = set()
    for t in name_tokens:
        hits = _fuzzy_by_token.get(t)
        if hits:
            out.update(hits)
    if joined_len > 0 and _fuzzy_by_len:
        # 2*min(a,b)/(a+b) >= thr, solved for b on each side of a.
        lo = int(math.ceil(threshold * joined_len / (2.0 - threshold)))
        hi = int(math.floor(joined_len * (2.0 - threshold) / threshold))
        for L in range(max(lo, 1), hi + 1):
            hits = _fuzzy_by_len.get(L)
            if hits:
                out.update(hits)
    return out


def _matches_on_boundary(haystack: str, needle: str, end_index: int) -> bool:
    """Verify an automaton hit sits on token boundaries.

    Aho-Corasick reports substrings, which is how "ific" was matching PACIFIC.
    The characters either side of the hit must not be alphanumeric for it to be
    a genuine name match rather than a fragment of a longer word.
    """
    start = end_index - len(needle) + 1
    if start > 0 and haystack[start - 1].isalnum():
        return False
    after = end_index + 1
    if after < len(haystack) and haystack[after].isalnum():
        return False
    return True


def _init_automaton(keywords: List[str] = None):
    global _automaton, _active_keywords
    keywords_to_load = keywords if keywords is not None else SANCTIONED_KEYWORDS

    # Recorded before the ahocorasick guard, deliberately: the slow fallback
    # path runs precisely when the automaton could not be built, and it needs
    # the same universe the fast path would have had.
    _active_keywords = [k for k in keywords_to_load if is_usable_keyword(k)]
    _build_fuzzy_index(_active_keywords)

    if not HAS_AHOCORASICK:
        return

    automaton = ahocorasick.Automaton()

    loaded = 0
    for idx, kw in enumerate(keywords_to_load):
        if not is_usable_keyword(kw):
            continue
        automaton.add_word(kw.lower(), (idx, kw))
        loaded += 1

    if loaded == 0:
        # Never leave the matcher with an empty automaton: an automaton that
        # matches nothing and one that was never built are indistinguishable at
        # the call site, and the second falls through to the slow path.
        logger.warning("No usable sanctions keywords after filtering; keeping previous automaton.")
        return

    automaton.make_automaton()
    _automaton = automaton  # Atomic pointer swap
    logger.info(
        "Sanctions automaton built with %d usable keyword(s) of %d supplied.",
        loaded, len(keywords_to_load),
    )

if HAS_AHOCORASICK:
    _init_automaton() # Boot with hardcoded defaults

# ── Fuzzy matching ────────────────────────────────────────────────────────────
# How close a name has to be to a sanctioned one to be worth a flag.
#
# 92, and measured against the list that is actually in force rather than
# inherited. Over 13,187 fuzzy-eligible SDN keywords and 1,913 real vessel
# names the exact path does not flag:
#
#   threshold 95 ->  0 newly flagged
#   threshold 92 ->  0 newly flagged      <- here
#   threshold 90 ->  6 newly flagged
#   threshold 88 -> 14 newly flagged
#
# The six that appear at 90 are the exact failure this file's history is about,
# in a new form -- merchant-vessel naming words colliding with short SDN
# entries:
#
#   ETERNAL ACE         ~ "eternal peace"   91.7
#   AZOV CONFIDENCE     ~ "confidence p"    90.9
#   DOUBLE HAPPINESS    ~ "happiness i"     90.0
#   BOSPHORUS S         ~ "bosphorus gate dis ticaret limited sirketi"  90.0
#
# So a one-character transliteration like SOVKOMFLOT (90.0 against SOVCOMFLOT)
# is deliberately *not* caught. That is the trade this threshold makes and it
# is the conservative side of it: the flag drives the CRITICAL tier, and six
# innocent vessels raised to CRITICAL costs more than one variant missed.
# Re-measure before moving it -- the number is only as good as the last time it
# was run against the live SDN download.
FUZZY_MATCH_THRESHOLD = 92.0

# The variant half of sanctions screening, and it has to be present.
#
# This read `from rapidfuzz import fuzz` inside a bare try/except that set a
# flag and logged nothing. rapidfuzz is declared in no requirements file and is
# in no image, so the flag was False in every deployment and the whole fuzzy
# branch below has never executed. Measured live in the running enricher:
#
#   SOVCOMFLOT  -> ['sanctioned_ofac', 'sanctioned_kw:sovcomflot']
#   SOVKOMFLOT  -> no flags
#
# One transliteration character, and shadow-fleet naming is exactly where
# transliteration varies. The flag it would have raised drives the CRITICAL
# tier, so the failure was not "fewer matches" -- it was that the platform's
# top severity was unreachable for any name spelled a little differently from
# the Treasury file, with nothing anywhere saying the capability was off.
#
# Implemented on difflib rather than by adding the dependency: CLAUDE.md's rule
# is that a third-party package is not added where the standard library serves,
# and token_set_ratio is twenty lines over SequenceMatcher. rapidfuzz is still
# preferred when it is present, because it is the faster implementation of the
# same number.
def _difflib_token_set_ratio(a: str, b: str) -> float:
    """rapidfuzz.fuzz.token_set_ratio, on the standard library.

    Tokenise both sides, score the shared tokens against each side's full token
    set, and take the best of the three comparisons. This is what makes
    "sovcomflot shipping ltd" and "sovcomflot" a 100 rather than a 61 -- the
    extra tokens on one side do not penalise a complete match on the other.
    """
    from difflib import SequenceMatcher

    ta, tb = set(a.split()), set(b.split())
    if not ta or not tb:
        return 0.0
    shared = sorted(ta & tb)
    rest_a, rest_b = sorted(ta - tb), sorted(tb - ta)

    t0 = " ".join(shared)
    t1 = (t0 + " " + " ".join(rest_a)).strip()
    t2 = (t0 + " " + " ".join(rest_b)).strip()

    def r(x: str, y: str) -> float:
        return SequenceMatcher(None, x, y).ratio() * 100.0

    return max(r(t0, t1), r(t0, t2), r(t1, t2))


try:
    from rapidfuzz import fuzz as _rapidfuzz

    def token_set_ratio(a: str, b: str) -> float:
        return float(_rapidfuzz.token_set_ratio(a, b))

    FUZZY_BACKEND = "rapidfuzz"
except ImportError:
    token_set_ratio = _difflib_token_set_ratio
    FUZZY_BACKEND = "difflib"

# Always true now. Kept as a name because callers and tests read it, and
# because a future backend that genuinely cannot load should say so here
# rather than silently disabling a control.
HAS_RAPIDFUZZ = True

logger.info(
    "Sanctions fuzzy matching enabled via %s (threshold %.0f, minimum keyword "
    "length %d).",
    FUZZY_BACKEND, FUZZY_MATCH_THRESHOLD, MIN_SYNCED_KEYWORD_LEN,
)

def rebuild_sanctions_from_list(keywords: List[str]):
    """Triggered by Enrichment Service when Redis pushes new OFAC payload."""
    if HAS_AHOCORASICK and keywords:
        _init_automaton(keywords)
        logger.info(f"Sanctions automaton successfully rebuilt in memory with {len(keywords)} keywords.")

async def fetch_and_sync_ofac_sdn_list():
    """
    Downloads and updates OFAC SDN list keywords into memory via Treasury CSV streams.
    """
    try:
        from services.enrichment.ofac_sync import fetch_ofac_keywords
        keywords = await fetch_ofac_keywords()
        if keywords:
            all_keywords = list(set(SANCTIONED_KEYWORDS + keywords))
            rebuild_sanctions_from_list(all_keywords)
            logger.info(f"✅ OFAC SDN list synced successfully. Total entities: {len(all_keywords)}")
            return all_keywords
    except Exception as e:
        logger.warning(f"OFAC SDN dynamic sync skipped (offline/timeout): {e}")
    rebuild_sanctions_from_list(SANCTIONED_KEYWORDS)
    return SANCTIONED_KEYWORDS

def check_sanctions(name: str, mmsi: str = "") -> List[str]:
    """
    Return list of flag strings for a vessel/aircraft/entity.
    Checks name against known sanctioned keywords (via Aho-Corasick & fuzzy ratio)
    and MMSI prefix against sanctioned flag states.

    Returns empty list if no flags.
    """
    flags = []
    name_lower = (name or "").strip().lower()
    if not name_lower:
        return flags

    # Every candidate hit is collected and the longest one wins. Breaking on the
    # first match recorded whichever term the automaton reached first, so a
    # vessel matching both a real entity and a junk token was tagged with
    # whichever came first in insertion order -- an arbitrary provenance on a
    # flag that drives the CRITICAL tier.
    matched_kw = None
    if _automaton is not None:
        # Aho-Corasick fast path: O(N) where N is length of name_lower.
        # Boundary-verified, because the automaton reports substrings.
        for end_index, (insert_order, original_value) in _automaton.iter(name_lower):
            if not _matches_on_boundary(name_lower, original_value.lower(), end_index):
                continue
            if matched_kw is None or len(original_value) > len(matched_kw):
                matched_kw = original_value
    else:
        # Fallback slow path, held to the same two rules and the same universe.
        for kw in _active_keywords:
            if re.search(rf"(?<![0-9a-z]){re.escape(kw.lower())}(?![0-9a-z])", name_lower):
                if matched_kw is None or len(kw) > len(matched_kw):
                    matched_kw = kw

    if matched_kw:
        flags.append("sanctioned_ofac")
        flags.append(f"sanctioned_kw:{matched_kw}")

    # Fuzzy matching only where an exact match did not trigger, and only for
    # keywords long enough that near-misses mean something. The old guard was
    # len(kw) >= 5, which is below the length at which a token is distinctive.
    #
    # Over `_active_keywords`, not the hardcoded constant: the synced SDN list
    # is the sanctions universe, and offering the variant check seventeen
    # curated names made it a rounding error against the thing it exists for.
    if not matched_kw and len(name_lower) >= MIN_SYNCED_KEYWORD_LEN:
        cached = _fuzzy_cache.get(name_lower)
        if cached is not None:
            best_kw, best_score = cached
        else:
            name_tokens = set(name_lower.split())
            joined_len = len(" ".join(sorted(name_tokens)))
            best_kw, best_score = None, 0.0
            for idx in _fuzzy_candidates(name_tokens, joined_len, FUZZY_MATCH_THRESHOLD):
                score = token_set_ratio(name_lower, _fuzzy_terms[idx].lower())
                if score > best_score:
                    best_kw, best_score = _fuzzy_terms[idx], score
            if len(_fuzzy_cache) >= _FUZZY_CACHE_MAX:
                _fuzzy_cache.clear()
            _fuzzy_cache[name_lower] = (best_kw, best_score)
        # The closest match, not the first one past the post. Breaking on the
        # first hit recorded whichever name the list happened to reach first,
        # which is the same arbitrary-provenance defect the exact path above
        # was already repaired for.
        if best_kw is not None and best_score >= FUZZY_MATCH_THRESHOLD:
            flags.append("sanctioned_ofac")
            flags.append(f"sanctioned_fuzzy:{best_kw}")

    prefix = (mmsi or "")[:3]
    # High risk: Iran (422), DPRK (442, 445, 447, 619), Russia (273), Syria (468), Cuba (323), Venezuela (775)
    # High-risk shadow fleet FoCs: Cameroon (613), Gabon (625), Palau (511), Zanzibar/Tanzania (670), Eswatini (666)
    if prefix in ("422", "442", "445", "447", "619", "273", "468", "323", "775", "613", "625", "511", "670", "666"):
        flags.append("sanctions_adjacent_flag_state")

    return flags

def mmsi_to_country(mmsi: str) -> str:
    """Return ISO 3166-1 alpha-2 country code for an MMSI, or empty string."""
    return MMSI_COUNTRY.get((mmsi or "")[:3], "")
