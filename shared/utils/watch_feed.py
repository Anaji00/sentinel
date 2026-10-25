"""
shared/utils/watch_feed.py

The surveillance watch feed: which news is about surveillance of the public,
by topic. Pure. Matching is precise rather than broad -- "flock to Europe" is
not Flock Safety, and war drones are not police drones -- because a feed that
cries wolf is not read.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Pattern, Tuple

# Curated civil-liberties sources (collector-news category "civic"): their
# items join the feed whole; everything else must match a topic.
CIVIC_SOURCES = ("eff", "404media", "therecord", "themarkup", "techdirt", "aclu", "epic")

TOPICS: Dict[str, List[str]] = {
    "plate readers": [
        # The company, by name or with surveillance context near it; "flock to
        # the EU" (lower case) never matches, nor "Delegations Flock To China".
        r"\bFlock(?:'s| Safety| Group| cameras?| camera network| surveillance| contracts?| ALPR| AI)\b",
        r"\bFlock\b(?=.{0,80}(?i:\b(?:surveillance|cameras?|police|plates?|ALPR|startup|licen[cs]e|readers?)\b))",
        r"(?i:\b(?:surveillance|cameras?|police|plates?|ALPR|startup)\b).{0,80}\bFlock\b",
        r"\blicen[cs]e[- ]plate readers?\b", r"\bALPRs?\b", r"\bplate[- ]reading cameras?\b",
        r"\b(?:Motorola|Vigilant) (?:Solutions )?(?:ALPR|plate)", r"\bRekor\b",
    ],
    "face recognition": [r"\bfac(?:e|ial) recognition\b", r"\bClearview(?: AI)?\b", r"\bPimEyes\b"],
    "gunshot detection": [r"\bShotSpotter\b", r"\bSoundThinking\b", r"\bgunshot detection\b"],
    "police drones": [r"\bdrones? as first responders?\b", r"\bDFR program", r"\bpolice drones?\b",
                      r"\bSkydio\b.*\bpolice\b", r"\bpolice\b.*\bSkydio\b"],
    # Measured on 30 days of collected news (10 Oct 2026): bare "surveillance
    # camera" caught a stolen bodega cat, bare "Stingray" the Navy's MQ-25
    # drone, bare "spyware" ordinary malware, bare "Palantir" its stock.
    "camera networks": [r"\bFusus\b", r"\breal[- ]time crime cent(?:er|re)s?\b",
                        r"\bRing\b.*\b(?:police|law enforcement|Community Requests)\b",
                        r"\b(?:police|city|citywide)\b.{0,40}\bsurveillance cameras?\b"],
    "phone tracking": [r"\bcell[- ]site simulators?\b", r"\bIMSI catchers?\b", r"\bgeofence warrants?\b",
                       r"\b(?:police|stingray)\b.{0,40}\b(?:cell|phone) (?:tracking|surveillance)\b",
                       r"\blocation data\b", r"\bPegasus spyware\b", r"\bNSO Group\b", r"\bParagon\b.{0,40}\bspyware\b",
                       r"\bstalkerware\b", r"\bCellebrite\b", r"\bphone extraction\b"],
    "data brokers": [r"\bdata brokers?\b", r"\bpeople[- ]search\b", r"\bVenntel\b", r"\bFog (?:Data|Reveal)\b",
                     r"\bBabel Street\b", r"\bGravy Analytics\b", r"\bKochava\b", r"\bLexisNexis Risk\b"],
    "federal surveillance": [
        r"\bPalantir\b.{0,80}(?i:\b(?:ICE|immigration|surveillance|police|DHS|deport\w*|ImmigrationOS|tracking)\b)",
        r"(?i:\b(?:ICE|immigration|surveillance|police|DHS|deport\w*|tracking)\b).{0,80}\bPalantir\b",
        r"\bICE\b.{0,60}(?i:\b(?:surveillance|tracking|facial|license plate|plate reader|data brokers?|location data)\b)",
        r"(?i:\b(?:surveillance|tracking|facial|license plate|plate reader|data brokers?|location data)\b).{0,60}\bICE\b",
        r"\bFISA\b", r"\bSection 702\b", r"\bDHS\b.{0,60}\bsurveillance\b", r"\bmass surveillance\b"],
    "privacy law": [r"\bprivacy (?:law|bill|act)\b", r"\bDelete Act\b", r"\bdata protection (?:law|bill|agency)\b",
                    r"\bsurveillance (?:ordinance|oversight)\b", r"\bCCOPS\b"],
}
_FLAGS = re.IGNORECASE
_COMPILED: Dict[str, List[Pattern[str]]] = {
    t: [re.compile(p, 0 if ("Flock" in p or "ICE" in p or "DHS" in p or "FISA" in p or "Ring" in p
                            or "Palantir" in p or "NSO" in p)
                   else _FLAGS) for p in pats]
    for t, pats in TOPICS.items()
}


def topics(text: Optional[str]) -> List[str]:
    """The surveillance topics a headline (plus summary) is about."""
    if not text:
        return []
    return [t for t, pats in _COMPILED.items() if any(p.search(text) for p in pats)]


def include(source: Optional[str], headline: Optional[str], summary: Optional[str] = None) -> Tuple[bool, List[str]]:
    """Whether an item joins the feed, and its topics. Civic sources join
    whole (topics shown when found); others only when a topic matches."""
    # General news is judged on its headline alone: summaries carried most of
    # the false matches in 30 days of collected news (10 Oct 2026). Curated
    # civil-liberties sources are judged on headline and summary together --
    # but still need a topic: whole, they brought radio astronomy and vaccine
    # polling into a surveillance feed.
    civic = (source or "") in CIVIC_SOURCES
    found = topics(" ".join(x for x in (headline, summary) if x) if civic else headline)
    return bool(found), found


__all__ = ["CIVIC_SOURCES", "TOPICS", "include", "topics"]
