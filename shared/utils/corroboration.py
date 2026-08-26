"""
shared/utils/corroboration.py

Independent corroboration of a claim across sources.

The platform already tracks whether a source has *historically* been right
(SourceScorecard, scored by Brier against confirmed scenarios). It has never
asked the other half of the question, which is the one an analyst asks first:
is anybody else reporting this, right now, and did they find it independently?

Those are different signals and both matter. A single-sourced claim from a
reliable outlet is a lead. The same claim carried by four unrelated outlets
within an hour is a fact. And four outlets running byte-identical wire copy are
not four sources at all -- they are one source repeated, which is the failure
mode that makes naive "mention counting" actively misleading.

Deliberately deterministic. Token-set comparison over a bounded sliding window,
no model call: this runs on every news and OSINT event, and the one lesson this
codebase has paid for repeatedly is that per-event LLM work does not survive
contact with a real stream.
"""

import math
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

# Words that carry no distinguishing signal. Kept short on purpose: an
# aggressive stoplist starts merging genuinely different claims.
_STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "if", "of", "to", "in", "on", "at",
    "for", "with", "as", "by", "from", "is", "are", "was", "were", "be", "been",
    "it", "its", "this", "that", "these", "those", "will", "has", "have", "had",
    "said", "says", "after", "over", "amid", "into", "than", "then",
})

_TOKEN_RE = re.compile(r"[a-z0-9']+")

# How long a claim stays open for corroboration. Long enough for slower outlets
# to pick a story up, short enough that a later unrelated story reusing the same
# words is not counted as confirmation.
DEFAULT_WINDOW_SEC = 6 * 3600

# Token overlap above which two reports are the same claim.
SAME_CLAIM_THRESHOLD = 0.45

# Overlap above which two reports are not merely the same claim but the same
# *text* -- syndicated wire copy. Two outlets running the same feed corroborate
# nothing, and counting them separately is how a single source masquerades as
# consensus.
SYNDICATION_THRESHOLD = 0.85

# Bound on tracked claims, so a busy stream cannot grow this without limit.
MAX_TRACKED_CLAIMS = 5_000


def tokenize(text: str) -> frozenset:
    """Content tokens of a claim, lowercased and stripped of filler."""
    if not text:
        return frozenset()
    return frozenset(
        t for t in _TOKEN_RE.findall(text.lower())
        if len(t) > 2 and t not in _STOPWORDS
    )


def similarity(a: frozenset, b: frozenset) -> float:
    """Jaccard overlap. 1.0 is identical wording, 0.0 shares no content word."""
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    if not intersection:
        return 0.0
    return intersection / len(a | b)


@dataclass
class Report:
    """One outlet's account of a claim."""
    source: str
    tokens: frozenset
    at: float
    reliability: float = 0.5


@dataclass
class Claim:
    """A claim and every report of it seen inside the window."""
    tokens: frozenset
    first_seen: float
    reports: List[Report] = field(default_factory=list)

    def independent_sources(self) -> List[Report]:
        """Reports that are genuinely separate accounts.

        Two rules, both necessary. A source is counted once however many times
        it repeats itself, and a report whose wording is near-identical to one
        already counted is treated as syndication of it rather than as
        independent confirmation.
        """
        kept: List[Report] = []
        seen_sources = set()
        for report in sorted(self.reports, key=lambda r: r.at):
            source_key = report.source.strip().lower()
            if source_key in seen_sources:
                continue
            if any(similarity(report.tokens, k.tokens) >= SYNDICATION_THRESHOLD for k in kept):
                # Same words, different masthead: one story, not two.
                continue
            seen_sources.add(source_key)
            kept.append(report)
        return kept


@dataclass
class CorroborationAssessment:
    """What is known about a claim's support at the moment it was observed."""
    independent_sources: int
    total_reports: int
    corroboration_score: float
    is_single_sourced: bool
    is_syndicated: bool
    minutes_to_corroboration: Optional[float]
    contributing_sources: List[str]

    def to_dict(self) -> Dict:
        return {
            "independent_sources": self.independent_sources,
            "total_reports": self.total_reports,
            "corroboration_score": round(self.corroboration_score, 4),
            "is_single_sourced": self.is_single_sourced,
            "is_syndicated": self.is_syndicated,
            "minutes_to_corroboration": (
                round(self.minutes_to_corroboration, 2)
                if self.minutes_to_corroboration is not None else None
            ),
            "contributing_sources": self.contributing_sources[:8],
        }


class CorroborationTracker:
    """Answers "who else is reporting this" for a stream of claims."""

    def __init__(
        self,
        window_sec: int = DEFAULT_WINDOW_SEC,
        max_claims: int = MAX_TRACKED_CLAIMS,
    ):
        self.window_sec = window_sec
        self.max_claims = max_claims
        self._claims: List[Claim] = []

    def observe(
        self,
        text: str,
        source: str,
        reliability: float = 0.5,
        now: Optional[float] = None,
    ) -> CorroborationAssessment:
        """Records a report and returns what is now known about its claim."""
        now = now if now is not None else time.time()
        self._expire(now)

        tokens = tokenize(text)
        if not tokens:
            return CorroborationAssessment(0, 0, 0.0, True, False, None, [])

        claim = self._match(tokens)
        if claim is None:
            claim = Claim(tokens=tokens, first_seen=now)
            self._claims.append(claim)
            if len(self._claims) > self.max_claims:
                # Oldest first: a claim that has not been mentioned in the whole
                # window is not going to be corroborated now.
                self._claims.sort(key=lambda c: c.first_seen)
                del self._claims[: len(self._claims) - self.max_claims]

        report = Report(source=source or "unknown", tokens=tokens, at=now,
                        reliability=max(0.0, min(1.0, reliability)))
        claim.reports.append(report)
        # The claim's own tokens accumulate, so later phrasings of the same story
        # still match it rather than opening a second claim.
        claim.tokens = claim.tokens | tokens

        return self._assess(claim, now)

    def _match(self, tokens: frozenset) -> Optional[Claim]:
        """The open claim this report is about, if any."""
        best, best_score = None, 0.0
        for claim in self._claims:
            score = similarity(tokens, claim.tokens)
            if score >= SAME_CLAIM_THRESHOLD and score > best_score:
                best, best_score = claim, score
        return best

    def _expire(self, now: float) -> None:
        cutoff = now - self.window_sec
        if self._claims and any(c.first_seen < cutoff for c in self._claims):
            self._claims = [c for c in self._claims if c.first_seen >= cutoff]

    def _assess(self, claim: Claim, now: float) -> CorroborationAssessment:
        independent = claim.independent_sources()
        count = len(independent)

        # Diminishing returns, weighted by who is doing the corroborating. The
        # second independent source is the one that changes an analyst's mind;
        # the fifth adds far less. Reliability weights it so confirmation by
        # outlets with a poor record counts for less than confirmation by ones
        # with a good record.
        weight = sum(r.reliability for r in independent) or 0.0
        score = 1.0 - math.exp(-weight) if count > 1 else 0.0

        minutes = None
        if count > 1:
            minutes = (independent[1].at - independent[0].at) / 60.0

        return CorroborationAssessment(
            independent_sources=count,
            total_reports=len(claim.reports),
            corroboration_score=score,
            is_single_sourced=count <= 1,
            # More reports than independent accounts means the extras were the
            # same outlet again, or the same copy under another masthead.
            is_syndicated=len(claim.reports) > count,
            minutes_to_corroboration=minutes,
            contributing_sources=[r.source for r in independent],
        )

    @property
    def tracked_claims(self) -> int:
        return len(self._claims)
