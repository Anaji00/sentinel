"""
shared/utils/comment_letters.py

What a company's reply to an SEC comment letter (form CORRESP on EDGAR) is
about. The SEC's own letters (UPLOAD) are PDFs and this platform reads no PDF;
a reply quotes each comment before answering it, as text. In Ryans (Review of
Accounting Studies 2021) letters a text classifier flagged were followed by
-5.8% over 90 days when investors read them, and predicted restatements and
write-downs. The model labels topics from a fixed list and whether the company
agrees to restate or amend, with a verbatim quote. Pure.
"""
from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field, field_validator

FEATURE = "ev_comment_letter"
TOPICS = ("revenue_recognition", "non_gaap", "impairment", "segments", "internal_control", "tax", "liquidity",
          "accounting_policy", "disclosure_only", "procedural", "other")
SUBSTANTIVE = frozenset({"revenue_recognition", "non_gaap", "impairment", "segments", "internal_control"})
MAX_CHARS = 12_000

SYSTEM = ("You read a company's reply to an SEC staff comment letter. Answer only from the text. Quote, word for "
          "word, the sentence that best shows what the staff questioned or what the company agreed to do. Then list "
          "the topics from: " + ", ".join(TOPICS) + " (procedural: only a request for more time or an "
          "acknowledgement, no substance). Set restates true only if the company agrees to restate, revise "
          "or amend previously filed financial statements or filings (not merely to expand disclosure in future "
          "filings). Answer in JSON only.")


class Reading(BaseModel):
    quote: str = Field(default="", max_length=500)
    topics: List[str] = Field(default_factory=list, max_length=5)
    restates: bool = False

    @field_validator("topics", mode="before")
    @classmethod
    def _topics(cls, v: object) -> List[str]:
        items = v if isinstance(v, list) else []
        return [t for t in dict.fromkeys(str(x).strip().lower() for x in items) if t in TOPICS]


def grounded(r: Reading, text: str) -> bool:
    """The quote is in the reply (whitespace-insensitive), at least 20 characters."""
    q = " ".join(r.quote.split()).lower().strip(" .\"'")
    return len(q) >= 20 and q in " ".join(text.split()).lower()


def severity(r: Reading) -> float:
    """1 for an agreed restatement or amendment, 0.5 for a substantive accounting
    topic, 0.25 for disclosure alone, 0 for a procedural reply (a request for
    more time: two of COIN's four in a trial on 30 Oct 2026), which is not written."""
    if r.restates:
        return 1.0
    if SUBSTANTIVE & set(r.topics):
        return 0.5
    return 0.0 if set(r.topics) <= {"procedural", "other"} and "procedural" in r.topics else 0.25


__all__ = ["FEATURE", "MAX_CHARS", "Reading", "SUBSTANTIVE", "SYSTEM", "TOPICS", "grounded", "severity"]
