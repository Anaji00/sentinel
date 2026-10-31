"""
shared/utils/activist_13d.py

What a Schedule 13D filer says it intends to do.

A 13D is filed by anyone taking more than 5% of a company with intent to
influence it (passive holders file a 13G). Item 4, "Purpose of Transaction",
is where the filer says why: to seek board seats, push a sale, demand buybacks,
replace management -- or, often, nothing beyond boilerplate. Brav, Jiang,
Partnoy & Thomas (Journal of Finance, 2008): activist 13Ds earn about 7%
abnormal return around the filing, with no reversal over the following year.

  item4     the Item 4 text, cut from the filing's plain text
  intent    the extraction model's reading, kept only when the quote it gives
            appears verbatim in Item 4
  feature   ev_13d_activist: 1 for an activist purpose, 0 for boilerplate,
            dated by the filing

Pure: text in, Item 4 and intent out.
"""
from __future__ import annotations

import re
from typing import List, Optional

from pydantic import BaseModel, Field

FEATURE = "ev_13d_activist"
FORMS = ("SCHEDULE 13D", "SCHEDULE 13D/A", "SC 13D", "SC 13D/A")
MAX_ITEM4_CHARS = 6000
INTENTS = ("board_seats", "sale_or_merger", "capital_return", "strategy_change", "management_change",
           "oppose_transaction", "other")

_ITEM4 = re.compile(r"item\s*4\s*[\.:\-–—]?\s*purpose\s+of\s+(the\s+)?transaction", re.I)
_ITEM5 = re.compile(r"item\s*5\s*[\.:\-–—]?\s*interest\s+in\s+securities", re.I)


class Intent(BaseModel):
    # The quote first, so the verdicts are read off a sentence already written.
    quote: str = Field(default="", max_length=400, description="the sentence of Item 4 that shows the intent, verbatim")
    activist: bool = Field(description="true if the filer states an intent to influence the company, "
                                       "false for investment-only boilerplate")
    intents: List[str] = Field(default_factory=list, max_length=6)
    hostile: bool = False


SYSTEM = ("You read Item 4 (Purpose of Transaction) of a Schedule 13D. Decide whether the filer states an intent to "
          "influence the company (activist) or only holds for investment (boilerplate such as 'acquired for "
          "investment purposes' and a reservation of rights is NOT activist). List intents from: "
          + ", ".join(INTENTS) + ". hostile is true only for an openly adversarial stance (proxy fight, demands "
          "opposed by the board). Copy the one sentence that shows it, verbatim, as quote. Answer in JSON only.")


def item4(text: str) -> Optional[str]:
    """Item 4's text: from its heading to Item 5's, or the next 6,000 characters.
    None when the filing has no Item 4 heading."""
    m = _ITEM4.search(text or "")
    if not m:
        return None
    rest = text[m.end():]
    end = _ITEM5.search(rest)
    body = " ".join((rest[:end.start()] if end else rest[:MAX_ITEM4_CHARS]).split()).lstrip(" .:;-–—")
    return body[:MAX_ITEM4_CHARS] or None


_XML_PURPOSE = re.compile(r"<(?:\w+:)?transactionPurpose[^>]*>(.*?)</(?:\w+:)?transactionPurpose>", re.I | re.S)
_XML_ITEM4 = re.compile(r"<(?:\w+:)?item4[^>]*>(.*?)</(?:\w+:)?item4>", re.I | re.S)


def item4_from_xml(xml: str) -> Optional[str]:
    """Item 4 from EDGAR's structured 13D (primary_doc.xml, in use since late
    2024): the transactionPurpose element, else the whole item4 element's text."""
    m = _XML_PURPOSE.search(xml or "") or _XML_ITEM4.search(xml or "")
    if not m:
        return None
    text = re.sub(r"<[^>]+>", " ", m.group(1))
    text = (text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            .replace("&quot;", '"').replace("&#39;", "'").replace("&apos;", "'"))
    body = " ".join(text.split())
    return body[:MAX_ITEM4_CHARS] or None


def grounded(intent: Intent, body: str) -> bool:
    """The model's quote must appear in Item 4 (whitespace-insensitive) when it
    calls the filer activist; boilerplate needs no quote."""
    if not intent.activist:
        return True
    q = " ".join(intent.quote.split()).lower().strip(" .\"'")
    return len(q) >= 20 and q in " ".join(body.split()).lower()


def clean_intents(intents: List[str]) -> List[str]:
    return [i for i in dict.fromkeys(str(x).strip().lower() for x in intents) if i in INTENTS]


__all__ = ["FEATURE", "FORMS", "INTENTS", "Intent", "SYSTEM", "clean_intents", "grounded", "item4", "item4_from_xml"]
