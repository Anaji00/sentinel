"""
services/collector-filings/edgar_firehose.py

Every 8-K filed, not the twenty companies someone typed into a dict.

The filings collector polls `data.sec.gov/submissions/CIK{n}.json` once per
ticker, over `BASE_CIK_MAP` -- twenty US mega-caps -- plus whatever is in
`sentinel:watched:equities`. Measured on the running deployment, seven distinct
companies had ever produced a filing event. The rest of the platform is not
scoped that way: it holds 13F positions in hundreds of issuers, ransomware
victims, sanctioned entities and graph companies, and a material 8-K from any of
them is exactly the kind of corroborating evidence the correlation tier exists
to find. None of it could arrive, because the feed could only ever see twenty
names.

EDGAR publishes the whole stream. `browse-edgar?action=getcurrent` is the
firehose of filings accepted in the last day, across every registrant, and it
costs one request per cycle rather than one per company. This reads it, filters
to the forms the enricher can do something with, and resolves each filer's
ticker through the SEC's own company registry -- the same registry the 13F
parser uses -- so the events join to the rest of the platform by identity rather
than by string.

The per-CIK poller stays. It sees the full filing history of a watched name and
is the only thing that can backfill one; this is the breadth, that is the depth.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from xml.etree import ElementTree as ET

import aiohttp

from shared.models.events import RawEvent
from shared.kafka import Topics
from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("collector-filings.firehose")

EDGAR_CURRENT_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type={form}&company=&dateb=&owner=include"
    "&count={count}&output=atom"
)

# The Atom namespace EDGAR emits.
_ATOM = "{http://www.w3.org/2005/Atom}"

# Which forms the firehose asks for. `type=` is a prefix match at EDGAR, so
# "8-K" also returns 8-K/A amendments.
#
# Deliberately narrow. The full stream is tens of thousands of filings a day,
# most of them Forms 3/4/5 ownership reports and fund paperwork that the
# enricher has no branch for; ingesting those would bury the material ones.
FIREHOSE_FORMS = ("8-K", "S-1", "424B")

# How many entries to ask for per form per cycle. The poller runs every 90s and
# EDGAR accepts a few hundred 8-Ks an hour at peak, so this is comfortable
# headroom without paging.
FIREHOSE_COUNT = 100

# Item codes carried in an EDGAR "current" entry summary, e.g. "Items 2.02,9.01".
# Anchored on the "Items" label rather than scanned across the whole summary, so
# a file size or a date cannot be read as an item number.
_ITEMS_BLOCK_RE = re.compile(r"Items?\s+([\d.,\s]+)")
_ITEM_CODE_RE = re.compile(r"(\d\.\d{2})")

# "8-K - ACME CORP (0000012345) (Filer)". The form itself contains hyphens, so
# the split is on the whitespace around the separator rather than on the
# character -- `[^-]+?` could never match "8-K", which is most of this feed.
_TITLE_RE = re.compile(r"^\s*(?P<form>\S.*?)\s+-\s+(?P<name>.+?)\s+\((?P<cik>\d{4,10})\)")

# "...Archives/edgar/data/320193/000032019325000073/0000320193-25-000073-index.htm"
_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")


def _parse_entry(entry: ET.Element) -> Optional[Dict[str, Any]]:
    """One Atom entry into the fields the filing payload needs.

    Returns None for an entry whose form, filer or accession cannot be read --
    a half-parsed filing published as a real one is worse than a dropped one,
    because nothing downstream can tell it apart.
    """
    title_el = entry.find(f"{_ATOM}title")
    title = (title_el.text or "").strip() if title_el is not None else ""
    match = _TITLE_RE.match(title)
    if not match:
        return None

    link_el = entry.find(f"{_ATOM}link")
    href = link_el.get("href", "") if link_el is not None else ""
    acc_match = _ACCESSION_RE.search(href)
    if not acc_match:
        return None

    summary_el = entry.find(f"{_ATOM}summary")
    summary_text = "".join(summary_el.itertext()).strip() if summary_el is not None else ""
    items_block = _ITEMS_BLOCK_RE.search(summary_text)
    items = _ITEM_CODE_RE.findall(items_block.group(1)) if items_block else []

    updated_el = entry.find(f"{_ATOM}updated")
    updated = (updated_el.text or "").strip() if updated_el is not None else ""

    return {
        "form": match.group("form").strip().upper(),
        "company_name": match.group("name").strip(),
        "cik": match.group("cik").lstrip("0") or match.group("cik"),
        "accession_number": acc_match.group(1),
        "items": items,
        "filed_at": updated,
        "index_url": href,
    }


async def _fetch_form_feed(
    session: aiohttp.ClientSession, headers: Dict[str, str], form: str
) -> List[Dict[str, Any]]:
    url = EDGAR_CURRENT_URL.format(form=form, count=FIREHOSE_COUNT)
    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                # Not an exception, so `swallowed` does not apply, but it is
                # the same class of silence: EDGAR rate-limits by User-Agent and
                # a 403 here would otherwise look like a quiet day.
                logger.warning("EDGAR firehose %s returned HTTP %s", form, resp.status)
                return []
            body = await resp.text()
    except Exception as e:
        swallowed("collector_filings.firehose.fetch", e, logger, detail=form)
        return []

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        swallowed("collector_filings.firehose.parse_feed", e, logger, detail=form)
        return []

    parsed: List[Dict[str, Any]] = []
    for entry in root.iter(f"{_ATOM}entry"):
        record = _parse_entry(entry)
        if record:
            parsed.append(record)
    return parsed


async def poll_edgar_firehose(
    session: aiohttp.ClientSession,
    producer,
    dedup,
    headers: Dict[str, str],
    item_descriptions: Dict[str, str],
    resolve_ticker,
) -> int:
    """Read the whole EDGAR current-filings stream and publish what is material.

    `resolve_ticker` is the 13F parser's registry-backed resolver, passed in
    rather than imported so this module does not depend on the load order of the
    collector's own dynamic import shim.
    """
    published = 0
    seen_this_cycle: Set[str] = set()

    for form_prefix in FIREHOSE_FORMS:
        for record in await _fetch_form_feed(session, headers, form_prefix):
            accession = record["accession_number"]
            if accession in seen_this_cycle:
                continue
            seen_this_cycle.add(accession)
            if await dedup.is_seen(accession):
                continue

            company_name = record["company_name"]
            ticker = None
            try:
                ticker = resolve_ticker(company_name)
            except Exception as e:
                swallowed(
                    "collector_filings.firehose.resolve_ticker", e, logger,
                    detail=company_name,
                )

            if not ticker:
                # A filer with no ticker in the SEC registry is a private
                # issuer, a fund or a trust. Nothing downstream can join it to a
                # price, a position or a graph node, so publishing it would add
                # a row that no panel, correlation or agent can use.
                continue

            # No overlap check against the per-CIK poller is needed: both paths
            # share one FilingDeduplicator keyed on the accession number, so
            # whichever sees a filing first is the one that publishes it.
            await dedup.mark_seen(accession)

            form = record["form"]
            is_8k = form.startswith("8-K")
            items = record["items"]
            item_labels = [
                f"{code}: {item_descriptions.get(code, 'Corporate Disclosure')}"
                for code in items
            ]
            filed_date = (record["filed_at"] or "")[:10] or datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d")

            title = f"SEC Filing: {company_name} ({ticker}) filed Form {form}"
            if is_8k and item_labels:
                summary = (
                    f"Material 8-K Event for {ticker}: {'; '.join(item_labels)}. "
                    f"Filed on {filed_date}."
                )
            else:
                summary = (
                    f"SEC filing Form {form} for {company_name} ({ticker}) on {filed_date}."
                )

            event = RawEvent(
                source="sec_edgar",
                occurred_at=datetime.now(timezone.utc),
                raw_payload={
                    "ticker": ticker,
                    "cik": record["cik"],
                    "company_name": company_name,
                    "form_type": form,
                    "filing_date": filed_date,
                    "report_date": filed_date,
                    "accession_number": accession,
                    "items": items,
                    "item_descriptions": item_labels,
                    "primary_doc_url": record["index_url"],
                    "is_material_8k": is_8k,
                    "source_type": "primary_filing",
                    "reliability": 0.99,
                    "title": title,
                    "summary": summary,
                    "tags": [
                        "filing", "sec_edgar", "edgar_firehose",
                        f"form:{form}", f"ticker:{ticker}", f"cik:{record['cik']}",
                    ],
                },
            )
            await producer.send(Topics.RAW_FILINGS, event.model_dump(), key=ticker)
            published += 1

    return published
