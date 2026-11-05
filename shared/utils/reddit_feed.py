"""
shared/utils/reddit_feed.py

A subreddit's public Atom feed, as the fields the social collector publishes. Pure.
"""
from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Union
from urllib.parse import urlsplit

_NS = {"a": "http://www.w3.org/2005/Atom"}
_TAGS = re.compile(r"<[^>]+>")
_BOILERPLATE = re.compile(r"submitted by\s+/u/.*$", re.S)

Post = Dict[str, Union[str, float]]


def _text(entry: ET.Element, path: str) -> str:
    node = entry.find(path, _NS)
    return (node.text or "").strip() if node is not None else ""


def parse_reddit_atom(feed: str) -> List[Post]:
    """Each entry's id (without the t3_ prefix), title, plain-text body, permalink,
    author and created_utc. An entry with no parseable date is skipped."""
    out: List[Post] = []
    for entry in ET.fromstring(feed).findall("a:entry", _NS):
        try:
            created = datetime.fromisoformat(_text(entry, "a:published").replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
        link = entry.find("a:link", _NS)
        body = html.unescape(_TAGS.sub(" ", html.unescape(_text(entry, "a:content"))))
        out.append({
            "id": _text(entry, "a:id").removeprefix("t3_"),
            "title": html.unescape(_text(entry, "a:title")),
            "selftext": " ".join(_BOILERPLATE.sub("", body).split()),
            "permalink": urlsplit(link.get("href", "")).path if link is not None else "",
            "author": _text(entry, "a:author/a:name").removeprefix("/u/"),
            "created_utc": created,
        })
    return out


__all__ = ["parse_reddit_atom"]
