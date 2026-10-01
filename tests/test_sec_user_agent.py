"""SEC fair access wants a contact that is read, from every path.

EDGAR blocks by IP for repeated anonymous traffic, so the User-Agent is not
cosmetic: one path still announcing a placeholder can get the whole host
blocked, including the paths that were configured correctly.

`collector-filings/main.py` reads SEC_USER_AGENT and warns on every boot while
it is still the placeholder. `thirteen_f.py` had the same header as a literal,
so setting the environment fixed the filing feed and left the 13F path
announcing research@sentinel.local. The Form 4 document fetch added in this
audit makes it matter more: it is a request per filing on top of the feed.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PLACEHOLDER = "sentinel.local"


def _sec_files():
    for path in (ROOT / "services").rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if "sec.gov" in text.lower() or "SEC_USER_AGENT" in text:
            yield path, text


def test_no_sec_header_hardcodes_a_user_agent():
    """A literal here cannot be fixed by configuration."""
    offenders = []
    for path, text in _sec_files():
        for match in re.finditer(r'"User-Agent"\s*:\s*(.+)', text):
            value = match.group(1).strip()
            if value.startswith('"') or value.startswith("'"):
                offenders.append(f"{path.name}: {value[:60]}")
    assert not offenders, (
        "SEC User-Agent hardcoded instead of read from SEC_USER_AGENT: "
        + "; ".join(offenders)
    )


def test_the_placeholder_survives_only_as_a_default_or_a_warning():
    """It must remain reachable as a fallback, and detectable as one."""
    live = []
    for path, text in _sec_files():
        for num, line in enumerate(text.splitlines(), 1):
            if PLACEHOLDER not in line:
                continue
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # A getenv default and the boot-time check are both legitimate.
            if "SEC_USER_AGENT" in line or "getenv" in line:
                continue
            live.append(f"{path.name}:{num}")
    assert not live, f"the placeholder is used as a live value at: {live}"


def test_the_boot_warning_still_exists():
    """Configuration can regress; the warning is what would say so."""
    text = (ROOT / "services/collector-filings/main.py").read_text(encoding="utf-8")
    assert f'if "{PLACEHOLDER}" in SEC_USER_AGENT:' in text


def test_the_form4_fetch_sends_the_configured_agent():
    """The per-filing document fetch is the newest and busiest SEC caller."""
    text = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    block = text[text.index("async def _fetch_form4_document"):]
    block = block[: block.index("\nasync def ", 10)]
    assert '"User-Agent": SEC_USER_AGENT' in block
