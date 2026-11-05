"""init.sql and the migrations must ask for the same continuous-aggregate policies.

Enrichment runs init.sql on every start. Asking for a refresh policy with
`if_not_exists` is a skip only when the offsets match the live policy; different
offsets raise "refresh interval overlaps". Migration 0076 rebuilt the monthly
rollup with a one-month offset while init.sql still said 30 days, and the next
restart of enrichment (31 Oct 2026) crash-looped it for four days: 7,124 failed
starts, and almost no events written. Nothing failed until a restart happened.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Tuple

ROOT = Path(__file__).resolve().parents[1]
_POLICY = re.compile(
    r"add_continuous_aggregate_policy\(\s*'(?P<view>\w+)',\s*"
    r"start_offset\s*=>\s*INTERVAL\s*'(?P<start>[^']+)',\s*"
    r"end_offset\s*=>\s*INTERVAL\s*'(?P<end>[^']+)'"
)


def _policies(text: str) -> Dict[str, Tuple[str, str]]:
    """Each view's policy offsets; a later statement replaces an earlier one, as a rebuild does."""
    return {m["view"]: (m["start"], m["end"]) for m in _POLICY.finditer(text)}


def test_the_parser_sees_the_policies() -> None:
    assert len(_policies((ROOT / "shared/db/init.sql").read_text(encoding="utf-8"))) >= 6


def test_init_sql_asks_for_the_policies_the_migrations_left_in_place() -> None:
    init = _policies((ROOT / "shared/db/init.sql").read_text(encoding="utf-8"))
    live = _policies((ROOT / "shared/db/migrate.py").read_text(encoding="utf-8"))
    differ = {v: (init[v], live[v]) for v in init if v in live and init[v] != live[v]}
    assert not differ, f"init.sql would crash enrichment on its next start: {differ}"
