"""A Redis key spelled out in two places is a key with two definitions.

`sentinel:watched:equities` is the reason this file exists. The maritime
enricher defines it as a constant and writes it; one site in the quant engine
typed `sentinel:watched:watchlist:` by hand instead. The zrange came back empty
on every call, `watched_set` resolved to None, and
`generate_covered_call_recommendation` skips its scoping check entirely when
that argument is None -- so the covered-call overlay was evaluated for every
ticker rather than the 44 on the watchlist. It failed open, which is why
nothing ever looked wrong.

The same shape is still here in quantity. `sentinel:calibration:market_forecasts`
is `FORECAST_KEY` in the reasoning service and a hand-typed literal in the
gateway. `sentinel:watched:vessels` is `WATCHED_VESSELS_KEY` in the maritime
enricher and a hand-typed literal twice in the anomaly scorer.

This check is deliberately narrow, because a narrow check that is always right
beats a broad one that trains people to write exemptions. It does not try to
decide whether a key has a reader -- resolving `quote_key(sym)` and
`REFDATA_PREFIX + symbol` statically is a different problem, and a scan that
guesses produces phantom orphans in both directions. It asserts one thing that
is fully decidable from the syntax tree: **the same key literal must not be
written out in two different files.** Import the constant instead.
"""
import ast
import pathlib
import sys
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ROOTS = (ROOT / "services", ROOT / "shared")

# Today's count of key literals typed out in more than one file.
#
# A ratchet, like the silent-drop and debug-handler counts beside it. Every one
# of these is a key that works until somebody changes one of its spellings, and
# the platform has already paid for that once. It may fall and must not rise:
# new code imports the constant.
MAX_DUPLICATED_KEY_LITERALS = 51


def _key_pattern(node):
    """A `sentinel:` key from a literal or an f-string, holes written as {}."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value if node.value.startswith("sentinel:") else None
    if isinstance(node, ast.JoinedStr):
        parts = []
        for piece in node.values:
            if isinstance(piece, ast.Constant) and isinstance(piece.value, str):
                parts.append(piece.value)
            else:
                parts.append("{}")
        joined = "".join(parts)
        return joined if joined.startswith("sentinel:") else None
    return None


def _scan():
    """Key pattern -> the files that spell it out."""
    spelled = defaultdict(set)
    for base in ROOTS:
        for path in sorted(base.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                pattern = _key_pattern(node)
                if pattern:
                    spelled[pattern].add(path.relative_to(ROOT).as_posix())
    return spelled


def _duplicated():
    return {k: sorted(v) for k, v in _scan().items() if len(v) > 1}


def test_a_key_is_not_spelled_out_in_two_files():
    duplicated = _duplicated()
    assert len(duplicated) <= MAX_DUPLICATED_KEY_LITERALS, (
        f"{len(duplicated)} Redis key literals are typed out in more than one "
        f"file, up from {MAX_DUPLICATED_KEY_LITERALS}. Each works until one "
        f"spelling changes, and this platform has already lost a watchlist "
        f"lookup that way. Import the constant: "
        + "; ".join(f"{k} in {v}" for k, v in sorted(duplicated.items())[:6])
    )


def test_the_keys_this_file_was_written_for_have_one_definition():
    """Three fixed to prove the mechanism, and pinned so they stay fixed."""
    duplicated = _duplicated()
    for key in (
        "sentinel:watched:equities",
        "sentinel:watched:vessels",
        "sentinel:calibration:market_forecasts",
        "sentinel:calibration:market_resolved",
    ):
        assert key not in duplicated, (
            f"{key} is spelled out in {duplicated[key]} again. It has a named "
            f"constant; import it."
        )


def test_the_watchlist_key_that_started_this_has_exactly_one_spelling():
    """The original: a zrange that came back empty on every call.

    `generate_covered_call_recommendation` skips its scoping check when
    `watched_equities` is None, so the mistyped key did not fail -- it widened
    the overlay from 44 tickers to every ticker, silently.
    """
    spelled = _scan()
    equities = {k: v for k, v in spelled.items() if "watched:equit" in k or "watched:watchlist" in k}
    assert "sentinel:watched:watchlist:" not in spelled, (
        "the mistyped watchlist key is back"
    )
    for key, files in equities.items():
        assert len(files) == 1, f"{key} spelled in {sorted(files)}"
