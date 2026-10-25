"""JSX text that follows an expression and contains an HTML entity loses its
leading space in the production build: `{state} law &rarr;` rendered "CAlaw →"
and `{n} sells` beside `S&amp;P` rendered "195sells". The fix is an explicit
`{' '}`; this keeps the pattern from coming back."""

import pathlib
import re

FRONTEND = pathlib.Path(__file__).resolve().parents[1] / "frontend" / "src"
PATTERN = re.compile(r"\}[ \t]+[^<{}\n]*&[a-z#0-9]+;", re.I)


def test_no_space_before_an_entity_run_is_left_to_the_compiler() -> None:
    hits = []
    for path in FRONTEND.rglob("*.tsx"):
        if "__tests__" in path.parts:
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("//")[0] if line.lstrip().startswith("//") else line
            if PATTERN.search(code) and "`" not in code:
                hits.append(f"{path.relative_to(FRONTEND)}:{n}: {line.strip()}")
    assert not hits, "write {' '} after the expression:\n" + "\n".join(hits)
