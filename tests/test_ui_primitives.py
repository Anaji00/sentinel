"""One Button and one Input, and no more hand-written ones than there are today.

194 buttons were written in 146 different recipes and 41 inputs in 25, 118 of the buttons with no `type`
and 7 of the inputs with no label. The shared components fix those by construction. The counts below can
only go down: when you convert a raw element, lower the number here so the gain is kept.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "frontend" / "src"
SHARED = {"components/ui/Button.tsx", "components/ui/Input.tsx"}

# Measured 5 Nov 2026 after the first conversion pass. Lower these as raw elements are converted.
MAX_RAW_BUTTONS = 171
MAX_RAW_INPUTS = 25


def _count(pattern: str) -> int:
    total = 0
    for p in SRC.rglob("*.tsx"):
        rel = str(p.relative_to(SRC)).replace("\\", "/")
        if "__tests__" in rel or rel in SHARED:
            continue
        total += len(re.findall(pattern, p.read_text(encoding="utf-8", errors="ignore")))
    return total


def test_no_new_raw_buttons_or_inputs() -> None:
    raw_buttons, raw_inputs = _count(r"<button\b"), _count(r"<input\b")
    assert raw_buttons <= MAX_RAW_BUTTONS, (
        f"{raw_buttons} raw <button> elements, limit {MAX_RAW_BUTTONS}: use <Button> from components/ui/Button"
    )
    assert raw_inputs <= MAX_RAW_INPUTS, (
        f"{raw_inputs} raw <input> elements, limit {MAX_RAW_INPUTS}: use <Input> from components/ui/Input"
    )
    assert raw_buttons >= MAX_RAW_BUTTONS - 25, "the limit is stale: lower MAX_RAW_BUTTONS to what is true now"


def test_the_shared_button_is_safe_by_default() -> None:
    button = (SRC / "components/ui/Button.tsx").read_text(encoding="utf-8")
    assert "type = 'button'" in button, "a button inside a form must not submit unless it says so"
    assert "aria-busy" in button and "aria-pressed" in button


def test_the_shared_input_always_has_a_label() -> None:
    inp = (SRC / "components/ui/Input.tsx").read_text(encoding="utf-8")
    assert "htmlFor={fieldId}" in inp and "label: string;" in inp, "the label is required, not optional"
    assert "aria-describedby" in inp and 'role={error ? \'alert\' : undefined}' in inp


def test_the_recipes_use_theme_tokens_only() -> None:
    recipes = (SRC / "lib/ui.ts").read_text(encoding="utf-8")
    assert not re.search(r"#[0-9a-fA-F]{3,6}\b|\b(?:text|bg|border)-(?:white|black|gray|slate|zinc|red|blue|green)\b", recipes), (
        "a hard-coded colour would not follow the light theme"
    )


def test_the_stock_search_uses_both() -> None:
    search = (SRC / "components/StockSearch.tsx").read_text(encoding="utf-8")
    assert "<Input" in search and "<Button" in search and "<input" not in search and "<button" not in search


def _input_tags(src: str):
    """Every <input ...> opening tag, with braces and quotes respected."""
    for m in re.finditer(r"<input\b", src):
        i, depth, quote = m.end(), 0, None
        while i < len(src):
            c = src[i]
            if quote:
                if c == quote and src[i - 1] != "\\":
                    quote = None
            elif c in "\"'`":
                quote = c
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            elif c == ">" and depth == 0 and src[i - 1] != "=":
                break
            i += 1
        yield m.start(), src[m.start():i + 1]


def test_every_raw_input_still_has_a_label() -> None:
    """Seven had none (two on the login page), so a screen reader announced "edit text"."""
    bare, scanned = [], 0
    for p in SRC.rglob("*.tsx"):
        rel = str(p.relative_to(SRC)).replace("\\", "/")
        if "__tests__" in rel or rel in SHARED:
            continue
        src = p.read_text(encoding="utf-8", errors="ignore")
        labels = [(m.start(), m.end()) for m in re.finditer(r"<label\b[\s\S]*?</label>", src)]
        for pos, tag in _input_tags(src):
            scanned += 1
            wrapped = any(a < pos < b for a, b in labels)
            if not (wrapped or re.search(r"\b(?:aria-label|aria-labelledby|id)=", tag) or re.search(r"type=.hidden", tag)):
                bare.append(f"{rel}:{src[:pos].count(chr(10)) + 1}")
    assert scanned >= 20, f"only {scanned} inputs scanned; the scan is broken"
    assert not bare, f"inputs with no label: {bare}. Use <Input label=...> from components/ui/Input"


def test_the_login_form_connects_its_labels() -> None:
    login = (SRC / "app/login/page.tsx").read_text(encoding="utf-8")
    assert 'label="Email"' in login and 'label="Password"' in login and "<Input" in login
    assert 'autoComplete="current-password"' in login
