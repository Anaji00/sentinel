"""`Depends(require_role(...))` returns a Role (a string enum), not a user record. A route that calls `.get(...)` on it raises
AttributeError the first time it runs (stock_thesis and attribution did, found 13 Nov 2026). Identity comes from request.state.identity."""
import re
from pathlib import Path

ROUTES = Path(__file__).resolve().parents[1] / "services" / "api_gateway" / "routes"
REAL_USER_FILES = {"auth.py", "billing.py", "me.py", "oidc.py"}     # these load a user row themselves
PATTERN = re.compile(r"\buser\.get\(|\buser\[[\"']")


def test_no_route_treats_the_required_role_as_a_user_record():
    bad = []
    for path in sorted(ROUTES.glob("*.py")):
        if path.name in REAL_USER_FILES:
            continue
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if PATTERN.search(line) and not line.lstrip().startswith("#"):
                bad.append(f"{path.name}:{n}: {line.strip()[:100]}")
    assert not bad, "user comes from require_role() and is a Role, not a dict; read request.state.identity:\n" + "\n".join(bad)
