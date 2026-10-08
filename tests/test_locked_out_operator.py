"""An operator locked out of their own deployment has a way back in.

With no SMTP configured every reset email was only logged, and the forgot page
still told the user a link was on its way. Accounts created then could never be
confirmed, and an unconfirmed address is refused a reset link by design.
"""

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_forgot_password_says_whether_email_can_be_sent():
    src = (ROOT / "services/api_gateway/routes/auth.py").read_text(encoding="utf-8")
    block = src[src.index("async def forgot_password"):src.index('@router.post("/reset-password")')]
    assert '"email_delivery": mailer_configured()' in block


def test_the_forgot_page_shows_it():
    page = (ROOT / "frontend/src/app/forgot/page.tsx").read_text(encoding="utf-8")
    assert "body.email_delivery !== false" in page
    assert "reset_password.py" in page


def test_the_reset_command_never_takes_the_password_as_an_argument():
    src = (ROOT / "scripts/reset_password.py").read_text(encoding="utf-8")
    assert "getpass.getpass" in src
    assert "isatty()" in src
    assert 'add_argument("password"' not in src


def test_the_reset_command_follows_the_reset_link_rules():
    src = (ROOT / "scripts/reset_password.py").read_text(encoding="utf-8")
    assert "MIN_PASSWORD_LENGTH" in src and "hash_password(" in src
    assert "UPDATE auth_tokens SET consumed_at = NOW()" in src
