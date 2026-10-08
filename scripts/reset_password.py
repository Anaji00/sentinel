"""
scripts/reset_password.py

Set an account's password from the host, for an operator locked out of their
own deployment -- the case a reset email cannot cover when SMTP is not set up,
or when the address was never confirmed because no confirmation email could be
sent.

Run it in your own terminal so the password is typed at a hidden prompt and
never appears in a command line, a log or shell history:

    docker exec -it sentinel-api_gateway python /app/scripts/reset_password.py you@example.com

It applies the same rules as the reset link: the minimum length, the same
hashing, and every outstanding reset link for the account is voided. Running it
also confirms the address, because whoever can run commands on the host already
controls the deployment.
"""

import argparse
import asyncio
import getpass
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.accounts import (  # noqa: E402
    MIN_PASSWORD_LENGTH, hash_password, normalize_email,
)
from shared.utils.auth_tokens import TokenPurpose  # noqa: E402
from shared.utils.session_revocation import revoke_sessions  # noqa: E402


def _prompt() -> str:
    first = getpass.getpass("New password: ")
    if len(first) < MIN_PASSWORD_LENGTH:
        sys.exit(f"Choose a password of at least {MIN_PASSWORD_LENGTH} characters.")
    if getpass.getpass("Repeat it: ") != first:
        sys.exit("The two entries did not match; nothing was changed.")
    return first


async def _reset(email: str, password: str) -> None:
    db = await get_timescale()
    row = await db.query_one("SELECT id, is_active FROM users WHERE email = $1", email)
    if not row:
        sys.exit(f"No account for {email}.")
    await db.execute(
        "UPDATE users SET password_hash = $1, email_verified = TRUE WHERE id = $2",
        hash_password(password), int(row["id"]),
    )
    await db.execute(
        """
        UPDATE auth_tokens SET consumed_at = NOW()
         WHERE user_id = $1 AND purpose = $2 AND consumed_at IS NULL
        """,
        int(row["id"]), TokenPurpose.RESET_PASSWORD.value,
    )
    # Sessions signed in before the change end, as they do after a reset link.
    await revoke_sessions(await get_redis(), email)
    note = "" if row.get("is_active", True) else " The account is deactivated; it still cannot sign in."
    print(f"Password changed for {email}. Sign in at https://localhost/login.{note}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set an account's password from the host.")
    parser.add_argument("email")
    args = parser.parse_args()
    if not sys.stdin.isatty():
        sys.exit("Run this with `docker exec -it` so the password can be typed at a hidden prompt.")
    email = normalize_email(args.email)
    asyncio.run(_reset(email, _prompt()))


if __name__ == "__main__":
    main()
