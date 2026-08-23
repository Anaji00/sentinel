"""
shared/utils/mailer.py

Transactional email over SMTP, using the standard library.

No email SDK. `smtplib` and `email.message` cover verification and reset mail
completely, and every hosted provider (SES, Postmark, Mailgun, Resend, Gmail)
speaks SMTP.

`smtplib` is synchronous and talks to a remote host, so every send is pushed to
a worker thread. Calling it inline would stall the gateway's event loop for the
duration of an SMTP handshake -- on a slow or unreachable relay, seconds during
which the process serves nobody.

When SMTP is not configured the message is logged instead of sent, and the
caller is told. That keeps local development and a not-yet-configured deployment
working, and it means a signup never fails because mail is down: the account is
created and the link can be resent.
"""

import asyncio
import logging
import os
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Optional

logger = logging.getLogger("shared.mailer")


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    from_address: str
    from_name: str
    use_tls: bool

    @property
    def configured(self) -> bool:
        return bool(self.host and self.from_address)


def load_config() -> SmtpConfig:
    return SmtpConfig(
        host=(os.getenv("SMTP_HOST") or "").strip(),
        port=int(os.getenv("SMTP_PORT") or 587),
        username=(os.getenv("SMTP_USERNAME") or "").strip() or None,
        password=os.getenv("SMTP_PASSWORD") or None,
        from_address=(os.getenv("SMTP_FROM") or os.getenv("SMTP_USERNAME") or "").strip(),
        from_name=(os.getenv("SMTP_FROM_NAME") or "Sentinel").strip(),
        # STARTTLS on 587 is the common case; 465 is implicit TLS.
        use_tls=(os.getenv("SMTP_TLS") or "true").strip().lower() not in ("0", "false", "no", "off"),
    )


def is_configured() -> bool:
    return load_config().configured


def _send_blocking(cfg: SmtpConfig, message: EmailMessage) -> None:
    """Runs on a worker thread. Never call this from the event loop."""
    context = ssl.create_default_context()
    if cfg.port == 465:
        with smtplib.SMTP_SSL(cfg.host, cfg.port, context=context, timeout=20) as server:
            if cfg.username and cfg.password:
                server.login(cfg.username, cfg.password)
            server.send_message(message)
        return

    with smtplib.SMTP(cfg.host, cfg.port, timeout=20) as server:
        server.ehlo()
        if cfg.use_tls:
            server.starttls(context=context)
            server.ehlo()
        if cfg.username and cfg.password:
            server.login(cfg.username, cfg.password)
        server.send_message(message)


async def send_email(to: str, subject: str, body_text: str, body_html: Optional[str] = None) -> bool:
    """Sends a message. Returns True when it actually went out.

    Never raises. A failed send must not fail the request that triggered it: an
    account whose welcome mail bounced is still an account, and the link can be
    resent. The caller decides what to tell the user.
    """
    cfg = load_config()
    if not cfg.configured:
        # Loudly, at INFO, including the body: on a deployment without SMTP this
        # is how an operator retrieves the link from the logs.
        logger.info(
            "SMTP is not configured; email not sent.\n"
            "  To: %s\n  Subject: %s\n%s", to, subject, body_text,
        )
        return False

    message = EmailMessage()
    message["From"] = f"{cfg.from_name} <{cfg.from_address}>"
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body_text)
    if body_html:
        message.add_alternative(body_html, subtype="html")

    try:
        # to_thread, not an inline call: smtplib blocks, and the gateway serves
        # every other request from this same loop.
        await asyncio.to_thread(_send_blocking, cfg, message)
        logger.info("Sent %r to %s", subject, to)
        return True
    except Exception as e:
        logger.error("Could not send %r to %s: %s", subject, to, e)
        return False


# ── Message bodies ────────────────────────────────────────────────────────────

def verification_email(link: str) -> tuple:
    text = (
        "Welcome to Sentinel.\n\n"
        "Confirm this address to finish setting up your account:\n\n"
        f"{link}\n\n"
        "The link works for 48 hours. Your account is already active on the free "
        "plan -- confirming just proves the address is yours.\n\n"
        "If you did not sign up, ignore this message and the account will stay "
        "unconfirmed.\n"
    )
    html = f"""<p>Welcome to Sentinel.</p>
<p>Confirm this address to finish setting up your account:</p>
<p><a href="{link}">Confirm my email</a></p>
<p>The link works for 48 hours. Your account is already active on the free plan &mdash;
confirming just proves the address is yours.</p>
<p>If you did not sign up, ignore this message and the account will stay unconfirmed.</p>"""
    return "Confirm your Sentinel account", text, html


def reset_email(link: str) -> tuple:
    text = (
        "Someone asked to reset the password on your Sentinel account.\n\n"
        f"{link}\n\n"
        "The link works for 30 minutes and can be used once.\n\n"
        "If this was not you, no action is needed -- your password has not "
        "changed and this link expires on its own.\n"
    )
    html = f"""<p>Someone asked to reset the password on your Sentinel account.</p>
<p><a href="{link}">Choose a new password</a></p>
<p>The link works for 30 minutes and can be used once.</p>
<p>If this was not you, no action is needed &mdash; your password has not changed and
this link expires on its own.</p>"""
    return "Reset your Sentinel password", text, html
