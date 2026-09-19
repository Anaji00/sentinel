"""
tests/test_edgar_connection_reuse.py

The insider feed was not intermittent. The connection was.

`agents.insider.clusters` had never carried a message, and the input explained
why: 924 insider events on 3 September, 102 on the 4th, then nothing for nine
days, then 4, then 5, then 35. A detector needing two distinct buyers on one
ticker inside fourteen days has nothing to work with on a feed like that.

The recorded symptom was "SEC EDGAR times out against a 15-second budget", and
the recorded doubt was that a 2% failure rate cannot explain nine empty days.
Both were right. The failure rate was not 2%: the poller was timing out every
few minutes, continuously.

Measured from inside the container:

    direct request, outside the pool      HTTP 200, 23,122 bytes in 2.7s
    placeholder User-Agent                accepted (403 only without a contact)
    three requests down a reused pool     TimeoutError, 200, 200
    the same three with force_close       200 in 0.3s, 1.4s, 0.4s

The session lives for the life of the process and polls once a minute. A
keep-alive connection idle that long is closed at the far end, and the next
request is handed a socket nobody is listening on -- which does not fail, it
hangs until the timeout fires.
"""

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
TRADFI = ROOT / "services" / "collector-tradfi" / "main.py"


def test_the_edgar_poller_does_not_reuse_connections():
    """At one request a minute, reuse buys nothing and costs the feed."""
    src = TRADFI.read_text(encoding="utf-8")
    fn = src.index("async def run_polling")
    body = src[fn : fn + 2500]
    assert "force_close=True" in body, (
        "a pooled connection idle for the poll interval is closed at the far "
        "end, and the next request hangs on it rather than failing"
    )


def test_the_edgar_timeout_has_headroom_for_a_cold_connect():
    """SEC answers in 0.3-5.5s on a sound connection."""
    src = TRADFI.read_text(encoding="utf-8")
    m = re.search(r'SEC_FORM4_TIMEOUT_SEC\s*=\s*int\(os\.getenv\("SEC_FORM4_TIMEOUT_SEC",\s*"(\d+)"\)\)', src)
    assert m, "the EDGAR timeout should be named and configurable"
    assert int(m.group(1)) >= 30


def test_the_poller_uses_the_named_timeout():
    src = TRADFI.read_text(encoding="utf-8")
    fn = src.index("async def poll_form4")
    body = src[fn : fn + 3000]
    assert "timeout=SEC_FORM4_TIMEOUT_SEC" in body
    assert "timeout=15" not in body


def test_a_user_agent_without_a_contact_address_is_still_refused():
    """SEC returns 403 for a UA with no contact, and the code says so.

    Kept because the placeholder is accepted today, which makes it easy to
    conclude the User-Agent does not matter. It does -- just not for this bug.
    """
    src = TRADFI.read_text(encoding="utf-8")
    fn = src.index("async def poll_form4")
    body = src[fn : fn + 3000]
    assert "SEC_USER_AGENT" in body
    assert "403" in body, "the 403 branch explains what a bad User-Agent looks like"
