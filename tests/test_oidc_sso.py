"""
tests/test_oidc_sso.py

Single sign-on, and the three places it can go wrong.

The flow itself is well-trodden; what these pin are the decisions around it that
a working happy path would not catch:

  * SSO is an option. An unconfigured deployment must behave exactly as it did
    before the module existed -- no button, no routes, no behaviour change.
  * An ID token is never the source of identity here (no JWT verifier in the
    gateway image), so the checks that stand in for a signature have to hold.
  * Linking a federated identity to an existing password account is the sharpest
    edge in the whole feature. Getting it wrong is account takeover.
"""

import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import oidc  # noqa: E402


@pytest.fixture
def configured(monkeypatch):
    """A fully configured provider."""
    monkeypatch.setenv("OIDC_ISSUER", "https://idp.example.com")
    monkeypatch.setenv("OIDC_CLIENT_ID", "sentinel-web")
    monkeypatch.setenv("OIDC_CLIENT_SECRET", "s3cret")
    monkeypatch.setenv("OIDC_REDIRECT_URI", "https://sentinel.example.com/auth/callback")
    yield


@pytest.fixture(autouse=True)
def _clear_oidc_env(monkeypatch):
    """Each test states its own configuration; none inherits the shell's."""
    for key in (
        "OIDC_ISSUER", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET", "OIDC_REDIRECT_URI",
        "OIDC_SCOPES", "OIDC_ROLE_MAP", "OIDC_GROUPS_CLAIM", "OIDC_DEFAULT_ROLE",
        "OIDC_PROVIDER_LABEL",
    ):
        monkeypatch.delenv(key, raising=False)
    oidc._discovery_cache.clear()
    yield


# -- SSO is an option ---------------------------------------------------------

def test_unconfigured_is_the_default():
    """No environment, no SSO. The feature has to be opt-in."""
    assert oidc.is_configured() is False


@pytest.mark.parametrize(
    "missing",
    ["OIDC_ISSUER", "OIDC_CLIENT_ID", "OIDC_CLIENT_SECRET", "OIDC_REDIRECT_URI"],
)
def test_partial_configuration_is_not_configuration(configured, monkeypatch, missing):
    """A button that appears and then fails at the callback is worse than none.

    The person cannot tell whether they mistyped something or the deployment is
    broken, so a half-configured provider counts as unconfigured.
    """
    monkeypatch.delenv(missing)
    assert oidc.is_configured() is False


# -- role mapping -------------------------------------------------------------

def test_unmapped_groups_land_on_viewer(configured):
    assert oidc.role_for_groups({"groups": ["some-unrelated-team"]}) == "VIEWER"


def test_no_groups_claim_at_all_lands_on_viewer(configured):
    assert oidc.role_for_groups({}) == "VIEWER"


def test_the_strongest_matching_group_wins(configured, monkeypatch):
    """Not the last one listed -- someone in two groups gets the better role."""
    monkeypatch.setenv("OIDC_ROLE_MAP", "analysts:ANALYST,platform-admins:ADMIN")
    claims = {"groups": ["analysts", "platform-admins"]}
    assert oidc.role_for_groups(claims) == "ADMIN"
    # Order in the claim must not change the answer.
    assert oidc.role_for_groups({"groups": ["platform-admins", "analysts"]}) == "ADMIN"


def test_group_matching_ignores_case(configured, monkeypatch):
    monkeypatch.setenv("OIDC_ROLE_MAP", "platform-admins:ADMIN")
    assert oidc.role_for_groups({"groups": ["Platform-Admins"]}) == "ADMIN"


def test_a_single_string_group_claim_is_accepted(configured, monkeypatch):
    """Some providers send one group as a bare string, not a list."""
    monkeypatch.setenv("OIDC_ROLE_MAP", "analysts:ANALYST")
    assert oidc.role_for_groups({"groups": "analysts"}) == "ANALYST"


def test_the_default_role_cannot_be_raised_to_admin(configured, monkeypatch):
    """The guard that stops a typo handing the platform to a whole tenant.

    An IdP asserts identity. It does not assert that everyone who can
    authenticate should administer this system -- and for a Google Workspace
    tenant that is a lot of people.
    """
    monkeypatch.setenv("OIDC_DEFAULT_ROLE", "ADMIN")
    assert oidc.default_role() == "VIEWER"


def test_the_default_role_may_be_raised_to_analyst(configured, monkeypatch):
    monkeypatch.setenv("OIDC_DEFAULT_ROLE", "ANALYST")
    assert oidc.default_role() == "ANALYST"


# -- PKCE ---------------------------------------------------------------------

def test_pkce_challenge_is_the_s256_of_the_verifier():
    import base64
    import hashlib

    verifier, challenge = oidc.new_pkce_pair()
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).decode("ascii").rstrip("=")
    assert challenge == expected


def test_pkce_pairs_are_not_reused():
    assert oidc.new_pkce_pair()[0] != oidc.new_pkce_pair()[0]
    assert oidc.new_state() != oidc.new_state()
    assert oidc.new_nonce() != oidc.new_nonce()


def test_the_authorization_url_carries_pkce_and_nonce(configured):
    url = oidc.authorization_url(
        {"authorization_endpoint": "https://idp.example.com/authorize"},
        state="st", nonce="no", challenge="ch",
    )
    assert "code_challenge=ch" in url
    assert "code_challenge_method=S256" in url
    assert "state=st" in url
    assert "nonce=no" in url
    assert "response_type=code" in url


def test_the_client_secret_never_reaches_the_authorization_url(configured):
    """It belongs in the back-channel token request, not the browser."""
    url = oidc.authorization_url(
        {"authorization_endpoint": "https://idp.example.com/authorize"},
        state="st", nonce="no", challenge="ch",
    )
    assert "s3cret" not in url


# -- the checks that stand in for signature verification ----------------------

def _claims(**overrides):
    base = {
        "iss": "https://idp.example.com",
        "aud": "sentinel-web",
        "nonce": "the-nonce",
        "exp": time.time() + 300,
    }
    base.update(overrides)
    return base


def test_a_valid_id_token_passes(configured):
    oidc._check_id_token_claims(_claims(), "the-nonce")


def test_a_replayed_token_is_rejected(configured):
    """A token minted for someone else's sign-in attempt."""
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(_claims(nonce="a-different-nonce"), "the-nonce")


def test_a_token_with_no_nonce_is_rejected(configured):
    """This flow always sends one, so its absence means it is not our answer."""
    claims = _claims()
    del claims["nonce"]
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(claims, "the-nonce")


def test_a_token_from_another_issuer_is_rejected(configured):
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(_claims(iss="https://evil.example.com"), "the-nonce")


def test_a_token_for_another_application_is_rejected(configured):
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(_claims(aud="some-other-client"), "the-nonce")


def test_an_expired_token_is_rejected(configured):
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(_claims(exp=time.time() - 3600), "the-nonce")


def test_a_token_with_no_expiry_is_rejected(configured):
    claims = _claims()
    del claims["exp"]
    with pytest.raises(oidc.OidcError):
        oidc._check_id_token_claims(claims, "the-nonce")


def test_an_audience_list_containing_us_is_accepted(configured):
    """`aud` is allowed to be an array, and often is."""
    oidc._check_id_token_claims(
        _claims(aud=["another-client", "sentinel-web"]), "the-nonce"
    )


def test_a_malformed_jwt_does_not_crash_the_endpoint(configured):
    for junk in ("", "not-a-jwt", "a.b", "a.b.c.d", "x.!!!not-base64!!!.z"):
        with pytest.raises(oidc.OidcError):
            oidc._decode_jwt_payload(junk)


# -- account resolution policy ------------------------------------------------

def test_linking_requires_a_verified_address():
    """The account-takeover edge, pinned.

    An IdP that lets a user set an arbitrary unverified email would otherwise
    let anyone claim an existing Sentinel account by asserting its address.
    """
    source = (ROOT / "services" / "api_gateway" / "routes" / "oidc.py").read_text(
        encoding="utf-8"
    )
    assert 'if not identity["email_verified"]:' in source
    assert "Refusing to link SSO identity" in source


def test_linking_does_not_change_an_existing_role():
    """An IdP group must not silently re-grade an account someone already has."""
    source = (ROOT / "services" / "api_gateway" / "routes" / "oidc.py").read_text(
        encoding="utf-8"
    )
    link = source[source.index("UPDATE users SET oidc_issuer"):]
    link = link[: link.index('"""', link.index("$3"))]
    assert "role" not in link, "the link statement writes a role"
    assert "subscription" not in link, "the link statement writes a tier"


def test_the_subject_is_scoped_to_its_issuer():
    """`sub` is unique per issuer, not globally. The pair is the identity."""
    migrations = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")
    assert "users_oidc_identity_idx" in migrations
    assert "ON users (oidc_issuer, oidc_subject)" in migrations
    assert "CREATE UNIQUE INDEX" in migrations


def test_sso_only_accounts_may_have_no_password():
    """A placeholder hash is a credential. Null says what is actually true."""
    migrations = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")
    assert "ALTER COLUMN password_hash DROP NOT NULL" in migrations


def test_a_null_hash_never_verifies():
    """What makes the nullable column safe: password login rejects these rows."""
    from shared.utils.accounts import verify_password

    assert verify_password("anything", None) is False
    assert verify_password("anything", "") is False
