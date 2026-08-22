#!/usr/bin/env bash
# Obtains or renews a real TLS certificate for the ingress.
#
# The ingress self-signs on first start so the stack is usable immediately. That
# is correct for localhost and unacceptable on a real hostname: browsers refuse
# it, and anything that checks certificates (webhooks, mobile clients, Stripe)
# will not connect at all.
#
# Two prerequisites this script cannot create for you:
#   1. A DNS A/AAAA record for $DOMAIN pointing at this host.
#   2. SENTINEL_BIND=0.0.0.0 in .env, so port 80 is reachable from the internet.
#      Let's Encrypt validates by fetching http://$DOMAIN/.well-known/acme-challenge/
#      and a loopback-only bind makes that impossible.
set -euo pipefail

DOMAIN="${1:-${SENTINEL_DOMAIN:-}}"
EMAIL="${2:-${ACME_EMAIL:-${ADMIN_EMAIL:-}}}"
SSL_DIR="deploy/nginx/ssl"
WEBROOT="deploy/nginx/certbot"

if [ -z "$DOMAIN" ]; then
  echo "usage: $0 <domain> [email]" >&2
  echo "   or: SENTINEL_DOMAIN=example.com $0" >&2
  exit 2
fi
if [ -z "$EMAIL" ]; then
  echo "An email is required for expiry notices. Pass it as the second argument." >&2
  exit 2
fi

echo "→ Checking that the ACME challenge path is reachable for $DOMAIN"
mkdir -p "$WEBROOT/.well-known/acme-challenge"
TOKEN="sentinel-preflight-$$"
echo "$TOKEN" > "$WEBROOT/.well-known/acme-challenge/$TOKEN"
if ! curl -fsS --max-time 15 "http://$DOMAIN/.well-known/acme-challenge/$TOKEN" 2>/dev/null | grep -q "$TOKEN"; then
  rm -f "$WEBROOT/.well-known/acme-challenge/$TOKEN"
  cat >&2 <<EOF
✗ http://$DOMAIN/.well-known/acme-challenge/ is not reachable from outside.

  Issuance will fail until it is. Check, in order:
    - DNS: does $DOMAIN resolve to this host's public address?
    - Bind: is SENTINEL_BIND=0.0.0.0 set in .env and the ingress recreated?
    - Firewall: is inbound TCP 80 open?

  Nothing has been requested, so no rate limit has been consumed.
EOF
  exit 1
fi
rm -f "$WEBROOT/.well-known/acme-challenge/$TOKEN"
echo "  reachable."

echo "→ Requesting certificate from Let's Encrypt"
# Run certbot as a throwaway container so no ACME client is installed on the host
# and nothing is added to the runtime image.
docker run --rm \
  -v "$PWD/$WEBROOT:/var/www/certbot" \
  -v "$PWD/$SSL_DIR/letsencrypt:/etc/letsencrypt" \
  certbot/certbot certonly \
    --webroot -w /var/www/certbot \
    -d "$DOMAIN" \
    --email "$EMAIL" \
    --agree-tos --no-eff-email \
    --non-interactive --keep-until-expiring

LIVE="$SSL_DIR/letsencrypt/live/$DOMAIN"
if [ ! -f "$LIVE/fullchain.pem" ]; then
  echo "✗ certbot did not produce $LIVE/fullchain.pem" >&2
  exit 1
fi

# The ingress reads fixed paths. Copy rather than symlink: the symlink targets
# sit outside the bind mount and would dangle inside the container.
cp "$LIVE/fullchain.pem" "$SSL_DIR/sentinel.crt"
cp "$LIVE/privkey.pem"   "$SSL_DIR/sentinel.key"
chmod 644 "$SSL_DIR/sentinel.crt"
chmod 600 "$SSL_DIR/sentinel.key"

echo "→ Reloading ingress"
docker compose exec -T ingress nginx -s reload 2>/dev/null || docker compose restart ingress

echo "✓ $DOMAIN is now served with a certificate trusted by browsers."
echo "  Certificates expire after 90 days. Re-run this script to renew; it is"
echo "  idempotent and will not reissue until renewal is actually due."
