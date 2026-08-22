#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Sentinel preflight — validate the environment BEFORE starting the stack.
#
# The stack fails closed, so a missing secret surfaces as a container that
# restart-loops with the real cause buried in logs. This turns that into one
# readable report before anything starts.
#
#   ./scripts/preflight.sh                     validate only
#   ./scripts/preflight.sh --generate-secrets  fill blank secrets in .env
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

ENV_FILE="${ENV_FILE:-.env}"
GENERATE=0
[ "${1:-}" = "--generate-secrets" ] && GENERATE=1

RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YLW=$'\033[0;33m'; DIM=$'\033[2m'; RST=$'\033[0m'
fail=0; warn=0

say()  { printf '%s\n' "$*"; }
ok()   { printf '  %sPASS%s  %s\n' "$GRN" "$RST" "$1"; }
bad()  { printf '  %sFAIL%s  %s\n' "$RED" "$RST" "$1"; fail=$((fail+1)); }
note() { printf '  %sWARN%s  %s\n' "$YLW" "$RST" "$1"; warn=$((warn+1)); }

# Secrets that must be present and non-trivial.
REQUIRED_SECRETS=(
  API_GATEWAY_KEY SESSION_SECRET ADMIN_PASSWORD
  POSTGRES_PASSWORD NEO4J_PASSWORD REDIS_PASSWORD GRAFANA_ADMIN_PASSWORD
)
REQUIRED_PLAIN=(POSTGRES_USER POSTGRES_DB NEO4J_USER ADMIN_EMAIL SENTINEL_ENV)

say ""
say "SENTINEL PREFLIGHT"
say "${DIM}──────────────────────────────────────────────────────────${RST}"

# ── 1. env file ──────────────────────────────────────────────────────────────
say ""
say "1. Environment file"
if [ ! -f "$ENV_FILE" ]; then
  bad "$ENV_FILE not found. Run: cp .env.example .env"
  say ""
  say "Cannot continue without an environment file."
  exit 1
fi
ok "$ENV_FILE present"

# Load without executing arbitrary shell.
while IFS='=' read -r k v; do
  case "$k" in ''|\#*) continue ;; esac
  printf -v "ENV_$k" '%s' "$v"
done < <(grep -E '^[A-Z0-9_]+=' "$ENV_FILE")

getval() { local n="ENV_$1"; printf '%s' "${!n-}"; }

# ── 2. secret generation ─────────────────────────────────────────────────────
if [ "$GENERATE" -eq 1 ]; then
  say ""
  say "2. Generating missing secrets"
  if ! command -v openssl >/dev/null 2>&1; then
    bad "openssl not found; cannot generate secrets"
  else
    for k in "${REQUIRED_SECRETS[@]}"; do
      cur="$(getval "$k")"
      if [ -z "$cur" ]; then
        new="$(openssl rand -hex 32)"
        # Portable in-place edit (BSD and GNU sed disagree on -i).
        tmp="$(mktemp)"; sed "s|^${k}=.*|${k}=${new}|" "$ENV_FILE" > "$tmp" && mv "$tmp" "$ENV_FILE"
        printf -v "ENV_$k" '%s' "$new"
        ok "generated $k"
      fi
    done
    # Keep the assembled URLs consistent with the credentials just written.
    pgp="$(getval POSTGRES_PASSWORD)"; pgu="$(getval POSTGRES_USER)"; pgd="$(getval POSTGRES_DB)"
    rdp="$(getval REDIS_PASSWORD)"
    if [ -n "$pgp" ]; then
      tmp="$(mktemp)"
      sed -e "s|^DATABASE_URL=.*|DATABASE_URL=postgresql://${pgu}:${pgp}@timescaledb:5432/${pgd}|" \
          -e "s|^REDIS_URL=.*|REDIS_URL=redis://:${rdp}@redis:6379/0|" "$ENV_FILE" > "$tmp" && mv "$tmp" "$ENV_FILE"
      ok "synced DATABASE_URL and REDIS_URL to generated credentials"
    fi
  fi
fi

# ── 3. required values ───────────────────────────────────────────────────────
say ""
say "3. Required configuration"
for k in "${REQUIRED_PLAIN[@]}"; do
  [ -n "$(getval "$k")" ] && ok "$k set" || bad "$k is empty"
done
for k in "${REQUIRED_SECRETS[@]}"; do
  v="$(getval "$k")"
  if [ -z "$v" ]; then
    bad "$k is empty  ${DIM}(re-run with --generate-secrets)${RST}"
  elif [ "${#v}" -lt 16 ]; then
    # Advisory, not blocking. A short random value is weaker than ideal but it
    # is not a published one, and POSTGRES_PASSWORD/NEO4J_PASSWORD are baked
    # into initialized volumes -- rotating them locks the platform out of its
    # own data. Refusing to start a working deployment over length would push
    # an operator toward a destructive fix.
    note "$k is only ${#v} chars; 16+ recommended (rotating a datastore password requires a volume rebuild)"
  elif printf '%s' "$v" | grep -qiE 'changeme|password|secret|sentinel-dev|admin|test'; then
    bad "$k looks like a placeholder, not a secret"
  else
    ok "$k set (${#v} chars)"
  fi
done

# ── 4. cross-service consistency ─────────────────────────────────────────────
say ""
say "4. Cross-service consistency"
dburl="$(getval DATABASE_URL)"; pgp="$(getval POSTGRES_PASSWORD)"
if [ -n "$dburl" ] && [ -n "$pgp" ]; then
  case "$dburl" in
    *"$pgp"*) ok "DATABASE_URL matches POSTGRES_PASSWORD" ;;
    *)        bad "DATABASE_URL password does not match POSTGRES_PASSWORD" ;;
  esac
fi
rurl="$(getval REDIS_URL)"; rdp="$(getval REDIS_PASSWORD)"
if [ -n "$rurl" ] && [ -n "$rdp" ]; then
  case "$rurl" in
    *"$rdp"*) ok "REDIS_URL matches REDIS_PASSWORD" ;;
    *)        bad "REDIS_URL password does not match REDIS_PASSWORD" ;;
  esac
fi

env_class="$(getval SENTINEL_ENV)"
case "$env_class" in
  dev|development|local|test|testing) ok "SENTINEL_ENV=$env_class (dev fallbacks permitted)" ;;
  *)
    ok "SENTINEL_ENV=$env_class (strict mode)"
    [ -z "$(getval CORS_ALLOWED_ORIGINS)" ] && \
      bad "strict mode requires CORS_ALLOWED_ORIGINS; browsers will be blocked"
    ;;
esac

# ── 5. execution safety ──────────────────────────────────────────────────────
say ""
say "5. Execution safety"
case "$(getval BROKER_TYPE)" in
  ""|paper)     ok "BROKER_TYPE=paper (simulated, no external orders)" ;;
  alpaca)       note "BROKER_TYPE=alpaca — orders reach Alpaca's PAPER endpoint" ;;
  alpaca_live|live)
                printf '  %sFAIL%s  BROKER_TYPE=%s — LIVE endpoint. Real money.\n' "$RED" "$RST" "$(getval BROKER_TYPE)"
                printf '        Remove this unless you intend live execution.\n'; fail=$((fail+1)) ;;
  *)            note "BROKER_TYPE unrecognized; falling back to paper" ;;
esac

# ── 6. host capacity ─────────────────────────────────────────────────────────
say ""
say "6. Host capacity"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ok "docker daemon reachable"
  memb="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  memg=$(( memb / 1073741824 ))
  if   [ "$memg" -ge 24 ]; then ok "docker memory ${memg}GiB (all profiles fit)"
  elif [ "$memg" -ge 18 ]; then note "docker memory ${memg}GiB — analyst profile fits; agents will not"
  else bad "docker memory ${memg}GiB — below the 18GiB analyst floor. See .wslconfig / Docker resources."
  fi
  docker compose config --quiet 2>/dev/null && ok "docker-compose.yml valid" || bad "docker-compose.yml failed to parse"
else
  bad "docker daemon not reachable"
fi

# ── verdict ──────────────────────────────────────────────────────────────────
say ""
say "${DIM}──────────────────────────────────────────────────────────${RST}"
if [ "$fail" -gt 0 ]; then
  printf '%sPREFLIGHT FAILED%s  %d blocking, %d advisory\n\n' "$RED" "$RST" "$fail" "$warn"
  exit 1
fi
printf '%sPREFLIGHT PASSED%s  %d advisory\n' "$GRN" "$RST" "$warn"
say ""
say "Next:  make up"
say ""
