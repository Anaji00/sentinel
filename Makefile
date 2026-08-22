# Sentinel — operational entry points.
#
# Two operating modes, because the full stack does not fit in memory at once:
#   analyst    ingestion + correlation + dashboard        ~17.5 GiB
#   reasoning  LLM swarm, collectors stopped              ~23.6 GiB
# See `make budget` for the measured ceilings.

SHELL := /bin/bash
COMPOSE := docker compose
.DEFAULT_GOAL := help

.PHONY: help setup preflight up analyst reasoning obs down stop ps logs health bootstrap \
        budget backup restore test verify migrate psql redis clean nuke

help: ## Show available targets
	@echo ""
	@echo "SENTINEL OPERATIONS"
	@echo ""
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'
	@echo ""

# ── Setup ────────────────────────────────────────────────────────────────────
setup: ## First run: create .env and generate all secrets
	@test -f .env || cp .env.example .env
	@./scripts/preflight.sh --generate-secrets

preflight: ## Validate environment without starting anything
	@./scripts/preflight.sh

# ── Lifecycle ────────────────────────────────────────────────────────────────
up: analyst ## Alias for `analyst`

analyst: preflight ## Start ingestion + dashboard (default mode)
	$(COMPOSE) --profile collectors up -d
	@$(MAKE) --no-print-directory health

reasoning: preflight ## Start the LLM swarm (stops collectors — they do not co-fit)
	$(COMPOSE) --profile collectors down
	$(COMPOSE) --profile agents up -d
	@$(MAKE) --no-print-directory health

obs: ## Add Prometheus + Grafana + Kafka UI to the running mode
	$(COMPOSE) --profile obs up -d

stop: ## Stop containers, keep volumes
	$(COMPOSE) --profile collectors --profile agents --profile obs stop

down: ## Remove containers, KEEP volumes (data survives)
	$(COMPOSE) --profile collectors --profile agents --profile obs down

# ── Observation ──────────────────────────────────────────────────────────────
ps: ## Running services with memory headroom
	@$(COMPOSE) ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'

logs: ## Tail logs (make logs S=api-gateway)
	@$(COMPOSE) logs -f --tail=120 $(S)

health: ## One-line health verdict per dependency
	@./scripts/health.sh

budget: ## Memory ceiling per operating mode vs the Docker VM
	@python -c "import yaml;d=yaml.safe_load(open('docker-compose.yml'));S=d['services'];\
p=lambda m:float(str(m)[:-1])*(1024 if str(m).upper().endswith('G') else 1);\
mem=lambda n:p(((S[n].get('deploy') or {}).get('resources') or {}).get('limits',{}).get('memory','0M'));\
sel=lambda pr:[n for n,s in S.items() if not s.get('profiles') or any(x in pr for x in s['profiles'])];\
[print(f'  {k:<22}{sum(mem(n) for n in sel(v))/1024:>7.2f} GiB   {len(sel(v))} services') for k,v in \
[('analyst',{'collectors'}),('analyst + obs',{'collectors','obs'}),('reasoning',{'agents'}),\
('reasoning + obs',{'agents','obs'}),('everything',{'collectors','agents','obs'})]]"

# ── Data ─────────────────────────────────────────────────────────────────────
backup: ## Snapshot Timescale, Neo4j, Redis and the audit ledger
	@./scripts/backup.sh

restore: ## Restore from a snapshot (make restore F=backups/2026-08-21T1200)
	@./scripts/restore.sh $(F)

bootstrap: ## Backfill real historical bars so the first run is not empty
	python scripts/bootstrap.py --days $(or $(DAYS),90)

migrate: ## Re-run database migrations
	$(COMPOSE) run --rm migrator

psql: ## Open a psql shell
	@$(COMPOSE) exec timescaledb psql -U $${POSTGRES_USER:-sentinel} -d $${POSTGRES_DB:-sentinel}

redis: ## Open a redis-cli shell
	@$(COMPOSE) exec redis redis-cli -a "$$(grep -E '^REDIS_PASSWORD=' .env | cut -d= -f2-)"

# ── Quality ──────────────────────────────────────────────────────────────────
test: ## Backend + frontend test suites
	PYTHONPATH=. python -m pytest tests/ -q
	cd frontend && node_modules/.bin/vitest run

verify: ## Full gate: types, tests, compose validity
	cd frontend && node_modules/.bin/tsc --noEmit
	PYTHONPATH=. python -m pytest tests/ -q --tb=short
	cd frontend && node_modules/.bin/vitest run
	$(COMPOSE) --profile collectors --profile agents --profile obs config --quiet
	@echo "verify: all gates passed"

# ── Destructive ──────────────────────────────────────────────────────────────
clean: ## Remove stopped containers and dangling images
	$(COMPOSE) --profile collectors --profile agents --profile obs down --remove-orphans
	docker image prune -f

nuke: ## DESTROY ALL DATA — containers and volumes. Requires CONFIRM=yes
ifeq ($(CONFIRM),yes)
	$(COMPOSE) --profile collectors --profile agents --profile obs down -v
	@echo "All volumes destroyed."
else
	@echo "Refusing: this deletes every volume (Timescale, Neo4j, Redis, audit ledger)."
	@echo "Take a backup first, then re-run:  make nuke CONFIRM=yes"
	@exit 1
endif
