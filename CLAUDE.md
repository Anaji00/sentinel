# CLAUDE.md

Sentinel: an event-driven market-intelligence platform. Collectors → Kafka → correlation, reasoning and an agent swarm
(local Ollama models) → TimescaleDB, Neo4j, Qdrant and Redis → a Next.js dashboard, plus a nightly cross-sectional factor
model that drives a **paper-trading** auto-trader (Alpaca paper account). Python 3.11 in `services/` and `shared/`,
TypeScript in `frontend/`, about 410 test files in `tests/`. See `README.md`, `GUIDE.md`, `DATA_PIPELINE.md` and
`MODEL_TRIALS.md` for depth; read them only when the task touches that area.

## Commands

- Tests (host Python): `python -m pytest tests/ -q --tb=short`, or `-k <name>` for the files that cover the change.
- Lint changed Python files: `ruff check --select F,E9 <files>`. Ruff is not a project dependency; run it from a throwaway venv and never install into the global host Python.
- Full gate: `make verify` (tsc, model-reference check, pytest, vitest, `next build`, compose config).
- Frontend only: `cd frontend && npm run typecheck && npm run lint && npm test`.
- Migrations: add a numbered step to `shared/db/migrate.py`, then apply it with
  `MSYS_NO_PATHCONV=1 docker exec -w /app sentinel-factor-model python shared/db/migrate.py`.
- Redeploy one service (code is bind-mounted read-only; a restart picks it up, a recreate also picks up `.env` and compose changes):
  `docker compose --profile agents --profile collectors up -d --no-build --force-recreate <service>`.
  Compose service names differ from container names (service `collector-tradfi` is container `sentinel-tradfi`).

## Environment gotchas (Windows host, Git Bash)

- Prefix `docker exec` with `MSYS_NO_PATHCONV=1` when an argument is a container path.
- Multi-line quoting in Bash heredocs breaks easily: write a script file with the Write tool, then run it.
- Containers have no `kill` or `ps`; signal a process with `python -c "import os,signal; os.kill(PID, signal.SIGTERM)"`.
- `docker logs --since` is unreliable here: pipe the full log through `grep`/`tail` instead.
- Redis and Postgres need credentials from `.env`. Reach them through the app (`from shared.db import get_redis`) or
  `psql -U "$POSTGRES_USER"` inside the container. Never print `.env` or `docker inspect` environment blocks.

## How to work

- **Find the root cause before patching.** Trace callers, consumers, schemas and tests of anything you change; grep every producer and consumer of a Kafka topic or Redis key before changing its shape.
- **Minimal diffs.** Change only what the task needs. Don't reformat, rename or rewrite comments in unrelated code. Match the surrounding comment density and idiom.
- **No placeholders.** Never leave `TODO`, `...` or "rest unchanged" in code.
- **Typed and validated.** No `Any` without a reason, return types on new functions, Pydantic models at every external boundary (HTTP, Kafka payloads, model output).
- **Keep pure logic apart from I/O.** Put pure functions in `shared/utils/<topic>.py`, put the service or job in `services/`, and give the pure part its own tests.
- **Never block the event loop.** Offload CPU-bound or synchronous I/O with `asyncio.to_thread` or an executor.
- **Read large files in targeted ranges** (`offset` and `limit`), and cap command output (`| tail -n 30`, `-q`).

## Invariants learned the hard way

- **Persist before publish.** A message that references a row (`correlation_id`, `scenario_id`) is sent only after that row is committed. Consumers check the row exists before spending inference on it.
- **Failures are counted, never silent.** Use `dropped(...)` or `swallowed(...)` from the shared telemetry utilities; a guard test scans for silent drops. A caught error must not be reported as success (counters, heartbeats, broadcasts).
- **Don't retry overload.** A model timeout means the host is saturated: don't retry the same model or resend the same long prompt to another model. Retry only malformed output.
- **Batch jobs bound their retries.** Contain each item's errors so one failure can't abort the run. Count attempts and park an item after a small limit.
- **Missing data is not "no filter."** When a live key (for example the monthly factor scores) is absent, fall back to the stored copy. Never treat an empty set as "everything qualifies".
- **Inference is the scarcest resource.** Every new model call states its trigger, a dedupe or cap, its admission check, and what consumes its output. Tier and grading gates apply to every producer of alerts.
- **Guard tests encode past incidents.** When one fails, fix the code or update the test deliberately with a comment explaining why. Never skip, delete or weaken a test to go green.

## Trading and data rules

- **Paper trading only.** The auto-trader trades the factor model's picks (book B1), never SPY, QQQ or index funds.
- Changes to sizing, caps, kill-switch or execution logic need explicit approval.
- **Free data only:** no paid feeds, and no new third-party packages when the standard library or existing dependencies suffice.
- **SEC requests** use `SEC_USER_AGENT` from `.env` and go through `sec_slot()` rate limiting.

## Safety

- Never hardcode or echo secrets; they live in `.env` (template: `.env.example`).
- Ask before anything destructive or hard to reverse: `git reset --hard`, `git clean`, `rm -rf`, dropping or truncating tables, deleting volumes (`make nuke`), or switching branches with uncommitted work.
- Commit only when asked.
- Don't hand-edit generated files or lockfiles (`package-lock.json`, `.next/`, `__generated__/`).

## Definition of done

1. Lint is clean on the changed Python files, and `tsc --noEmit` passes if the frontend changed.
2. The tests covering the changed path pass. If a public interface, topic payload or shared utility changed, the adjacent suites pass too (or the full suite).
3. If it is deployed, check the live logs or data for the expected effect; don't infer success from a green test alone.
4. Report a short changelog (files, failure modes fixed, edge cases), anything skipped or unverified, then a two-sentence plain-English summary.
