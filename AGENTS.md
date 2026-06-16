# AGENTS.md

Guidance for AI coding agents working in this repo. See [CLAUDE.md](./CLAUDE.md) for the full
architecture and system context; this file is the quick operational summary.

## What this is

A thin **Signal** gateway for BT Servant: it relays messages between a local `signal-cli`
daemon and `../bt-servant-worker`. It does **no** AI processing. Unlike the other gateways
(TS Cloudflare Workers), this is a **long-running Python service** on Fly.io with a persistent
volume for signal-cli state.

## Setup

```bash
uv sync
cp .env.example .env   # then edit
```

## Quality gate (must pass before every commit)

```bash
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
# shortcut: make check
```

These are enforced in CI (`.github/workflows/ci.yml`). Do not commit red.

## Conventions

- Python ≥3.11, full type hints; tooling is **uv / ruff / ty / pytest** (matches `../hermes-agent`).
- Keep modules small and single-purpose (see the `src/bt_signal_gateway/` layout in CLAUDE.md).
- Port Signal protocol handling from `../hermes-agent/gateway/platforms/signal.py`; port the
  engine/worker contract from `../bt-servant-telegram-gateway`.
- New external behavior gets a test; network/daemon-dependent tests use `@pytest.mark.integration`.

## Don't

- Don't add AI/LLM logic here — that belongs in the worker.
- Don't commit secrets or signal-cli account state.
