# bt-servant-signal-gateway

A [Signal](https://signal.org/) messenger gateway for **BT Servant** — a thin relay between
Signal and the [`bt-servant-worker`](../bt-servant-worker) (the shared AI "brain"). It is a
sibling to the [Telegram](../bt-servant-telegram-gateway) and
[WhatsApp](../bt-servant-whatsapp-gateway) gateways.

Unlike those gateways (TypeScript Cloudflare Workers), this one is a **long-running Python
service**, because Signal has no hosted webhook API: it talks to a local
[`signal-cli`](https://github.com/AsamK/signal-cli) daemon, which needs persistent disk and a
long-lived connection. See [CLAUDE.md](./CLAUDE.md) for the architecture and rationale.

> Status: **scaffolding**. Only `/health` is implemented; the relay is built out across the
> issues tracked in the [project epic](https://github.com/unfoldingWord/bt-servant-signal-gateway/issues/11).

## Architecture

```
Signal app  ⇄  signal-cli daemon (JSON-RPC + SSE)  ⇄  this gateway  ⇄  bt-servant-worker
```

- **Inbound:** subscribe to signal-cli's SSE event stream → normalize → `POST /api/v1/chat/callback`
  on the worker (`client_id="signal-gateway"`, `progress_mode="complete"`).
- **Outbound:** the worker calls back `{GATEWAY_PUBLIC_URL}/progress-callback` → chunk + send via
  signal-cli JSON-RPC.

```
src/bt_signal_gateway/
├── app.py            # async entrypoint (listener + callback server)
├── config.py         # typed settings
├── signal_client.py  # signal-cli JSON-RPC client (send, reactions, attachments)
├── signal_listener.py# inbound SSE listener
├── envelope.py       # Signal envelope → InboundMessage
├── engine_client.py  # POST /api/v1/chat/callback (worker contract)
├── callback_server.py# FastAPI: /health, /progress-callback
├── dispatch.py       # worker callback → Signal reply
├── chunking.py       # message splitting
└── dedup.py          # message_key TTL dedup
```

## Requirements

- Python **3.12** (`.python-version`)
- [`uv`](https://docs.astral.sh/uv/)
- A `signal-cli` daemon for running against Signal (see Deployment)

## Environment variables

Copy `.env.example` → `.env` and fill in. Key vars: `SIGNAL_ACCOUNT`, `SIGNAL_HTTP_URL`,
`ENGINE_BASE_URL`, `ENGINE_ORG`, `ENGINE_API_KEY`, `GATEWAY_PUBLIC_URL`, `CHUNK_SIZE`,
`MESSAGE_AGE_CUTOFF_SECONDS`. _(Full table: TODO once config lands.)_

## Local development

```bash
uv sync                      # install deps into .venv
cp .env.example .env         # then edit
uv run python -m bt_signal_gateway   # serves /health (full wiring: TODO)

# quality gate
uv run ruff format --check . && uv run ruff check . && uv run ty check && uv run pytest
# or:
make check
```

Pre-commit hooks mirror the **full CI gate** (`.github/workflows/ci.yml`) so red code never
reaches a commit/push — fast checks (ruff format + check, ty, `uv lock --check`) run on every
commit, the test suite runs on push. Install both hook types once:

```bash
uv run pre-commit install   # installs pre-commit AND pre-push hooks
```

## Deployment

Fly.io, one always-on Machine + a persistent **Volume** for signal-cli state. _(TODO: filled in
by the containerization and Fly deploy issues, including the one-time `signal-cli link` QR
bootstrap.)_

## Engine contract

Implements the standard BT Servant gateway contract against `bt-servant-worker`
(`POST /api/v1/chat/callback` + a `/progress-callback` receiver). See
[`../bt-servant-worker`](../bt-servant-worker) and CLAUDE.md.
