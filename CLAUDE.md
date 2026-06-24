# Claude Instructions - bt-servant-signal-gateway

## System Context

**BT Servant** is an AI-powered Bible translation assistant developed by unfoldingWord. The
system helps Bible translators with AI-assisted drafting, checking, and guidance in many
languages.

It consists of:

1. **bt-servant-worker** (`../bt-servant-worker`) — the core AI worker: language model
   interactions (Claude), user/session state, MCP tool orchestration. All the "brains".
2. **Gateways** — thin per-channel relays: `../bt-servant-telegram-gateway`,
   `../bt-servant-whatsapp-gateway`, and **this repo** (Signal).

This gateway is intentionally "dumb" — it does NO AI processing. The worker serves every
channel; each gateway only does protocol translation and channel auth.

## What makes this gateway different

The Telegram/WhatsApp gateways are stateless TypeScript Cloudflare Workers driven by inbound
webhooks. **Signal has no hosted webhook API.** It requires a local
[`signal-cli`](https://github.com/AsamK/signal-cli) daemon, which needs:

- **persistent, consistent disk** for its linked-device identity + double-ratchet session state, and
- a **long-lived process** holding the inbound SSE connection.

So this gateway is a **long-running Python service** deployed on **Fly.io with a persistent
Volume** (Cloudflare Containers/Sandboxes were evaluated and rejected — ephemeral disk +
stale-snapshot risk is unsafe for signal-cli's live crypto state).

## Architecture

```
Signal app ⇄ signal-cli daemon (JSON-RPC /api/v1/rpc + SSE /api/v1/events) ⇄ this gateway ⇄ worker
```

```
src/bt_signal_gateway/
├── __main__.py       # module entrypoint: `python -m bt_signal_gateway`
├── app.py            # async entrypoint: runs the SSE listener + the uvicorn callback server
├── config.py         # typed settings (pydantic-settings)
├── logging_config.py # logging setup
├── signal_client.py  # signal-cli JSON-RPC client (send + voice notes/attachments, sendReaction, listContacts, getAttachment)
├── signal_listener.py# inbound SSE listener (reconnect/backoff)
├── signal_rate_limit.py # attachment-send pacing scheduler
├── envelope.py       # Signal envelope → normalized InboundMessage + filters (group + mention gating)
├── engine_client.py  # POST {ENGINE_BASE_URL}/api/v1/chat/callback
├── callback_server.py# FastAPI: GET /health, POST /progress-callback (X-Engine-Token)
├── dispatch.py       # progress/complete/error callback → chunk + send to Signal (text + media) + ✅/❌ react
├── media.py          # inbound audio encode + outbound media (voice/attachment) download
├── chunking.py       # split long replies at CHUNK_SIZE
└── dedup.py          # in-memory TTL dedup on message_key
```

Much of `signal_client.py` / `signal_listener.py` / `envelope.py` is ported from
`../hermes-agent/gateway/platforms/signal.py`.

## Engine contract (worker integration)

- Inbound → worker: `POST /api/v1/chat/callback`, `Authorization: Bearer ENGINE_API_KEY`,
  `client_id="signal-gateway"`, `progress_mode="iteration"` (+ `progress_throttle_seconds=3`) so the
  worker streams intermediate `progress` updates we relay as new messages (Signal has no editing, but
  the sibling gateways don't edit either). Group messages set `chat_type="group"`, `chat_id`,
  `speaker`.
- Worker → gateway: callbacks to `{GATEWAY_PUBLIC_URL}/progress-callback` guarded by the
  `X-Engine-Token` header. `progress` is fire-and-forget; only the terminal `complete` is deduped on
  `message_key` (the worker does not retry idempotently). A 👀 reaction acks inbound receipt and is
  replaced by ✅/❌ on the terminal callback.
- The worker is channel-neutral; **no worker changes are needed** for Signal.

## Coding Standards

### Style & tooling

- **Python ≥3.11** (CI/dev on 3.12), 4-space indent, type hints everywhere.
- `snake_case` functions/vars, `PascalCase` classes, `UPPER_SNAKE_CASE` constants.
- Tooling (per `../hermes-agent`): **uv** (deps + lockfile), **ruff** (lint + format),
  **ty** (type check), **pytest**.

### Quality gate — run before committing

```bash
uv run ruff format --check . && uv run ruff check . && uv run ty check && uv run pytest
# or: make check
```

### CRITICAL: linting/types/tests are mandatory

**Never commit unless `ruff` (format + check), `ty`, and `pytest` all pass.** This is
non-negotiable and is enforced in CI (`.github/workflows/ci.yml`) and via pre-commit hooks
(`uv run pre-commit install`).

### Testing

- `uv run pytest` for unit tests. Integration tests are marked `@pytest.mark.integration`
  and excluded by default (they need signal-cli/worker/network).
