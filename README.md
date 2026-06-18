# bt-servant-signal-gateway

A [Signal](https://signal.org/) messenger gateway for **BT Servant** — a thin relay between
Signal and the [`bt-servant-worker`](../bt-servant-worker) (the shared AI "brain"). It is a
sibling to the [Telegram](../bt-servant-telegram-gateway) and
[WhatsApp](../bt-servant-whatsapp-gateway) gateways.

Unlike those gateways (TypeScript Cloudflare Workers), this one is a **long-running Python
service**, because Signal has no hosted webhook API: it talks to a local
[`signal-cli`](https://github.com/AsamK/signal-cli) daemon, which needs persistent disk and a
long-lived connection. See [CLAUDE.md](./CLAUDE.md) for the architecture and rationale.

> Status: **in progress**. `/health` and the outbound signal-cli JSON-RPC client
> (`signal_client.py` — send, reactions, contacts, attachments) are implemented; the inbound
> listener and reply dispatch are built out across the
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

Copy `.env.example` → `.env` and fill in. Secrets (`ENGINE_API_KEY`) are set as Fly secrets in
production (`fly secrets set`), never committed.

| Variable | Required | Default | Description |
|---|---|---|---|
| `SIGNAL_ACCOUNT` | ✅ | — | The bot's Signal number in E.164 (e.g. `+15551234567`); the linked-device account `signal-cli` runs as. |
| `SIGNAL_HTTP_URL` |  | `http://127.0.0.1:8080` | Base URL of the local `signal-cli` daemon (JSON-RPC at `/api/v1/rpc`, SSE at `/api/v1/events`). |
| `ENGINE_BASE_URL` | ✅ | — | Base URL of `bt-servant-worker`. Inbound messages POST to `{ENGINE_BASE_URL}/api/v1/chat/callback`. |
| `ENGINE_ORG` |  | `unfoldingWord` | Organization slug sent as `org` on each request. |
| `ENGINE_API_KEY` | ✅ | — | Bearer token for the worker **and** the shared secret the worker echoes back as `X-Engine-Token`. **Secret.** |
| `GATEWAY_PUBLIC_URL` | ✅ | — | Public URL of *this* gateway. The worker calls back `{GATEWAY_PUBLIC_URL}/progress-callback`. |
| `HOST` |  | `0.0.0.0` | Bind address for this gateway's callback server (`/health`, `/progress-callback`). |
| `PORT` |  | `8081` | Bind port for this gateway's callback server. |
| `CHUNK_SIZE` |  | `1500` | Max characters per outbound Signal message; longer replies are split. |
| `MESSAGE_AGE_CUTOFF_SECONDS` |  | `3600` | Drop inbound messages older than this (avoids replaying a backlog after downtime). |
| `SIGNAL_GROUP_ALLOWED_USERS` |  | _(empty)_ | Comma-separated allowed group member ids, or `*` for all. Empty = groups disabled. |
| `SIGNAL_REQUIRE_MENTION` |  | `true` | In groups, only respond when the bot is @mentioned. |

## Local development

```bash
uv sync                      # install deps into .venv
cp .env.example .env         # then edit
uv run python -m bt_signal_gateway   # boots the callback server (/health) + listener (stub); Ctrl-C to stop

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

This gateway implements the standard, channel-neutral BT Servant gateway contract against
[`bt-servant-worker`](../bt-servant-worker) — **no worker changes are needed for Signal**.

**Inbound (gateway → worker).** `POST {ENGINE_BASE_URL}/api/v1/chat/callback` with
`Authorization: Bearer {ENGINE_API_KEY}`. Body:

| Field | Value |
|---|---|
| `client_id` | `"signal-gateway"` |
| `user_id` | Signal source UUID (E.164 number fallback) |
| `message_type` | `"text"` or `"audio"` |
| `message` / `audio_base64` + `audio_format` | text body, or base64 audio for voice notes |
| `message_key` | the Signal message timestamp (used for dedup) |
| `progress_callback_url` | `{GATEWAY_PUBLIC_URL}/progress-callback` |
| `progress_mode` | `"complete"` — Signal has no message editing, so we want one final reply, not streamed edits |
| `org` | `ENGINE_ORG` |
| `chat_type` / `chat_id` / `speaker` | set for group messages (`chat_type="group"`) |

The worker returns `202 Accepted` immediately.

**Outbound (worker → gateway).** The worker POSTs progress to
`{GATEWAY_PUBLIC_URL}/progress-callback`, guarded by the `X-Engine-Token` header (which must
equal `ENGINE_API_KEY`). Payloads are typed `status` / `progress` / `complete` / `error`; on
`complete` the gateway takes `text` (plus any `voice_audio_url` / `attachments[]`), splits it at
`CHUNK_SIZE`, and sends via the `signal-cli` JSON-RPC `send` method. Because the worker does not
retry idempotently, the gateway **dedups on `message_key`**.

See [`../bt-servant-worker`](../bt-servant-worker) and [CLAUDE.md](./CLAUDE.md) for the full
contract.
