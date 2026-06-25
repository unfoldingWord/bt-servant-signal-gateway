# bt-servant-signal-gateway

A [Signal](https://signal.org/) messenger gateway for **BT Servant** — a thin relay between
Signal and the [`bt-servant-worker`](../bt-servant-worker) (the shared AI "brain"). It is a
sibling to the [Telegram](../bt-servant-telegram-gateway) and
[WhatsApp](../bt-servant-whatsapp-gateway) gateways.

Unlike those gateways (TypeScript Cloudflare Workers), this one is a **long-running Python
service**, because Signal has no hosted webhook API: it talks to a local
[`signal-cli`](https://github.com/AsamK/signal-cli) daemon, which needs persistent disk and a
long-lived connection. See [CLAUDE.md](./CLAUDE.md) for the architecture and rationale.

> Status: **shipped**. DM text, group (mention-gated), and voice-note round-trips all work
> end-to-end against the worker. Implemented: `/health`, the outbound signal-cli JSON-RPC client
> (`signal_client.py` — send, reactions, contacts, attachments, voice notes), the inbound SSE
> listener (`signal_listener.py` + `envelope.py` — parse, filter, normalize, group + mention
> gating), the engine client (`engine_client.py` — relays accepted messages to the worker via
> `POST /api/v1/chat/callback`), reply dispatch (`callback_server.py` + `dispatch.py`), media
> handling (`media.py` — inbound audio + outbound voice/attachments), and the container + Fly.io
> deploy pipeline (`Dockerfile`, `supervisord.conf`, `docker-compose.yml`, `fly.toml`, deploy
> workflows). See **[Groups](#groups)** and the **[manual smoke checklist](#manual-smoke-checklist)**
> below for operating + verifying the gateway.

## Architecture

```
Signal app  ⇄  signal-cli daemon (JSON-RPC + SSE)  ⇄  this gateway  ⇄  bt-servant-worker
```

- **Inbound:** subscribe to signal-cli's SSE event stream → normalize → `POST /api/v1/chat/callback`
  on the worker (`client_id="signal-gateway"`, `progress_mode="iteration"`); a best-effort 👀
  reaction acknowledges receipt.
- **Outbound:** the worker calls back `{GATEWAY_PUBLIC_URL}/progress-callback` → chunk + send via
  signal-cli JSON-RPC. Intermediate `progress` updates stream as new messages; on `complete`/`error`
  the 👀 is replaced with ✅/❌.

```
src/bt_signal_gateway/
├── app.py            # async entrypoint (listener + callback server)
├── config.py         # typed settings
├── signal_client.py  # signal-cli JSON-RPC client (send, reactions, attachments)
├── signal_listener.py# inbound SSE listener
├── envelope.py       # Signal envelope → InboundMessage
├── engine_client.py  # POST /api/v1/chat/callback (worker contract)
├── callback_server.py# FastAPI: /health, /progress-callback
├── dispatch.py       # worker callback → Signal reply (text + media)
├── media.py          # inbound audio encode + outbound media download
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
| `SIGNAL_GROUP_ALLOWED_USERS` |  | _(empty)_ | Comma-separated allowed **group IDs** (the `groupId` from each group's `groupInfo`), or `*` for all groups. Empty = groups disabled. ⚠️ The name says USERS, but it gates on the group ID, not member ids. See [Groups](#groups). |
| `SIGNAL_REQUIRE_MENTION` |  | `true` | In groups, only respond when the bot is @mentioned. |

## Groups

The gateway supports Signal group chats, but they are **disabled by default** — you must opt in
per group.

- **Enable:** put the group's ID (or `*` for every group) in `SIGNAL_GROUP_ALLOWED_USERS`. An
  empty value means groups are off and group messages are silently dropped.
- **Mention gate:** with `SIGNAL_REQUIRE_MENTION=true` (the default) the bot only replies when it
  is **@mentioned** in the group; messages that don't mention it are ignored. Set it to `false`
  to answer every (allowed-group) message.
- **Finding a group's ID:** the `groupId` arrives on every inbound group envelope as
  `groupInfo.groupId` (log at `DEBUG` to see dropped/allowed decisions), or list known groups
  with `signal-cli -a "$SIGNAL_ACCOUNT" listGroups`.
- **Speaker + history:** for group messages the gateway sends `chat_type="group"`, the group's
  `chat_id`, and the sender's display name as `speaker`, so the worker keeps shared per-group
  history and knows who spoke. Replies route back to the group via its `groupId`.

### Enabling a group

```bash
# allow one specific group
fly secrets set SIGNAL_GROUP_ALLOWED_USERS='<groupId>' --app bt-servant-signal-gateway
# …or allow all groups the bot is added to
fly secrets set SIGNAL_GROUP_ALLOWED_USERS='*' --app bt-servant-signal-gateway
```

Then add the bot's Signal number to the group and @mention it.

## Manual smoke checklist

There is no automated live end-to-end test (Signal has no inbound webhook to inject against, and
a real round-trip needs a second registered account). Use this checklist to verify a deployment
by hand — each line is an action and the expected result:

- **DM text round-trip** — DM the bot; an AI reply arrives.
- **Group message (mention-gated)** — in an allowed group, @mention the bot → it replies; send a
  message **without** the mention → it stays silent.
- **Voice-note round-trip** — send a voice note; the worker transcribes it and the reply arrives
  (delivered as a voice note when the worker returns one).
- **Long-message chunking** — trigger a reply longer than `CHUNK_SIZE`; it arrives split across
  multiple Signal messages, in order.
- **`error` fallback** — when the worker posts an `error` callback, the bot sends the fixed
  fallback message rather than going silent.
- **Duplicate-callback dedup** — a repeated `complete` callback with the same `message_key` does
  not double-send.
- **Reconnect after signal-cli restart** — restart the daemon (`supervisorctl restart
  signal-cli`); the SSE listener reconnects and a fresh DM still gets a reply.

## Local development

```bash
uv sync                      # install deps into .venv
cp .env.example .env         # then edit
uv run python -m bt_signal_gateway   # boots the callback server (/health) + inbound SSE listener; Ctrl-C to stop

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

### With Docker Compose (full stack)

`docker compose` runs the **same image as production** — signal-cli and the gateway together in
one container — with a named volume standing in for the Fly Volume, so signal-cli state survives
`down`/`up`. Use this when you need a live signal-cli daemon, not just the Python halves.

```bash
cp .env.example .env         # then edit (SIGNAL_ACCOUNT, ENGINE_*, GATEWAY_PUBLIC_URL)
docker compose up --build
curl localhost:8081/health   # {"ok": true, "service": "bt-servant-signal-gateway"}
```

`SIGNAL_HTTP_URL` is forced to `http://127.0.0.1:8080` inside the container (both processes share
loopback). On first boot signal-cli has no registered account yet and will restart until you run
the one-time registration below (against the compose volume):

```bash
docker compose exec gateway supervisorctl stop signal-cli
docker compose exec gateway signal-cli --config /data/signal-cli -a "$SIGNAL_ACCOUNT" register --voice --captcha "<token>"
docker compose exec gateway signal-cli --config /data/signal-cli -a "$SIGNAL_ACCOUNT" verify <code>
docker compose exec gateway supervisorctl start signal-cli
```

See **Provisioning the Signal account** below for where the captcha token and code come from.

## Deployment

Hosted on **[Fly.io](https://fly.io)** as **one always-on Machine** with a persistent **Volume**
mounted at `/data` for signal-cli's account + double-ratchet state. The container runs two
processes under `supervisord` (`supervisord.conf`): the `signal-cli` JSON-RPC daemon (loopback
`127.0.0.1:8080`, never publicly exposed) and the Python gateway (callback server on `8081`,
which Fly publishes as `GATEWAY_PUBLIC_URL`). Config lives in `fly.toml`; the build is the repo
`Dockerfile`.

> ⚠️ **Single instance only.** A Signal account can be primary in exactly one place — never run
> two `signal-cli` daemons against one number (it corrupts the session). Scale-to-zero is disabled
> (`min_machines_running = 1`, `auto_stop_machines = false`) because signal-cli must stay
> connected. On deploy/restart Fly sends `SIGTERM` and waits (`kill_timeout`) so signal-cli can
> flush ratchet state before `SIGKILL`.

All infrastructure lives under the **unfoldingWord** Fly org, never a personal account.

### CI/CD

| Workflow | Trigger | Target |
|---|---|---|
| `deploy-staging.yml` | **manual `workflow_dispatch` only** (auto-deploy on green CI disabled until staging has a registered Signal number — see [Staging's account](#one-time-fly-setup-unfoldingword-org)) | `bt-servant-signal-gateway-staging` |
| `deploy.yml` | manual `workflow_dispatch` (+ a guard that refuses a commit whose CI isn't green) | `bt-servant-signal-gateway` |

Both authenticate via a `FLY_API_TOKEN` secret and run `flyctl deploy`. The workflows are scoped
to GitHub **Environments** (`staging` / `production`), each holding its own **app-scoped**
`FLY_API_TOKEN`; GitHub holds only those tokens, while every runtime value lives in Fly secrets.
A required reviewer on the `production` environment is recommended.

### One-time Fly setup (unfoldingWord org)

The org slug is `unfoldingword-949` and the apps live in `iad` (must match `fly.toml`'s
`primary_region` and the Volume region).

```bash
fly auth login                                   # authenticate (interactive)
fly orgs list                                    # confirm the org slug (unfoldingword-949)

# Apps (staging + production)
fly apps create bt-servant-signal-gateway --org unfoldingword-949
fly apps create bt-servant-signal-gateway-staging --org unfoldingword-949

# One Volume per app, in fly.toml's primary_region
fly volumes create signal_data --app bt-servant-signal-gateway --region iad --size 1
fly volumes create signal_data --app bt-servant-signal-gateway-staging --region iad --size 1

# App-scoped deploy token → per-environment GitHub secret
fly tokens create deploy --app bt-servant-signal-gateway-staging | gh secret set FLY_API_TOKEN --env staging
fly tokens create deploy --app bt-servant-signal-gateway         | gh secret set FLY_API_TOKEN --env production

# Runtime secrets in Fly (NOT GitHub), per app. Staging points at the staging worker.
fly secrets set \
  ENGINE_API_KEY=… \
  SIGNAL_ACCOUNT=+1XXXXXXXXXX \
  ENGINE_BASE_URL=https://api.btservant.ai \
  ENGINE_ORG=unfoldingWord \
  GATEWAY_PUBLIC_URL=https://bt-servant-signal-gateway.fly.dev \
  --app bt-servant-signal-gateway
# staging: ENGINE_BASE_URL=https://staging-api.btservant.ai
#          GATEWAY_PUBLIC_URL=https://bt-servant-signal-gateway-staging.fly.dev

fly deploy --app bt-servant-signal-gateway       # first deploy (or let the workflow do it)
```

> ⚠️ **Staging's account — do NOT leave an unregistered number running.** Because a Signal number
> is single-homed, staging **cannot** share the production GV number. It is tempting to point
> staging's `SIGNAL_ACCOUNT` at a placeholder (e.g. `+15555550100`) and let it ride — **don't.**
> `signal-cli` can't open a session for an unregistered number, so it logs `User … is not
> registered` and exits **~90s** after start. Because that's longer than supervisord's
> `startsecs=5`, supervisord counts each run as a *successful* start, **resets the retry counter,
> and respawns it forever** — `startretries=5` never caps it. The result is an **infinite restart
> loop**: a fresh JVM every ~90s plus the gateway logging `signal sse: stream error` /
> `health check error` **every ~5s**, indefinitely. `/health` stays green, so nothing alerts —
> it just quietly burns CPU and floods `fly logs` (this is what "Signal staging is hammering
> fly.io" looks like). **Don't run a staging Signal daemon against an unregistered number.**
> Instead, do one of:
>
> - **Don't keep staging running** — `fly machine destroy` it when idle (staging is only a
>   deploy/health target). ⚠️ note `deploy-staging.yml` **recreates it on the next green CI on
>   `main`**, so to stop it permanently also disable that workflow or make staging gateway-only.
> - **Run staging gateway-only** — gate the `signal-cli` supervisord program off on staging
>   (no daemon → no loop, `/health` still works). *(durable fix — not yet implemented; see the
>   tracking issue.)*
> - **Give staging its own registered number** — the full fix, if staging needs a live daemon.

## Provisioning the Signal account

The bot uses a **Google Voice number** (provisioned under an unfoldingWord Google account), and
`signal-cli` is **registered as the primary device** on it — no physical phone or smartphone
Signal app. This is a one-time bootstrap that writes state to the persistent Volume; the
supervised daemon holds a lock on the config dir, so stop it first.

```bash
fly ssh console --app bt-servant-signal-gateway
# inside the Machine:
supervisorctl stop signal-cli
signal-cli --config /data/signal-cli -a +1XXXXXXXXXX register --voice --captcha "<signalcaptcha://…>"
signal-cli --config /data/signal-cli -a +1XXXXXXXXXX verify <code>
signal-cli --config /data/signal-cli -a +1XXXXXXXXXX updateProfile --name "BT Servant"
signal-cli --config /data/signal-cli -a +1XXXXXXXXXX updateAccount --username btservant   # returns the username (e.g. btservant.45) + signal.me link
supervisorctl start signal-cli
```

- **Captcha token:** open <https://signalcaptchas.org/registration/generate.html>, solve it, and
  copy the resulting `signalcaptcha://…` link.
- **`--voice`:** Google Voice often won't receive Signal's SMS, so request the **voice call** and
  read the code from the Google Voice inbox/transcript.
- **Profile name + username (both matter for first contact):** `updateProfile --name` sets the
  display name users see ("BT Servant"); `updateAccount --username` registers a username and returns
  a `signal.me` link — the recommended way for users to start a chat (see **[Contacting the
  bot](#contacting-the-bot-username-link)**). Pass a bare nickname; Signal appends a `.NN`
  discriminator and the link encodes it. You can also set/change the username on the **running**
  daemon via the JSON-RPC `updateAccount` method instead of stopping it.
- **State persists:** restart the Machine (`fly apps restart bt-servant-signal-gateway`) and
  confirm signal-cli comes back up **without re-registering** — proof the Volume holds the state.

### Trust model (`--trust-new-identities=always`)

The daemon runs with `--trust-new-identities=always` (see `supervisord.conf`). signal-cli's
default mode, `on-first-use`, trusts a contact's identity key the first time it sees it but then
**refuses to send** if that key ever changes — and a key changes every time a user reinstalls
Signal, switches phones, or resets app data. In that default, the bot silently stops replying to
that user until an operator manually runs `signal-cli … trust …`, which is untenable for a public
bot. `always` auto-trusts new **and changed** keys, so conversations survive reinstalls with no
intervention.

This is the standard configuration for an automated/bot account. The tradeoff is that signal-cli
no longer warns on a mid-conversation key change (the safety-number "this could be a MITM"
check) — a protection that requires out-of-band safety-number verification we can't do with
strangers anyway. It does **not** weaken Signal's end-to-end message encryption.

### Contacting the bot (username link)

**Share the username link, not the phone number.** A Signal account has two identities: a
permanent **ACI** (account identity, carries the "BT Servant" profile name) and a **PNI**
(phone-number identity). Starting a chat by *typing the number* keys it to the PNI, while the
bot replies from its ACI — Signal doesn't reliably merge the two, so the user sees a confusing
**duplicate conversation** (one "unknown"/number thread, one "BT Servant" thread). Starting from
the **username link** resolves straight to the ACI, so the chat opens as a single clean
**BT Servant** conversation. (This was the [first-contact quirk](https://github.com/unfoldingWord/bt-servant-signal-gateway/issues/37).)

- **Username:** `btservant.45`
- **Link + QR:** [`qr_codes/`](./qr_codes) — PNG + SVG + the regen command. Hand these out for
  onboarding.

Two things are **inherent Signal UX** and can't be removed by the gateway, even via the link:
the one-time **"message request → Accept"** tap on first contact, and the **"unverified"**
safety-number label (cosmetic; clears once the session establishes).

> The username/link is tied to the account; if the username is ever deleted/reset the link
> changes and the QR codes must be regenerated (see [`qr_codes/README.md`](./qr_codes/README.md)).

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
| `progress_mode` | `"iteration"` (+ `progress_throttle_seconds: 3`) — the worker streams intermediate updates we relay as new messages, matching the Telegram/WhatsApp gateways |
| `org` | `ENGINE_ORG` |
| `chat_type` / `chat_id` / `speaker` | set for group messages (`chat_type="group"`) |

The worker returns `202 Accepted` immediately.

**Outbound (worker → gateway).** The worker POSTs progress to
`{GATEWAY_PUBLIC_URL}/progress-callback`, guarded by the `X-Engine-Token` header (which must
equal `ENGINE_API_KEY`; a missing/wrong token gets `401`). Payloads are typed `status` /
`progress` / `complete` / `error`. The gateway acks quickly and delivers off the ack path, so a
slow signal-cli send never blocks the worker's webhook. `status` (text-less) is acked and dropped;
`progress` splits its intermediate `text` at `CHUNK_SIZE` and sends each chunk as a **new** message
(Signal has no in-place editing, but the sibling gateways don't edit either — they send new messages
too); on `complete` it splits `text` at `CHUNK_SIZE` and sends each chunk via the `signal-cli`
JSON-RPC `send` method; on `error` it sends a fixed fallback message. Replies route to the
originating group (`chat_id`) or DM (`user_id`). Because the worker does not retry idempotently, the
gateway **dedups `complete` on `message_key`** — `progress` is fire-and-forget and never deduped.

A 👀 reaction is placed on the inbound message when it's received; the terminal callback replaces it
with ✅ (`complete`) or ❌ (`error`) — Signal keeps one reaction per author per message. Reactions
are best-effort: a failure is logged and never blocks the relay or the reply.

Media on `complete` is delivered after the text: a `voice_audio_url` (with `voice_audio_base64`
fallback) is sent as a playable Signal **voice note**, and `attachments[]` (pdf/audio) are sent as
file attachments (batched ≤32 per RPC, paced by the attachment rate-limit scheduler). All media is
downloaded HTTPS-only with the engine bearer token to a temp workspace signal-cli reads off the
shared volume, then cleaned up. Inbound audio attachments are fetched, base64-encoded, and sent to
the worker as an `audio` request; **non-audio inbound attachments are not yet relayed** (the
worker's inbound contract is text/audio only) — tracked separately.

See [`../bt-servant-worker`](../bt-servant-worker) and [CLAUDE.md](./CLAUDE.md) for the full
contract.
