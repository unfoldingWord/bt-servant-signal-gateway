# Claude Instructions - bt-servant-signal-gateway

## CRITICAL: Branching Model (develop → main)

Two long-lived branches, since 2026-11:

- **`develop`** is the integration branch and the default branch. Every feature branch and
  every PR targets `develop`. Bot review plus CI is enough to merge there.
- **`main`** is the release branch. It only ever receives `develop` (a release PR, "Release
  vX.Y.Z: develop → main") or a `hotfix/*` branch. Only the repo owner can merge to `main`
  (branch protection restricts pushes and requires a code-owner review, see
  `.github/CODEOWNERS`). Production deploys from `main` by tag.
- **Rule: `develop` is always releasable.** Work that is not ready to ship stays on its
  feature branch, or merges behind a flag that is off in production. Never cherry-pick into
  `main`; if something on `develop` must not ship, revert it on `develop` or flag it off, then
  release.
- **Hotfix:** branch from `main` as `hotfix/<name>`, PR into `main`, then open a PR from
  `main` into `develop` the same day so `develop` stays ahead of `main`.
- **Release:** merge the release PR, tag `v<version>` on the resulting `main` commit, then
  dispatch the production deploy on that tag: `gh workflow run deploy.yml --ref v<version>`.
- **Merge methods:** feature PRs into `develop` are **squash**-merged (`develop` requires
  linear history). Release and hotfix PRs into `main` use a **merge commit**, never squash or
  rebase. A squash would copy `develop`'s work into a new commit that `develop` never
  contains, so the branches would diverge and every later release PR would re-list
  already-shipped commits. With merge commits, the next release PR shows only what is new, and
  `git log --first-parent main` lists one commit per release. Never delete `develop` when
  merging a release PR.
- **Up to date before merge:** `develop` requires a PR branch to be current with `develop`
  before it merges ("Update branch"), so two PRs that pass alone cannot break together. `main`
  does not, because `develop` never contains `main`'s release merge commits.

CI's `Release Source` job refuses a PR into `main` from any other branch. The production
deploy (`deploy.yml`) refuses to run on anything but a `v*` tag that is on `main`, and also
refuses a commit whose CI isn't green.

## CRITICAL: Ask Before Pushing

**ALWAYS ask the user before pushing changes.** Always create a feature branch and a PR
against `develop`; never push to `develop` or `main` directly.

## CRITICAL: Never Deploy Directly

**NEVER run `fly deploy` / `flyctl deploy` directly.** All deploys go through the GitHub
Actions workflows:

1. Commit, push a feature branch, open a PR against `develop`, wait for CI, get approval, merge.
2. **Staging is deployed manually** (`deploy-staging.yml` is `workflow_dispatch` only — staging
   has no registered Signal number, so auto-deploy is deliberately disabled; see the README
   "Staging's account"). Dispatch it on `develop`: `gh workflow run deploy-staging.yml --ref develop`.
3. **Production:** the owner merges a release PR (`develop` → `main`), tags `v<version>` on
   `main`, and dispatches `deploy.yml` on that tag.

The ONLY exception is if the user explicitly asks for a direct deploy (emergency hotfix).

## Version Bumping and Release Tags

Every PR bumps the version in `pyproject.toml` (patch for fixes/docs/CI, minor for new
behavior) and refreshes `uv.lock` with `uv lock` so the lockfile check stays green. Feature
PRs into `develop` are not tagged. After a release PR (`develop` → `main`) merges, tag the
release on `main`:

```bash
git checkout main && git pull
git tag v<version> && git push --tags
```

Tags are created on `main` after a release merge, never on `develop`.

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
├── signal_client.py  # signal-cli JSON-RPC client (send, sendTyping, voice notes/attachments, sendReaction, listContacts, getAttachment)
├── signal_listener.py# inbound SSE listener (reconnect/backoff)
├── signal_rate_limit.py # attachment-send pacing scheduler
├── envelope.py       # Signal envelope → normalized InboundMessage + filters (group + mention gating)
├── engine_client.py  # POST {ENGINE_BASE_URL}/api/v1/chat/callback
├── callback_server.py# FastAPI: GET /health, POST /progress-callback (X-Engine-Token)
├── dispatch.py       # progress/complete/error callback → chunk + send to Signal (text + media) + ✅/❌ react
├── media.py          # inbound audio encode + outbound media (voice/attachment) download
├── chunking.py       # split long replies at CHUNK_SIZE
├── dedup.py          # in-memory TTL dedup on message_key
└── typing_indicator.py # per-chat typing refresh (10s) and stop on complete/error
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
  replaced by ✅/❌ on the terminal callback. `status` refreshes signal-cli `sendTyping` (no chat
  message, at most once per 10 seconds for the same turn; a new message in that chat starts it
  immediately). `complete` / `error` send `sendTyping` with `stop: true`. The typing indicator also
  starts when the user's message arrives.
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
