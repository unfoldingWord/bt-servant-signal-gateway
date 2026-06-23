"""Engine client: submits inbound messages to bt-servant-worker.

Relays each accepted :class:`~bt_signal_gateway.envelope.InboundMessage` to the
worker over the shared gateway contract:
``POST {ENGINE_BASE_URL}/api/v1/chat/callback`` (the *callback* transport), with
``Authorization: Bearer {ENGINE_API_KEY}``. The worker acks with ``202`` and
later calls back into ``{GATEWAY_PUBLIC_URL}/progress-callback`` with the reply
(that delivery path lives in a separate issue).

``progress_mode`` is fixed to ``"complete"``: Signal has no message editing, so
the worker should emit a single final reply rather than incremental progress.

Per the worker's ``ChatRequest`` contract
(``../bt-servant-worker/src/types/engine.ts`` /
``../bt-servant-worker/src/utils/chat-validation.ts``):

- the callback transport requires ``progress_callback_url`` and ``message_key``;
- group messages set ``chat_type="group"`` + ``chat_id`` (required), and carry the
  sender's display name as ``speaker``;
- a ``429 CONCURRENT_REQUEST_REJECTED`` carries ``retry_after_ms`` (and a
  ``Retry-After`` header) which we honor with a bounded retry. The callback
  transport currently enqueues rather than 429-ing, but we handle it defensively.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from bt_signal_gateway.config import Settings
from bt_signal_gateway.envelope import InboundMessage
from bt_signal_gateway.media import InboundAudio

logger = logging.getLogger(__name__)

#: Identifies this gateway to the worker (selects channel-neutral routing).
CLIENT_ID = "signal-gateway"

#: Total POST attempts before giving up on a rate-limited submit.
_MAX_ATTEMPTS = 3
#: Fallback wait when a 429 carries no usable hint (matches the worker's 5000ms).
_DEFAULT_RETRY_AFTER_S = 5.0
#: Upper bound on any honored retry delay, so a pathological hint can't stall us.
_RETRY_AFTER_CAP_S = 60.0


def build_chat_request(
    message: InboundMessage,
    settings: Settings,
    *,
    audio: InboundAudio | None = None,
) -> dict[str, Any]:
    """Build the worker ``ChatRequest`` body for an inbound Signal message.

    When *audio* is provided the request is an ``audio`` type carrying
    ``audio_base64`` + ``audio_format`` (any text rides along as the ``message``
    caption); otherwise it's a plain ``text`` request. Group messages add the
    ``chat_type``/``chat_id``/``speaker`` fields the worker needs for group
    context.
    """
    body: dict[str, Any] = {
        "client_id": CLIENT_ID,
        "user_id": message.user_id,
        "message_type": "audio" if audio else "text",
        "message": message.text,
        "message_key": str(message.timestamp_ms),
        "progress_callback_url": settings.progress_callback_url,
        "progress_mode": "complete",
        "org": settings.engine_org,
    }
    if audio:
        body["audio_base64"] = audio.audio_base64
        body["audio_format"] = audio.audio_format
    if message.is_group:
        body["chat_type"] = "group"
        body["chat_id"] = message.chat_id
        if message.sender_name:
            body["speaker"] = message.sender_name
    return body


def _retry_after_seconds(response: httpx.Response) -> float:
    """Seconds to wait before retrying a 429, from the body or ``Retry-After``.

    Prefers the worker's ``retry_after_ms`` body field, falls back to the
    ``Retry-After`` header (seconds), then to :data:`_DEFAULT_RETRY_AFTER_S`. The
    result is clamped to ``[0, _RETRY_AFTER_CAP_S]``.
    """
    wait: float | None = None

    try:
        retry_after_ms = response.json().get("retry_after_ms")
    except (ValueError, AttributeError):
        retry_after_ms = None
    if isinstance(retry_after_ms, (int, float)):
        wait = retry_after_ms / 1000.0

    if wait is None:
        header = response.headers.get("Retry-After")
        if header is not None:
            try:
                wait = float(header)
            except ValueError:
                wait = None

    if wait is None:
        wait = _DEFAULT_RETRY_AFTER_S
    return min(max(wait, 0.0), _RETRY_AFTER_CAP_S)


class EngineClient:
    """Async HTTP client that relays inbound messages to bt-servant-worker."""

    def __init__(self, settings: Settings, *, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._url = f"{settings.engine_base_url.rstrip('/')}/api/v1/chat/callback"
        # When a client is injected (tests / shared client) we don't own its
        # lifecycle and must not close it in aclose().
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=30.0)

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this instance owns it."""
        if self._owns_client:
            await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._settings.engine_api_key}",
            "Content-Type": "application/json",
        }

    async def submit(self, message: InboundMessage, *, audio: InboundAudio | None = None) -> bool:
        """Submit *message* to the worker; return ``True`` on a ``202`` ack.

        When *audio* is provided the submission is an ``audio`` request carrying
        the encoded bytes. Retries on ``429`` honoring its retry hint (up to
        :data:`_MAX_ATTEMPTS` total attempts). Any other non-2xx response, or a
        transport error, returns ``False`` without retrying.
        """
        body = build_chat_request(message, self._settings, audio=audio)
        headers = self._headers()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                response = await self._client.post(self._url, json=body, headers=headers)
            except httpx.HTTPError as exc:
                logger.warning(
                    "engine: submit failed",
                    extra={"message_key": body["message_key"], "error": str(exc)},
                )
                return False

            if response.is_success:
                logger.info(
                    "engine: submitted",
                    extra={"message_key": body["message_key"], "status": response.status_code},
                )
                return True

            if response.status_code == 429:
                wait = _retry_after_seconds(response)
                if attempt >= _MAX_ATTEMPTS:
                    logger.error(
                        "engine: rate-limit retries exhausted",
                        extra={"message_key": body["message_key"], "attempts": attempt},
                    )
                    return False
                logger.warning(
                    "engine: rate-limited, retrying",
                    extra={
                        "message_key": body["message_key"],
                        "attempt": attempt,
                        "retry_after_s": wait,
                    },
                )
                await asyncio.sleep(wait)
                continue

            logger.error(
                "engine: submit rejected",
                extra={
                    "message_key": body["message_key"],
                    "status": response.status_code,
                    "body": response.text[:500],
                },
            )
            return False

        return False
