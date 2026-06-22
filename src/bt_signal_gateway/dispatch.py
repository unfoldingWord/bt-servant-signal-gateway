"""Dispatch worker callbacks (complete/error) back to Signal.

The worker calls back into ``/progress-callback`` with one of four payload
types (``status`` / ``progress`` / ``complete`` / ``error``). Signal has no
in-place message editing, so the intermediate ``status``/``progress`` events are
ignored at the server layer; this module handles the two terminal events:

- ``complete`` — chunk ``text`` to ``CHUNK_SIZE`` and send each chunk to the
  originating DM or group.
- ``error`` — send a fixed fallback message so the user isn't left hanging.

Recipient routing mirrors the inbound contract in
:func:`~bt_signal_gateway.engine_client.build_chat_request`: a ``chat_id`` is
present only for **group** callbacks (``"group:<groupId>"``); for DMs it is
absent and we fall back to ``user_id``. Either value is handed to
:meth:`~bt_signal_gateway.signal_client.SignalClient.send`, which resolves the
``"group:"`` prefix vs. a direct recipient and applies native Signal formatting.

Media (``voice_audio_url`` / ``attachments``) is tolerated in the payload but
not yet delivered — that lands with inbound/outbound media handling (issue #7).

Ported from ``../bt-servant-telegram-gateway/src/services/response-dispatch.ts``
and ``callback-payload.ts``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from bt_signal_gateway.chunking import chunk_message
from bt_signal_gateway.config import Settings
from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

#: Sent to the user when the worker reports an ``error`` callback.
DEFAULT_FALLBACK_MESSAGE = (
    "Sorry — something went wrong while processing your message. Please try again."
)

_VALID_TYPES = frozenset({"status", "progress", "complete", "error"})


@dataclass(frozen=True, slots=True)
class CallbackPayload:
    """A normalized worker -> gateway callback.

    Only the fields this gateway acts on are modeled. ``text`` carries the reply
    on ``complete``; ``error`` carries the worker's error string (delivery uses
    :data:`DEFAULT_FALLBACK_MESSAGE` regardless). ``chat_id`` is set for group
    callbacks only.
    """

    type: str
    user_id: str
    message_key: str
    text: str | None = None
    error: str | None = None
    chat_id: str | None = None


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def parse_callback_payload(body: Any) -> CallbackPayload | None:
    """Normalize a raw callback body, or return ``None`` if it's unrecognized.

    Requires a known ``type`` plus non-empty ``user_id`` and ``message_key``.
    Unknown fields (including media) are ignored, not rejected.
    """
    if not isinstance(body, dict):
        return None

    type_ = body.get("type")
    user_id = body.get("user_id")
    message_key = body.get("message_key")

    if type_ not in _VALID_TYPES:
        return None
    if not isinstance(user_id, str) or not user_id:
        return None
    if not isinstance(message_key, str) or not message_key:
        return None

    return CallbackPayload(
        type=type_,
        user_id=user_id,
        message_key=message_key,
        text=_str_or_none(body.get("text")),
        error=_str_or_none(body.get("error")),
        chat_id=_str_or_none(body.get("chat_id")),
    )


def _recipient(payload: CallbackPayload) -> str:
    """Signal recipient for *payload*: the group ``chat_id`` or the DM sender."""
    return payload.chat_id or payload.user_id


async def dispatch_callback(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
) -> bool:
    """Deliver a terminal (``complete`` / ``error``) callback to Signal.

    ``complete`` chunks ``text`` to ``CHUNK_SIZE`` and sends each chunk in order;
    an empty/blank reply sends nothing. ``error`` sends the fallback message.
    Other types are no-ops (the server layer already filters them). Per-chunk
    send failures are logged but do not abort the remaining chunks.

    Returns ``True`` when delivery is fully accounted for (every chunk sent, the
    fallback sent, or there was nothing to send) and ``False`` when any send
    failed. The caller uses this to decide whether to mark the ``message_key``
    as completed: a ``False`` leaves the key eligible for re-delivery so a
    repeated callback can finish the reply.
    """
    recipient = _recipient(payload)
    log_ctx = {
        "message_key": payload.message_key,
        "user_id": payload.user_id,
        "recipient": recipient,
        "type": payload.type,
    }

    if payload.type == "error":
        logger.error("callback: worker reported error", extra={**log_ctx, "error": payload.error})
        return await signal_client.send(recipient, DEFAULT_FALLBACK_MESSAGE)

    if payload.type != "complete":
        logger.debug("callback: ignoring non-terminal type", extra=log_ctx)
        return True

    chunks = chunk_message(payload.text or "", settings.chunk_size)
    if not chunks:
        logger.info("callback: complete with empty text, nothing to send", extra=log_ctx)
        return True

    logger.info("callback: dispatching complete", extra={**log_ctx, "chunks": len(chunks)})
    sent = 0
    for index, chunk in enumerate(chunks):
        if await signal_client.send(recipient, chunk):
            sent += 1
        else:
            logger.warning(
                "callback: chunk send failed",
                extra={**log_ctx, "chunk_index": index, "chunk_count": len(chunks)},
            )

    fully_delivered = sent == len(chunks)
    logger.info(
        "callback: complete dispatched",
        extra={**log_ctx, "sent": sent, "expected": len(chunks), "delivered": fully_delivered},
    )
    return fully_delivered
