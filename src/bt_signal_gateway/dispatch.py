"""Dispatch worker callbacks (complete/error) back to Signal.

The worker calls back into ``/progress-callback`` with one of four payload
types (``status`` / ``progress`` / ``complete`` / ``error``). Signal has no
in-place message editing, so the intermediate ``status``/``progress`` events are
ignored at the server layer; this module handles the two terminal events:

- ``complete`` — chunk ``text`` to ``CHUNK_SIZE`` and send each chunk to the
  originating DM or group, then deliver any media: a ``voice_audio_url`` /
  ``voice_audio_base64`` voice note and ``attachments[]`` files.
- ``error`` — send a fixed fallback message so the user isn't left hanging.

Recipient routing mirrors the inbound contract in
:func:`~bt_signal_gateway.engine_client.build_chat_request`: a ``chat_id`` is
present only for **group** callbacks (``"group:<groupId>"``); for DMs it is
absent and we fall back to ``user_id``. Either value is handed to
:meth:`~bt_signal_gateway.signal_client.SignalClient.send`, which resolves the
``"group:"`` prefix vs. a direct recipient and applies native Signal formatting.

Media is downloaded (HTTPS-only, engine bearer auth) to a per-delivery temp
workspace that signal-cli reads off the shared volume, then removed wholesale —
see :mod:`bt_signal_gateway.media`. The voice reply prefers ``voice_audio_url``
and falls back to ``voice_audio_base64``; it is delivered as a playable Signal
voice note.

Ported from ``../bt-servant-telegram-gateway/src/services/response-dispatch.ts``
and ``callback-payload.ts``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from bt_signal_gateway.chunking import chunk_message
from bt_signal_gateway.config import Settings
from bt_signal_gateway.media import (
    OutboundAttachment,
    decode_base64_to_temp,
    download_to_temp,
    parse_outbound_attachments,
    temp_workspace,
)
from bt_signal_gateway.signal_client import SignalClient

logger = logging.getLogger(__name__)

#: Filesystem suffix for a voice note downloaded without an extension hint.
_VOICE_FALLBACK_SUFFIX = ".m4a"
#: Timeout for worker media downloads (large files upload serially after fetch).
_DOWNLOAD_TIMEOUT_S = 60.0

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
    voice_audio_url: str | None = None
    voice_audio_base64: str | None = None
    attachments: list[OutboundAttachment] = field(default_factory=list)


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def parse_callback_payload(body: Any) -> CallbackPayload | None:
    """Normalize a raw callback body, or return ``None`` if it's unrecognized.

    Requires a known ``type`` plus non-empty ``user_id`` and ``message_key``.
    Unknown fields are ignored, not rejected; media fields (``voice_audio_url`` /
    ``voice_audio_base64`` / ``attachments``) are parsed when present.
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
        voice_audio_url=_str_or_none(body.get("voice_audio_url")),
        voice_audio_base64=_str_or_none(body.get("voice_audio_base64")),
        attachments=parse_outbound_attachments(body.get("attachments")),
    )


def _recipient(payload: CallbackPayload) -> str:
    """Signal recipient for *payload*: the group ``chat_id`` or the DM sender."""
    return payload.chat_id or payload.user_id


async def dispatch_callback(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> bool:
    """Deliver a terminal (``complete`` / ``error``) callback to Signal.

    ``complete`` chunks ``text`` to ``CHUNK_SIZE`` and sends each chunk in order,
    then delivers any media (voice note + file attachments). A reply with neither
    text nor media sends nothing. ``error`` sends the fallback message. Other
    types are no-ops (the server layer already filters them). Per-item send
    failures are logged but do not abort the rest of the reply.

    ``http_client`` injects an :class:`httpx.AsyncClient` for media downloads
    (tests); when omitted a short-lived client is created and closed here.

    Returns ``True`` when delivery is fully accounted for (every chunk + all
    media sent, the fallback sent, or there was nothing to send) and ``False``
    when any send failed. The caller uses this to decide whether to mark the
    ``message_key`` as completed: a ``False`` leaves the key eligible for
    re-delivery so a repeated callback can finish the reply.
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

    has_media = bool(payload.voice_audio_url or payload.voice_audio_base64 or payload.attachments)
    chunks = chunk_message(payload.text or "", settings.chunk_size)
    if not chunks and not has_media:
        logger.info("callback: complete with empty text, nothing to send", extra=log_ctx)
        return True

    logger.info(
        "callback: dispatching complete",
        extra={**log_ctx, "chunks": len(chunks), "has_media": has_media},
    )

    sent = 0
    for index, chunk in enumerate(chunks):
        if await signal_client.send(recipient, chunk):
            sent += 1
        else:
            logger.warning(
                "callback: chunk send failed",
                extra={**log_ctx, "chunk_index": index, "chunk_count": len(chunks)},
            )
    text_ok = sent == len(chunks)

    media_ok = True
    if has_media:
        media_ok = await _deliver_media(
            payload, signal_client, settings, recipient, log_ctx, http_client
        )

    fully_delivered = text_ok and media_ok
    logger.info(
        "callback: complete dispatched",
        extra={
            **log_ctx,
            "sent": sent,
            "expected": len(chunks),
            "media_ok": media_ok,
            "delivered": fully_delivered,
        },
    )
    return fully_delivered


async def _deliver_media(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    recipient: str,
    log_ctx: dict[str, Any],
    http_client: httpx.AsyncClient | None,
) -> bool:
    """Download and deliver the voice note + file attachments on a ``complete``.

    Everything goes through one per-delivery temp workspace, removed wholesale on
    exit. Returns ``True`` only when every media item is delivered.
    """
    owns_client = http_client is None
    client = http_client or httpx.AsyncClient(timeout=_DOWNLOAD_TIMEOUT_S)
    try:
        with temp_workspace() as workspace:
            ok = True
            if payload.voice_audio_url or payload.voice_audio_base64:
                if not await _deliver_voice_note(
                    payload, signal_client, settings, recipient, workspace, client, log_ctx
                ):
                    ok = False
            if payload.attachments:
                if not await _deliver_attachments(
                    payload.attachments,
                    signal_client,
                    settings,
                    recipient,
                    workspace,
                    client,
                    log_ctx,
                ):
                    ok = False
            return ok
    finally:
        if owns_client:
            await client.aclose()


async def _deliver_voice_note(
    payload: CallbackPayload,
    signal_client: SignalClient,
    settings: Settings,
    recipient: str,
    workspace: Path,
    client: httpx.AsyncClient,
    log_ctx: dict[str, Any],
) -> bool:
    """Deliver the voice reply: try ``voice_audio_url``, fall back to base64."""
    sub = workspace / "voice"
    sub.mkdir(parents=True, exist_ok=True)

    path: Path | None = None
    if payload.voice_audio_url:
        path = await download_to_temp(
            client,
            payload.voice_audio_url,
            sub,
            settings,
            fallback_suffix=_VOICE_FALLBACK_SUFFIX,
        )
        if path is None:
            logger.warning("callback: voice url download failed; trying base64", extra=log_ctx)

    if path is None and payload.voice_audio_base64:
        path = decode_base64_to_temp(
            payload.voice_audio_base64, sub, f"voice{_VOICE_FALLBACK_SUFFIX}"
        )
        if path is None:
            logger.warning("callback: voice base64 decode failed", extra=log_ctx)

    if path is None:
        logger.warning("callback: no deliverable voice audio", extra=log_ctx)
        return False

    if not await signal_client.send_voice_note(recipient, str(path)):
        logger.warning("callback: voice note send failed", extra=log_ctx)
        return False
    return True


async def _deliver_attachments(
    attachments: list[OutboundAttachment],
    signal_client: SignalClient,
    settings: Settings,
    recipient: str,
    workspace: Path,
    client: httpx.AsyncClient,
    log_ctx: dict[str, Any],
) -> bool:
    """Download each attachment (own subdir, so equal filenames don't clash) and
    send them, batched by :meth:`SignalClient.send_attachments`."""
    paths: list[str] = []
    download_ok = True
    for index, att in enumerate(attachments):
        sub = workspace / f"att-{index}"
        sub.mkdir(parents=True, exist_ok=True)
        path = await download_to_temp(client, att.url, sub, settings, filename=att.filename)
        if path is None:
            download_ok = False
            logger.warning(
                "callback: attachment download failed",
                extra={**log_ctx, "attachment_index": index},
            )
            continue
        paths.append(str(path))

    send_ok = True
    if paths:
        send_ok = await signal_client.send_attachments(recipient, paths)
        if not send_ok:
            logger.warning("callback: attachment send failed", extra=log_ctx)

    return download_ok and send_ok
