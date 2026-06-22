"""Signal envelope parsing into a normalized inbound message.

Turns a raw signal-cli envelope (as delivered over the SSE event stream) into a
gateway-neutral :class:`InboundMessage`, applying the same inbound filtering the
sibling gateways use. The mapping onto the worker ``ChatRequest`` shape lives in
``engine_client`` — this layer stays channel-internal.

Ported from ``../hermes-agent/gateway/platforms/signal.py``
(``_handle_envelope`` / ``_render_mentions``), trimmed to the parse + filter
surface: attachment **references** are captured here, but byte fetching and media
typing are a later issue.

Filtering decisions (see issue #4): every ``syncMessage`` is dropped — receipts,
typing, own-send echoes, and Note-to-Self alike (Note-to-Self is spike #19). With
sync envelopes gone, own-echo is covered by the ``sender == account`` self-source
filter, so no recently-sent-timestamp tracking is needed here.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from bt_signal_gateway.config import Settings

logger = logging.getLogger(__name__)

_GROUP_PREFIX = "group:"
# Signal encodes an @mention as this single placeholder character in the body,
# with the mentioned user's name/number/uuid carried out-of-band in `mentions`.
_MENTION_PLACEHOLDER = "￼"


@dataclass(frozen=True, slots=True)
class AttachmentRef:
    """A reference to an inbound attachment (bytes are fetched later, issue #7)."""

    id: str
    content_type: str | None = None
    size: int | None = None
    filename: str | None = None


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """A normalized inbound Signal message, ready for the engine client.

    ``chat_id`` follows the :class:`~bt_signal_gateway.signal_client.SignalClient`
    recipient convention: ``"group:<groupId>"`` for groups, otherwise the direct
    sender identifier (service id preferred, number fallback).
    """

    user_id: str
    chat_id: str
    text: str
    timestamp_ms: int
    is_group: bool = False
    source_number: str | None = None
    source_uuid: str | None = None
    sender_name: str = ""
    group_id: str | None = None
    group_name: str | None = None
    reply_to_id: str | None = None
    reply_to_text: str | None = None
    attachments: list[AttachmentRef] = field(default_factory=list)


def _render_mentions(text: str, mentions: list[dict[str, Any]]) -> str:
    """Replace Signal mention placeholders with readable ``@name`` tags.

    Replaces from the end backwards so earlier offsets stay valid as we splice.
    """
    if not mentions or _MENTION_PLACEHOLDER not in text:
        return text
    for mention in sorted(mentions, key=lambda m: m.get("start", 0), reverse=True):
        start = mention.get("start", 0)
        length = mention.get("length", 1)
        identifier = mention.get("name") or mention.get("number") or mention.get("uuid") or "user"
        text = text[:start] + f"@{identifier}" + text[start + length :]
    return text


def _group_allowed(group_id: str, allowed: list[str]) -> bool:
    """Apply ``SIGNAL_GROUP_ALLOWED_USERS`` semantics: empty disables groups."""
    if not allowed:
        return False
    return "*" in allowed or group_id in allowed


def _bot_mentioned(account: str, text: str, mentions: list[dict[str, Any]]) -> bool:
    """True if the bot account is @mentioned (rendered tag or raw metadata)."""
    if account and f"@{account}" in text:
        return True
    return any(m.get("number") == account or m.get("uuid") == account for m in mentions)


def _parse_attachments(raw: list[dict[str, Any]]) -> list[AttachmentRef]:
    """Capture attachment references (id required); bytes are fetched in #7."""
    refs: list[AttachmentRef] = []
    for att in raw:
        att_id = att.get("id")
        if not att_id:
            continue
        refs.append(
            AttachmentRef(
                id=str(att_id),
                content_type=att.get("contentType"),
                size=att.get("size"),
                filename=att.get("filename"),
            )
        )
    return refs


def parse_envelope(
    raw: dict[str, Any],
    settings: Settings,
    *,
    now: float | None = None,
) -> InboundMessage | None:
    """Normalize a raw signal-cli envelope, or return ``None`` if it's filtered.

    ``now`` overrides the wall clock used for the age-cutoff check (tests pass a
    fixed value); it defaults to :func:`time.time`.
    """
    envelope = raw.get("envelope", raw)

    # 1. Drop non-message / metadata-only envelope types outright.
    if "syncMessage" in envelope:
        logger.debug("envelope: dropping syncMessage")
        return None
    if "receiptMessage" in envelope or "typingMessage" in envelope:
        logger.debug("envelope: dropping receipt/typing")
        return None
    if envelope.get("storyMessage"):
        logger.debug("envelope: dropping story")
        return None

    # 2. Sender identifiers.
    source_number = envelope.get("sourceNumber")
    source_uuid = envelope.get("sourceUuid")
    fallback = envelope.get("source")
    user_id = source_uuid or source_number or fallback
    if not user_id:
        logger.debug("envelope: no sender")
        return None

    # 3. Self-source filter — never react to our own account (own-echo guard).
    account = settings.signal_account
    if account in (source_number, source_uuid, fallback):
        logger.debug("envelope: dropping self-source")
        return None

    # 4. Require an actual data message (also carried inside an edit).
    data_message = envelope.get("dataMessage") or (envelope.get("editMessage") or {}).get(
        "dataMessage"
    )
    if not data_message:
        logger.debug("envelope: no dataMessage")
        return None

    # 5. Group detection + gating.
    group_info = data_message.get("groupInfo") or {}
    group_id = group_info.get("groupId")
    is_group = bool(group_id)
    if is_group and not _group_allowed(group_id, settings.signal_group_allowed_users):
        logger.debug("envelope: group not allowed")
        return None

    # 6. Text + mention rendering.
    text = data_message.get("message") or ""
    mentions = data_message.get("mentions") or []
    if text and mentions:
        text = _render_mentions(text, mentions)

    # 7. Require-mention gate (groups only).
    if is_group and settings.signal_require_mention and not _bot_mentioned(account, text, mentions):
        logger.debug("envelope: group message does not mention the bot")
        return None

    # 8. Quote (reply-to) + attachment references.
    quote = data_message.get("quote") or {}
    reply_to_id = str(quote["id"]) if quote.get("id") is not None else None
    attachments = _parse_attachments(data_message.get("attachments") or [])

    # 9. Drop contentless envelopes (profile-key updates, empty messages, ...).
    if not text.strip() and not attachments:
        logger.debug("envelope: contentless")
        return None

    # 10. Age cutoff — avoid replaying a backlog after downtime.
    timestamp_ms = envelope.get("timestamp") or 0
    clock = time.time() if now is None else now
    if timestamp_ms and clock - timestamp_ms / 1000 > settings.message_age_cutoff_seconds:
        logger.debug("envelope: older than cutoff")
        return None

    chat_id = f"{_GROUP_PREFIX}{group_id}" if is_group else user_id
    return InboundMessage(
        user_id=user_id,
        chat_id=chat_id,
        text=text,
        timestamp_ms=int(timestamp_ms),
        is_group=is_group,
        source_number=source_number,
        source_uuid=source_uuid,
        sender_name=envelope.get("sourceName") or "",
        group_id=group_id,
        group_name=group_info.get("groupName"),
        reply_to_id=reply_to_id,
        reply_to_text=quote.get("text"),
        attachments=attachments,
    )
